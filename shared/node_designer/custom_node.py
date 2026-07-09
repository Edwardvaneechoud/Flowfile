import inspect
import logging
import re
import textwrap
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar

import polars as pl
from pydantic import BaseModel, Field, model_validator

from shared.node_designer.secrets import SecretResolver
from shared.node_designer.ui_components import (
    AvailableArtifacts,
    FlowfileInComponent,
    IncomingColumns,
    SecretSelector,
    Section,
)

if TYPE_CHECKING:
    from flowfile_core.schemas.schemas import NodeTemplate

# Kept in sync with flowfile_core.schemas.schemas — the SDK must not import core.
NodeTypeLiteral = Literal["input", "output", "process"]
TransformTypeLiteral = Literal["narrow", "wide", "other"]

logger = logging.getLogger(__name__)

_legacy_kernel_warned: set[str] = set()


def _warn_legacy_once(cls: type, message: str) -> None:
    key = f"{cls.__module__}.{cls.__qualname__}:{message}"
    if key not in _legacy_kernel_warned:
        _legacy_kernel_warned.add(key)
        logger.warning("%s (%s)", message, cls.__qualname__)


@dataclass
class PopulateReport:
    """Outcome of applying stored settings values onto a settings schema."""

    unknown_sections: list[str] = field(default_factory=list)
    unknown_components: list[str] = field(default_factory=list)  # "section.component"
    applied_count: int = 0

    @property
    def has_drift(self) -> bool:
        return bool(self.unknown_sections or self.unknown_components)


def to_frontend_schema(model_instance: BaseModel) -> dict:
    """
    Recursively converts a Pydantic model instance into a JSON-serializable
    dictionary suitable for the frontend.

    This function handles special marker classes like `IncomingColumns` and
    nested `Section` and `FlowfileInComponent` instances.

    Args:
        model_instance: The Pydantic model instance to convert.

    Returns:
        A dictionary representation of the model.
    """
    result = {}
    extra_fields = getattr(model_instance, "__pydantic_extra__", {})
    model_fields = {k: getattr(model_instance, k) for k in model_instance.model_fields.keys()}
    for key, value in (extra_fields | model_fields).items():
        result[key] = _convert_value(value)
    return result


def _convert_value(value: Any) -> Any:
    """
    Helper function to convert any value to a frontend-ready format.
    """
    if isinstance(value, Section):
        section_data = value.model_dump(include={"title", "description", "hidden", "layout"}, exclude_none=True)
        section_data["component_type"] = "Section"
        section_data["components"] = {key: _convert_value(comp) for key, comp in value.get_components().items()}
        return section_data

    elif isinstance(value, FlowfileInComponent):
        component_dict = value.model_dump(exclude_none=True)
        if "options" in component_dict:
            if component_dict["options"] is IncomingColumns or (
                isinstance(component_dict["options"], type) and issubclass(component_dict["options"], IncomingColumns)
            ):
                component_dict["options"] = {"__type__": "IncomingColumns"}
            if component_dict["options"] is AvailableArtifacts or (
                isinstance(component_dict["options"], type)
                and issubclass(component_dict["options"], AvailableArtifacts)
            ):
                component_dict["options"] = {"__type__": "AvailableArtifacts"}
        return component_dict
    elif isinstance(value, BaseModel):
        return to_frontend_schema(value)
    elif isinstance(value, list):
        return [_convert_value(item) for item in value]
    elif isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}
    elif isinstance(value, tuple):
        return tuple(_convert_value(item) for item in value)
    else:
        return value


T = TypeVar("T", bound=Section)


def create_section(**components: FlowfileInComponent) -> Section:
    """
    Factory function to create a Section with proper type hints.

    This is a convenience function that makes it easier to create `Section`
    objects with autocomplete and type checking in modern editors.

    Usage:
        advanced_config_section = create_section(
            case_sensitive=case_sensitive_toggle
        )

    Args:
        **components: Keyword arguments where each key is the component name
                      and the value is a `FlowfileInComponent` instance.

    Returns:
        A new `Section` instance containing the provided components.
    """
    return Section(**components)


class NodeSettings(BaseModel):
    """
    The top-level container for all sections in a node's UI.

    This class holds all the `Section` objects that make up the settings panel
    for a custom node.

    Example:
        class MyNodeSettings(NodeSettings):
            main_config = main_config_section
            advanced_options = advanced_config_section
    """

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True

    def has_sections(self) -> bool:
        """Check if this settings class has any sections defined."""
        if self.model_fields:
            return True
        extra = getattr(self, "__pydantic_extra__", {})
        return bool(extra)

    def is_empty(self) -> bool:
        """Check if this is an empty settings class with no configuration."""

        return not self.has_sections()

    def __init__(self, **sections):
        """
        Initialize NodeSettings with sections as keyword arguments.
        """
        super().__init__(**sections)

    def populate_values(self, values: dict[str, Any]) -> "NodeSettings":
        """
        Populates the settings with values received from the frontend.

        This method is used internally to update the node's state based on
        user input in the UI.

        Args:
            values: A dictionary of values from the frontend, where keys are
                    section names and values are dictionaries of component
                    values.

        Returns:
            The `NodeSettings` instance with updated component values.
        """
        self.populate_values_report(values)
        return self

    def _get_sections(self) -> dict[str, Section]:
        all_sections: dict[str, Section] = {}
        extra_fields = getattr(self, "__pydantic_extra__", {}) or {}
        all_sections.update({k: v for k, v in extra_fields.items() if isinstance(v, Section)})
        for field_name in self.model_fields:
            field_value = getattr(self, field_name, None)
            if isinstance(field_value, Section):
                all_sections[field_name] = field_value
        return all_sections

    def populate_values_report(self, values: dict[str, Any]) -> PopulateReport:
        """Like ``populate_values`` but reports stored keys that no longer match the schema."""
        report = PopulateReport()
        if not isinstance(values, dict):
            return report
        all_sections = self._get_sections()
        for section_name, section_values in values.items():
            section = all_sections.get(section_name)
            if section is None:
                report.unknown_sections.append(section_name)
                continue
            if not isinstance(section_values, dict):
                continue
            components = section.get_components()
            for component_name, component_value in section_values.items():
                component = components.get(component_name)
                if component is None:
                    report.unknown_components.append(f"{section_name}.{component_name}")
                    continue
                component.set_value(component_value)
                report.applied_count += 1
        return report

    def get_value(self, field_name: str) -> Any:
        """
        Gets the current value of a field by name.

        Searches through direct fields, extra fields, and sections.

        Args:
            field_name: The name of the field to retrieve.

        Returns:
            The current value of the field, or None if not found.
        """
        if field_name in self.model_fields:
            component = getattr(self, field_name, None)
            if component is not None:
                if isinstance(component, FlowfileInComponent):
                    return component.value
                return component

        extras = getattr(self, "__pydantic_extra__", {}) or {}
        if field_name in extras:
            component = extras[field_name]
            if isinstance(component, FlowfileInComponent):
                return component.value
            return component

        all_fields = {**{k: getattr(self, k) for k in self.model_fields}, **extras}
        for value in all_fields.values():
            if isinstance(value, Section):
                components = value.get_components()
                if field_name in components:
                    component = components[field_name]
                    if isinstance(component, FlowfileInComponent):
                        return component.value
                    return component

        return None

    def get_all_components(self) -> dict[str, FlowfileInComponent]:
        """
        Returns all UI components in the settings, including those nested in sections.

        Returns:
            Dictionary mapping field names to their FlowfileInComponent instances.
        """
        components = {}

        for field_name in self.model_fields:
            value = getattr(self, field_name, None)
            if isinstance(value, FlowfileInComponent):
                components[field_name] = value
            elif isinstance(value, Section):
                components.update(value.get_components())

        extras = getattr(self, "__pydantic_extra__", {}) or {}
        for field_name, value in extras.items():
            if isinstance(value, FlowfileInComponent):
                components[field_name] = value
            elif isinstance(value, Section):
                components.update(value.get_components())

        return components

    def set_secret_context(self, user_id: int, accessed_secrets: set, resolver: SecretResolver | None = None):
        """Inject execution context into all SecretSelector components."""
        for component in self.get_all_components().values():
            if isinstance(component, SecretSelector):
                component.set_execution_context(user_id, accessed_secrets, resolver)


def create_node_settings(**sections: Section) -> NodeSettings:
    """
    Factory function to create NodeSettings with proper type hints.

    This is a convenience function for creating `NodeSettings` instances.

    Usage:
        FilterNodeSchema = create_node_settings(
            main_config=main_config_section,
            advanced_options=advanced_config_section
        )

    Args:
        **sections: Keyword arguments where each key is the section name
                    and the value is a `Section` instance.

    Returns:
        A new `NodeSettings` instance containing the provided sections.
    """
    return NodeSettings(**sections)


class SectionBuilder:
    """
    A builder pattern for creating `Section` objects with proper type hints.

    This provides a more fluent and readable way to construct complex sections,
    especially when the number of components is large.

    Usage:
        builder = SectionBuilder(title="Advanced Settings")
        builder.add_component("timeout", NumericInput(label="Timeout (s)"))
        builder.add_component("retries", NumericInput(label="Number of Retries"))
        advanced_section = builder.build()
    """

    def __init__(
        self,
        title: str | None = None,
        description: str | None = None,
        hidden: bool = False,
        layout: Literal["vertical", "horizontal"] = "vertical",
    ):
        self._section = Section(title=title, description=description, hidden=hidden, layout=layout)

    def add_component(self, name: str, component: FlowfileInComponent) -> "SectionBuilder":
        """Add a component to the section."""
        setattr(self._section, name, component)
        extra = getattr(self._section, "__pydantic_extra__", {})
        extra[name] = component
        return self

    def build(self) -> Section:
        """Build and return the Section."""
        return self._section


class NodeSettingsBuilder:
    """
    A builder pattern for creating `NodeSettings` objects.

    Provides a fluent interface for constructing the entire settings schema
    for a custom node.

    Usage:
        settings_builder = NodeSettingsBuilder()
        settings_builder.add_section("main", main_section)
        settings_builder.add_section("advanced", advanced_section)
        my_node_settings = settings_builder.build()
    """

    def __init__(self):
        self._settings = NodeSettings()

    def add_section(self, name: str, section: Section) -> "NodeSettingsBuilder":
        """Add a section to the node settings."""
        setattr(self._settings, name, section)
        extra = getattr(self._settings, "__pydantic_extra__", {})
        extra[name] = section
        return self

    def build(self) -> NodeSettings:
        """Build and return the NodeSettings."""
        return self._settings


class CustomNodeBase(BaseModel):
    """
    The base class for creating a custom node in Flowfile.

    To create a new node, you should inherit from this class and define its
    attributes and the `process` method.
    """

    # Core node properties
    node_name: str
    node_category: str = "Custom"
    node_icon: str = "user-defined-icon.png"
    settings_schema: NodeSettings | None = None

    # I/O configuration
    number_of_inputs: int = 1
    number_of_outputs: int = 1

    # Execution environment: "local" runs in the Flowfile process/worker,
    # "kernel" runs in an isolated Docker kernel. dependencies are pip specs
    # (auto-installed only for kernel environments).
    environment: Literal["local", "kernel"] = "local"
    dependencies: list[str] = Field(default_factory=list)

    # Optional sample data for dry-runs: one column-oriented dict per input port
    # ({column: [values]}), plus the settings values to test with.
    example_inputs: list[dict[str, list]] | None = None
    example_settings: dict[str, dict[str, Any]] | None = None

    # Deprecated kernel flags — mapped onto `environment` / default kernel
    # binding by _apply_legacy_kernel_flags. Kept so existing node files load.
    requires_kernel: bool = False
    kernel_id: str | None = None
    output_names: list[str] = Field(default_factory=lambda: ["main"])

    # Display properties in the UI
    node_group: str | None = "custom"
    title: str | None = "Custom Node"
    intro: str | None = "A custom node for data processing"

    # Behavior properties
    node_type: NodeTypeLiteral = "process"
    transform_type: TransformTypeLiteral = "wide"

    _user_id: int | None = None
    _secret_resolver: SecretResolver | None = None
    accessed_secrets: set[str] = set()

    @property
    def item(self):
        """A unique identifier for the node, derived from its name."""
        return self.node_name.replace(" ", "_").lower()

    class Config:
        arbitrary_types_allowed = True

    @model_validator(mode="after")
    def _apply_legacy_kernel_flags(self) -> "CustomNodeBase":
        if self.requires_kernel and self.environment == "local":
            self.environment = "kernel"
            _warn_legacy_once(type(self), 'requires_kernel is deprecated; use environment="kernel"')
        if self.kernel_id is not None:
            _warn_legacy_once(
                type(self), "kernel_id on the node class is deprecated; it only seeds the default kernel binding"
            )
        return self

    def __init__(self, **data):
        """
        Initialize the node, optionally populating settings from initial values.
        """
        initial_values = data.pop("initial_values", None)
        super().__init__(**data)
        if self.settings_schema and initial_values:
            self.settings_schema.populate_values(initial_values)

    def set_execution_context(self, user_id: int, resolver: SecretResolver | None = None):
        """
        Sets the execution context for the node.
        Called by the framework before executing the node.

        Args:
            user_id: The ID of the user executing this node.
            resolver: Optional secret resolver for the hosting process.
        """
        self._user_id = user_id
        self._secret_resolver = resolver
        self.accessed_secrets = set()
        if self.settings_schema:
            self.settings_schema.set_secret_context(user_id, self.accessed_secrets, resolver)

    def get_accessed_secrets(self) -> set[str]:
        """
        Returns the set of secret values accessed during this execution.
        Used by the output scanner to detect accidental leaks.
        """
        return self.accessed_secrets.copy()

    def get_secret_names(self) -> list[str]:
        """
        Returns a list of all SecretSelector field names in the settings schema.
        Useful for validation and debugging.
        """
        if self.settings_schema is None:
            return []

        secret_fields = []
        for name, component in self.settings_schema.get_all_components().items():
            if isinstance(component, SecretSelector):
                secret_fields.append(name)

        return secret_fields

    def get_frontend_schema(self) -> dict:
        """
        Get the frontend-ready schema with current values.

        This method is called by the backend to send the node's UI definition
        and current state to the frontend.

        Returns:
            A dictionary representing the node's schema and values.
        """
        schema = {
            "node_name": self.node_name,
            "node_category": self.node_category,
            "node_icon": self.node_icon,
            "number_of_inputs": self.number_of_inputs,
            "number_of_outputs": self.number_of_outputs,
            "environment": self.environment,
            "dependencies": list(self.dependencies),
            "requires_kernel": self.requires_kernel,
            "kernel_id": self.kernel_id,
            "output_names": self.output_names,
            "node_group": self.node_group,
            "title": self.title,
            "intro": self.intro,
        }

        if self.settings_schema:
            schema["settings_schema"] = to_frontend_schema(self.settings_schema)
        else:
            schema["settings_schema"] = {}

        return schema

    @classmethod
    def from_frontend_schema(cls, schema: dict) -> "CustomNodeBase":
        """
        Create a node instance from a frontend schema.

        This is used when loading a node from a saved flow.
        """
        settings_values = schema.pop("settings_schema", {})
        node = cls(**schema)
        if settings_values and node.settings_schema:
            node.settings_schema.populate_values(settings_values)
        return node

    @classmethod
    def from_settings(cls, settings_values: dict) -> "CustomNodeBase":
        """
        Create a node instance with just its settings values.

        Useful for creating a configured node instance programmatically.
        """
        node = cls()
        if settings_values and node.settings_schema:
            node.settings_schema.populate_values(settings_values)
        return node

    def update_settings(self, values: dict[str, Any]) -> "CustomNodeBase":
        """
        Update the settings with new values from the frontend.
        """
        if self.settings_schema:
            self.settings_schema.populate_values(values)
        return self

    @property
    def uses_kernel(self) -> bool:
        """Whether this node executes on an isolated kernel (or carries a legacy kernel binding)."""
        return self.environment == "kernel" or self.kernel_id is not None

    def _extract_settings_values(self) -> dict[str, dict[str, Any]]:
        """Extract current settings values as a nested dict: {section: {component: value}}."""
        if not self.settings_schema:
            return {}
        result: dict[str, dict[str, Any]] = {}
        all_sections: dict[str, Section] = {}
        extra = getattr(self.settings_schema, "__pydantic_extra__", {})
        all_sections.update({k: v for k, v in extra.items() if isinstance(v, Section)})
        for field_name in self.settings_schema.model_fields:
            val = getattr(self.settings_schema, field_name, None)
            if isinstance(val, Section):
                all_sections[field_name] = val
        for section_name, section in all_sections.items():
            section_vals: dict[str, Any] = {}
            for comp_name, comp in section.get_components().items():
                val = comp.value
                if isinstance(val, BaseModel):
                    val = val.model_dump()
                section_vals[comp_name] = val
            result[section_name] = section_vals
        return result

    def generate_kernel_code(self) -> str:
        """Deprecated. Use ``flowfile_core...user_defined.kernel_codegen.generate_kernel_script``.

        This inspect-based, return-rewriting generator is kept only as the
        fallback for inline node classes that have no source file on disk
        (registry-backed nodes go through the AST generator, which bakes
        settings as JSON and defines a real class instead of rewriting
        ``return`` statements). Do not build new callers on this.

        The generated script:
        - Creates lightweight proxy classes that replicate the
          ``self.settings_schema.section.component.value`` access pattern
        - Reads inputs via ``flowfile_ctx.read_input()``
        - Executes the user's process method body
        - Publishes each named output via ``flowfile_ctx.publish_output()``
        """
        # --- Build settings proxy code ---
        settings_values = self._extract_settings_values()
        proxy_lines: list[str] = []
        proxy_lines.append("class _AttrDict(dict):")
        proxy_lines.append("    def __getattr__(self, name):")
        proxy_lines.append("        try:")
        proxy_lines.append("            return self[name]")
        proxy_lines.append("        except KeyError:")
        proxy_lines.append("            raise AttributeError(name)")
        proxy_lines.append("")
        proxy_lines.append("def _wrap(v):")
        proxy_lines.append("    if isinstance(v, dict):")
        proxy_lines.append("        return _AttrDict({k: _wrap(x) for k, x in v.items()})")
        proxy_lines.append("    if isinstance(v, list):")
        proxy_lines.append("        return [_wrap(x) for x in v]")
        proxy_lines.append("    return v")
        proxy_lines.append("")
        proxy_lines.append("class _V:")
        proxy_lines.append("    def __init__(self, v): self.value = _wrap(v)")
        proxy_lines.append("")
        proxy_lines.append("class _Self:")

        if settings_values:
            proxy_lines.append("    class settings_schema:")
            for section_name, components in settings_values.items():
                proxy_lines.append(f"        class {section_name}:")
                if components:
                    for comp_name, comp_value in components.items():
                        proxy_lines.append(f"            {comp_name} = _V({comp_value!r})")
                else:
                    proxy_lines.append("            pass")
        else:
            proxy_lines.append("    settings_schema = None")

        proxy_code = "\n".join(proxy_lines)

        # --- Extract process method body ---
        try:
            source = inspect.getsource(self.process)
        except (OSError, TypeError) as exc:
            raise RuntimeError(
                "Cannot extract process method source. "
                "Ensure the custom node class is defined in a .py file (not dynamically)."
            ) from exc
        source = textwrap.dedent(source)
        # Strip the 'def process(...):\n' header
        lines = source.split("\n")
        body_start = 0
        for i, line in enumerate(lines):
            if line.rstrip().endswith(":") and "def process" in line:
                body_start = i + 1
                break
        body_lines = lines[body_start:]
        body = textwrap.dedent("\n".join(body_lines))

        # Transform 'return <expr>' into 'result = <expr>' so the script
        # works at module level and the publish code can reference 'result'.
        transformed_lines = []
        for line in body.split("\n"):
            stripped = line.lstrip()
            if stripped.startswith("return "):
                indent = line[: len(line) - len(stripped)]
                expr = stripped[len("return ") :]
                transformed_lines.append(f"{indent}result = {expr}")
            elif stripped == "return":
                # bare return — skip
                continue
            else:
                transformed_lines.append(line)
        body = "\n".join(transformed_lines)

        # --- Build output publishing code ---
        output_names = self.output_names or ["main"]
        if len(output_names) == 1:
            publish_code = f'flowfile_ctx.publish_output(result, name="{output_names[0]}")'
        else:
            # Multi-output: process returns a dict
            pub_lines = ["if isinstance(result, dict):"]
            for name in output_names:
                pub_lines.append(f'    if "{name}" in result:')
                pub_lines.append(f'        flowfile_ctx.publish_output(result["{name}"], name="{name}")')
            pub_lines.append("else:")
            pub_lines.append(f'    flowfile_ctx.publish_output(result, name="{output_names[0]}")')
            publish_code = "\n".join(pub_lines)

        # --- Assemble full kernel script ---
        # ``flowfile_ctx`` is injected into globals by the kernel runtime;
        # binding it locally keeps the script readable and fails loudly if
        # the runtime is misconfigured (rather than producing a ``NoneType
        # has no attribute …`` deep inside the user body).
        script = f"""\
import polars as pl

flowfile_ctx = globals()["flowfile_ctx"]

# --- Settings proxy (auto-generated) ---
{proxy_code}

self = _Self()

# --- Read inputs ---
inputs = flowfile_ctx.read_inputs().get("main", [])
if not inputs:
    inputs = [flowfile_ctx.read_input()] if hasattr(flowfile_ctx, "read_input") else []

# --- Process body ---
{body}
# --- Publish outputs ---
{publish_code}
"""
        return script

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame | pl.DataFrame:
        """
        The main data processing logic for the node.

        This method must be implemented by all subclasses. Inputs are always
        ``pl.LazyFrame`` (one per connected input port); a node that needs
        eager data calls ``.collect()`` inside ``process``. Return a
        ``LazyFrame`` or ``DataFrame`` — the framework normalizes it. For a
        multi-output node (``output_names`` has more than one entry) return a
        ``dict[str, pl.LazyFrame | pl.DataFrame]`` keyed by output name.

        Args:
            *inputs: One ``pl.LazyFrame`` per connected input, in port order.

        Returns:
            A ``pl.LazyFrame`` / ``pl.DataFrame`` (single output) or a
            ``dict`` of them keyed by output name (multi-output).
        """
        raise NotImplementedError

    def _palette_group(self) -> tuple[str, str | None]:
        """Palette placement: node_category becomes a real group (slug + label);
        the default "Custom" category keeps today's node_group behavior."""
        category = (self.node_category or "").strip()
        if category and category.lower() != "custom":
            slug = re.sub(r"[^a-z0-9]+", "_", category.lower()).strip("_")
            if slug:
                return slug, category
        return self.node_group or "custom", None

    def to_node_template(self) -> "NodeTemplate":
        """
        Convert the node to a `NodeTemplate` for storage or transmission.
        """
        # NodeTemplate is a core/frontend contract; resolving it lazily keeps
        # the SDK importable in processes without flowfile_core (worker, dry-run).
        from flowfile_core.schemas.schemas import NodeTemplate

        node_group, node_group_label = self._palette_group()
        return NodeTemplate(
            name=self.node_name,
            item=self.item,
            input=self.number_of_inputs,
            output=self.number_of_outputs,
            image=self.node_icon,
            node_group=node_group,
            node_group_label=node_group_label,
            drawer_title=self.title or "Custom Node",
            drawer_intro=self.intro or "A custom node for data processing",
            node_type=self.node_type,
            transform_type=self.transform_type,
            custom_node=True,
            execution_environment=self.environment,
            dependencies=self.dependencies or None,
        )
