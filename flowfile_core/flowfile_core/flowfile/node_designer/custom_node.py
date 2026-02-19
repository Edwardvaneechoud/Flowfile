# Fixed custom_node.py with proper type hints

import inspect
import textwrap
from typing import Any, TypeVar

import polars as pl
from pydantic import BaseModel, Field

from flowfile_core.flowfile.node_designer.ui_components import (
    FlowfileInComponent,
    AvailableArtifacts,
    IncomingColumns,
    SecretSelector,
    Section,
)
from flowfile_core.schemas.schemas import NodeTemplate, NodeTypeLiteral, TransformTypeLiteral


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
        section_data = value.model_dump(include={"title", "description", "hidden"}, exclude_none=True)
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


# Type variable for the Section factory
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
        # Handle both extra fields and defined fields
        all_sections = {}

        # Get extra fields
        extra_fields = getattr(self, "__pydantic_extra__", {})
        all_sections.update(extra_fields)

        # Get defined fields that are Sections
        for field_name in self.model_fields:
            field_value = getattr(self, field_name, None)
            if isinstance(field_value, Section):
                all_sections[field_name] = field_value

        for section_name, section in all_sections.items():
            if section_name in values:
                section_values = values[section_name]
                for component_name, component in section.get_components().items():
                    if component_name in section_values:
                        component.set_value(section_values[component_name])
        return self

    def get_value(self, field_name: str) -> Any:
        """
        Gets the current value of a field by name.

        Searches through direct fields, extra fields, and sections.

        Args:
            field_name: The name of the field to retrieve.

        Returns:
            The current value of the field, or None if not found.
        """
        # Check direct model fields
        if field_name in self.model_fields:
            component = getattr(self, field_name, None)
            if component is not None:
                if isinstance(component, FlowfileInComponent):
                    return component.value
                return component

        # Check pydantic extra fields
        extras = getattr(self, "__pydantic_extra__", {}) or {}
        if field_name in extras:
            component = extras[field_name]
            if isinstance(component, FlowfileInComponent):
                return component.value
            return component

        # Check within sections (both in model_fields and extras)
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

        # Get from model fields
        for field_name in self.model_fields:
            value = getattr(self, field_name, None)
            if isinstance(value, FlowfileInComponent):
                components[field_name] = value
            elif isinstance(value, Section):
                components.update(value.get_components())

        # Get from extra fields
        extras = getattr(self, "__pydantic_extra__", {}) or {}
        for field_name, value in extras.items():
            if isinstance(value, FlowfileInComponent):
                components[field_name] = value
            elif isinstance(value, Section):
                components.update(value.get_components())

        return components

    def set_secret_context(self, user_id: int, accessed_secrets: set):
        """Inject execution context into all SecretSelector components."""
        for component in self.get_all_components().values():
            if isinstance(component, SecretSelector):
                component.set_execution_context(user_id, accessed_secrets)


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

    def __init__(self, title: str | None = None, description: str | None = None, hidden: bool = False):
        self._section = Section(title=title, description=description, hidden=hidden)

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

    # Kernel execution configuration
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
    accessed_secrets: set[str] = set()

    @property
    def item(self):
        """A unique identifier for the node, derived from its name."""
        return self.node_name.replace(" ", "_").lower()

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """
        Initialize the node, optionally populating settings from initial values.
        """
        initial_values = data.pop("initial_values", None)
        super().__init__(**data)
        if self.settings_schema and initial_values:
            self.settings_schema.populate_values(initial_values)

    def set_execution_context(self, user_id: int):
        """
        Sets the execution context for the node.
        Called by the framework before executing the node.

        Args:
            user_id: The ID of the user executing this node.
        """
        self._user_id = user_id
        self.accessed_secrets = set()

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
        from flowfile_core.flowfile.node_designer.ui_components import SecretSelector

        if self.settings_schema is None:
            return []

        secret_fields = []
        for name, field in self.settings_schema.get_all_components().items():
            if isinstance(field, SecretSelector):
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
        """Whether this node is configured to execute on a kernel."""
        return self.kernel_id is not None

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
                section_vals[comp_name] = comp.value
            result[section_name] = section_vals
        return result

    def generate_kernel_code(self) -> str:
        """Generate a self-contained Python script for kernel execution.

        The generated script:
        - Creates lightweight proxy classes that replicate the
          ``self.settings_schema.section.component.value`` access pattern
        - Reads inputs via ``flowfile.read_input()``
        - Executes the user's process method body
        - Publishes each named output via ``flowfile.publish_output()``
        """
        # --- Build settings proxy code ---
        settings_values = self._extract_settings_values()
        proxy_lines: list[str] = []
        proxy_lines.append("class _V:")
        proxy_lines.append("    def __init__(self, v): self.value = v")
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
        # Dedent the body
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
            publish_code = f'flowfile.publish_output(result, name="{output_names[0]}")'
        else:
            # Multi-output: process returns a dict
            pub_lines = ["if isinstance(result, dict):"]
            for name in output_names:
                pub_lines.append(f'    if "{name}" in result:')
                pub_lines.append(f'        flowfile.publish_output(result["{name}"], name="{name}")')
            pub_lines.append("else:")
            pub_lines.append(f'    flowfile.publish_output(result, name="{output_names[0]}")')
            publish_code = "\n".join(pub_lines)

        # --- Assemble full kernel script ---
        script = f"""\
import polars as pl

try:
    import flowfile as flowfile
except ModuleNotFoundError:
    flowfile = globals().get("flowfile")

# --- Settings proxy (auto-generated) ---
{proxy_code}

self = _Self()

# --- Read inputs ---
inputs = [flowfile.read_input()]

# --- Process body ---
{body}
# --- Publish outputs ---
{publish_code}
"""
        return script

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        """
        The main data processing logic for the node.

        This method must be implemented by all subclasses. It receives one or
        more Polars DataFrames as input and should return a single DataFrame
        as output.

        Args:
            *inputs: A variable number of Polars DataFrames, corresponding to
                     the inputs connected to the node.

        Returns:
            A Polars DataFrame containing the processed data.
        """
        raise NotImplementedError

    def to_node_template(self) -> NodeTemplate:
        """
        Convert the node to a `NodeTemplate` for storage or transmission.
        """
        return NodeTemplate(
            name=self.node_name,
            item=self.item,
            input=self.number_of_inputs,
            output=self.number_of_outputs,
            image=self.node_icon,
            node_group=self.node_group or "custom",
            drawer_title=self.title or "Custom Node",
            drawer_intro=self.intro or "A custom node for data processing",
            node_type=self.node_type,
            transform_type=self.transform_type,
            custom_node=True,
        )
