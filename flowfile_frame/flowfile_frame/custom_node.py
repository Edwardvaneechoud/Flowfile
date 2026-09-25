"""``fl.CustomNode`` and ``fl.custom_node``: place a user-defined (custom) node from Python.

``CustomNode`` is the canonical form: settings nested as ``{section: {component: value}}``,
one-to-one with what the node stores. ``custom_node(...)`` wraps a node class in a factory
whose keyword arguments are the node's settings components.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.user_defined.registry import KernelRequiredError, missing_custom_node_error, registry
from flowfile_core.schemas import input_schema
from flowfile_frame.native import NativeNode, NativeNodeError, _kernel_id, predicted_schema_without_running
from shared.node_designer.custom_node import CustomNodeBase, node_key_for

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame

_FACTORY_KEYWORDS: frozenset[str] = frozenset({"kernel", "description", "settings", "flow_graph"})

_INSTANCE_EXEMPT_FIELDS: frozenset[str] = frozenset({"settings_schema", "accessed_secrets"})


def _register_class(cls: type[CustomNodeBase]) -> type[CustomNodeBase]:
    """Make ``cls`` placeable under its node key; a session (notebook) class refreshes its template.

    A key that names a built-in node or a different installed (file-backed) custom node is
    refused: the saved flow would reopen as that other node. The template is re-registered
    even when the key is known, so a class redefined in a notebook replaces the stale one.
    """
    try:
        instance = cls()
    except Exception as exc:
        raise NativeNodeError(f"Could not instantiate custom node class {cls.__name__}: {exc}") from exc
    key = instance.item
    template = node_store.node_dict.get(key)
    if template is not None and not template.custom_node:
        raise NativeNodeError(
            f"Custom node class {cls.__name__} has the node key {key!r} of a built-in node; rename its node_name"
        )
    entry = registry.get(key)
    if entry is not None:
        if entry.node_class is not cls:
            raise NativeNodeError(
                f"Custom node class {cls.__name__} has the node key {key!r} of the installed node in "
                f"{entry.file_name}; rename its node_name, or place the installed node with fl.CustomNode({key!r}, ...)"
            )
        return cls
    node_store.add_to_custom_node_store(cls)
    node_store.register_custom_node(instance.to_node_template())
    return cls


def _instance_settings(node: CustomNodeBase) -> dict[str, dict[str, Any]]:
    """The settings values of a configured instance; any other field it overrides is refused.

    A saved flow stores only the settings values and reopens the node as
    ``cls.from_settings(values)``, so an instance-level ``node_name``, ``output_names`` and
    the like would not survive a reopen.
    """
    cls = type(node)
    fresh = cls()
    changed = sorted(
        name
        for name in cls.model_fields
        if name not in _INSTANCE_EXEMPT_FIELDS and getattr(node, name) != getattr(fresh, name)
    )
    if changed:
        raise NativeNodeError(
            f"Custom node instance overrides {changed}; only settings values are saved with a flow, "
            f"so declare those fields on the class {cls.__name__} instead"
        )
    return node._extract_settings_values()


def _resolve(node: type[CustomNodeBase] | CustomNodeBase | str) -> tuple[type[CustomNodeBase], dict[str, Any]]:
    """The node class and the base settings values (an instance's configured values, else none)."""
    if isinstance(node, str):
        key = node_key_for(node)
        cls = node_store.CUSTOM_NODE_STORE.get(key)
        if cls is None:
            template = node_store.node_dict.get(key)
            if template is not None and not template.custom_node:
                raise NativeNodeError(f"{key!r} is a built-in node; place it with fl.Node({key!r}, ...)")
            raise NativeNodeError(missing_custom_node_error(key))
        return cls, {}
    if isinstance(node, CustomNodeBase):
        return _register_class(type(node)), _instance_settings(node)
    if isinstance(node, type) and issubclass(node, CustomNodeBase):
        return _register_class(node), {}
    raise NativeNodeError(
        "A custom node is given as a CustomNodeBase subclass, an instance of one, or its node type name; "
        f"got {type(node).__name__}"
    )


def _valid_settings(instance: CustomNodeBase) -> dict[str, list[str]]:
    """Section name -> component names, for error messages."""
    if instance.settings_schema is None:
        return {}
    return {
        name: sorted(section.get_components()) for name, section in instance.settings_schema._get_sections().items()
    }


def _canonical_settings(
    cls: type[CustomNodeBase], base: dict[str, Any], settings: Mapping[str, Any] | None
) -> dict[str, dict[str, Any]]:
    """The full settings envelope the node stores: ``base`` with ``settings`` applied on top.

    Unknown sections or components raise instead of being dropped, and so does a flat
    ``{component: value}`` dict (its keys are no section). The result is what
    ``cls.from_settings(...)._extract_settings_values()`` yields, JSON round-tripped because
    the flow file stores it that way.
    """
    fresh = cls.from_settings(base)
    key = fresh.item
    if settings:
        if not isinstance(settings, Mapping):
            raise NativeNodeError(
                f"settings for custom node {key!r} are a dict of {{section: {{component: value}}}}, "
                f"got {type(settings).__name__}"
            )
        valid = _valid_settings(fresh)
        if fresh.settings_schema is None:
            raise NativeNodeError(f"Custom node {key!r} has no settings, got {sorted(settings)}")
        values = {name: dict(v) if isinstance(v, Mapping) else v for name, v in settings.items()}
        report = fresh.settings_schema.populate_values_report(values)
        if report.has_drift:
            unknown = report.unknown_sections + report.unknown_components
            raise NativeNodeError(
                f"Unknown settings for custom node {key!r}: {unknown}; "
                f"settings are nested as {{section: {{component: value}}}} with {valid}"
            )
        flat = sorted(name for name, v in values.items() if not isinstance(v, dict))
        if flat:
            raise NativeNodeError(
                f"settings[{flat[0]!r}] of custom node {key!r} must be a dict of component values: {valid[flat[0]]}"
            )
    try:
        return json.loads(json.dumps(fresh._extract_settings_values()))
    except (TypeError, ValueError) as exc:
        raise NativeNodeError(
            f"Settings of custom node {key!r} must be JSON-serialisable (they are saved with the flow): {exc}"
        ) from exc


class CustomNode(NativeNode):
    """A user-defined (custom) node, placed from its class, an instance of it, or its node type name.

    ``settings`` is nested as ``{section: {component: value}}``, one-to-one with what the node
    stores; an instance contributes its configured values first. Input frames are wired to
    ``input-0`` to ``input-2`` in order and must match the node's ``number_of_inputs``.
    ``kernel`` binds an ``environment="kernel"`` node to a kernel, by id or an object with an
    ``.id`` (required there, refused on a local node); the id is not validated until the flow runs.

    A local node on a local graph runs its ``process()`` when it is built, like canvas
    prediction does (a lazy plan for a well-behaved node). A kernel node, a node whose
    schema cannot be predicted without data, or a local node on a remote graph is deferred:
    its outputs are typed zero-row placeholders until ``collect()`` runs the flow. A class
    registered here (not installed as a file) opens on the canvas in this process only.
    """

    node_class: type[CustomNodeBase]
    settings: dict[str, dict[str, Any]]
    kernel: str | None

    def __init__(
        self,
        node: type[CustomNodeBase] | CustomNodeBase | str,
        *inputs: FlowFrame,
        settings: dict[str, dict[str, Any]] | None = None,
        kernel: str | Any | None = None,
        description: str | None = None,
        flow_graph: FlowGraph | None = None,
    ) -> None:
        cls, base = _resolve(node)
        instance = cls()
        kernel = _kernel_id(kernel)
        if kernel is not None and not instance.uses_kernel:
            raise NativeNodeError(
                f"Custom node {instance.item!r} runs locally (environment='local'); "
                "kernel= only applies to environment='kernel' nodes"
            )
        self.node_class = cls
        self.settings = _canonical_settings(cls, base, settings)
        self.kernel = kernel
        self._on_kernel = kernel is not None or instance.kernel_id is not None
        has_hook = cls.predict_output_schema is not CustomNodeBase.predict_output_schema
        needs_run = self._on_kernel or (instance.requires_data_for_prediction and not has_hook)
        output_names = list(instance.output_names or ["main"])

        def make_settings(base_fields: dict[str, Any]) -> input_schema.UserDefinedNode:
            try:
                return input_schema.UserDefinedNode(
                    settings=json.loads(json.dumps(self.settings)),
                    kernel_id=kernel,
                    output_names=output_names,
                    is_user_defined=True,
                    **base_fields,
                )
            except ValidationError as exc:
                raise NativeNodeError(f"Invalid settings for custom node {instance.item!r}: {exc}") from exc

        self._build(
            instance.item,
            input_schema.UserDefinedNode,
            inputs,
            make_settings,
            deferred=True if needs_run else None,
            description=description,
            flow_graph=flow_graph,
        )

    def _add(self, settings: input_schema.UserDefinedNode) -> None:
        """The drawer's fail-loud ``add_user_defined_node``, after the base's promise and wiring.

        A local node on a remote graph is deferred before it is added: its function would be
        offloaded to the worker. A data-needing hook blocked behind an un-run kernel defers too.
        """
        graph = self.flow_graph
        if not self._on_kernel and graph.execution_location != "local":
            self.deferred = graph.get_node(self.node_id).deferred_until_run = True
        try:
            graph.add_user_defined_node(
                custom_node=self.node_class.from_settings(self.settings), user_defined_node_settings=settings
            )
        except KernelRequiredError as exc:
            raise NativeNodeError(f"{exc} Pass kernel='<kernel id>'.") from exc
        node = graph.get_node(self.node_id)
        if not self.deferred and node._prediction_requires_data:
            node.get_predicted_schema()
            self.deferred = bool(node._schema_prediction_blocked)

    def _seed_schemas(
        self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]
    ) -> dict[str, list[FlowfileColumn]]:
        """Each handle's schema from the node's hook (or blocked callback), ``[]`` when unknown.

        A hookless start node is not asked: its fallback schema callback runs the node
        function itself, whatever ``deferred_until_run`` says.
        """
        if node.is_start and node.user_provided_schema_callback is None:
            default: list[FlowfileColumn] = []
        else:
            default = predicted_schema_without_running(node)
        named = node._named_schemas
        return {
            handle: list(default if handle == DEFAULT_OUTPUT_HANDLE else named.get(handle) or []) for handle in handles
        }


def _flat_parameters(instance: CustomNodeBase) -> tuple[dict[str, tuple[str, str]], dict[str, Any]]:
    """Keyword name -> ``(section, component)``, and each keyword's current value.

    A component name shared by several sections, or clashing with a factory keyword, is
    only reachable as ``section__component``.
    """
    values = instance._extract_settings_values()
    located: dict[str, list[str]] = {}
    sections = instance.settings_schema._get_sections() if instance.settings_schema is not None else {}
    for section_name, section in sections.items():
        for component_name in section.get_components():
            located.setdefault(component_name, []).append(section_name)
    parameters: dict[str, tuple[str, str]] = {}
    defaults: dict[str, Any] = {}
    for component_name, section_names in located.items():
        unique = len(section_names) == 1 and component_name not in _FACTORY_KEYWORDS
        for section_name in section_names:
            name = component_name if unique else f"{section_name}__{component_name}"
            parameters[name] = (section_name, component_name)
            defaults[name] = values[section_name][component_name]
    return parameters, defaults


def _factory_signature(parameters: dict[str, tuple[str, str]], defaults: dict[str, Any]) -> inspect.Signature:
    """``(*inputs, <component>=<value>, ..., kernel=None, description=None, settings=None, flow_graph=None)``."""
    keyword = inspect.Parameter.KEYWORD_ONLY
    params = [inspect.Parameter("inputs", inspect.Parameter.VAR_POSITIONAL, annotation="FlowFrame")]
    params += [inspect.Parameter(name, keyword, default=defaults[name]) for name in parameters]
    params += [
        inspect.Parameter("kernel", keyword, default=None, annotation="str | Any | None"),
        inspect.Parameter("description", keyword, default=None, annotation="str | None"),
        inspect.Parameter("settings", keyword, default=None, annotation="dict[str, dict[str, Any]] | None"),
        inspect.Parameter("flow_graph", keyword, default=None, annotation="FlowGraph | None"),
    ]
    return inspect.Signature(params, return_annotation="FlowFrame")


class CustomNodeFactory:
    """Places one custom node type, its settings components given as keyword arguments.

    ``factory(*inputs, trim=True)`` returns the output frame of a single-output node;
    ``factory.node(*inputs, ...)`` returns the :class:`CustomNode` (``.output``, ``[name]``,
    ``.outputs``) and is the way to reach a multi-output node's frames. Keywords are the
    component names; a name used by several sections is written ``section__component``.
    ``settings`` takes the nested ``{section: {component: value}}`` form and combines with
    the keywords, but one component may not be given both ways. The signature
    (``inspect.signature(factory)``, ``help``) lists every component with its current value.
    """

    node_class: type[CustomNodeBase]
    node_type: str
    output_names: list[str]
    parameters: dict[str, tuple[str, str]]

    def __init__(self, node: type[CustomNodeBase] | CustomNodeBase | str) -> None:
        cls, base = _resolve(node)
        fresh = cls.from_settings(base)
        self._node = node if isinstance(node, CustomNodeBase | str) else cls
        self.node_class = cls
        self.node_type = fresh.item
        self.output_names = list(fresh.output_names or ["main"])
        self.parameters, defaults = _flat_parameters(fresh)
        self.__signature__ = _factory_signature(self.parameters, defaults)

    def __call__(
        self,
        *inputs: FlowFrame,
        kernel: str | Any | None = None,
        description: str | None = None,
        settings: dict[str, dict[str, Any]] | None = None,
        flow_graph: FlowGraph | None = None,
        **components: Any,
    ) -> FlowFrame:
        if len(self.output_names) != 1:
            raise NativeNodeError(
                f"Custom node {self.node_type!r} has outputs {self.output_names}; "
                "place it with .node(...) and pick an output with [name]"
            )
        return self.node(
            *inputs, kernel=kernel, description=description, settings=settings, flow_graph=flow_graph, **components
        ).output

    def node(
        self,
        *inputs: FlowFrame,
        kernel: str | Any | None = None,
        description: str | None = None,
        settings: dict[str, dict[str, Any]] | None = None,
        flow_graph: FlowGraph | None = None,
        **components: Any,
    ) -> CustomNode:
        """Place the node and return it, for its ``.output``, ``[name]`` and ``.outputs``."""
        return CustomNode(
            self._node,
            *inputs,
            settings=self._merge(settings, components),
            kernel=kernel,
            description=description,
            flow_graph=flow_graph,
        )

    def _merge(self, settings: Mapping[str, Any] | None, components: dict[str, Any]) -> dict[str, Any]:
        """The nested ``settings`` with every keyword component written into its section."""
        unknown = sorted(set(components) - set(self.parameters))
        if unknown:
            qualified = sorted(name for name, (_, comp) in self.parameters.items() if comp in unknown and name != comp)
            hint = f"; in several sections, write it as one of {qualified}" if qualified else ""
            raise NativeNodeError(
                f"Unknown settings {unknown} for custom node {self.node_type!r}{hint}; "
                f"valid names are {sorted(self.parameters)}"
            )
        if settings is not None and not isinstance(settings, Mapping):
            raise NativeNodeError(
                f"settings for custom node {self.node_type!r} are a dict of {{section: {{component: value}}}}, "
                f"got {type(settings).__name__}"
            )
        merged = {name: dict(v) if isinstance(v, Mapping) else v for name, v in (settings or {}).items()}
        for name, value in components.items():
            section, component = self.parameters[name]
            target = merged.setdefault(section, {})
            if not isinstance(target, dict):
                raise NativeNodeError(
                    f"settings[{section!r}] of custom node {self.node_type!r} must be a dict of component values"
                )
            if component in target:
                raise NativeNodeError(
                    f"{component!r} of section {section!r} is given twice, as {name}= and in settings[{section!r}]"
                )
            target[component] = value
        return merged


def custom_node(node: type[CustomNodeBase] | CustomNodeBase | str) -> CustomNodeFactory:
    """A factory placing custom node ``node`` (a class, an instance, or its node type name).

    ``fl.custom_node(NodeCleaner)(orders, trim=True)`` is the output frame of the node built
    with ``trim`` set; see :class:`CustomNodeFactory`. The class is resolved (and a class
    that is not installed registered for this process) once, here.
    """
    return CustomNodeFactory(node)
