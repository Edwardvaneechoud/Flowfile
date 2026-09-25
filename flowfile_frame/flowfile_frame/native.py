"""Helpers shared by native node classes and deferred frames.

A *deferred* node is one whose output cannot be materialised lazily in-process at build
time (a subflow run, a kernel script, an external source, or a side-effect node below a
deferred frame). Such a node is seeded with typed zero-row outputs instead of being
executed; only ``FlowGraph.run_graph()`` runs it for real.

This module must not import ``flowfile_frame.flow_frame`` at module level: ``flow_frame``
imports from here.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException
from pydantic import BaseModel, ValidationError

from flowfile_core.auth.utils import get_local_user_id
from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_graph_utils import combine_flow_graphs_with_mapping
from flowfile_core.flowfile.flow_node.flow_node import DeferredNodeError, FlowNode
from flowfile_core.flowfile.flow_node.input_handles import input_handle
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE, output_handle
from flowfile_core.flowfile.parameter_resolver import find_unresolved_in_model, node_parameters_resolved
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import GraphicWalkerInput
from flowfile_core.schemas.schemas import NodeTemplate, get_settings_class_for_node_type
from flowfile_frame.enums import NodeType, NodeTypes, _literal
from flowfile_frame.utils import create_flow_graph, generate_node_id, set_node_id
from flowfile_frame.utils import data as node_id_data

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.run_flow import FlowOutput


class NativeNodeError(ValueError):
    """A native node or deferred frame could not be built or materialised."""


DEFERRED_NODE_TYPES: frozenset[str] = frozenset(
    {"run_flow", "python_script", "google_analytics_reader", "external_source"}
)

SIDE_EFFECT_NODE_TYPES: frozenset[str] = frozenset({"train_model", "apply_model", "evaluate_model"})


def is_side_effect_node_type(node_type: str) -> bool:
    """Whether the node writes, publishes or trains: every ``output``-group template plus the model nodes.

    Such a node must not run at build time below a deferred frame: it would write an
    empty file or train on zero rows. ``random_split`` shares the ``ml`` group but is a
    plain lazy transform, so the group alone is not the test. Unknown types are not
    side-effect nodes.
    """
    if node_type in SIDE_EFFECT_NODE_TYPES:
        return True
    template = node_store.node_dict.get(node_type)
    return template is not None and template.node_group == "output"


def seed_deferred_node(node: FlowNode, schemas: dict[str, list[FlowfileColumn]]) -> None:
    """Give ``node`` typed zero-row outputs so building downstream never executes it.

    With ``results.resulting_data`` set, ``get_resulting_data``/``get_output`` return the
    seed instead of running the node function; ``_named_outputs`` keeps ``get_output`` from
    silently serving output-0 for another handle. Run flags and ``cache_results`` stay
    untouched, so ``run_graph()`` ignores the seed and executes the node for real, which
    also clears ``deferred_until_run``.
    """
    default_schema = schemas.get(DEFAULT_OUTPUT_HANDLE) or []
    with node._execution_lock_held():
        node.results.resulting_data = FlowDataEngine.create_from_schema(list(default_schema))
        node._named_outputs = {
            handle: FlowDataEngine.create_from_schema(list(schema or []))
            for handle, schema in schemas.items()
            if handle != DEFAULT_OUTPUT_HANDLE
        }
        node._named_schemas = dict(schemas)
        node.node_schema.result_schema = default_schema
        node.node_schema.predicted_schema = default_schema
        node.deferred_until_run = True


def predicted_schema_without_running(node: FlowNode) -> list[FlowfileColumn]:
    """``node``'s predicted schema from its schema callback only, never from its function.

    The flag is raised before predicting: a node without a usable schema callback would
    otherwise fall back to running its function on the zero-row input (a writer would
    write an empty file); it gets an empty schema instead.
    """
    node.deferred_until_run = True
    try:
        return node.get_predicted_schema() or []
    except DeferredNodeError:
        return []


def seed_from_predicted_schema(node: FlowNode) -> None:
    """Seed ``node`` on output-0 with its own predicted schema, never executing it."""
    seed_deferred_node(node, {DEFAULT_OUTPUT_HANDLE: predicted_schema_without_running(node)})


def _undeclared_parameters_error(node: FlowNode, names: set[str]) -> NativeNodeError:
    return NativeNodeError(
        f"{node.node_type} node {node.node_id} references undeclared flow parameter(s) {sorted(names)}; "
        "declare them with fl.add_flow_parameter(graph, fl.Parameter(name, default=...))"
    )


def materialise(node: FlowNode, handle: str | None = None) -> FlowDataEngine:
    """Build-time read of ``node``'s output, with its flow ``${name}`` references resolved.

    Every build-time read goes through here so a node sees its parameters the way the run loop
    does (``node_parameters_resolved``): expression fields get typed literals, other strings plain
    text, and the stored settings keep the reference. A reference to an undeclared parameter
    raises instead of reaching Polars as text, also on a graph that declares no parameters (where
    the run loop would leave it untouched).

    Build-time data reflects the parameter values at the moment the node is built; ``run_graph()``
    (and ``collect()`` on deferred or gated frames) re-resolves with the values of that run.
    ``handle`` reads one output handle through ``get_output``; ``None`` reads the default output.
    """
    params_getter = getattr(node, "_params_getter", None)
    params = params_getter() if params_getter is not None else {}
    if not params:
        unresolved = find_unresolved_in_model(node.setting_input)
        if unresolved:
            raise _undeclared_parameters_error(node, unresolved)
    with contextlib.ExitStack() as stack:
        try:
            stack.enter_context(node_parameters_resolved(node))
        except ValueError as exc:
            undeclared = find_unresolved_in_model(node.setting_input) - set(params)
            raise _undeclared_parameters_error(node, undeclared) from exc
        return node.get_output(handle) if handle is not None else node.get_resulting_data()


def ancestors(node: FlowNode) -> dict[int, FlowNode]:
    """``node`` plus every node it transitively reads from (``all_inputs``), keyed by id in visit order."""
    lineage, stack = {}, [node]
    while stack:
        current = stack.pop()
        if current.node_id not in lineage:
            lineage[current.node_id] = current
            stack.extend(current.all_inputs)
    return lineage


def lost_placeholder_error(node: FlowNode) -> NativeNodeError:
    """The error for a deferred node (``node`` or an ancestor) whose seed was reset away.

    A seeded node whose settings change after it was built (``set_group``, ``cache()``) is
    reset when the next edge is wired, which drops its placeholder output.
    """
    lost_id = next(
        (
            node_id
            for node_id, current in ancestors(node).items()
            if current.deferred_until_run and current.results.resulting_data is None
        ),
        node.node_id,
    )
    return NativeNodeError(
        f"node {lost_id} lost its deferred placeholder because its settings changed after it was built; "
        "collect the frame first or apply the change before building on it"
    )


def allocate_node_id(flow_graph: FlowGraph) -> int:
    """Next node id that is free in ``flow_graph`` and never behind the process counter.

    A foreign graph (opened from disk, or ``FlowGraph()``) can already hold ids beyond the
    counter, and ``add_node_step`` silently replaces a node with the same id.
    """
    new_id = max(generate_node_id(), max((n.node_id for n in flow_graph.nodes), default=0) + 1)
    set_node_id(new_id)
    if flow_graph.get_node(new_id) is not None:
        raise NativeNodeError(f"Node id {new_id} is already used in flow {flow_graph.flow_id}")
    return new_id


def add_connection_checked(flow_graph: FlowGraph, connection: input_schema.NodeConnection) -> None:
    """``add_connection`` with the FastAPI ``HTTPException`` translated into ``NativeNodeError``."""
    try:
        add_connection(flow_graph, connection)
    except HTTPException as exc:
        raise NativeNodeError(str(exc.detail)) from exc


def merge_frames(frames: Sequence[FlowFrame]) -> FlowGraph:
    """Bring every frame onto one graph and return it.

    Merges only when the frames live on graphs with different flow ids; each passed frame is
    then remapped in place (``node_id`` and ``flow_graph``). Other handles on the old graphs
    go stale. The merge carries deferred seeds, so it never executes a deferred node.
    """
    unique_graphs: list[FlowGraph] = []
    seen_flow_ids: set[int] = set()
    for frame in frames:
        if frame.flow_graph.flow_id not in seen_flow_ids:
            seen_flow_ids.add(frame.flow_graph.flow_id)
            unique_graphs.append(frame.flow_graph)
    if len(unique_graphs) <= 1:
        return frames[0].flow_graph

    try:
        combined_graph, node_mappings = combine_flow_graphs_with_mapping(*unique_graphs)
    except HTTPException as exc:
        raise NativeNodeError(str(exc.detail)) from exc
    for frame in frames:
        if frame.flow_graph is combined_graph:
            continue  # the same frame object passed twice
        new_id = node_mappings.get((frame.flow_graph.flow_id, frame.node_id))
        if new_id is None:
            raise NativeNodeError(f"Cannot remap node {frame.node_id} from flow {frame.flow_graph.flow_id}")
        frame.node_id = new_id
        frame.flow_graph = combined_graph
    node_id_data["c"] = node_id_data["c"] + len(combined_graph.nodes)
    return combined_graph


_BASE_MANAGED_FIELDS: frozenset[str] = frozenset(
    {"flow_id", "node_id", "pos_x", "pos_y", "is_setup", "user_id", "depending_on_id", "depending_on_ids"}
)


def _declared_fields(settings: BaseModel) -> list[FlowfileColumn]:
    """Columns a source node declares in its settings (``fields``, or ``source_settings.fields``)."""
    fields = getattr(settings, "fields", None) or getattr(getattr(settings, "source_settings", None), "fields", None)
    return [FlowfileColumn.from_input(f.name, f.data_type) for f in fields or []]


def _input_handles(node_type: str, template: NodeTemplate, count: int) -> list[str]:
    """Target handle per input frame: all on ``input-0`` for ``multi`` templates, else frame i on ``input-i``."""
    if template.multi:
        return [input_handle(0)] * count
    if not template.dynamic_inputs and count > 3:
        raise NativeNodeError(f"{node_type} takes at most three input frames (input-0 to input-2), got {count}")
    return [input_handle(i) for i in range(count)]


def _check_arity(node_type: str, template: NodeTemplate, count: int) -> None:
    """Refuse a wrong number of input frames up front: an incorrect node is silently dropped from saves."""
    if template.dynamic_inputs:
        return
    if template.multi:
        if count == 0 and not template.can_be_start:
            raise NativeNodeError(f"{node_type} needs at least one input frame")
        return
    low = template.min_inputs if template.min_inputs is not None else template.input
    if not low <= count <= template.input:
        expected = str(template.input) if low == template.input else f"{low} to {template.input}"
        raise NativeNodeError(f"{node_type} takes {expected} input frame(s), got {count}")


class NativeNode:
    """Base of the classes that place one canvas node from Python.

    A subclass ``__init__`` validates its arguments and calls :meth:`_build`, which runs the
    placement steps in canvas order: bring the input frames onto one graph, allocate a free
    node id, build the settings model with the base-managed fields, place a promise, wire
    every input, call ``graph.add_<node_type>`` and surface its add-time diagnostics, then
    wrap each output handle as a frame. A node whose output only exists once the flow runs
    is seeded with typed zero-row outputs and its frames are deferred. A node that fails to
    build is removed again, and every failure is a :class:`NativeNodeError`. Subclasses adapt
    the steps through :meth:`_add`, :meth:`_seed_schemas` and ``_build(handles=...)``.
    """

    node_type: str
    node_id: int
    flow_graph: FlowGraph
    output_names: list[str]
    deferred: bool

    @property
    def node(self) -> FlowNode:
        """The placed core node."""
        return self.flow_graph.get_node(self.node_id)

    @property
    def outputs(self) -> list[str]:
        """The output names, in handle order (``output-0`` first)."""
        return list(self.output_names)

    @property
    def output(self) -> FlowFrame:
        """The only output frame; a node with several outputs is indexed by name instead."""
        if len(self.output_names) != 1:
            raise NativeNodeError(
                f"{self.node_type} node {self.node_id} has outputs {self.output_names}; pick one with node[name]"
            )
        return self._frames[output_handle(0)]

    def __getitem__(self, name: str | FlowOutput) -> FlowFrame:
        from flowfile_frame.run_flow import FlowOutput

        if isinstance(name, FlowOutput):
            name = name.name
        if name not in self.output_names:
            raise NativeNodeError(f"{self.node_type} node {self.node_id} has no output {name!r}: {self.output_names}")
        return self._frames[output_handle(self.output_names.index(name))]

    def get_output(self, name: str | FlowOutput) -> FlowFrame:
        """The output frame named ``name`` (a name or a ``FlowOutput``); the spelled-out ``node[name]``."""
        return self[name]

    def _build(
        self,
        node_type: str,
        settings_cls: type[BaseModel],
        frames: Sequence[FlowFrame],
        make_settings: Callable[[dict[str, Any]], BaseModel],
        *,
        deferred: bool | None,
        description: str | None,
        flow_graph: FlowGraph | None = None,
        handles: list[str] | None = None,
    ) -> None:
        """Place the node; ``make_settings`` turns the base-managed fields into the settings model.

        ``handles`` names the target handle of each frame; by default every frame lands on
        ``input-0`` for ``multi`` templates, else frame i on ``input-i``.
        """
        template = node_store.node_dict[node_type]
        _check_arity(node_type, template, len(frames))
        if handles is None:
            handles = _input_handles(node_type, template, len(frames))
        self.node_type = node_type
        self.deferred = self._decide_deferred(node_type, frames, deferred)
        self.flow_graph = self._resolve_graph(frames, flow_graph)
        self.node_id = allocate_node_id(self.flow_graph)
        settings = make_settings(self._base_fields(settings_cls, frames, description))
        try:
            node = self._place(settings, frames, handles, template)
            self._build_outputs(node, frames)
        except Exception as exc:
            error = self._build_error(node_type, exc)
            with contextlib.suppress(Exception):
                self.flow_graph.delete_node(self.node_id)
            if error is exc:
                raise
            raise error from exc

    def _build_error(self, node_type: str, exc: Exception) -> NativeNodeError:
        """``exc`` as the :class:`NativeNodeError` a caller sees."""
        if isinstance(exc, NativeNodeError):
            return exc
        if isinstance(exc, DeferredNodeError):
            return lost_placeholder_error(self.flow_graph.get_node(self.node_id))
        detail = exc.detail if isinstance(exc, HTTPException) else exc
        return NativeNodeError(f"Could not build the {node_type} node: {detail}")

    @staticmethod
    def _decide_deferred(node_type: str, frames: Sequence[FlowFrame], deferred: bool | None) -> bool:
        """Deferred types, and side-effect nodes below a deferred frame, only run with the flow."""
        side_effect_below_deferred = is_side_effect_node_type(node_type) and any(f._deferred for f in frames)
        if deferred is None:
            return node_type in DEFERRED_NODE_TYPES or side_effect_below_deferred
        if not deferred and side_effect_below_deferred:
            raise NativeNodeError(
                f"{node_type} writes or trains when it is built, and its input only holds placeholder rows "
                "until the flow runs; leave deferred unset or collect the input first"
            )
        return deferred

    @staticmethod
    def _resolve_graph(frames: Sequence[FlowFrame], flow_graph: FlowGraph | None) -> FlowGraph:
        """The graph to place on: the input frames' (merged when they differ), else ``flow_graph`` or a new one."""
        if not frames:
            return flow_graph if flow_graph is not None else create_flow_graph()
        if flow_graph is not None and all(f.flow_graph is not flow_graph for f in frames):
            raise NativeNodeError("flow_graph= must be the input frames' graph; a node is placed where its inputs are")
        return merge_frames(frames)

    def _base_fields(
        self, settings_cls: type[BaseModel], frames: Sequence[FlowFrame], description: str | None
    ) -> dict[str, Any]:
        """The settings fields every native node sets itself (never ``None``: the canvas payload needs them).

        ``depending_on_id(s)`` is bookkeeping only (the edges carry the wiring) and is set on
        whichever of the two fields ``settings_cls`` has.
        """
        fields: dict[str, Any] = {
            "flow_id": self.flow_graph.flow_id,
            "node_id": self.node_id,
            "pos_x": 0.0,
            "pos_y": 0.0,
            "is_setup": True,
            "user_id": get_local_user_id(),
        }
        if description is not None:
            fields["description"] = description
        if frames:
            fields["depending_on_id"] = frames[0].node_id
            fields["depending_on_ids"] = [f.node_id for f in frames]
        return {key: value for key, value in fields.items() if key in settings_cls.model_fields}

    def _connect(self, frame: FlowFrame, handle: str) -> None:
        """Wire ``frame``'s output handle into this node's ``handle``."""
        connection = input_schema.NodeConnection.create_from_simple_input(
            frame.node_id, self.node_id, input_type=handle, output_handle=frame.output_handle
        )
        add_connection_checked(self.flow_graph, connection)

    def _place(
        self, settings: BaseModel, frames: Sequence[FlowFrame], handles: list[str], template: NodeTemplate
    ) -> FlowNode:
        """Promise, wire, ``add_<type>`` (keyed-input nodes: add first, the keyed validator refuses a promise)."""
        graph = self.flow_graph
        if template.dynamic_inputs:
            result = self._add(settings)
            graph.get_node(self.node_id).deferred_until_run = self.deferred
            for frame, handle in zip(frames, handles, strict=True):
                self._connect(frame, handle)
        else:
            graph.add_node_promise(
                input_schema.NodePromise(
                    flow_id=graph.flow_id, node_id=self.node_id, node_type=self.node_type, pos_x=0.0, pos_y=0.0
                )
            )
            # Before add_<type>: a deferred start node's eager schema prefetch would run its function.
            graph.get_node(self.node_id).deferred_until_run = self.deferred
            for frame, handle in zip(frames, handles, strict=True):
                self._connect(frame, handle)
            result = self._add(settings)
        node = graph.get_node(self.node_id)
        if isinstance(result, tuple) and result and result[0] is False:
            raise NativeNodeError(f"{self.node_type} node {self.node_id}: {result[1]}")
        if self.node_type in ("sql_query", "polars_code") and node.results.errors:
            raise NativeNodeError(f"{self.node_type} node {self.node_id}: {node.results.errors}")
        return node

    def _add(self, settings: BaseModel) -> Any:
        """``graph.add_<node_type>(settings)``; its return value carries the add-time diagnostics."""
        return getattr(self.flow_graph, f"add_{self.node_type}")(settings)

    def _build_outputs(self, node: FlowNode, frames: Sequence[FlowFrame]) -> None:
        """One frame per output handle; a deferred node is seeded first, so nothing executes it."""
        from flowfile_frame.flow_frame import FlowFrame

        self.output_names = list(getattr(node.setting_input, "output_names", None) or ["main"])
        handles = [output_handle(i) for i in range(len(self.output_names))]
        if self.deferred:
            seed_deferred_node(node, self._seed_schemas(node, frames, handles))
        inherited = any(f._deferred for f in frames)
        self._frames = {
            handle: FlowFrame(
                data=materialise(node, handle).data_frame,
                flow_graph=self.flow_graph,
                node_id=self.node_id,
                parent_node_id=frames[0].node_id if frames else None,
                output_handle=handle,
                deferred=self.deferred or inherited,
            )
            for handle in handles
        }

    def _seed_schemas(
        self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]
    ) -> dict[str, list[FlowfileColumn]]:
        """Schema per seeded output handle; by default every handle carries :meth:`_seed_schema`."""
        schema = self._seed_schema(node, frames)
        return {handle: list(schema) for handle in handles}

    def _seed_schema(self, node: FlowNode, frames: Sequence[FlowFrame]) -> list[FlowfileColumn]:
        """Schema of every seeded output handle.

        A source node takes the columns its settings declare. A deferred node type or a
        side-effect node predicts from its schema callback only; any other node (deferred on
        request) predicts like the canvas does, lazily over its inputs' placeholders.
        """
        if not frames:
            return _declared_fields(node.setting_input)
        if self.node_type in DEFERRED_NODE_TYPES or is_side_effect_node_type(self.node_type):
            return predicted_schema_without_running(node)
        node.deferred_until_run = False
        try:
            return node.get_predicted_schema() or []
        finally:
            node.deferred_until_run = True


def _settings_class(node_type: Any) -> type[BaseModel]:
    """The settings model of a built-in node type; custom and internal types are refused."""
    if node_type == "promise":
        raise NativeNodeError("promise is the canvas placeholder of an unconfigured node; pass a real node type")
    if node_type == "polars_lazy_frame":
        raise NativeNodeError("polars_lazy_frame wraps an in-memory LazyFrame; use fl.FlowFrame(lazy_frame) instead")
    if node_type == "run_flow":
        raise NativeNodeError("run_flow keys its inputs by slot (input-0 is the parameter frame); use fl.RunFlow(...)")
    settings_cls = get_settings_class_for_node_type(node_type) if isinstance(node_type, str) else None
    if settings_cls is input_schema.UserDefinedNode:
        raise NativeNodeError(f"{node_type!r} is a custom node; place it with fl.CustomNode(...)")
    if settings_cls is None:
        raise NativeNodeError(f"Unknown node type {node_type!r}; fl.NodeTypes lists the built-in types")
    return settings_cls


def _check_settings_keys(node_type: str, settings_cls: type[BaseModel], keys: set[str]) -> None:
    """Refuse unknown and base-managed top-level keys (the settings models silently ignore extras)."""
    known = set(settings_cls.model_fields) | {f.alias for f in settings_cls.model_fields.values() if f.alias}
    unknown = sorted(keys - known)
    if unknown:
        raise NativeNodeError(
            f"Unknown settings for {node_type}: {unknown}; valid keys are {sorted(known - _BASE_MANAGED_FIELDS)}"
        )
    managed = sorted(keys & _BASE_MANAGED_FIELDS)
    if managed:
        raise NativeNodeError(f"{managed} are set by the node itself; leave them out of settings")


class Node(NativeNode):
    """Any built-in node type, placed from its type and settings.

    ``settings`` is the node's settings model or a dict of its fields (see
    ``flowfile_core.schemas.input_schema``); the base sets ``flow_id``, ``node_id``, the
    position, ``is_setup``, ``user_id`` and ``depending_on_id(s)``. Input frames are wired in
    order: every frame on ``input-0`` for multi-input nodes (``union``, ``polars_code``,
    ``sql_query``), else frame i on ``input-i``. Outputs are deferred for subflow, script and
    external-source nodes, and for writers below a deferred frame; ``deferred`` overrides
    that. The dedicated classes (``fl.Gate`` and the like) are the normal route for the
    nodes they cover; custom nodes go through ``fl.CustomNode``.
    """

    def __init__(
        self,
        node_type: NodeType | NodeTypes,
        *inputs: FlowFrame,
        settings: dict[str, Any] | BaseModel | None = None,
        deferred: bool | None = None,
        description: str | None = None,
        flow_graph: FlowGraph | None = None,
    ) -> None:
        node_type = _literal(node_type)
        settings_cls = _settings_class(node_type)
        if settings is None:
            settings = {}
        if isinstance(settings, Mapping):
            _check_settings_keys(node_type, settings_cls, set(settings))
        elif not isinstance(settings, settings_cls):
            raise NativeNodeError(
                f"settings for {node_type} must be a dict or a {settings_cls.__name__}, got {type(settings).__name__}"
            )

        def make_settings(base: dict[str, Any]) -> BaseModel:
            if isinstance(settings, BaseModel):
                model = settings.model_copy(deep=True, update=base)
            else:
                try:
                    model = settings_cls.model_validate({**settings, **base})
                except ValidationError as exc:
                    raise NativeNodeError(f"Invalid settings for {node_type}: {exc}") from exc
            if node_type == "explore_data" and model.graphic_walker_input is None:
                model.graphic_walker_input = GraphicWalkerInput()
            return model

        self._build(
            node_type,
            settings_cls,
            inputs,
            make_settings,
            deferred=deferred,
            description=description,
            flow_graph=flow_graph,
        )
