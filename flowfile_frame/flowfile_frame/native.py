"""Helpers shared by native node classes and deferred frames.

A *deferred* node is one whose output cannot be materialised lazily in-process at build
time (a subflow run, a kernel script, an external source, or a side-effect node below a
deferred frame). Such a node is seeded with typed zero-row outputs instead of being
executed; only ``FlowGraph.run_graph()`` runs it for real. In a notebook sync (a mode entered
with ``sync=True``) more nodes are held (:func:`held_in_sync`) and every held node is seeded by
:func:`sync_seed_schemas`, which predicts nothing.

This module must not import ``flowfile_frame.flow_frame`` at module level: ``flow_frame``
imports from here.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException
from pydantic import BaseModel, ValidationError

from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_graph_utils import combine_flow_graphs_with_mapping
from flowfile_core.flowfile.flow_node.flow_node import DeferredNodeError, FlowNode
from flowfile_core.flowfile.flow_node.input_handles import input_handle
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE, output_handle
from flowfile_core.flowfile.parameter_resolver import find_unresolved_in_model, node_parameters_resolved
from flowfile_core.notebook.compare import DROPPED_FIELDS, normalise
from flowfile_core.notebook.relabel import _SETTING_ID_KEYS, _SETTING_ID_LIST_KEYS
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import GraphicWalkerInput
from flowfile_core.schemas.schemas import NodeTemplate, get_settings_class_for_node_type
from flowfile_frame._identity import current_user_id
from flowfile_frame.enums import NodeType, NodeTypeLiteral, _literal
from flowfile_frame.notebook import current
from flowfile_frame.utils import _implicit_graph, generate_node_id, set_node_id
from flowfile_frame.utils import data as node_id_data
from shared.path_utils import is_url
from shared.sql_validation import uses_table_function

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.run_flow import FlowOutput


class NativeNodeError(ValueError):
    """A native node or deferred frame could not be built or materialised."""


DEFERRED_NODE_TYPES: frozenset[str] = frozenset(
    {"run_flow", "python_script", "google_analytics_reader", "external_source"}
)

SIDE_EFFECT_NODE_TYPES: frozenset[str] = frozenset({"train_model", "apply_model", "evaluate_model"})

NOTEBOOK_DEFERRED_NODE_TYPES: frozenset[str] = frozenset(
    {"database_reader", "rest_api_reader", "kafka_source", "api_response", "pivot", "polars_code"}
)

SYNC_HELD_NODE_TYPES: frozenset[str] = frozenset({"fuzzy_match", "random_split", "pivot"})
"""Transforms a sync holds: building them computes over their input's rows (a match, a shuffle, pivot values)."""

LITERAL_SOURCE_TYPES: frozenset[str] = frozenset({"manual_input", "flow_input"})
"""The sources a sync builds: their rows are the cell's own literals."""

PROBED_FILE_TYPES: frozenset[str] = frozenset({"csv", "json", "parquet", "ipc", "ndjson"})
"""File types whose canvas read node predicts its schema from the file's header or footer."""

_FORMULA_RULE_TYPES: frozenset[str] = frozenset({"formula", "filter", "gate"})
"""Types whose settings normalisation translates formulas; :func:`_twin_settings` compares them structurally."""


def is_side_effect_node_type(node_type: str) -> bool:
    """Whether the node writes, publishes or trains: every ``output``-group template plus the model nodes.

    Such a node must not run at build time below a deferred frame: it would write an
    empty file or train on zero rows. ``random_split`` shares the ``ml`` group but is a
    plain lazy transform, so the group alone is not the test. A custom node counts when
    its class declares ``node_type="output"``, whatever palette group its category gives
    it. Unknown types are not side-effect nodes.
    """
    if node_type in SIDE_EFFECT_NODE_TYPES:
        return True
    template = node_store.node_dict.get(node_type)
    if template is None:
        return False
    return template.node_group == "output" or (template.custom_node and template.node_type == "output")


def notebook_defers(node_type: str, setting_input: Any = None) -> bool:
    """Whether notebook mode seeds a node of ``node_type`` from its schema instead of executing it at build.

    The node types built without executing: ``DEFERRED_NODE_TYPES``, every side-effect
    type, the sources and transforms of ``NOTEBOOK_DEFERRED_NODE_TYPES`` (they do real I/O, or
    may, when built in a local graph) and SQL-mode or virtual catalog readers (the latter
    re-execute their producer); in a sync also every node :func:`held_in_sync` names. Always
    ``False`` outside notebook mode.
    """
    mode = current()
    if mode is None:
        return False
    if node_type in DEFERRED_NODE_TYPES or node_type in NOTEBOOK_DEFERRED_NODE_TYPES:
        return True
    if node_type == "catalog_reader" and setting_input is not None:
        if setting_input.sql_query or setting_input.is_virtual_optimized is not None:
            return True
    if is_side_effect_node_type(node_type):
        return True
    return mode.sync and held_in_sync(node_type, setting_input)


def held_in_sync(node_type: str, setting_input: Any = None) -> bool:
    """Whether a sync holds a node that notebook mode alone would build (a sync builds no data but literals).

    Held: every source except ``LITERAL_SOURCE_TYPES`` (reads, catalog and cloud readers,
    ``list_files``, network sources), every custom node and unknown type, the
    ``SYNC_HELD_NODE_TYPES``, a first-row ``dynamic_rename`` (it reads a row), a ``data_cleansing``
    that removes null columns (it counts every column's nulls, on the worker) and a ``sql_query``
    that uses a table function (``read_*`` / ``scan_*`` read files), by the canvas's own gate
    (``shared.sql_validation.uses_table_function``), or holds a ``${name}`` reference (a parameter
    can resolve to one when the node runs).
    """
    template = node_store.node_dict.get(node_type)
    if template is None or template.custom_node or node_type in SYNC_HELD_NODE_TYPES:
        return True
    if template.input == 0:
        return node_type not in LITERAL_SOURCE_TYPES
    if setting_input is None:
        return False
    if node_type == "dynamic_rename":
        return setting_input.dynamic_rename_input.rename_mode == "first_row"
    if node_type == "data_cleansing":
        return setting_input.cleansing_input.remove_null_columns
    if node_type == "sql_query":
        sql = setting_input.sql_query_input.sql_code or ""
        return "${" in sql or uses_table_function(sql)
    return False


def _in_sync() -> bool:
    mode = current()
    return mode is not None and mode.sync


def _held(node: FlowNode) -> bool:
    """Whether building ``node`` in the active mode seeds it instead of executing it."""
    if notebook_defers(node.node_type, node.setting_input):
        return True
    return _in_sync() and getattr(node, "_prediction_requires_data", False)


def _handles(node: FlowNode) -> list[str]:
    return [output_handle(i) for i in range(len(output_names_of(node.setting_input)))]


def seeded_at_build(
    node_type: str,
    frames: Sequence[FlowFrame],
    *,
    inputs_deferred: bool | None = None,
    setting_input: Any = None,
) -> bool:
    """Whether a node of ``node_type`` over ``frames`` is seeded instead of executed when it is built.

    Deferred node types always are, and in notebook mode so is every node :func:`notebook_defers`
    names (``setting_input`` lets it judge the settings too). A side-effect node is when an input
    frame is deferred (it would write or train on placeholder rows) or below a gate (only a run
    decides which exit is live, so building would also write the dead side); the run then executes
    it on the live side only. ``inputs_deferred`` replaces the frames' own ``_deferred`` for a
    caller that tracks it across more inputs than it passes. The gate walk only happens for
    side-effect types.
    """
    if node_type in DEFERRED_NODE_TYPES or notebook_defers(node_type, setting_input):
        return True
    if not is_side_effect_node_type(node_type):
        return False
    if inputs_deferred is None:
        inputs_deferred = any(f._deferred for f in frames)
    return inputs_deferred or any(f._below_a_gate() for f in frames)


def output_names_of(setting_input: Any) -> list[str]:
    """The node's output names from its settings; ``["main"]`` when it declares none."""
    return list(getattr(setting_input, "output_names", None) or ["main"])


def _kernel_id(kernel: str | Any | None) -> str | None:
    """A ``kernel=`` argument as the id to store: an id as given, or the ``.id`` of a kernel object; never looked up."""
    if kernel is None or isinstance(kernel, str):
        return kernel
    kernel_id = getattr(kernel, "id", None)
    if isinstance(kernel_id, str):
        return kernel_id
    raise NativeNodeError(f"kernel= takes a kernel id or an object with an .id, got {type(kernel).__name__}")


def seed_deferred_node(node: FlowNode, schemas: dict[str, list[FlowfileColumn]]) -> None:
    """Give ``node`` typed zero-row outputs so building downstream never executes it.

    With ``results.resulting_data`` set, ``get_resulting_data``/``get_output`` return the
    seed instead of running the node function; ``_named_outputs`` keeps ``get_output`` from
    silently serving output-0 for another handle. Run flags and ``cache_results`` stay
    untouched, so ``run_graph()`` ignores the seed and executes the node for real, which
    also clears ``deferred_until_run``. ``placed_deferred`` stays set, so a later reset of
    the node re-arms ``deferred_until_run`` and the next build read seeds it again.
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
        node.placed_deferred = True


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
    """Seed ``node`` on output-0 with its own predicted schema, never executing it.

    A ``polars_code`` transform (seeded only in notebook mode) has no schema callback, so it
    predicts lazily over its inputs the way the canvas does; the frame's own writer fallbacks, the
    only fluent code that writes, are refused in notebook mode. A ``polars_code`` source would
    read to predict, so it gets the callback-only (empty) schema like any other source. In a sync
    nothing is predicted: every handle takes :func:`sync_seed_schemas`.
    """
    if _in_sync():
        seed_deferred_node(node, sync_seed_schemas(node, _handles(node)))
        return
    if node.node_type == "polars_code" and node.all_inputs:
        seed_deferred_node(node, {DEFAULT_OUTPUT_HANDLE: _placeholder_schema(node)})
        return
    seed_deferred_node(node, {DEFAULT_OUTPUT_HANDLE: predicted_schema_without_running(node)})


def source_frame(flow_graph: FlowGraph, node_id: int) -> FlowFrame:
    """The frame of a source node just added to ``flow_graph``.

    In notebook mode a source that :func:`notebook_defers` names is seeded from its
    schema callback and wrapped as a deferred frame, so building it never reads; otherwise the
    node's build-time result is wrapped.
    """
    from flowfile_frame.flow_frame import FlowFrame

    node = flow_graph.get_node(node_id)
    if notebook_defers(node.node_type, node.setting_input):
        seed_from_predicted_schema(node)
        return FlowFrame(
            data=node.results.resulting_data.data_frame, flow_graph=flow_graph, node_id=node_id, deferred=True
        )
    return FlowFrame(data=node.get_resulting_data().data_frame, flow_graph=flow_graph, node_id=node_id)


def _placeholder_schema(node: FlowNode) -> list[FlowfileColumn]:
    """``node``'s output-0 schema, predicted without executing it.

    A deferred, side-effect or custom node type asks its schema callback only (a custom start
    node without a hook gets none: its fallback callback runs the node); any other type
    predicts lazily over its inputs' placeholders, the way the canvas does. In a sync it is
    :func:`sync_seed_schemas`' output-0.
    """
    if _in_sync():
        return sync_seed_schemas(node, [DEFAULT_OUTPUT_HANDLE])[DEFAULT_OUTPUT_HANDLE]
    if isinstance(node.setting_input, input_schema.UserDefinedNode):
        if node.is_start and node.user_provided_schema_callback is None:
            return []
        return predicted_schema_without_running(node)
    if node.node_type in DEFERRED_NODE_TYPES or is_side_effect_node_type(node.node_type):
        return predicted_schema_without_running(node)
    node.deferred_until_run = False
    try:
        return node.get_predicted_schema() or []
    finally:
        node.deferred_until_run = True


def sync_seed_schemas(
    node: FlowNode, handles: Sequence[str], declared: Mapping[str, list[FlowfileColumn]] | None = None
) -> dict[str, list[FlowfileColumn]]:
    """Per-handle schemas a sync seeds the held ``node`` with, predicted without running anything.

    The first that applies: (1) the schemas of :func:`_snapshot_twin`, when they hold columns;
    (2) what the cell declares, ``declared`` (a script's ``returns=``, a custom node's ``schemas=``),
    else what the settings declare (:func:`_declared_schemas`); (3) what the canvas reads to show a
    schema (:func:`_canvas_probe`); (4) no columns, and the node is recorded on the mode's ``column_less``.

    Never a node function, a schema callback, a custom-node hook, a child flow, polars code, a
    directory glob, an eager reader, a connection or a decrypt.
    """
    twin = _snapshot_twin(node)
    if twin is not None and any(twin.schemas.values()):
        return _per_handle(twin.schemas, handles)
    if declared is None:
        declared = _declared_schemas(node)
    if declared is not None:
        return _per_handle(declared, handles)
    probed = _canvas_probe(node)
    if probed is not None:
        return {handle: list(probed) for handle in handles}
    mode = current()
    if mode is not None:
        mode.column_less.add(node.node_id)
    return {handle: [] for handle in handles}


def _per_handle(schemas: Mapping[str, Sequence[FlowfileColumn]], handles: Sequence[str]) -> dict[str, list]:
    default = schemas.get(DEFAULT_OUTPUT_HANDLE) or []
    return {handle: list(schemas.get(handle) or default) for handle in handles}


def _without_ids(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_ids(item)
            for key, item in value.items()
            if key not in _SETTING_ID_KEYS and key not in _SETTING_ID_LIST_KEYS
        }
    if isinstance(value, list):
        return [_without_ids(item) for item in value]
    return value


def _twin_settings(settings: BaseModel, node_type: str) -> Any:
    """``settings`` as :func:`_snapshot_twin` compares them: without ids, and normalised like a push compares them.

    The push's per-type normalisation (``flowfile_core.notebook.compare.normalise``: layout,
    labels, the node user, a read's display name, a script's cell ids) applies, except for the
    types whose rules translate formulas; those drop the same top-level fields and compare the
    rest as is, so the seed never evaluates anything.
    """
    dumped = _without_ids(settings.model_dump(mode="json"))
    if node_type in _FORMULA_RULE_TYPES:
        return {key: value for key, value in dumped.items() if key not in DROPPED_FIELDS}
    return normalise(dumped, node_type)


def _snapshot_twin(node: FlowNode) -> Any | None:
    """The unchanged canvas node the running cell rendered ``node`` from, else ``None`` (:func:`sync_seed_schemas`).

    The first canvas id of ``node``'s type the cell rendered, in render order, that no other node of
    the cell has claimed and whose settings equal ``node``'s (a ``polars_code`` twin also needs the
    same input column names); it is then claimed (``mode.claimed``). Without edits this is the
    relabel rule (the k-th node of a type takes the k-th canvas id), and a node the cell creates but
    does not keep (an unbound line) shifts nothing.
    """
    mode = current()
    if mode is None or not mode.sync or mode.cell_id is None or node.setting_input is None:
        return None
    if node.node_id not in mode.cell_nodes:
        return None
    taken = {canvas_id for node_id, canvas_id in mode.claimed.items() if node_id != node.node_id}
    own = _twin_settings(node.setting_input, node.node_type)
    for node_type, canvas_id in mode.expected.get(mode.cell_id, ()):
        if node_type != node.node_type or canvas_id in taken:
            continue
        twin = mode.snapshot.get(canvas_id)
        if twin is None or twin.node_type != node.node_type or twin.setting_input is None:
            continue
        if _twin_settings(twin.setting_input, node.node_type) != own:
            continue
        if node.node_type == "polars_code" and _input_columns(node) != _canvas_input_columns(twin, mode.snapshot):
            continue
        mode.claimed[node.node_id] = canvas_id
        return twin
    mode.claimed.pop(node.node_id, None)
    return None


def _column_names(columns: Sequence[FlowfileColumn] | None) -> tuple[str, ...]:
    return tuple(column.column_name for column in columns or [])


def _input_columns(node: FlowNode) -> list[tuple[str, ...]] | None:
    """The column names on each input edge of ``node`` (in no particular order), from what its inputs hold."""
    names = []
    for source, handle in node._incoming_edges():
        engine = source.results.resulting_data if handle == DEFAULT_OUTPUT_HANDLE else source._named_outputs.get(handle)
        if engine is None:
            return None
        names.append(_column_names(engine.schema))
    return sorted(names)


def _canvas_input_columns(twin: Any, snapshot: Mapping[int, Any]) -> list[tuple[str, ...]] | None:
    names = []
    for source_id, handle in twin.inputs:
        source = snapshot.get(source_id)
        if source is None:
            return None
        names.append(_column_names(source.schemas.get(handle) or source.schemas.get(DEFAULT_OUTPUT_HANDLE)))
    return sorted(names)


def _declared_schemas(node: FlowNode) -> dict[str, list[FlowfileColumn]] | None:
    """The schemas ``node``'s settings declare, per handle; ``None`` when they declare none."""
    from flowfile_core.flowfile.flow_graph import list_files_schema
    from flowfile_core.flowfile.subflow import predict_run_summary_schema

    settings = node.setting_input
    if node.node_type == "list_files":
        return {DEFAULT_OUTPUT_HANDLE: list_files_schema()}
    if isinstance(settings, input_schema.NodeRead):
        received = settings.received_file
        if not received.fields:
            return None
        columns = [FlowfileColumn.from_input(f.name, f.data_type) for f in received.fields]
        if received.include_file_paths and received.include_file_paths not in {f.name for f in received.fields}:
            columns.append(FlowfileColumn.from_input(received.include_file_paths, "String"))
        return {DEFAULT_OUTPUT_HANDLE: columns}
    if isinstance(settings, input_schema.NodePythonScript):
        first = node.node_inputs.main_inputs[0] if node.node_inputs.main_inputs else None
        engine = first.results.resulting_data if first is not None else None
        passed = list(engine.schema) if engine is not None else []
        declared = settings.output_schemas or {}
        return {
            output_handle(index): (
                [FlowfileColumn.from_input(f.name, f.data_type) for f in declared[name]]
                if name in declared
                else list(passed)
            )
            for index, name in enumerate(settings.output_names)
        }
    if isinstance(settings, input_schema.NodeRunFlow) and not settings.output_slots:
        return {DEFAULT_OUTPUT_HANDLE: predict_run_summary_schema(settings)}
    if not node.all_inputs:
        fields = _declared_fields(settings)
        if fields:
            return {DEFAULT_OUTPUT_HANDLE: fields}
    return None


def _canvas_probe(node: FlowNode) -> list[FlowfileColumn] | None:
    """What the canvas reads to show a held source's schema: a local file's header or a catalog table's record.

    Only a local single-file read of ``PROBED_FILE_TYPES`` without a ``${name}`` in its path is
    probed, exactly as the read node's schema callback does; a probe that fails gives no schema.
    A table-mode catalog reader takes the registered ``schema_json`` (plus the change-feed columns
    for a change read); SQL-mode and virtual readers take none.
    """
    settings = node.setting_input
    if isinstance(settings, input_schema.NodeRead):
        received = settings.received_file
        path = received.path or ""
        if received.file_type not in PROBED_FILE_TYPES or received.scan_mode == "directory":
            return None
        if is_url(path) or "${" in path:
            return None
        try:
            return list(FlowDataEngine.create_from_path(received).schema)
        except Exception:
            return None
    if isinstance(settings, input_schema.NodeCatalogReader):
        if settings.sql_query or settings.is_virtual_optimized is not None:
            return None
        from flowfile_core.flowfile.flow_graph import _CDF_COLUMN_DTYPES, _resolve_catalog_table_info

        schema_json = _resolve_catalog_table_info(settings).schema_json
        if not schema_json:
            return None
        columns = [FlowfileColumn.from_input(entry["name"], entry["dtype"]) for entry in json.loads(schema_json)]
        if settings.cdc_mode != "off":
            columns.extend(FlowfileColumn.from_input(name, dtype) for name, dtype in _CDF_COLUMN_DTYPES)
        return columns
    return None


def _reseed_lost_placeholders(nodes: Sequence[FlowNode]) -> None:
    """Seed again, upstream first, every deferred node of ``nodes`` whose result a reset dropped.

    Each keeps its last known per-handle schemas (its seed, or a multi-output run's outputs);
    output-0 is predicted when none is known. Reading below it then serves placeholders. In a
    sync a held node not seeded yet (a transform a fluent method placed) is seeded here too, from
    :func:`sync_seed_schemas`, so the read never executes it.
    """
    in_read = {n.node_id for n in nodes}
    visited: set[int] = set()

    def reseed(node: FlowNode) -> None:
        visited.add(node.node_id)
        for upstream in node.all_inputs:
            if upstream.node_id in in_read and upstream.node_id not in visited:
                reseed(upstream)
        if node.results.resulting_data is not None:
            return
        if node.deferred_until_run:
            schemas = dict(node._named_schemas)
            if DEFAULT_OUTPUT_HANDLE not in schemas:
                schemas[DEFAULT_OUTPUT_HANDLE] = _placeholder_schema(node)
            seed_deferred_node(node, schemas)
        elif _held(node) and _in_sync():
            seed_deferred_node(node, sync_seed_schemas(node, _handles(node)))

    for node in nodes:
        if node.node_id not in visited:
            reseed(node)


def _undeclared_parameters_error(node: FlowNode, names: set[str]) -> NativeNodeError:
    return NativeNodeError(
        f"{node.node_type} node {node.node_id} references undeclared flow parameter(s) {sorted(names)}; "
        "declare them with fl.add_flow_parameter(graph, fl.Parameter(name, default=...))"
    )


def _resolve_parameters_for_read(stack: contextlib.ExitStack, node: FlowNode) -> None:
    """Enter ``node_parameters_resolved(node)`` on ``stack``; an undeclared reference raises."""
    params_getter = getattr(node, "_params_getter", None)
    params = params_getter() if params_getter is not None else {}
    if not params:
        unresolved = find_unresolved_in_model(node.setting_input)
        if unresolved:
            raise _undeclared_parameters_error(node, unresolved)
    try:
        stack.enter_context(node_parameters_resolved(node))
    except ValueError as exc:
        undeclared = find_unresolved_in_model(node.setting_input) - set(params)
        raise _undeclared_parameters_error(node, undeclared) from exc


def ancestors(node: FlowNode, follow: Callable[[FlowNode], bool] | None = None) -> dict[int, FlowNode]:
    """``node`` plus every node it transitively reads from (``all_inputs``), keyed by id in visit order.

    ``follow`` limits the walk to the inputs it accepts; the walk does not pass a refused input.
    """
    lineage, stack = {}, [node]
    while stack:
        current = stack.pop()
        if current.node_id not in lineage:
            lineage[current.node_id] = current
            stack.extend(n for n in current.all_inputs if follow is None or follow(n))
    return lineage


def _nodes_a_read_executes(node: FlowNode) -> list[FlowNode]:
    """``node`` plus every ancestor without a result, walked up to the nodes that hold one.

    Reading ``node`` executes exactly these: an upstream node reads its own inputs directly, so
    one that lost its result (a cross-graph merge rebuilds every node, ``FlowGraph.reset()``)
    re-executes inside this read.
    """
    return list(
        ancestors(node, follow=lambda n: n.results.resulting_data is None and n.results.errors is None).values()
    )


def materialise(node: FlowNode, handle: str | None = None) -> FlowDataEngine:
    """Build-time read of ``node``'s output, with its flow ``${name}`` references resolved.

    Every build-time read goes through here so a node sees its parameters the way the run loop
    does (``node_parameters_resolved``): expression fields get typed literals, a string literal in
    Polars code is re-rendered as an escaped literal of its substituted text, other strings get
    plain text, and the stored settings keep the reference. The same holds for every ancestor the read
    re-executes. A reference to an undeclared parameter raises instead of reaching Polars as text,
    also on a graph that declares no parameters (where the run loop would leave it untouched). A
    deferred node in that lineage whose placeholder or run result was reset away is seeded again
    first, so the read never executes it.

    Build-time data reflects the parameter values at the moment the node is built; ``run_graph()``
    (and ``collect()`` on deferred or gated frames) re-resolves with the values of that run.
    ``handle`` reads one output handle through ``get_output``; ``None`` reads the default output.

    In a sync a node whose read fails below a node seeded without columns (a new source the sync
    holds, an edited ``polars_code``) is seeded without columns too, so a cell building on such a
    source still syncs; the run then computes the real schema. The failure is recorded on the
    mode's ``unchecked``, so the sync can say which nodes it placed without checking them.
    """
    _reseed_lost_placeholders(_nodes_a_read_executes(node))
    try:
        return _read(node, handle)
    except NativeNodeError:
        raise
    except Exception as exc:
        if not _below_column_less(node):
            raise
        mode, text = current(), str(exc).strip()
        error = f"{type(exc).__name__}: {text.splitlines()[0]}" if text else type(exc).__name__
        mode.unchecked[node.node_id] = (mode.cell_id, node.node_type, error)
    node.results.errors = None
    seed_deferred_node(node, {h: [] for h in _handles(node)})
    current().column_less.add(node.node_id)
    return _read(node, handle)


def _read(node: FlowNode, handle: str | None) -> FlowDataEngine:
    with contextlib.ExitStack() as stack:
        for upstream in _nodes_a_read_executes(node):
            _resolve_parameters_for_read(stack, upstream)
        try:
            return node.get_output(handle) if handle is not None else node.get_resulting_data()
        except DeferredNodeError as exc:
            raise lost_placeholder_error(node) from exc


def _below_column_less(node: FlowNode) -> bool:
    """Whether a sync is active and ``node`` reads, directly or not, from a node it seeded without columns."""
    mode = current()
    if mode is None or not mode.sync or not mode.column_less:
        return False
    return any(node_id in mode.column_less for node_id in ancestors(node) if node_id != node.node_id)


def columns_unknown(node: FlowNode | None) -> bool:
    """Whether a sync seeded ``node`` or a node it reads from without columns, so its columns say nothing yet.

    A build-time column check on such a frame would refuse a cell the run can still satisfy.
    """
    mode = current()
    if node is None or mode is None or not mode.sync:
        return False
    return node.node_id in mode.column_less or _below_column_less(node)


def lost_placeholder_error(node: FlowNode) -> NativeNodeError:
    """The error for a deferred node (``node`` or an ancestor) read without its placeholder.

    A seeded node whose settings change after it was built (``set_group``, ``cache()``) is
    reset when the next edge is wired, which drops its placeholder output; a read outside
    :func:`materialise` does not seed it again.
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


def set_node_reference(flow_graph: FlowGraph, node_id: int, value: str | None) -> None:
    """Set node ``node_id``'s ``node_reference`` as the designer's reference edit does.

    ``None`` or ``""`` clears it back to the default ``df_<node id>``. A value is checked by the
    settings model's own rule and must not name another node of ``flow_graph`` (uniqueness is
    per graph; a later merge of two graphs does not re-check it). Only the node's own hash is
    cleared: ``FlowNode.reset()`` would drop a deferred node's zero-row placeholder.
    """
    node = flow_graph.get_node(node_id)
    if value is None or value == "":
        value = None
    else:
        try:
            input_schema.NodeBase.validate_node_reference(value)
        except ValueError as exc:
            raise NativeNodeError(f"Invalid node_reference {value!r}: {exc}") from exc
        for other in flow_graph.nodes:
            if other.node_id != node_id and getattr(other.setting_input, "node_reference", None) == value:
                raise NativeNodeError(f"node_reference {value!r} is already used by node {other.node_id}")
    node.setting_input.node_reference = value
    node._hash = None


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
    mode = current()
    if mode is not None and len(unique_graphs) > 1 and any(graph is not mode.graph for graph in unique_graphs):
        raise NativeNodeError(
            "In a notebook every frame lives on the session graph; this one comes from another graph. "
            "Build on the session graph: drop the explicit flow_graph= (and fl.create_flow_graph())"
        )
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
    the steps through :meth:`_add`, :meth:`_seed_schemas`, :meth:`_declared_seed` (what a sync
    seeds from) and ``_build(handles=...)``.
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

    @property
    def node_reference(self) -> str | None:
        """The node's reference: its variable name in exported code and its input name in a kernel script.

        ``None`` means the default ``df_<node id>``. Setting it checks the designer's rule
        (lowercase letter first, then lowercase letters, digits and underscores) and that no
        other node in the graph uses it; ``None`` or ``""`` clears it.
        """
        return getattr(self.node.setting_input, "node_reference", None)

    @node_reference.setter
    def node_reference(self, value: str | None) -> None:
        set_node_reference(self.flow_graph, self.node_id, value)

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
        self.flow_graph = self._resolve_graph(frames, flow_graph)
        self.node_id = allocate_node_id(self.flow_graph)
        settings = make_settings(self._base_fields(settings_cls, frames, description))
        self.deferred = self._decide_deferred(node_type, frames, deferred, settings)
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
    def _decide_deferred(
        node_type: str, frames: Sequence[FlowFrame], deferred: bool | None, setting_input: Any = None
    ) -> bool:
        """:func:`seeded_at_build` unless ``deferred`` is given; no node is forced to run on placeholder rows.

        In notebook mode a node that :func:`notebook_defers` names is always deferred, whatever
        ``deferred`` says; ``setting_input`` lets it judge the settings too (a first-row
        ``dynamic_rename`` in a sync). ``deferred=True`` defers without judging them (a canvas
        placeholder may carry only a promise).
        """
        if deferred or notebook_defers(node_type, setting_input):
            return True
        if deferred is None:
            return seeded_at_build(node_type, frames, setting_input=setting_input)
        if not deferred and any(f._deferred for f in frames):
            raise NativeNodeError(
                f"deferred=False builds {node_type} by running it, and its input only holds placeholder rows "
                "until the flow runs; leave deferred unset or collect the input first"
            )
        return deferred

    @staticmethod
    def _resolve_graph(frames: Sequence[FlowFrame], flow_graph: FlowGraph | None) -> FlowGraph:
        """The graph to place on: the input frames' (merged when they differ), else ``flow_graph``.

        Without either, the implicit graph (``utils._implicit_graph``).
        """
        if not frames:
            return flow_graph if flow_graph is not None else _implicit_graph()
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
            "user_id": current_user_id(),
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
        """One frame per output handle; a deferred node is seeded first, so nothing executes it.

        In a sync the seed is :func:`sync_seed_schemas` over :meth:`_declared_seed`, never a prediction.
        """
        from flowfile_frame.flow_frame import FlowFrame

        self.output_names = output_names_of(node.setting_input)
        handles = [output_handle(i) for i in range(len(self.output_names))]
        if self.deferred and _in_sync():
            seed_deferred_node(node, sync_seed_schemas(node, handles, self._declared_seed(node, frames, handles)))
        elif self.deferred:
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

    def _declared_seed(
        self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]
    ) -> dict[str, list[FlowfileColumn]] | None:
        """What the call itself declares about the outputs, per handle, for a sync; ``None`` defers to the settings."""
        return None

    def _seed_schemas(
        self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]
    ) -> dict[str, list[FlowfileColumn]]:
        """Schema per seeded output handle; by default every handle carries :meth:`_seed_schema`."""
        schema = self._seed_schema(node, frames)
        return {handle: list(schema) for handle in handles}

    def _seed_schema(self, node: FlowNode, frames: Sequence[FlowFrame]) -> list[FlowfileColumn]:
        """Schema of every seeded output handle.

        A source node takes the columns its settings declare; any other predicts with
        :func:`_placeholder_schema`, never running a deferred or side-effect node type.
        """
        if not frames:
            return _declared_fields(node.setting_input)
        return _placeholder_schema(node)


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
        raise NativeNodeError(f"Unknown node type {node_type!r}; fl.NodeType lists the built-in types")
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
    external-source nodes, and for writers below a deferred frame or a gate; ``deferred``
    overrides that. In a canvas notebook session the node types :func:`notebook_defers` names
    are always deferred, whatever ``deferred`` says. The dedicated classes (``fl.Gate`` and the
    like) are the normal route for the nodes they cover; custom nodes go through ``fl.CustomNode``.
    """

    def __init__(
        self,
        node_type: NodeTypeLiteral | NodeType,
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
