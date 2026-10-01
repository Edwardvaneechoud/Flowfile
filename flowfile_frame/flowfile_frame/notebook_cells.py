"""Canvas notebook cells: seed a session from the canvas, execute cells, display, and the clean run.

Everything here runs inside notebook build mode (:mod:`flowfile_frame.notebook`). A session is
seeded from the live flow (:func:`seed_session`): the flow is rebuilt on a local session graph,
each node is bound to one variable, and the mode keeps a snapshot that :func:`canvas_node`
adopts. A sync needs only the snapshot (:func:`enter_snapshot_session` parses it without
rebuilding anything). Cells run through :func:`execute_cell`, which hands the cell text to an executor (a
callable every caller names: :func:`exec_cell` runs it as Python, for kernels and tests; core
passes an interpreter that never executes it), records which nodes each cell created
(provenance), turns the variable names they bind into ``node_reference`` and shows the last
expression (a frame by its schema only). :func:`clean_run` runs every cell on a fresh graph,
as an explicit user, and returns the save-format payload relabelled onto the canvas ids. Per-run
state lives on the mode or in context variables, never in module globals, and goes when the
mode ends.
"""

from __future__ import annotations

import ast
import builtins
import itertools
import keyword
import linecache
import re
import traceback
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, Literal, TypeAlias

import polars as pl
from pydantic import BaseModel

from flowfile_core.flowfile.code_generator.code_generator import NODE_TYPE_VAR_LABEL, node_label
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph, placement_check
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE, output_handle
from flowfile_core.flowfile.manage.io_flowfile import (
    _flowfile_data_to_flow_information,
    populate_graph_from_flow_information,
)
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.notebook.relabel import provenance_mapping, relabel
from flowfile_core.schemas import input_schema
from flowfile_core.schemas import schemas as core_schemas
from flowfile_frame import notebook
from flowfile_frame._identity import current_user_id
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.native import (
    NativeNode,
    NativeNodeError,
    _placeholder_schema,
    ancestors,
    is_side_effect_node_type,
    materialise,
    notebook_defers,
    output_names_of,
    seed_deferred_node,
    set_node_reference,
)
from flowfile_frame.run_flow import FlowOutput
from shared.notebook_display import DISPLAY_MAX_ROWS, TABLE_MIME, build_table_payload

LIVE_SOURCE_TYPES: frozenset[str] = frozenset(
    {"manual_input", "read", "list_files", "catalog_reader", "cloud_storage_reader"}
)
SEEDED_NODE_TYPES: frozenset[str] = frozenset({"gate", "run_flow", "python_script"})
KEPT_NODE_TYPES: frozenset[str] = frozenset({"gate", "run_flow", "python_script", "flow_input", "flow_output"})

_REFERENCE = re.compile(r"^[a-z][a-z0-9_]*$")
_NOT_CAPTURED: frozenset[str] = frozenset(keyword.kwlist) | frozenset(dir(builtins)) | {"ff", "pl", "main"}
_CELL_RUNS = itertools.count(1)
_CELL_OUTPUTS: ContextVar[list[dict[str, Any]] | None] = ContextVar("notebook_cell_outputs", default=None)

FailureKind: TypeAlias = Literal["needs_kernel", "refused", "error"]
CellExecutor: TypeAlias = Callable[[str, str, dict[str, Any]], Any]
"""``executor(filename, code, namespace)`` runs one cell and returns its last expression's value (or ``None``)."""


class CellFailure(Exception):
    """A cell that failed on a line the executor names, with its message and kind.

    ``kind`` is ``"needs_kernel"`` (outside what the executor runs), ``"refused"`` or ``"error"``;
    ``None`` leaves it to :func:`execute_cell`, which classifies the chained ``__cause__`` the way
    it classifies an exception :func:`exec_cell` lets through. ``line`` is 1-based within the cell.
    """

    def __init__(self, message: str, *, line: int | None, kind: FailureKind | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.line = line
        self.kind = kind


@dataclass
class _SnapshotNode:
    """A canvas node as data: what :func:`canvas_node` adopts and a sync seeds its unchanged twin from.

    ``inputs`` lists ``(source canvas id, source output handle)`` per incoming edge.
    """

    node_type: str
    setting_input: BaseModel | None
    schemas: dict[str, list[FlowfileColumn]]
    input_handles: list[str] | None
    inputs: list[tuple[int, str]] = field(default_factory=list)


@dataclass
class CellResult:
    """What one cell execution did; ``error`` holds the text to show when it failed.

    On a failure ``error`` is the traceback from the cell down when the cell ran as Python, else
    the message; ``message`` is the exception text alone (``traceback.format_exception_only``),
    ``line`` the failing line (1-based within the cell, as Python reports it), ``kind``
    ``"needs_kernel"``, ``"refused"`` (a notebook-mode refusal) or ``"error"``, and
    ``traceback`` the full traceback of the underlying exception.
    ``created`` lists ``(node_type, node_id)`` for every node the cell created, ``names`` the
    variables it bound or rebound, ``references`` the ``node_reference`` each name capture set,
    ``display`` the payload of the last expression (schema only for a frame) and ``outputs`` the
    payloads of explicit ``display()`` calls, in order.
    """

    cell_id: str
    filename: str
    error: str | None = None
    message: str | None = None
    line: int | None = None
    kind: FailureKind | None = None
    traceback: str | None = None
    display: dict[str, Any] | None = None
    outputs: list[dict[str, Any]] = field(default_factory=list)
    created: list[tuple[str, int]] = field(default_factory=list)
    names: list[str] = field(default_factory=list)
    references: dict[int, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """Whether the cell ran without an error."""
        return self.error is None


class SeededNode(NativeNode):
    """A node with several outputs (or a native node) bound as one variable in a notebook session.

    A :class:`NativeNode` handle over an already placed node (built without ``_build``), so
    rendered cells run unchanged. On top of the native handle it adds ``.then`` / ``.otherwise``
    (output-0 / output-1), a gate's ``.output`` (its ``then``) and ``node["output-<n>"]``; a node
    with one output also forwards frame methods to that output.
    """

    node_id: int
    node_type: str
    flow_graph: FlowGraph
    output_names: list[str]

    def __init__(
        self, flow_graph: FlowGraph, node_id: int, node_type: str, output_names: list[str], frames: dict[str, FlowFrame]
    ) -> None:
        self.flow_graph = flow_graph
        self.node_id = node_id
        self.node_type = node_type
        self.output_names = list(output_names)
        self._frames = frames

    @property
    def then(self) -> FlowFrame:
        """The first exit (``output-0``): a gate's live-when-true side, a split filter's ``pass``."""
        return self._frames[output_handle(0)]

    @property
    def otherwise(self) -> FlowFrame:
        """The second exit (``output-1``): a gate's else side, a split filter's ``fail``."""
        if output_handle(1) not in self._frames:
            raise NativeNodeError(f"{self.node_type} node {self.node_id} has one output; use .output")
        return self._frames[output_handle(1)]

    @property
    def else_(self) -> FlowFrame:
        """Alias of :attr:`otherwise`."""
        return self.otherwise

    @property
    def output(self) -> FlowFrame:
        """The only output frame (a gate's ``then``); a node with several outputs is indexed by name."""
        if len(self._frames) != 1 and self.node_type != "gate":
            raise NativeNodeError(
                f"{self.node_type} node {self.node_id} has outputs {self.output_names}; pick one with node[name]"
            )
        return self._frames[DEFAULT_OUTPUT_HANDLE]

    def __getitem__(self, name: str | FlowOutput) -> FlowFrame:
        if isinstance(name, str) and name in self._frames:
            return self._frames[name]
        return super().__getitem__(name)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_") or len(self.__dict__.get("_frames", {})) != 1:
            raise AttributeError(name)
        return getattr(self._frames[DEFAULT_OUTPUT_HANDLE], name)

    def __repr__(self) -> str:
        return f"SeededNode({self.node_type} {self.node_id}, outputs={self.output_names})"


def _binds_seeded_node(node_type: str, setting_input: Any) -> bool:
    return (
        node_type in SEEDED_NODE_TYPES
        or isinstance(setting_input, input_schema.UserDefinedNode)
        or len(output_names_of(setting_input)) > 1
    )


def _type_label(node_type: str) -> str:
    return NODE_TYPE_VAR_LABEL.get(node_type, re.sub(r"\W", "_", node_type))


def _columns(entries: Sequence[Any]) -> list[FlowfileColumn]:
    """Schema entries (``{"name", "data_type"}`` dicts, ``(name, dtype)`` pairs or columns) as columns."""
    columns = []
    for entry in entries or []:
        if isinstance(entry, FlowfileColumn):
            columns.append(entry)
        elif isinstance(entry, Mapping):
            columns.append(FlowfileColumn.from_input(entry.get("name") or entry["column_name"], entry["data_type"]))
        else:
            name, data_type = entry
            columns.append(FlowfileColumn.from_input(name, str(data_type)))
    return columns


def _handle_schemas(given: Mapping[str, Any] | None, handles: list[str], node: FlowNode) -> dict[str, list]:
    """Per-handle seed schemas: the given ones, else output-0's, else predicted without running.

    A node whose prediction needs data (a pivot collects its pivot values, through the worker's
    cache when one is up) is never predicted: without a given schema it seeds no columns, so
    seeding a session never writes under storage.
    """
    given = {handle: _columns(entries) for handle, entries in (given or {}).items()}
    if DEFAULT_OUTPUT_HANDLE not in given and getattr(node, "_prediction_requires_data", False):
        given[DEFAULT_OUTPUT_HANDLE] = []
    if DEFAULT_OUTPUT_HANDLE not in given:
        try:
            given[DEFAULT_OUTPUT_HANDLE] = _placeholder_schema(node)
        except Exception:
            given[DEFAULT_OUTPUT_HANDLE] = []
    return {handle: list(given.get(handle, given[DEFAULT_OUTPUT_HANDLE])) for handle in handles}


def _topological(graph: FlowGraph) -> list[FlowNode]:
    order: list[FlowNode] = []
    seen: set[int] = set()

    def visit(node: FlowNode) -> None:
        if node.node_id in seen:
            return
        seen.add(node.node_id)
        for upstream in node.all_inputs:
            visit(upstream)
        order.append(node)

    for node in graph.nodes:
        visit(node)
    return order


def _seeds_live(node: FlowNode, live: set[int]) -> bool:
    """Whether ``node`` seeds live: a ``LIVE_SOURCE_TYPES`` source, or a pure transform whose inputs are all live."""
    settings = node.setting_input
    if settings is None or isinstance(settings, input_schema.NodePromise | input_schema.UserDefinedNode):
        return False
    if node.node_type == "gate" or notebook_defers(node.node_type, settings):
        return False
    if node.node_type in LIVE_SOURCE_TYPES:
        return True
    return bool(node.all_inputs) and all(upstream.node_id in live for upstream in node.all_inputs)


def _deferred_source(info: core_schemas.NodeInformation) -> bool:
    """A start node the session seeds from its schema: held deferred while rebuilding, so no prefetch runs it."""
    if info.input_ids or info.left_input_id or info.right_input_id or info.input_connections:
        return False
    return info.type not in LIVE_SOURCE_TYPES or notebook_defers(info.type, info.setting_input)


def _snapshot_input_handles(node_info: core_schemas.NodeInformation) -> list[str] | None:
    connections = getattr(node_info, "input_connections", None)
    return [c.input_handle for c in connections] if connections else None


def _snapshot_inputs(flow_info: core_schemas.FlowInformation) -> dict[int, list[tuple[int, str]]]:
    """Per canvas node, ``(source id, source output handle)`` for every incoming edge, from the sources' outputs."""
    inputs: dict[int, list[tuple[int, str]]] = {}
    for source_id, info in flow_info.data.items():
        handles = list(info.output_handles or [])
        for index, target_id in enumerate(info.outputs or []):
            handle = handles[index] if index < len(handles) else DEFAULT_OUTPUT_HANDLE
            inputs.setdefault(target_id, []).append((source_id, handle))
    return inputs


def enter_snapshot_session(snapshot: Mapping[str, Any], *, user_id: int) -> notebook.NotebookMode:
    """Enter a sync on a canvas snapshot, as ``user_id``: the canvas as data, never rebuilt.

    ``snapshot`` is the push's seed payload (``flowfile_data`` and per-node ``schemas``, as
    ``flowfile_core.notebook.push.seed_snapshot`` returns them). The payload is validated into
    settings models and nothing else: no graph is populated, no node placed, no custom node
    class loaded, nothing read. The mode (``sync=True``, on a fresh graph of its own) keeps each
    node as the snapshot :func:`canvas_node` adopts and :func:`clean_run` seeds unchanged nodes
    from. Any active mode is ended first. ``notebook.exit()`` ends the session.
    """
    if not isinstance(user_id, int) or isinstance(user_id, bool):
        raise ValueError("enter_snapshot_session needs the user it runs as: pass user_id=<int>")
    flow_info = _flowfile_data_to_flow_information(_flowfile_data_model(snapshot["flowfile_data"]))
    given = {int(node_id): entries for node_id, entries in (snapshot.get("schemas") or {}).items()}
    inputs = _snapshot_inputs(flow_info)
    nodes = {
        node_id: _SnapshotNode(
            node_type=info.type,
            setting_input=info.setting_input,
            schemas={handle: _columns(entries) for handle, entries in (given.get(node_id) or {}).items()}
            or {DEFAULT_OUTPUT_HANDLE: []},
            input_handles=_snapshot_input_handles(info),
            inputs=inputs.get(node_id, []),
        )
        for node_id, info in flow_info.data.items()
    }
    if notebook.current() is not None:
        notebook.exit()
    mode = notebook.enter(user_id=user_id, sync=True)
    mode.snapshot.update(nodes)
    return mode


def seed_session(
    flowfile_data: dict[str, Any],
    parameters: list[Any],
    names: Mapping[int, str],
    schemas: Mapping[int, Mapping[str, list[Any]]],
    *,
    user_id: int | None = None,
) -> dict[str, Any]:
    """Rebuild the canvas flow as the session graph, enter notebook mode on it, and bind one variable per node.

    The graph is local, history-off and has its own flow id; ``parameters`` (``FlowParameter``
    models or dicts) are declared on it. A node that seeds live (``manual_input``,
    ``read``, ``list_files``, non-virtual non-SQL ``catalog_reader``, ``cloud_storage_reader`` and
    pure transforms whose inputs are all live) holds its lazy plan; every other node is seeded
    from ``schemas[node_id][handle]`` (``{"name", "data_type"}`` entries) and its frames are
    deferred. A variable is ``names[node_id]``, else ``<type_label>_<id>``; a multi-output or
    native node binds a :class:`SeededNode`, any other node a ``FlowFrame``. ``flow`` is bound to
    the session graph. Once seeded, the graph accepts a subflow port name already in use, since a
    cell re-places the seeded ``flow_input``/``flow_output``. The new mode holds the snapshot
    :func:`canvas_node` adopts. Runs as ``user_id``, else as the active mode's user; with neither
    it raises ``ValueError`` instead of running as anyone. Any active mode is ended first.
    ``notebook.exit()`` ends the session and releases the graph's flow logger with the snapshot.
    """
    flow_info = _flowfile_data_to_flow_information(_flowfile_data_model(flowfile_data))
    previous = notebook.current()
    if user_id is None and previous is not None:
        user_id = previous.user_id
    if user_id is None:
        raise ValueError("seed_session needs the user it runs as: pass user_id=...")
    if previous is not None:
        notebook.exit()
    mode = notebook.enter(user_id=user_id)
    graph = mode.graph
    try:
        graph.flow_settings.parameters = [
            p if isinstance(p, FlowParameter) else FlowParameter.model_validate(p) for p in parameters
        ]
        owner = current_user_id()

        def hold(node_id: int | str, node_type: str, settings: Any, is_new: bool) -> None:
            info, node = flow_info.data.get(node_id), graph.get_node(node_id)
            if node is not None and info is not None and _deferred_source(info):
                node.deferred_until_run = True

        # The canvas opens a flow without checking connections; only cell placements are checked.
        token = placement_check.set(None)
        try:
            with graph.observe_nodes(hold), graph.rebuilding():
                populate_graph_from_flow_information(graph, flow_info, owner_of=lambda _node_id: owner)
        finally:
            placement_check.reset(token)
        graph.unique_subflow_port_names = False
        names = {int(k): v for k, v in names.items()}
        given = {int(k): v for k, v in schemas.items()}
        bound: dict[str, Any] = {"flow": graph}
        live: set[int] = set()
        inputs = _snapshot_inputs(flow_info)
        for node in _topological(graph):
            bound[names.get(node.node_id) or node_label(node.node_type, node.node_id)] = _seed_node(
                graph, node, live, given.get(node.node_id)
            )
            info = flow_info.data.get(node.node_id)
            mode.snapshot[node.node_id] = _SnapshotNode(
                node_type=node.node_type,
                setting_input=info.setting_input if info is not None else None,
                schemas=_snapshot_schemas(node, given.get(node.node_id)),
                input_handles=_snapshot_input_handles(info) if info is not None else None,
                inputs=inputs.get(node.node_id, []),
            )
        mode.provenance.clear()
        return bound
    except Exception:
        notebook.exit()
        raise


def _snapshot_schemas(node: FlowNode, given: Mapping[str, Any] | None) -> dict[str, list[FlowfileColumn]]:
    """The schemas :func:`canvas_node` seeds from: the given ones, else what seeding found."""
    if given:
        return {handle: _columns(entries) for handle, entries in given.items()}
    return dict(node._named_schemas) or {DEFAULT_OUTPUT_HANDLE: list(node.schema or [])}


def _flowfile_data_model(flowfile_data: dict[str, Any] | core_schemas.FlowfileData) -> core_schemas.FlowfileData:
    if isinstance(flowfile_data, core_schemas.FlowfileData):
        return flowfile_data
    return core_schemas.FlowfileData.model_validate(flowfile_data)


def _seed_node(graph: FlowGraph, node: FlowNode, live: set[int], given: Mapping[str, Any] | None) -> Any:
    """Seed one rebuilt node as live or deferred and return its binding (a frame or a ``SeededNode``)."""
    output_names = output_names_of(node.setting_input)
    handles = [output_handle(i) for i in range(len(output_names))]
    is_live = _seeds_live(node, live)
    if is_live:
        node.deferred_until_run = False
        try:
            frames = {h: _frame(graph, node, h, deferred=False) for h in handles}
            live.add(node.node_id)
        except Exception:
            is_live = False
            node.deferred_until_run = True
    if not is_live:
        seed_deferred_node(node, _handle_schemas(given, handles, node))
        frames = {h: _frame(graph, node, h, deferred=True) for h in handles}
    if _binds_seeded_node(node.node_type, node.setting_input):
        return SeededNode(graph, node.node_id, node.node_type, output_names, frames)
    return frames[DEFAULT_OUTPUT_HANDLE]


def _frame(graph: FlowGraph, node: FlowNode, handle: str, *, deferred: bool) -> FlowFrame:
    data = materialise(node, handle if handle != DEFAULT_OUTPUT_HANDLE else None).data_frame
    return FlowFrame(data=data, flow_graph=graph, node_id=node.node_id, output_handle=handle, deferred=deferred)


class _CanvasNode(SeededNode):
    """A canvas node adopted from the seeded snapshot: its settings, wired to the frames given, deferred."""

    def __init__(self, snapshot: _SnapshotNode, frames: Sequence[FlowFrame]) -> None:
        self._snapshot = snapshot
        settings = snapshot.setting_input
        node_type = snapshot.node_type

        def make_settings(base: dict[str, Any]) -> BaseModel:
            if settings is None:
                return input_schema.NodePromise(
                    flow_id=base["flow_id"], node_id=base["node_id"], node_type=node_type, pos_x=0.0, pos_y=0.0
                )
            return settings.model_copy(deep=True, update=base)

        handles = snapshot.input_handles
        if not handles or len(handles) != len(frames):
            handles = None
        settings_cls = type(settings) if settings is not None else input_schema.NodePromise
        self._build(node_type, settings_cls, frames, make_settings, deferred=True, description=None, handles=handles)

    def _place(self, settings: BaseModel, frames: Sequence[FlowFrame], handles: list[str], template: Any) -> FlowNode:
        if not isinstance(settings, input_schema.NodePromise):
            return super()._place(settings, frames, handles, template)
        self.flow_graph.add_node_promise(settings)
        self.flow_graph.get_node(self.node_id).deferred_until_run = True
        for frame, handle in zip(frames, handles, strict=True):
            self._connect(frame, handle)
        return self.flow_graph.get_node(self.node_id)

    def _add(self, settings: BaseModel) -> Any:
        if isinstance(settings, input_schema.UserDefinedNode):
            return self.flow_graph._place_user_defined_node(self.node_type, settings)
        return super()._add(settings)

    def _seed_schemas(self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]) -> dict[str, list]:
        known = self._snapshot.schemas
        default = known.get(DEFAULT_OUTPUT_HANDLE) or []
        return {handle: list(known.get(handle) or default) for handle in handles}

    def _declared_seed(self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]) -> dict[str, list] | None:
        """The snapshot's schemas; ``None`` when they hold no columns, so the sync's own fallbacks apply."""
        return self._seed_schemas(node, frames, handles) if any(self._snapshot.schemas.values()) else None


def canvas_node(node_id: int, *inputs: FlowFrame, output: str | FlowOutput | None = None) -> FlowFrame | SeededNode:
    """Adopt canvas node ``node_id`` from the seeded session: a placeholder for a node not editable as code.

    Only valid in a notebook session seeded from the canvas (:func:`seed_session`). The new node
    takes the snapshot's settings, is wired to ``inputs`` (in handle order) and every output is
    seeded from the snapshot's schema, so its frames are deferred. ``output`` (an output name or
    ``output-<n>``) returns that handle's frame; without it a single-output node returns its frame
    and a multi-output node a :class:`SeededNode`.
    """
    mode = notebook.current()
    if mode is None or not mode.snapshot:
        raise NativeNodeError(
            "ff.canvas_node only works in a notebook session seeded from the canvas; "
            "build the node with its ff.* call instead"
        )
    snapshot = mode.snapshot.get(node_id)
    if snapshot is None:
        raise NativeNodeError(
            f"Canvas node {node_id} is not in this session's snapshot; reset the session to reseed it "
            "from the canvas, or build the node with its ff.* call"
        )
    node = _CanvasNode(snapshot, inputs)
    if output is not None:
        name = output.name if isinstance(output, FlowOutput) else output
        if name in node._frames:
            return node._frames[name]
        return node[name]
    if len(node._frames) == 1:
        return node._frames[DEFAULT_OUTPUT_HANDLE]
    return node


def _schema_entries(frame: FlowFrame) -> list[dict[str, str]]:
    data = frame.data
    schema = data.collect_schema() if isinstance(data, pl.LazyFrame) else data.schema
    return [{"name": name, "data_type": str(dtype)} for name, dtype in schema.items()]


def _lazy_safe(frame: FlowFrame) -> bool:
    return not frame._deferred and not frame._below_a_gate()


def _frame_payload(frame: FlowFrame, max_rows: int | None) -> dict[str, Any]:
    """The frame payload; ``max_rows=None`` keeps it to the schema and never executes the plan."""
    payload: dict[str, Any] = {
        "kind": "frame",
        "node_id": frame.node_id,
        "output_handle": frame.output_handle,
        "schema": _schema_entries(frame),
        "lazy_safe": _lazy_safe(frame),
    }
    if max_rows is not None and payload["lazy_safe"]:
        data = frame.data.lazy() if isinstance(frame.data, pl.DataFrame) else frame.data
        head = data.head(max_rows + 1).collect()
        total = data.select(pl.len()).collect().item() if head.height > max_rows else head.height
        payload[TABLE_MIME] = build_table_payload(head, max_rows, total)
    return payload


def display_payload(value: Any, max_rows: int = DISPLAY_MAX_ROWS) -> dict[str, Any]:
    """The display payload of ``value``, as an explicit :func:`display` call shows it.

    A ``FlowFrame`` always shows its schema and, only when its lineage is lazy-safe (not deferred,
    not below a gate), its first ``max_rows`` rows as the ``application/vnd.flowfile.table+json``
    payload. A native node or :class:`SeededNode` shows each output that way; anything else its repr.
    A cell's last expression is shown without rows (:func:`execute_cell`).
    """
    return _payload(value, max_rows)


def _payload(value: Any, max_rows: int | None) -> dict[str, Any]:
    if isinstance(value, FlowFrame):
        return _frame_payload(value, max_rows)
    if isinstance(value, NativeNode):
        return {
            "kind": "node",
            "node_id": value.node_id,
            "node_type": value.node_type,
            "outputs": {
                name: _frame_payload(value._frames[output_handle(i)], max_rows)
                for i, name in enumerate(value.output_names)
            },
        }
    return {"kind": "text", "text/plain": repr(value)}


def display(value: Any) -> dict[str, Any] | None:
    """Show ``value`` below the running cell (up to ``DISPLAY_MAX_ROWS`` rows); outside a cell, return its payload."""
    payload = display_payload(value, DISPLAY_MAX_ROWS)
    outputs = _CELL_OUTPUTS.get()
    if outputs is not None:
        outputs.append(payload)
        return None
    return payload


def new_namespace() -> dict[str, Any]:
    """A fresh cell namespace: ``ff``, ``pl``, ``display`` and ``flow`` (the session graph, when a mode is active).

    ``ff`` is a new module holding the names of ``flowfile.__all__`` except ``open_graph_in_editor``
    and ``start_web_ui``, the same objects, taken from ``flowfile_frame`` so that ``flowfile``
    itself (whose import writes the process environment) is never imported.
    """
    from flowfile_frame import _fl_namespace

    ff = ModuleType("flowfile", "The flowfile API of a notebook cell (``import flowfile as ff``).")
    ff.__dict__.update({name: getattr(_fl_namespace, name) for name in _fl_namespace.__all__})
    mode = notebook.current()
    return {
        "__name__": "__main__",
        "__builtins__": builtins,
        "ff": ff,
        "pl": pl,
        "display": display,
        "flow": mode.graph if mode is not None else None,
    }


def _cell_filename(mode: notebook.NotebookMode | None, cell_id: str, code: str) -> str:
    """A unique ``<cell-{id}-{n}>`` name, registered in ``linecache`` so ``inspect`` and tracebacks read the cell.

    The entry stays until ``mode`` ends, since a later cell may read a function an earlier one
    defined; without a mode nothing runs and nothing is registered.
    """
    filename = f"<cell-{cell_id}-{next(_CELL_RUNS)}>"
    if mode is not None:
        linecache.cache[filename] = (len(code), None, code.splitlines(True), filename)
        mode.cell_files.append(filename)
    return filename


def _compile(filename: str, code: str) -> tuple[Any, Any]:
    """The cell body and its last expression (``None`` when the cell does not end in one), at ``optimize=0``."""
    tree = ast.parse(code, filename, "exec")
    last = None
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        last = ast.Expression(tree.body.pop().value)
    body = compile(tree, filename, "exec", dont_inherit=True, optimize=0)
    expression = compile(last, filename, "eval", dont_inherit=True, optimize=0) if last is not None else None
    return body, expression


def _bound_node_id(value: Any, graph: FlowGraph, *, any_handle: bool = False) -> int | None:
    """The node a bound value stands for on ``graph``: a frame (output-0 unless ``any_handle``) or a node handle."""
    if isinstance(value, FlowFrame):
        if value.flow_graph is graph and (any_handle or value.output_handle == DEFAULT_OUTPUT_HANDLE):
            return value.node_id
    elif isinstance(value, NativeNode) and value.flow_graph is graph:
        return value.node_id
    return None


def _capturable(name: str, node_type: str) -> bool:
    """The name-capture rules of a reference: lowercase identifier, not reserved, not a generated label.

    A generated label is the node's own (``filtered_2``) or one derived from it for an output
    (``filtered_2_pass``, ``random_split_2_train``).
    """
    if not _REFERENCE.match(name) or name in _NOT_CAPTURED:
        return False
    return re.fullmatch(rf"{re.escape(_type_label(node_type))}_\d+(_[a-z0-9_]+)?", name) is None


def _clear_reference(graph: FlowGraph, name: str, keep: int | None = None) -> None:
    for node in graph.nodes:
        if node.node_id != keep and getattr(node.setting_input, "node_reference", None) == name:
            set_node_reference(graph, node.node_id, None)


def _capture_names(
    graph: FlowGraph, namespace: dict[str, Any], before: dict[str, Any], created: set[int], bound: list[str]
) -> dict[int, str]:
    """Turn names bound in the cell into ``node_reference`` on the nodes it created; last binding wins."""
    references: dict[int, str] = {}
    for name in bound:
        old_id = _bound_node_id(before.get(name), graph)
        if old_id is not None and graph.get_node(old_id) is not None:
            if getattr(graph.get_node(old_id).setting_input, "node_reference", None) == name:
                set_node_reference(graph, old_id, None)
        node_id = _bound_node_id(namespace[name], graph)
        if node_id is None or node_id not in created or not _capturable(name, graph.get_node(node_id).node_type):
            continue
        _clear_reference(graph, name, keep=node_id)
        try:
            set_node_reference(graph, node_id, name)
        except NativeNodeError:
            continue
        references = {k: v for k, v in references.items() if k != node_id and v != name}
        references[node_id] = name
    return references


def exec_cell(filename: str, code: str, namespace: dict[str, Any]) -> Any:
    """Compile ``code`` under ``filename`` and run it in ``namespace``; the value of its last expression, else ``None``.

    The executor that runs a cell as Python (every builtin, ``optimize=0``): for kernels and
    tests. Core never passes it; its notebook runner interprets cells instead.
    """
    body, expression = _compile(filename, code)
    exec(body, namespace)
    return eval(expression, namespace) if expression is not None else None


def _failure_kind(exc: BaseException | None, mode: notebook.NotebookMode) -> FailureKind:
    """``"refused"`` for a notebook-mode refusal (a message :func:`notebook.refuse` recorded), else ``"error"``."""
    return "refused" if isinstance(exc, NativeNodeError) and str(exc) in mode.refusals else "error"


def _cell_line(exc: BaseException, filename: str) -> int | None:
    """The cell line ``exc`` was raised on: a syntax error's own line, else the cell's last traceback entry."""
    if isinstance(exc, SyntaxError) and exc.filename == filename:
        return exc.lineno
    lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == filename]
    return lines[-1] if lines else None


def _exception_text(exc: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(exc), exc))


def execute_cell(cell_id: str, code: str, namespace: dict[str, Any], *, executor: CellExecutor) -> CellResult:
    """Run one cell in ``namespace`` on the session graph through ``executor``; errors come back in the result.

    The cell's filename is ``<cell-{cell_id}-{n}>``, registered in ``linecache`` first (until the
    mode ends, so ``inspect.getsource`` works for functions a cell defines and tracebacks name the
    cell). ``executor`` has no default: :func:`exec_cell` runs the cell as Python, an interpreter
    describes it without executing it. A failure is classified once, for any executor, into
    ``message``, ``line`` and ``kind`` (a :class:`CellFailure` names its own line). A
    last-expression value is shown without computing anything: a frame (or each output of a node)
    by its schema only, anything else by its repr; rows come from an explicit ``display()`` call.
    Every node the cell created is recorded on the mode's ``provenance`` as ``(cell_id, node_type,
    node_id)``, and on success the names it bound become ``node_reference`` per the capture rules.
    """
    mode = notebook.current()
    result = CellResult(cell_id=cell_id, filename=_cell_filename(mode, cell_id, code))
    if mode is None:
        result.error = "execute_cell needs an active notebook session (notebook.enter or seed_session)"
        return result
    graph = mode.graph
    namespace.setdefault("__builtins__", builtins)
    namespace.setdefault("display", display)
    before = dict(namespace)
    new_ids: list[int] = []

    def observe(node_id: int | str, node_type: str, settings: Any, is_new: bool) -> None:
        if is_new and node_id not in new_ids:
            new_ids.append(node_id)

    outputs_token = _CELL_OUTPUTS.set(result.outputs)
    mode.cell_id, mode.cell_nodes, mode.claimed = cell_id, new_ids, {}
    try:
        with graph.observe_nodes(observe):
            value = executor(result.filename, code, namespace)
        if value is not None:
            result.display = _payload(value, None)
    except CellFailure as failure:
        cause = failure.__cause__
        result.error = result.message = failure.message
        result.line = failure.line
        result.kind = failure.kind or _failure_kind(cause, mode)
        if cause is not None:
            result.traceback = "".join(traceback.format_exception(type(cause), cause, cause.__traceback__))
    except SyntaxError as exc:
        result.error = result.message = _exception_text(exc)
        result.line, result.kind = _cell_line(exc, result.filename), "error"
    except BaseException as exc:
        tb = exc.__traceback__.tb_next if exc.__traceback__ is not None else None
        result.error = result.traceback = "".join(traceback.format_exception(type(exc), exc, tb))
        result.message = _exception_text(exc)
        result.line, result.kind = _cell_line(exc, result.filename), _failure_kind(exc, mode)
    finally:
        _CELL_OUTPUTS.reset(outputs_token)
        mode.cell_id, mode.cell_nodes, mode.claimed = None, [], {}
    for node_id in new_ids:
        node = graph.get_node(node_id)
        if node is not None:
            result.created.append((node.node_type, node.node_id))
            mode.provenance.append((cell_id, node.node_type, node.node_id))
    result.names = [
        name
        for name, value in namespace.items()
        if not name.startswith("__") and (name not in before or before[name] is not value)
    ]
    if result.ok:
        created = {node_id for _, node_id in result.created}
        result.references = _capture_names(graph, namespace, before, created, result.names)
    return result


def _kept(node: FlowNode) -> bool:
    return (
        node.node_type in KEPT_NODE_TYPES
        or is_side_effect_node_type(node.node_type)
        or isinstance(node.setting_input, input_schema.UserDefinedNode)
    )


def _prune(graph: FlowGraph, namespace: dict[str, Any]) -> set[int]:
    """Delete every node not upstream of a bound variable, a side-effect node or a native node; return the kept ids."""
    roots = {node_id for value in namespace.values() if (node_id := _bound_node_id(value, graph, any_handle=True))}
    roots |= {node.node_id for node in graph.nodes if _kept(node)}
    keep: set[int] = set()
    for node_id in roots:
        keep |= set(ancestors(graph.get_node(node_id)))
    for node in list(graph.nodes):
        if node.node_id not in keep:
            graph.delete_node(node.node_id)
    return keep


def clean_run(
    cells: list[tuple[str, str]],
    ceiling: int,
    provenance: Mapping[str, list[tuple[str, int]]] | None = None,
    *,
    user_id: int | None = None,
    executor: CellExecutor,
) -> dict[str, Any]:
    """Run every cell, in order, through ``executor`` on a fresh parameter-free session graph; return the push payload.

    ``executor`` has no default, so every caller names it (:func:`execute_cell`). Runs as
    ``user_id``, else as the active (seeded) session's user; with neither it raises
    ``ValueError`` instead of running as anyone. Runs in a fresh namespace under its own notebook
    mode, a sync (``notebook.enter(sync=True)``), whose graph, flow logger and ``linecache``
    entries are released when it returns; any active session is set aside and activated again,
    and its snapshot is what :func:`canvas_node` adopts and what nodes the sync holds are seeded
    from when unchanged (``provenance`` names each cell's canvas twins). A failing cell aborts:
    the result is ``{"ok": False, "cell_id", "error", "message",
    "line", "kind", "traceback", "refusals"}`` (see :class:`CellResult`). Otherwise nodes not upstream of a bound
    variable, a side-effect node or a native node are pruned and the result is ``{"ok": True,
    "flowfile_data", "cells", "names", "refusals", "warnings"}``: the save-format payload relabelled onto
    ``provenance`` (``cell_id -> [(node_type, canvas_id)]``, matched by type in creation order)
    with new nodes above ``ceiling``, ``{cell_id: [node ids]}``, ``{node id: node_reference}``, the
    refusal messages raised and ``warnings``: one per kept node whose build failed below a node
    with no known columns, placed unchecked (``NotebookMode.unchecked``).
    """
    previous = notebook.current()
    if user_id is None and previous is not None:
        user_id = previous.user_id
    if user_id is None:
        raise ValueError(
            "clean_run needs the user it runs as: pass user_id=..., or seed the session with "
            "seed_session(..., user_id=...) first"
        )
    if previous is not None:
        notebook._deactivate()
    known = {cell_id: [tuple(entry) for entry in entries] for cell_id, entries in (provenance or {}).items()}
    try:
        with notebook.notebook_mode(user_id=user_id, sync=True) as mode:
            if previous is not None:
                mode.snapshot.update(previous.snapshot)
            mode.expected = known
            namespace = new_namespace()
            for cell_id, code in cells:
                result = execute_cell(cell_id, code, namespace, executor=executor)
                if not result.ok:
                    return {
                        "ok": False,
                        "cell_id": cell_id,
                        "error": result.error,
                        "message": result.message,
                        "line": result.line,
                        "kind": result.kind,
                        "traceback": result.traceback,
                        "refusals": list(mode.refusals),
                    }
            kept = _prune(mode.graph, namespace)
            created = [entry for entry in mode.provenance if entry[2] in kept]
            mapping = provenance_mapping(created, known, ceiling)
            payload = relabel(mode.graph.get_flowfile_data().model_dump(mode="json"), mapping)
            cell_nodes: dict[str, list[int]] = {cell_id: [] for cell_id, _ in cells}
            for cell_id, _, node_id in created:
                cell_nodes[cell_id].append(mapping[node_id])
            names = {
                mapping.get(node.node_id, node.node_id): reference
                for node in mode.graph.nodes
                if (reference := getattr(node.setting_input, "node_reference", None))
            }
            warnings = [
                f"Cell {cell}: node {mapping[node_id]} ({node_type}) was placed unchecked because a node above it "
                f"has no known columns yet ({error}); the run checks it"
                for node_id, (cell, node_type, error) in mode.unchecked.items()
                if node_id in mapping
            ]
            return {
                "ok": True,
                "flowfile_data": payload,
                "cells": cell_nodes,
                "names": names,
                "refusals": list(mode.refusals),
                "warnings": warnings,
            }
    finally:
        if previous is not None:
            notebook._activate(previous)
