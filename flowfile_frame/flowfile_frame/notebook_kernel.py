"""The canvas notebook session that runs inside a notebook kernel (a kernel with ``flowfile`` installed).

Core (``flowfile_core.notebook.kernel_runner``) drives it through the kernel's ``/execute`` with one snippet::

    from flowfile_frame import notebook_kernel as _nb
    _nb.handle('<request json>', globals())

:func:`handle` runs one op (``hello``, ``open``, ``reset``, ``execute``, ``clean_run``, ``schemas``, ``close``) and
prints its JSON result on one ``shared.notebook_display.KERNEL_RESULT_MARKER`` line, which core cuts, so what a
cell prints stays the call's stdout. A flow's session keeps its variables in the kernel's namespace for the call's
flow id (the snippet's ``globals()``), where the kernel's Jedi reads them. Notebook mode is context-local and every
kernel call runs in a fresh context, so an op on the session resumes its mode (``notebook.resumed``), and an op that
builds nodes keeps file paths as written (``notebook.paths_as_written``); core turns them back into host paths on a
push. Rows the kernel cannot compute come from the canvas (:meth:`_Session.canvas_rows`): a canvas node's own, or,
for a node only the cells hold, a run core makes of that node from its settings (:meth:`_Session._run_held`). The
catalog database is a copy core writes under a new name when a call first connects to it (:func:`_rearm`).
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import threading
import traceback
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
from pydantic import BaseModel

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.schemas.schemas import FlowfileNode
from flowfile_frame import notebook
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.native import (
    NativeNode,
    NativeNodeError,
    _handles,
    _kernel_hidden_path,
    _kernel_twin_id,
    _per_handle,
    _twin_settings,
    ancestors,
    materialise,
    seed_deferred_node,
)
from flowfile_frame.notebook_cells import (
    _CELL_OUTPUTS,
    _columns,
    _schema_entries,
    display,
    exec_cell,
    execute_cell,
    new_namespace,
    seed_session,
)
from shared.notebook_display import KERNEL_RESULT_MARKER

CANVAS_CHANGED = "The canvas changed since this session started: Reset session to pick it up."

COMPUTED_HERE_TYPES: frozenset[str] = frozenset({"pivot", "polars_code"})
"""Types notebook mode defers that a session still computes here, when every node above them can be."""


def _rows_settings(settings: BaseModel, node_type: str) -> Any:
    """``settings`` as far as they decide a node's rows: as a push compares them, minus what its hash leaves out.

    A script rendered from the canvas carries the ``returns=`` the export derived from the canvas node's
    seeded schemas, which the canvas node itself does not store.
    """
    compared = _twin_settings(settings, node_type, translate=True)
    excluded = getattr(type(settings), "hash_excluded_fields", None)
    if excluded and isinstance(compared, dict):
        return {key: value for key, value in compared.items() if key not in excluded}
    return compared


def _column_types(schemas: Mapping[str, Any]) -> dict[str, list[tuple[str, str]]]:
    return {handle: [(c.column_name, c.data_type) for c in columns or []] for handle, columns in schemas.items()}


def _bound_frames(namespace: Mapping[str, Any]) -> Iterator[FlowFrame]:
    """Every frame a session variable holds: a frame itself, or a node's outputs."""
    for value in list(namespace.values()):
        if isinstance(value, FlowFrame):
            yield value
        elif isinstance(value, NativeNode):
            yield from value.__dict__.get("_frames", {}).values()


def _post_core(route: str, body: dict[str, Any], what: str) -> dict[str, Any]:
    """POST ``body`` to core's ``route`` as this kernel; a failure is a ``NativeNodeError`` about ``what``."""
    import httpx

    url = os.environ.get("FLOWFILE_CORE_URL", "http://host.docker.internal:63578").rstrip("/")
    headers = {
        name: value
        for name, value in (
            ("X-Internal-Token", os.environ.get("FLOWFILE_INTERNAL_TOKEN")),
            ("X-Kernel-Id", os.environ.get("FLOWFILE_KERNEL_ID")),
        )
        if value
    }
    try:
        response = httpx.post(
            f"{url}{route}",
            json=body,
            headers=headers,
            timeout=httpx.Timeout(None, connect=10.0),
        )
    except httpx.HTTPError as exc:
        raise NativeNodeError(f"Could not reach Flowfile for {what}: {exc}") from exc
    if response.status_code != 200:
        try:
            detail = response.json().get("detail")
        except ValueError:
            detail = None
        raise NativeNodeError(str(detail or response.text or f"HTTP {response.status_code}"))
    return response.json()


def post_node_result(body: dict[str, Any]) -> dict[str, Any]:
    """Ask core for a canvas node's result (``POST /notebook/session/node_result``) as this kernel."""
    return _post_core("/notebook/session/node_result", body, f"node {body['node_id']}'s rows")


def post_node_run(body: dict[str, Any]) -> dict[str, Any]:
    """Ask core to run a node only this session holds (``POST /notebook/session/node_run``) as this kernel."""
    return _post_core("/notebook/session/node_run", body, f"node {body['node']['id']}'s run")


def post_database_refresh() -> dict[str, Any]:
    """Ask core to bring this kernel's copy of the catalog database up to date (``POST /notebook/session/database``)."""
    return _post_core("/notebook/session/database", {}, "a fresh copy of the catalog database")


transport: Callable[[dict[str, Any]], dict[str, Any]] = post_node_result
"""How a session asks core for a canvas node's result; tests route it to core in-process."""

run_transport: Callable[[dict[str, Any]], dict[str, Any]] = post_node_run
"""How a session asks core to run a node only its cells hold; tests route it to core in-process."""

database_transport: Callable[[], dict[str, Any]] = post_database_refresh
"""How a call asks core to refresh the kernel's catalog copy; tests route it to core in-process."""


@dataclass
class _Session:
    """One flow's session: its (set aside) notebook mode, namespace and the identity the editor's schemas use.

    ``seeded`` holds the canvas node ids the session was seeded with, ``rows`` the parquet path core handed out
    per ``(node_id, output_handle)`` (a canvas node's, or a held node's after its run), ``fetch`` and ``run``
    the transports to core, ``results_dir`` the session's results folder as this kernel sees it (where it
    writes the inputs of a held node), ``ran`` the held nodes core ran and ``canvas_settings`` each canvas
    node's :func:`_rows_settings`, worked out once.
    """

    flow_id: int
    mode: notebook.NotebookMode
    namespace: dict[str, Any]
    generation: str
    fetch: Callable[[dict[str, Any]], dict[str, Any]]
    run: Callable[[dict[str, Any]], dict[str, Any]]
    seeded: frozenset[int] = frozenset()
    results_dir: str = ""
    rows: dict[tuple[int, str], str] = field(default_factory=dict)
    ran: set[int] = field(default_factory=set)
    canvas_settings: dict[int, Any] = field(default_factory=dict)
    revision: int = 0

    def stamp(self) -> dict[str, Any]:
        return {"namespace_generation": self.generation, "revision": self.revision}

    def canvas_rows(self, frame: FlowFrame) -> pl.LazyFrame | None:
        """The rows of a frame without rows here: the canvas's for a seeded node no cell replaced, else resolved
        node by node (:meth:`_resolve`)."""
        if frame.flow_graph is not self.mode.graph:
            return None
        created = {entry[2] for entry in self.mode.provenance}
        if frame.node_id in self.seeded and frame.node_id not in created:
            return self._fetch(frame.node_id, frame.output_handle)
        return self._resolve(frame, created)

    def refresh(self, schemas: Mapping[Any, Mapping[str, Any]] | None = None) -> None:
        """Take over the columns the canvas found since the session was seeded.

        ``schemas`` holds the outputs of canvas nodes that ran (core sends them with a call); where they differ
        the snapshot takes them and the rows handed out for that node are asked again. Every deferred node
        standing for a canvas node (:meth:`_twin`) whose seed differs from that node's schemas is then seeded
        with them, a node a cell built since included. The nodes computed below it are read again, and the
        deferred frames bound to any of them take the new placeholder; a frame whose read now fails keeps its
        old one. A read of a file this kernel cannot see holds its canvas rows and is left alone; telling one
        needs paths as written, which a ``schemas`` call does not run under, so they are kept here.
        """
        with notebook.paths_as_written():
            self._refresh(schemas)

    def _refresh(self, schemas: Mapping[Any, Mapping[str, Any]] | None) -> None:
        for canvas_id, by_handle in (schemas or {}).items():
            twin = self.mode.snapshot.get(int(canvas_id))
            found = {handle: _columns(entries) for handle, entries in by_handle.items()}
            if twin is None or _column_types(found) == _column_types(twin.schemas):
                continue
            twin.schemas = found
            for key in [key for key in self.rows if key[0] == int(canvas_id)]:
                del self.rows[key]
        created = {entry[2] for entry in self.mode.provenance}
        twins: dict[int, int | None] = {}
        stale: dict[int, FlowNode] = {}
        for node in self.mode.graph.nodes:
            if not node.deferred_until_run or _kernel_hidden_path(node) is not None:
                continue
            twin = self.mode.snapshot.get(self._twin(node, created, twins))
            if twin is None or not any(twin.schemas.values()):
                continue
            seed = _per_handle(twin.schemas, _handles(node))
            if _column_types(seed) != _column_types(node._named_schemas):
                seed_deferred_node(node, seed)
                stale[node.node_id] = node
        for node in list(stale.values()):
            for below in node.get_all_dependent_nodes():
                if not below.deferred_until_run and below.node_id not in stale:
                    below.results.resulting_data, below.results.errors, below._named_outputs = None, None, {}
                    stale[below.node_id] = below
        for frame in _bound_frames(self.namespace):
            node = stale.get(frame.node_id)
            if node is None or not frame._deferred or frame.flow_graph is not self.mode.graph:
                continue
            handle = None if frame.output_handle == DEFAULT_OUTPUT_HANDLE else frame.output_handle
            try:
                frame.data = materialise(node, handle).data_frame
            except Exception:
                continue

    def _fetch(self, node_id: int, handle: str) -> pl.LazyFrame:
        key = (node_id, handle)
        if key not in self.rows or not os.path.exists(self.rows[key]):
            self.rows[key], changed = _canvas_answer(self.fetch, self.flow_id, node_id, handle)
            if changed:
                print(CANVAS_CHANGED)
        return pl.scan_parquet(self.rows[key])

    def _twin(self, node: FlowNode, created: set[int], twins: dict[int, int | None]) -> int | None:
        """The canvas node ``node`` stands for, so its rows are that node's; ``None`` when the canvas has none.

        A seeded node no cell replaced is its own. A node a cell built stands for the first canvas node of its
        type with equal settings (:func:`_rows_settings`) whose inputs are the twins of its own inputs, which a
        cell run again unchanged builds, alone or with the cells above it. Input slots compare as sorted
        ``(source, handle)`` pairs, and a parameter value changed in the session is not seen.
        """
        if node.node_id in twins:
            return twins[node.node_id]
        twins[node.node_id] = None
        if node.node_id in self.seeded and node.node_id not in created:
            twins[node.node_id] = node.node_id
            return node.node_id
        if node.setting_input is None:
            return None
        edges = []
        for source, handle in node._incoming_edges():
            source_twin = self._twin(source, created, twins)
            if source_twin is None:
                return None
            edges.append((source_twin, handle))
        mine = _rows_settings(node.setting_input, node.node_type)
        for canvas_id, twin in self.mode.snapshot.items():
            if twin.node_type != node.node_type or twin.setting_input is None:
                continue
            if sorted(twin.inputs) != sorted(edges):
                continue
            if canvas_id not in self.canvas_settings:
                self.canvas_settings[canvas_id] = _rows_settings(twin.setting_input, twin.node_type)
            if self.canvas_settings[canvas_id] == mine:
                twins[node.node_id] = canvas_id
                return canvas_id
        return None

    def _resolve(self, frame: FlowFrame, created: set[int]) -> pl.LazyFrame:
        """A new frame's rows: canvas nodes fetched, held nodes run by core, every other node computed here.

        The walk up from the frame sorts each node. A seeded node no cell replaced, or a read of a file this
        kernel cannot see whose canvas twin was seeded, holds its canvas rows when it is deferred or at or
        below a gate (*canvas*), else keeps its own result and the walk stops. A new gate or deferred node
        outside ``COMPUTED_HERE_TYPES`` with a canvas twin (:meth:`_twin`) is canvas too; without one, like a
        read this kernel cannot see without a seeded twin, core runs it from its settings over its inputs' rows
        (*held*, :meth:`_run_held`). Every other node computes again here (*computed*). Sources come first:
        canvas and held rows become the results of their nodes, so the computed nodes below read them, and an
        input of a held node this kernel computed is written as parquet for core.
        """
        canvas: dict[int, int] = {}
        held: dict[int, FlowNode] = {}
        computed: dict[int, FlowNode] = {}
        twins: dict[int, int | None] = {}
        root = self.mode.graph.get_node(frame.node_id)
        stack = [root]
        while stack:
            node = stack.pop()
            if node.node_id in canvas or node.node_id in held or node.node_id in computed:
                continue
            if node.node_id in self.seeded and node.node_id not in created:
                if node.deferred_until_run or any(n.node_type == "gate" for n in ancestors(node).values()):
                    canvas[node.node_id] = node.node_id
                continue
            if _kernel_hidden_path(node) is not None:
                twin = _kernel_twin_id(node)
                if twin in self.seeded:
                    canvas[node.node_id] = twin
                    continue
                held[node.node_id] = node
            elif node.node_type == "gate" or (node.deferred_until_run and node.node_type not in COMPUTED_HERE_TYPES):
                twin = self._twin(node, created, twins)
                if twin is not None:
                    canvas[node.node_id] = twin
                    continue
                held[node.node_id] = node
            else:
                computed[node.node_id] = node
            stack.extend(node.all_inputs)
        done: set[int] = set()

        def ready(node: FlowNode) -> None:
            if node.node_id in done or node.node_id in canvas:
                return
            done.add(node.node_id)
            for source, handle in node._incoming_edges():
                ready(source)
                if source.node_id in canvas:
                    self._inject(source, handle, self._fetch(canvas[source.node_id], handle))
            if node.node_id not in held:
                node.results.resulting_data, node.results.errors, node._named_outputs = None, None, {}
                node.deferred_until_run = False
            elif self._held_current(node):
                self._take(node)
            else:
                paths = {
                    source.node_id: self._input_path(source, handle, canvas, held)
                    for source, handle in node._incoming_edges()
                }
                self._run_held(node, paths)

        ready(root)
        handle = frame.output_handle
        if root.node_id in canvas:
            return self._fetch(canvas[root.node_id], handle)
        if root.node_id in held:
            return self._held_rows(root, handle)
        data = materialise(root, None if handle == DEFAULT_OUTPUT_HANDLE else handle).data_frame
        return data.lazy() if isinstance(data, pl.DataFrame) else data

    @staticmethod
    def _inject(node: FlowNode, handle: str, rows: pl.LazyFrame) -> None:
        """Make ``rows`` the result of ``node``'s output ``handle`` here, so nodes below read them."""
        engine = FlowDataEngine(rows)
        if handle == DEFAULT_OUTPUT_HANDLE:
            node.results.resulting_data = engine
        else:
            node._named_outputs[handle] = engine

    def _held_current(self, node: FlowNode) -> bool:
        """Whether core ran ``node`` in this session and every file it answered with is still there."""
        paths = [path for (node_id, _), path in self.rows.items() if node_id == node.node_id]
        return node.node_id in self.ran and all(os.path.exists(path) for path in paths)

    def _held_rows(self, node: FlowNode, handle: str) -> pl.LazyFrame:
        path = self.rows.get((node.node_id, handle))
        if path is None:
            raise NativeNodeError(
                f"Node {node.node_id} has no rows for output {handle}: a gate routed it away in this run"
            )
        return pl.scan_parquet(path)

    def _input_path(
        self, source: FlowNode, handle: str, canvas: Mapping[int, int], held: Mapping[int, FlowNode]
    ) -> str:
        """Output ``handle`` of ``source`` as a parquet core can read: a canvas or held node's own file, rows this
        kernel computed written to the session's results folder."""
        if source.node_id in canvas:
            self._fetch(canvas[source.node_id], handle)
            return self.rows[(canvas[source.node_id], handle)]
        if source.node_id in held:
            self._held_rows(source, handle)
            return self.rows[(source.node_id, handle)]
        if not self.results_dir:
            raise NativeNodeError("This session has no results folder the canvas can read: Reset session")
        os.makedirs(self.results_dir, exist_ok=True)
        path = os.path.join(self.results_dir, f"input_{uuid4().hex}.parquet")
        data = materialise(source, None if handle == DEFAULT_OUTPUT_HANDLE else handle).data_frame
        (data if isinstance(data, pl.DataFrame) else data.collect()).write_parquet(path)
        return path

    def _ask_core(self, node: FlowNode, inputs: list[dict[str, Any]], *, schema_only: bool) -> dict[str, Any]:
        body = {
            "flow_id": self.flow_id,
            "node": _node_payload(node),
            "inputs": inputs,
            "parameters": [p.model_dump(mode="json") for p in self.mode.graph.flow_settings.parameters],
            "schema_only": schema_only,
        }
        try:
            return self.run(body)
        except NativeNodeError:
            raise
        except Exception as exc:
            raise NativeNodeError(f"Could not run node {node.node_id} on the canvas: {exc}") from exc

    def _run_held(self, node: FlowNode, paths: Mapping[int, str]) -> None:
        """Have core run ``node`` from its settings over ``paths`` (source node id -> its rows as parquet) and take
        the rows of every live output (:meth:`_take`); a gate's dead output gets none."""
        inputs = [{"node_id": node_id, "path": path} for node_id, path in paths.items()]
        answer = self._ask_core(node, inputs, schema_only=False)
        for handle, path in (answer.get("paths") or {}).items():
            self.rows[(node.node_id, handle)] = path
        self.ran.add(node.node_id)
        self._take(node)

    def _take(self, node: FlowNode) -> None:
        """Make the rows core answered for ``node`` its results here, keep their schemas and give them to the
        deferred frames bound to the node in the namespace, so ``.columns`` knows them."""
        schemas = dict(node._named_schemas)
        for (node_id, handle), path in list(self.rows.items()):
            if node_id != node.node_id:
                continue
            rows = pl.scan_parquet(path)
            self._inject(node, handle, rows)
            schemas[handle] = FlowDataEngine(rows).schema
        node._named_schemas = schemas
        for frame in _bound_frames(self.namespace):
            path = self.rows.get((frame.node_id, frame.output_handle))
            if frame.node_id == node.node_id and frame._deferred and path is not None:
                frame.data = pl.scan_parquet(path)

    def held_schemas(self, node: FlowNode) -> dict[str, list[FlowfileColumn]] | None:
        """The columns of a deferred node a cell built, for its seed (``native.resolved_seed``): its canvas twin's
        when the twin has columns, else what core predicts from the node's settings and its inputs' columns
        (``schema_only``, nothing runs). ``None`` when neither answers, and for a node this session computes."""
        if node.node_type in COMPUTED_HERE_TYPES:
            return None
        created = {entry[2] for entry in self.mode.provenance}
        twin = self.mode.snapshot.get(self._twin(node, created, {}))
        if twin is not None and any(twin.schemas.values()):
            return _per_handle(twin.schemas, _handles(node))
        try:
            inputs = [
                {"node_id": source.node_id, "columns": _entries(source, handle)}
                for source, handle in node._incoming_edges()
            ]
            answer = self._ask_core(node, inputs, schema_only=True)
            return {handle: _columns(entries) for handle, entries in (answer.get("schemas") or {}).items()} or None
        except Exception:
            return None


def _canvas_answer(
    fetch: Callable[[dict[str, Any]], dict[str, Any]], flow_id: int, node_id: int, handle: str
) -> tuple[str, bool]:
    """Core's parquet path of canvas node ``node_id``'s output ``handle``, and whether the canvas changed since the
    session was seeded; any failure is a ``NativeNodeError``."""
    body = {"flow_id": flow_id, "node_id": node_id, "output_handle": handle}
    try:
        answer = fetch(body)
        return answer["path"], bool(answer.get("canvas_changed"))
    except NativeNodeError:
        raise
    except Exception as exc:
        raise NativeNodeError(f"Could not get node {node_id}'s rows from the canvas: {exc}") from exc


def _clean_run_rows(flow_id: int) -> Callable[[int, str], pl.LazyFrame]:
    """A push's reader of canvas rows: a canvas node's output through :data:`transport`, asked once per clean run."""
    paths: dict[tuple[int, str], str] = {}

    def rows(node_id: int, handle: str) -> pl.LazyFrame:
        if (node_id, handle) not in paths:
            paths[node_id, handle], _ = _canvas_answer(transport, flow_id, node_id, handle)
        return pl.scan_parquet(paths[node_id, handle])

    return rows


def _node_payload(node: FlowNode) -> dict[str, Any]:
    """``node`` as the session graph's ``FlowfileData`` lists it, for core's ``node_run``."""
    info = node.get_node_information()
    return FlowfileNode(
        id=info.id,
        type=info.type,
        is_start_node=node.is_start,
        description=info.description,
        node_reference=info.node_reference,
        x_position=int(info.x_position or 0),
        y_position=int(info.y_position or 0),
        group_id=info.group_id,
        left_input_id=info.left_input_id,
        right_input_id=info.right_input_id,
        input_ids=info.input_ids or None,
        outputs=info.outputs,
        output_handles=info.output_handles,
        input_connections=info.input_connections,
        setting_input=info.setting_input,
    ).model_dump(mode="json")


def _entries(source: FlowNode, handle: str) -> list[dict[str, str]]:
    """The columns of ``source``'s output ``handle`` here, as ``{"name", "data_type"}`` entries."""
    engine = materialise(source, None if handle == DEFAULT_OUTPUT_HANDLE else handle)
    return [column.get_minimal_field_info().model_dump() for column in engine.schema or []]


_SESSIONS: dict[int, _Session] = {}


def _schema_head() -> str | None:
    """The Alembic head this kernel's flowfile_core ships, which core compares with the catalog's revision;
    ``None`` when it cannot be read."""
    from flowfile_core.database.migration import package_head

    try:
        return package_head()
    except Exception:
        return None


_database_lock = threading.Lock()
_refresh_pending = False
_database_copy: str | None = None


def _refresh_database() -> str | None:
    """The catalog copy to open, after having core refresh it if this call has not yet; a missing copy raises.

    Core answers with the copy's path in this kernel. ``None`` until core has named one (no SQLite file catalog).
    """
    global _refresh_pending, _database_copy
    with _database_lock:
        if _refresh_pending:
            path = database_transport().get("path")
            if path is not None and not Path(path).exists():
                raise RuntimeError(
                    f"The Flowfile database copy {path} is missing in this kernel; recreate the notebook kernel"
                )
            _database_copy = path
            _refresh_pending = False
        return _database_copy


def _refresh_before_connect(dialect, conn_rec, cargs, cparams) -> None:
    """Point every new SQLite connection at the copy core named last (pysqlite's first argument is the file)."""
    copy = _refresh_database()
    if copy is not None and dialect.name == "sqlite" and cargs:
        cargs[0] = copy


def _rearm(engine) -> None:
    """Close ``engine``'s pooled connections and have its next new connection refresh the copy first.

    Each copy is a plain rollback-journal file under a name of its own, so the engine's WAL switch is taken off,
    and the refresh runs before the connection opens the file (``do_connect``), which then opens that copy
    instead of ``FLOWFILE_DB_PATH``. Docker Desktop keeps serving a replaced file's old entry for a few
    milliseconds, so a copy replaced in place would exist and fail to open.
    """
    global _refresh_pending
    from sqlalchemy import event

    from shared.database import _enable_wal

    if event.contains(engine, "connect", _enable_wal):
        event.remove(engine, "connect", _enable_wal)
    if not event.contains(engine, "do_connect", _refresh_before_connect):
        event.listen(engine, "do_connect", _refresh_before_connect)
    with _database_lock:
        _refresh_pending = True
    engine.dispose()


def _release_database() -> None:
    """In a notebook kernel (``FLOWFILE_KERNEL_ID`` and ``FLOWFILE_DB_PATH`` set), re-arm the catalog engine for
    this call (:func:`_rearm`); elsewhere nothing happens."""
    if not (os.environ.get("FLOWFILE_KERNEL_ID") and os.environ.get("FLOWFILE_DB_PATH")):
        return
    from shared.database import get_catalog_engine

    _rearm(get_catalog_engine())


def _hello(request: dict[str, Any]) -> dict[str, Any]:
    from shared._version import get_version

    return {"ok": True, "version": get_version(), "schema_head": _schema_head()}


def _kernel_client() -> Any:
    """The kernel runtime's ``flowfile_client``, or ``None`` outside a kernel."""
    try:
        from kernel_runtime import flowfile_client
    except ImportError:
        return None
    return flowfile_client


def _through_kernel(name: str, value: Any, *args: Any, **kwargs: Any) -> Any:
    """Show ``value`` through the kernel's own ``display`` or ``explore`` and move what it rendered onto the running
    cell's outputs, so it keeps its place among the notebook's displays; outside a kernel, the notebook's
    ``display``."""
    client = _kernel_client()
    if client is None:
        return display(value)
    start = len(client._get_displays())
    getattr(client, name)(value, *args, **kwargs)
    shown = client._get_displays()
    outputs = _CELL_OUTPUTS.get()
    if outputs is not None:
        outputs.extend({entry["mime_type"]: entry["data"], "title": entry.get("title", "")} for entry in shown[start:])
        del shown[start:]
    return None


def _display(value: Any, *args: Any, **kwargs: Any) -> Any:
    """A cell's ``display``: frames and nodes as the notebook shows them, anything else through the kernel's own."""
    if isinstance(value, FlowFrame | NativeNode):
        return display(value)
    return _through_kernel("display", value, *args, **kwargs)


def _explore(value: Any, *args: Any, **kwargs: Any) -> Any:
    """A cell's ``explore``: frames and nodes as the notebook shows them, anything else through the kernel's own."""
    if isinstance(value, FlowFrame | NativeNode):
        return display(value)
    return _through_kernel("explore", value, *args, **kwargs)


def _close(flow_id: int, generation: str | None = None) -> None:
    """Close the flow's session; with ``generation``, only when it is that session (not one opened since)."""
    session = _SESSIONS.get(flow_id)
    if session is None or (generation is not None and session.generation != generation):
        return
    del _SESSIONS[flow_id]
    session.namespace.clear()
    session.mode.close()


def _adopt(session: _Session, namespace: dict[str, Any]) -> None:
    """Keep the session's variables in the kernel's namespace for this call, moved over when it is a new one."""
    if namespace is not session.namespace:
        namespace.update(session.namespace)
        session.namespace = namespace


def _open(flow_id: int, request: dict[str, Any], namespace: dict[str, Any]) -> dict[str, Any]:
    """Seed the flow's session from the canvas snapshot (closing any previous one) and bind one variable per node.

    The session's namespace is the kernel's ``namespace``, emptied first.
    """
    snapshot = request.get("snapshot") or {}
    user_id = int(request["user_id"])
    _close(flow_id)
    with notebook.paths_as_written():
        if snapshot.get("flowfile_data"):
            bound = seed_session(
                snapshot["flowfile_data"],
                snapshot.get("parameters") or [],
                snapshot.get("names") or {},
                snapshot.get("schemas") or {},
                user_id=user_id,
            )
        else:
            notebook.enter(user_id=user_id).graph.unique_subflow_port_names = False
            bound = {}
        try:
            namespace.clear()
            namespace.update(new_namespace())
            namespace.update(bound)
            namespace["display"] = _display
            namespace["explore"] = _explore
        finally:
            mode = notebook._deactivate()
    seeded = frozenset(node.node_id for node in mode.graph.nodes) if snapshot.get("flowfile_data") else frozenset()
    results_dir = str(request.get("results_dir") or "")
    session = _Session(flow_id, mode, namespace, uuid4().hex, transport, run_transport, seeded, results_dir)
    mode.row_resolver = session.canvas_rows
    mode.schema_resolver = session.held_schemas
    _SESSIONS[flow_id] = session
    return {"ok": True, **session.stamp()}


def _error_line(text: str | None) -> str | None:
    lines = [line for line in (text or "").strip().splitlines() if line.strip()]
    return lines[-1] if lines else text


def _refreshed(session: _Session, schemas: Mapping[Any, Mapping[str, Any]] | None = None) -> None:
    """``session.refresh``, best effort: failing to take over the canvas's columns never fails the call."""
    with contextlib.suppress(Exception):
        session.refresh(schemas)


def _execute(session: _Session, request: dict[str, Any]) -> dict[str, Any]:
    """Run one cell as Python in the session; its outputs, then the last expression's schema display."""
    session.revision += 1
    with notebook.resumed(session.mode), notebook.paths_as_written():
        _refreshed(session, request.get("schemas"))
        result = execute_cell(str(request["cell_id"]), request["code"], session.namespace, executor=exec_cell)
        _refreshed(session)
    displays = [*result.outputs, *([result.display] if result.display is not None else [])]
    return {
        "ok": result.ok,
        "error": _error_line(result.error),
        "traceback": result.error,
        "line": result.line,
        "kind": result.kind,
        "nodes_created": [list(entry) for entry in result.created],
        "names_bound": list(result.names),
        "references": {str(node_id): name for node_id, name in result.references.items()},
        "displays": displays,
        **session.stamp(),
    }


def _clean_run(flow_id: int, request: dict[str, Any]) -> dict[str, Any]:
    """The push's clean run: core's runner with cells run as Python, on a sync of the request's snapshot.

    It runs in a mode of its own; a mode active in this context is set aside and put back afterwards. A cell
    that reads a frame's rows gets the canvas's, for a node the canvas already has (:func:`_clean_run_rows`).
    """
    from flowfile_core.notebook.bridge import CleanRunRequest
    from flowfile_core.notebook.runner import NotebookRunner

    class _PythonRunner(NotebookRunner):
        executor = staticmethod(lambda: exec_cell)
        canvas_rows = staticmethod(_clean_run_rows(flow_id))

    previous = notebook._deactivate()
    try:
        with notebook.paths_as_written():
            result = _PythonRunner().clean_run(
                int(request["user_id"]), flow_id, CleanRunRequest.model_validate(request["request"])
            )
    finally:
        if previous is not None:
            notebook._activate(previous)
    return {"ok": True, "result": result.model_dump(mode="json"), "traceback": result.traceback}


def _schemas(session: _Session, request: dict[str, Any]) -> dict[str, Any]:
    frames: dict[str, list[dict[str, str]]] = {}
    with notebook.resumed(session.mode):
        _refreshed(session, request.get("schemas"))
        for name, value in list(session.namespace.items()):
            if name.startswith("_") or not isinstance(value, FlowFrame):
                continue
            try:
                frames[name] = _schema_entries(value)
            except Exception:
                continue
    return {"ok": True, "frames": frames, **session.stamp()}


def _dispatch(request: dict[str, Any], namespace: dict[str, Any]) -> dict[str, Any]:
    op = request.get("op")
    if op == "hello":
        return _hello(request)
    flow_id = int(request["flow_id"])
    if op in ("open", "reset"):
        return _open(flow_id, request, namespace)
    if op == "clean_run":
        return _clean_run(flow_id, request)
    if op == "close":
        _close(flow_id, request.get("generation"))
        return {"ok": True}
    session = _SESSIONS.get(flow_id)
    if op not in ("execute", "schemas"):
        raise ValueError(f"Unknown notebook op {op!r}")
    if session is None:
        return {"ok": False, "no_session": True, "error": "No notebook session is open for this flow"}
    _adopt(session, namespace)
    return _execute(session, request) if op == "execute" else _schemas(session, request)


def handle(request_json: str, namespace: dict[str, Any]) -> None:
    """Run one notebook op and print its JSON result on the marker line; a failure is a result too.

    ``namespace`` is the kernel's namespace the call runs in, where the flow's session keeps its variables.
    """
    try:
        _release_database()
        result = _dispatch(json.loads(request_json), namespace)
    except BaseException as exc:
        text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        result = {"ok": False, "error": _error_line(text), "traceback": text}
    sys.stdout.write(f"\n{KERNEL_RESULT_MARKER}{json.dumps(result, default=str)}\n")
    sys.stdout.flush()
