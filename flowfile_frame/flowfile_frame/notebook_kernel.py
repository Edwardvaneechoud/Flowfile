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
push. Rows the kernel
cannot compute come from the canvas (:meth:`_Session.canvas_rows`), and the catalog database is a copy core
writes under a new name when a call first connects to it (:func:`_rearm`).
"""

from __future__ import annotations

import json
import os
import sys
import threading
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
from pydantic import BaseModel

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_frame import notebook
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.native import (
    NativeNode,
    NativeNodeError,
    _kernel_hidden_path,
    _kernel_twin_id,
    _twin_settings,
    ancestors,
    materialise,
)
from flowfile_frame.notebook_cells import (
    _CELL_OUTPUTS,
    _schema_entries,
    display,
    exec_cell,
    execute_cell,
    new_namespace,
    seed_session,
)
from shared.notebook_display import KERNEL_RESULT_MARKER

CANVAS_CHANGED = "The canvas changed since this session started: Reset session to pick it up."
PUSH_FIRST = "Push, then it runs on the canvas."

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


def post_database_refresh() -> dict[str, Any]:
    """Ask core to bring this kernel's copy of the catalog database up to date (``POST /notebook/session/database``)."""
    return _post_core("/notebook/session/database", {}, "a fresh copy of the catalog database")


transport: Callable[[dict[str, Any]], dict[str, Any]] = post_node_result
"""How a session asks core for a canvas node's result; tests route it to core in-process."""

database_transport: Callable[[], dict[str, Any]] = post_database_refresh
"""How a call asks core to refresh the kernel's catalog copy; tests route it to core in-process."""


@dataclass
class _Session:
    """One flow's session: its (set aside) notebook mode, namespace and the identity the editor's schemas use.

    ``seeded`` holds the canvas node ids the session was seeded with, ``rows`` the parquet path core
    handed out per ``(node_id, output_handle)``, ``fetch`` the transport to core and ``canvas_settings``
    each canvas node's :func:`_rows_settings`, worked out once.
    """

    flow_id: int
    mode: notebook.NotebookMode
    namespace: dict[str, Any]
    generation: str
    fetch: Callable[[dict[str, Any]], dict[str, Any]]
    seeded: frozenset[int] = frozenset()
    rows: dict[tuple[int, str], str] = field(default_factory=dict)
    canvas_settings: dict[int, Any] = field(default_factory=dict)
    revision: int = 0

    def stamp(self) -> dict[str, Any]:
        return {"namespace_generation": self.generation, "revision": self.revision}

    def canvas_rows(self, frame: FlowFrame) -> pl.LazyFrame | None:
        """The rows of a frame without rows here: the canvas's for a seeded node no cell replaced or the canvas
        twin of a file read this kernel cannot open (``native._kernel_twin_id``), else computed here
        (:meth:`_computed_here`)."""
        if frame.flow_graph is not self.mode.graph:
            return None
        node_id, hidden = frame.node_id, False
        created = {entry[2] for entry in self.mode.provenance}
        if node_id not in self.seeded or node_id in created:
            node = self.mode.graph.get_node(node_id)
            hidden = node is not None and _kernel_hidden_path(node) is not None
            node_id = _kernel_twin_id(node) if hidden else None
        if node_id is None and not hidden:
            computed = self._computed_here(frame, created)
            if computed is not None:
                return computed
        if node_id not in self.seeded:
            raise NativeNodeError(_new_node_message(frame))
        return self._fetch(node_id, frame.output_handle)

    def _fetch(self, node_id: int, handle: str) -> pl.LazyFrame:
        key = (node_id, handle)
        if key not in self.rows:
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

    def _computed_here(self, frame: FlowFrame, created: set[int]) -> pl.LazyFrame | None:
        """A new frame's rows computed here; ``None`` when a node above it needs the canvas to run it.

        The walk up from the frame stops at canvas nodes: a seeded node no cell replaced, or a read of a file
        this kernel cannot see whose canvas twin was seeded. Such a node holds its canvas rows when it is
        deferred or at or below a gate, else keeps its own. Every other node computes again here, a deferred
        node of ``COMPUTED_HERE_TYPES`` too, so a new frame needs no canvas ancestor. A new gate or any other
        deferred node (an external source, a subflow, a script, a writer) holds the rows of the canvas node it
        stands for (:meth:`_twin`); without one, as for a read of a file this kernel cannot see without a
        seeded twin, the canvas has to run it first.
        """
        canvas: dict[int, int] = {}
        computed: dict[int, FlowNode] = {}
        twins: dict[int, int | None] = {}
        stack = [self.mode.graph.get_node(frame.node_id)]
        while stack:
            node = stack.pop()
            if node.node_id in canvas or node.node_id in computed:
                continue
            if node.node_id in self.seeded and node.node_id not in created:
                if node.deferred_until_run or any(n.node_type == "gate" for n in ancestors(node).values()):
                    canvas[node.node_id] = node.node_id
                continue
            if _kernel_hidden_path(node) is not None:
                if _kernel_twin_id(node) not in self.seeded:
                    return None
                canvas[node.node_id] = _kernel_twin_id(node)
                continue
            if node.node_type == "gate" or (node.deferred_until_run and node.node_type not in COMPUTED_HERE_TYPES):
                twin = self._twin(node, created, twins)
                if twin is None:
                    return None
                canvas[node.node_id] = twin
                continue
            computed[node.node_id] = node
            stack.extend(node.all_inputs)
        if frame.node_id in canvas:
            return self._fetch(canvas[frame.node_id], frame.output_handle)
        for node in computed.values():
            for source, handle in node._incoming_edges():
                if source.node_id in canvas:
                    engine = FlowDataEngine(self._fetch(canvas[source.node_id], handle))
                    if handle == DEFAULT_OUTPUT_HANDLE:
                        source.results.resulting_data = engine
                    else:
                        source._named_outputs[handle] = engine
        for node in computed.values():
            node.results.resulting_data, node.results.errors, node._named_outputs = None, None, {}
            node.deferred_until_run = False
        handle = frame.output_handle
        data = materialise(computed[frame.node_id], None if handle == DEFAULT_OUTPUT_HANDLE else handle).data_frame
        return data.lazy() if isinstance(data, pl.DataFrame) else data


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


def _new_node_message(frame: FlowFrame) -> str:
    node = frame.flow_graph.get_node(frame.node_id)
    hidden = _kernel_hidden_path(node) if node is not None else None
    if hidden is not None:
        return (
            f"This kernel cannot see {hidden}: add its folder to the kernel's Folders this kernel can read, "
            f"or {PUSH_FIRST[0].lower()}{PUSH_FIRST[1:]}"
        )
    return f"Node {frame.node_id} is new in this session and its rows need the canvas to run it. {PUSH_FIRST}"


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
    session = _Session(flow_id, mode, namespace, uuid4().hex, transport, seeded)
    mode.row_resolver = session.canvas_rows
    _SESSIONS[flow_id] = session
    return {"ok": True, **session.stamp()}


def _error_line(text: str | None) -> str | None:
    lines = [line for line in (text or "").strip().splitlines() if line.strip()]
    return lines[-1] if lines else text


def _execute(session: _Session, request: dict[str, Any]) -> dict[str, Any]:
    """Run one cell as Python in the session; its outputs, then the last expression's schema display."""
    session.revision += 1
    with notebook.resumed(session.mode), notebook.paths_as_written():
        result = execute_cell(str(request["cell_id"]), request["code"], session.namespace, executor=exec_cell)
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


def _schemas(session: _Session) -> dict[str, Any]:
    frames: dict[str, list[dict[str, str]]] = {}
    with notebook.resumed(session.mode):
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
    return _execute(session, request) if op == "execute" else _schemas(session)


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
