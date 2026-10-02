"""The canvas notebook session that runs inside a notebook kernel (a kernel with ``flowfile`` installed).

Core drives it through the kernel's ``/execute`` with one constant snippet::

    from flowfile_frame import notebook_kernel as _nb
    _nb.handle('<request json>', globals())

:func:`handle` runs the request's op and prints its JSON result on one stdout line that starts with
``shared.notebook_display.KERNEL_RESULT_MARKER``; core strips that line, so whatever the cell printed
stays the call's stdout. One session per flow id lives in this module; its variables live in the kernel's
namespace for the call's flow id (the snippet's ``globals()``), so the kernel's code intelligence (Jedi)
reads them. Every kernel call runs in a fresh context and notebook mode
is context-local, so each op resumes the session's mode (``notebook.resumed``) and runs with file
paths kept as written (``notebook.paths_as_written``); core recomputes them on the host.

Ops: ``hello`` (flowfile version and the Alembic head its flowfile_core ships), ``open`` / ``reset`` (seed a
session from the canvas snapshot), ``execute`` (one cell as Python), ``clean_run`` (every cell in a
fresh namespace, the push's run), ``schemas`` (the frames bound in the session) and ``close`` (the session of
the generation it names). A cell's ``display`` and ``explore`` go to the session's one list of outputs, in call
order, whether the notebook or the kernel's own ``display`` renders the value.

Rows the kernel cannot compute come from the canvas: a seeded canvas node that no cell replaced asks
core's ``POST /notebook/session/node_result`` (through :data:`transport`), which runs it on the canvas
when needed and answers with a parquet path on the kernel's shared folder, read with ``pl.scan_parquet``
and kept per node and output until the session is reset.

The catalog database is a copy core keeps in the kernel's shared folder (``FLOWFILE_DB_PATH``). A call that
reads it asks core to refresh it first (``POST /notebook/session/database``, through
:data:`database_transport`), once per call and just before the call's first connection; calls that never
touch the catalog never copy it.
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

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_frame import notebook
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.native import (
    NativeNode,
    NativeNodeError,
    _kernel_hidden_path,
    _kernel_twin_id,
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
    handed out per ``(node_id, output_handle)`` and ``fetch`` the transport to core.
    """

    flow_id: int
    mode: notebook.NotebookMode
    namespace: dict[str, Any]
    generation: str
    fetch: Callable[[dict[str, Any]], dict[str, Any]]
    seeded: frozenset[int] = frozenset()
    rows: dict[tuple[int, str], str] = field(default_factory=dict)
    revision: int = 0

    def stamp(self) -> dict[str, Any]:
        return {"namespace_generation": self.generation, "revision": self.revision}

    def canvas_rows(self, frame: FlowFrame) -> pl.LazyFrame | None:
        """The canvas's rows for a frame without rows here: a seeded node no cell replaced, the canvas twin of
        a file read this kernel cannot open (``native._kernel_twin_id``), or a new frame computed here on those."""
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
            body = {"flow_id": self.flow_id, "node_id": node_id, "output_handle": handle}
            try:
                answer = self.fetch(body)
                self.rows[key] = answer["path"]
            except NativeNodeError:
                raise
            except Exception as exc:
                raise NativeNodeError(f"Could not get node {node_id}'s rows from the canvas: {exc}") from exc
            if answer.get("canvas_changed"):
                print(CANVAS_CHANGED)
        return pl.scan_parquet(self.rows[key])

    def _computed_here(self, frame: FlowFrame, created: set[int]) -> pl.LazyFrame | None:
        """A new frame's rows computed here on its canvas ancestors' rows; ``None`` when another node above has none.

        Each canvas ancestor (a seeded node no cell replaced, or a read of a file this kernel cannot see)
        then holds its canvas rows and the nodes below it compute again, so later frames compute here too.
        """
        lineage = ancestors(self.mode.graph.get_node(frame.node_id))
        canvas: dict[int, int] = {}
        for node_id, node in lineage.items():
            if node_id in self.seeded and node_id not in created:
                if node.deferred_until_run:
                    canvas[node_id] = node_id
            elif _kernel_hidden_path(node) is not None and _kernel_twin_id(node) in self.seeded:
                canvas[node_id] = _kernel_twin_id(node)
            elif node.deferred_until_run:
                return None
        if not canvas:
            return None
        edges = {(source.node_id, handle) for node in lineage.values() for source, handle in node._incoming_edges()}
        for source_id, handle in edges:
            if source_id in canvas:
                engine = FlowDataEngine(self._fetch(canvas[source_id], handle))
                if handle == DEFAULT_OUTPUT_HANDLE:
                    lineage[source_id].results.resulting_data = engine
                else:
                    lineage[source_id]._named_outputs[handle] = engine
        for node_id, node in lineage.items():
            if node_id not in canvas and not (node_id in self.seeded and node_id not in created):
                node.results.resulting_data, node.results.errors, node._named_outputs = None, None, {}
        handle = frame.output_handle
        data = materialise(lineage[frame.node_id], None if handle == DEFAULT_OUTPUT_HANDLE else handle).data_frame
        return data.lazy() if isinstance(data, pl.DataFrame) else data


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


def _database_path() -> Path | None:
    from shared.database import sqlite_database_path

    return sqlite_database_path()


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


def _refresh_database() -> None:
    """Have core refresh the catalog copy if this call has not yet; a copy still missing afterwards raises."""
    global _refresh_pending
    with _database_lock:
        if not _refresh_pending:
            return
        answer = database_transport()
        path = _database_path()
        if path is not None and not path.exists():
            raise RuntimeError(
                f"The Flowfile database {path} is missing in this kernel (core copied it to {answer.get('path')}); "
                "recreate the notebook kernel"
            )
        _refresh_pending = False


def _refresh_before_connect(dialect, conn_rec, cargs, cparams) -> None:
    _refresh_database()


def _rearm(engine) -> None:
    """Close ``engine``'s pooled connections and have its next new connection refresh the copy first.

    The copy is a plain rollback-journal file core replaces between calls, so the engine's WAL switch is taken
    off, and the refresh runs before the connection opens the file (``do_connect``).
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


def _adopt(session: _Session, namespace: dict[str, Any] | None) -> None:
    """Keep the session's variables in the kernel's namespace for this call, moved over when it is a new one."""
    if namespace is not None and namespace is not session.namespace:
        namespace.update(session.namespace)
        session.namespace = namespace


def _open(flow_id: int, request: dict[str, Any], kernel_namespace: dict[str, Any] | None = None) -> dict[str, Any]:
    """Seed the flow's session from the canvas snapshot (closing any previous one) and bind one variable per node.

    The session's namespace is ``kernel_namespace`` (emptied first) when given, else a new dict.
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
            namespace = {} if kernel_namespace is None else kernel_namespace
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

    It runs in a mode of its own; a mode active in this context is set aside and put back afterwards.
    """
    from flowfile_core.notebook.bridge import CleanRunRequest
    from flowfile_core.notebook.runner import NotebookRunner

    class _PythonRunner(NotebookRunner):
        executor = staticmethod(lambda: exec_cell)

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


def _dispatch(request: dict[str, Any], namespace: dict[str, Any] | None = None) -> dict[str, Any]:
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


def handle(request_json: str, namespace: dict[str, Any] | None = None) -> None:
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
