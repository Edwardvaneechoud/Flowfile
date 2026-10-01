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

Ops: ``hello`` (flowfile version and the database's schema revision), ``open`` / ``reset`` (seed a
session from the canvas snapshot), ``execute`` (one cell as Python), ``clean_run`` (every cell in a
fresh namespace, the push's run), ``schemas`` (the frames bound in the session) and ``close``.

Rows the kernel cannot compute come from the canvas: a seeded canvas node that no cell replaced asks
core's ``POST /notebook/session/node_result`` (through :data:`transport`), which runs it on the canvas
when needed and answers with a parquet path on the kernel's shared folder, read with ``pl.scan_parquet``
and kept per node and output until the session is reset.
"""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import sys
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


def post_node_result(body: dict[str, Any]) -> dict[str, Any]:
    """Ask core for a canvas node's result (``POST /notebook/session/node_result``) as this kernel."""
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
            f"{url}/notebook/session/node_result",
            json=body,
            headers=headers,
            timeout=httpx.Timeout(None, connect=10.0),
        )
    except httpx.HTTPError as exc:
        raise NativeNodeError(f"Could not reach Flowfile for node {body['node_id']}'s rows: {exc}") from exc
    if response.status_code != 200:
        try:
            detail = response.json().get("detail")
        except ValueError:
            detail = None
        raise NativeNodeError(str(detail or response.text or f"HTTP {response.status_code}"))
    return response.json()


transport: Callable[[dict[str, Any]], dict[str, Any]] = post_node_result
"""How a session asks core for a canvas node's result; tests route it to core in-process."""


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


def _schema_revision() -> str | None:
    """The Alembic revision of the catalog database copy, read-only; ``None`` when it cannot be read."""
    path = _database_path()
    if path is None or not path.exists():
        return None
    try:
        with contextlib.closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as conn:
            row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def _release_database() -> None:
    """In a kernel, close the pooled catalog connections so the next read opens core's latest copy.

    Core replaces the copy (``FLOWFILE_DB_PATH``) between calls; it is a plain rollback-journal file,
    so the engine's WAL switch is taken off too. Outside a kernel (``FLOWFILE_KERNEL_ID`` unset) nothing happens.
    """
    if not os.environ.get("FLOWFILE_KERNEL_ID"):
        return
    from sqlalchemy import event

    from shared.database import _enable_wal, get_catalog_engine

    engine = get_catalog_engine()
    if event.contains(engine, "connect", _enable_wal):
        event.remove(engine, "connect", _enable_wal)
    engine.dispose()


def _hello(request: dict[str, Any]) -> dict[str, Any]:
    from shared._version import get_version

    return {"ok": True, "version": get_version(), "schema_revision": _schema_revision()}


def _display(value: Any) -> Any:
    """A cell's ``display``: frames and nodes as the notebook shows them, anything else through the kernel's own."""
    if isinstance(value, FlowFrame | NativeNode):
        return display(value)
    try:
        from kernel_runtime import flowfile_client
    except ImportError:
        return display(value)
    return flowfile_client.display(value)


def _close(flow_id: int) -> None:
    session = _SESSIONS.pop(flow_id, None)
    if session is not None:
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
    path = _database_path()
    if path is not None and not path.exists():
        raise RuntimeError(f"The Flowfile database {path} is missing in this kernel; recreate the notebook kernel")
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
        _close(flow_id)
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
