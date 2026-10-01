"""The canvas notebook on a notebook kernel: core's side of the session that runs in the kernel.

A notebook kernel is one of the user's kernels with ``flowfile`` installed
(``kernel.notebook_support.is_notebook_kernel_config``). Core never runs the cells: it sends the kernel one
constant snippet through ``KernelManager.execute_sync`` (a flow id of ``-flow_id``, :func:`kernel_flow_id`, so
the call never shares a namespace with the flow's Python Script nodes; the session keeps its variables in
that namespace, where the editor's code intelligence reads them), and the frame's ``notebook_kernel`` module in
the kernel runs the op and prints its JSON result on a line starting with
``shared.notebook_display.KERNEL_RESULT_MARKER``, which is cut from the call's stdout here. Results
come back in the kernel routes' own shapes (``ExecuteResult``, plus a failed cell's ``line``, and the
``dataframe_schemas`` payload). The first call to a kernel checks its flowfile version against core's.
Before every call the kernel's copy of the catalog database is refreshed (``kernel.notebook_db``).
:class:`KernelCleanRunner` is the ``bridge.CleanRunner`` a push uses when it names a kernel; its result
goes through ``notebook.validate`` before it is reconciled. :func:`node_result` is the canvas fallback a
session calls back for rows it cannot compute: the node's canvas result as parquet under the kernel's
shared folder (``notebook/<flow_id>/``, removed when the flow's sessions close).
"""

from __future__ import annotations

import json
import os
import shutil
import threading
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from flowfile_core.configs import logger
from flowfile_core.kernel import notebook_db
from flowfile_core.kernel.models import DisplayOutput, ExecuteRequest, ExecuteResult
from flowfile_core.kernel.notebook_support import is_notebook_kernel_config
from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult
from flowfile_core.notebook.gate import DISABLED_DETAIL, kernel_sessions_allowed
from flowfile_core.notebook.push import seed_snapshot
from shared.notebook_display import KERNEL_RESULT_MARKER

SNIPPET = "from flowfile_frame import notebook_kernel as _nb\n_nb.handle({request!r}, globals())\n"

_lock = threading.Lock()
_running: dict[tuple[str, int], str] = {}
_sessions: dict[int, set[str]] = {}
_verified: set[tuple[str, str | None]] = set()
_fingerprints: dict[tuple[str, int], str] = {}
_results: dict[int, dict[tuple[int, str], tuple[Any, str]]] = {}


def _manager():
    import flowfile_core.kernel as kernel_package

    try:
        return kernel_package.get_kernel_manager()
    except Exception as exc:
        raise HTTPException(503, "Docker is not available. Please ensure Docker is installed and running.") from exc


def _schema_revision() -> str | None:
    try:
        from sqlalchemy import text

        from flowfile_core.database.connection import get_db_context

        with get_db_context() as db:
            row = db.execute(text("SELECT version_num FROM alembic_version")).first()
        return row[0] if row else None
    except Exception:
        return None


def kernel_flow_id(flow_id: int) -> int:
    """The flow id a flow's session runs under in the kernel: its namespace there, which the editor's code
    intelligence reads too (the notebook store's ``sessionFlowId`` for a flow tab)."""
    return -flow_id


def _call(manager, kernel_id: str, flow_id: int, op: str, **fields: Any) -> tuple[dict | None, ExecuteResult]:
    """Run one op in the kernel; its result (``None`` when none came back) and the call with that line cut out."""
    request = ExecuteRequest(
        node_id=0,
        flow_id=kernel_flow_id(flow_id),
        code=SNIPPET.format(request=json.dumps({"op": op, "flow_id": flow_id, **fields}, default=str)),
        exec_token=uuid4().hex,
    )
    key = (kernel_id, flow_id)
    if op != "close":
        try:
            notebook_db.refresh(manager.shared_volume_path, kernel_id)
        except Exception as exc:
            raise HTTPException(502, f"Could not copy the catalog database for the notebook kernel: {exc}") from exc
    with _lock:
        _running[key] = request.exec_token
    try:
        raw = manager.execute_sync(kernel_id, request)
    except Exception as exc:
        raise HTTPException(502, f"The notebook kernel call failed: {exc}") from exc
    finally:
        with _lock:
            if _running.get(key) == request.exec_token:
                del _running[key]
    head, marker, tail = raw.stdout.rpartition("\n" + KERNEL_RESULT_MARKER)
    if not marker:
        return None, raw
    try:
        payload = json.loads(tail)
    except ValueError:
        return None, raw
    return payload, raw.model_copy(update={"stdout": head})


def _succeeded(payload: dict | None, raw: ExecuteResult) -> dict:
    if payload is None:
        raise HTTPException(502, raw.error or "The notebook kernel returned no result")
    if not payload.get("ok"):
        raise HTTPException(422, payload.get("error") or "The notebook kernel call failed")
    return payload


def _checked(kernel_id: str, user, flow_id: int):
    """The manager, after the mode gate, the kernel's owner and packages, and (once per container) its version."""
    from shared._version import get_version

    if not kernel_sessions_allowed(user):
        raise HTTPException(403, DISABLED_DETAIL)
    manager = _manager()
    kernel = manager.get_kernel_sync(kernel_id)
    if kernel is None:
        raise HTTPException(404, f"Kernel '{kernel_id}' not found")
    if manager.get_kernel_owner(kernel_id) != user.id:
        raise HTTPException(403, "Not authorized to access this kernel")
    if not is_notebook_kernel_config(kernel):
        raise HTTPException(422, f"Kernel '{kernel_id}' has no flowfile package; pick or create a notebook kernel")
    key = (kernel_id, kernel.container_id)
    if key not in _verified:
        hello = _succeeded(*_call(manager, kernel_id, flow_id, "hello"))
        if hello.get("version") != get_version():
            raise HTTPException(
                409,
                f"Kernel '{kernel_id}' has flowfile {hello.get('version')} and this app is {get_version()}: "
                "recreate the notebook kernel",
            )
        revision, core_revision = hello.get("schema_revision"), _schema_revision()
        if revision and core_revision and revision != core_revision:
            raise HTTPException(
                409,
                f"Kernel '{kernel_id}' sees database schema {revision}, core has {core_revision}: "
                "recreate the notebook kernel",
            )
        _verified.add(key)
    return manager


def _open(manager, flow, user, kernel_id: str, op: str) -> None:
    from flowfile_core.notebook.render import code_fingerprint

    fingerprint = code_fingerprint(flow)
    _succeeded(*_call(manager, kernel_id, flow.flow_id, op, user_id=user.id, snapshot=seed_snapshot(flow)))
    with _lock:
        _sessions.setdefault(flow.flow_id, set()).add(kernel_id)
        _fingerprints[(kernel_id, flow.flow_id)] = fingerprint


def open_session(flow, user, kernel_id: str) -> None:
    """Seed the flow's session in the kernel from the canvas as it is now (replacing any previous one)."""
    _open(_checked(kernel_id, user, flow.flow_id), flow, user, kernel_id, "open")


def reset_session(flow, user, kernel_id: str) -> None:
    """Drop the session's variables and re-seed it from the canvas as it is now."""
    _open(_checked(kernel_id, user, flow.flow_id), flow, user, kernel_id, "reset")


def _rows_hint(lazy_safe: bool, hint: str | None = None) -> str:
    if hint:
        return hint
    canvas = "Run and preview on canvas in the ⋯ menu of the cell that builds it"
    if lazy_safe:
        return f"Rows are not computed here. Use {canvas}, or call display(...) to compute them in this session."
    return f"Rows are not computed here. Use {canvas}."


def _displays(payload: dict[str, Any], title: str = "") -> list[DisplayOutput]:
    """A session display payload as kernel display outputs: its MIME value, JSON-serialised unless text.

    A frame without rows becomes text: its schema, one column per line (none without columns), then where
    its rows come from.
    """
    if payload.get("kind") == "node":
        return [out for name, sub in (payload.get("outputs") or {}).items() for out in _displays(sub, name)]
    for key, value in payload.items():
        if "/" in key:
            data = value if isinstance(value, str) else json.dumps(value, default=str)
            return [DisplayOutput(mime_type=key, data=data, title=title)]
    columns = "".join(f"\n  {c['name']}: {c['data_type']}" for c in payload.get("schema") or [])
    hint = _rows_hint(bool(payload.get("lazy_safe")), payload.get("rows_hint"))
    text = f"Schema:{columns}\n\n{hint}" if columns else hint
    return [DisplayOutput(mime_type="text/plain", data=text, title=title)]


class SessionExecuteResult(ExecuteResult):
    """A session cell's result: the kernel's ``ExecuteResult`` plus the failing line (1-based within the cell)."""

    line: int | None = None


def run_cell(flow, user, kernel_id: str, cell_id: str, code: str) -> SessionExecuteResult:
    """Run one cell in the flow's session on the kernel, opening the session first when there is none.

    A failure's ``error`` is the traceback from the cell down, as the editor shows a kernel error, with its ``line``.
    """
    manager = _checked(kernel_id, user, flow.flow_id)
    payload, raw = _call(manager, kernel_id, flow.flow_id, "execute", cell_id=cell_id, code=code)
    if payload is not None and payload.get("no_session"):
        _open(manager, flow, user, kernel_id, "open")
        payload, raw = _call(manager, kernel_id, flow.flow_id, "execute", cell_id=cell_id, code=code)
    if payload is None:
        error = raw.error or "The notebook kernel returned no result"
        return SessionExecuteResult(**{**raw.model_dump(), "success": False, "error": error})
    ok = bool(payload.get("ok"))
    session_outputs = [out for display in payload.get("displays") or [] for out in _displays(display)]
    return SessionExecuteResult(
        success=ok,
        display_outputs=[*session_outputs, *raw.display_outputs],
        stdout=raw.stdout,
        stderr=raw.stderr,
        error=None if ok else payload.get("traceback") or payload.get("error"),
        line=None if ok else payload.get("line"),
        execution_time_ms=raw.execution_time_ms,
        namespace_generation=payload.get("namespace_generation"),
        revision=payload.get("revision"),
    )


def dataframe_schemas(flow, user, kernel_id: str) -> dict:
    """The frames bound in the flow's session, in the kernel's ``dataframe_schemas`` shape; never opens one."""
    empty = {"namespace_generation": "", "revision": 0, "dataframes": []}
    with _lock:
        if (kernel_id, flow.flow_id) in _running:
            return {**empty, "state": "busy"}
    manager = _checked(kernel_id, user, flow.flow_id)
    payload, _ = _call(manager, kernel_id, flow.flow_id, "schemas")
    if payload is None or not payload.get("ok"):
        return {**empty, "state": "unavailable"}
    frames = [
        {
            "name": name,
            "kind": "LazyFrame",
            "state": "ready",
            "columns": [{"name": c["name"], "dtype": c["data_type"]} for c in columns],
            "truncated": False,
        }
        for name, columns in (payload.get("frames") or {}).items()
    ]
    return {
        "namespace_generation": payload.get("namespace_generation"),
        "revision": payload.get("revision"),
        "state": "ready",
        "dataframes": frames,
    }


def interrupt(flow, user, kernel_id: str) -> dict:
    """Interrupt the call running for this flow on the kernel, if any."""
    if not kernel_sessions_allowed(user):
        raise HTTPException(403, DISABLED_DETAIL)
    manager = _manager()
    if manager.get_kernel_owner(kernel_id) != user.id:
        raise HTTPException(403, "Not authorized to access this kernel")
    with _lock:
        token = _running.get((kernel_id, flow.flow_id))
    if token is None or not manager.interrupt_execution_sync(kernel_id, token):
        return {"status": "no_execution_running"}
    return {"status": "interrupted"}


def close_flow_sessions(flow_id: int) -> None:
    """Close the flow's sessions on every kernel that holds one, in the background; a no-op when there are none."""
    with _lock:
        kernel_ids = _sessions.pop(flow_id, set())
        _results.pop(flow_id, None)
        for kernel_id in kernel_ids:
            _fingerprints.pop((kernel_id, flow_id), None)
    if not kernel_ids:
        return

    def close() -> None:
        for kernel_id in kernel_ids:
            try:
                _call(_manager(), kernel_id, flow_id, "close")
            except Exception:
                logger.debug(f"notebook: could not close the session of flow {flow_id} on {kernel_id}", exc_info=True)
        try:
            shutil.rmtree(_results_dir(_manager(), flow_id), ignore_errors=True)
        except Exception:
            logger.debug(f"notebook: could not remove the canvas results of flow {flow_id}", exc_info=True)

    threading.Thread(target=close, name="notebook-session-close", daemon=True).start()


def _results_dir(manager, flow_id: int) -> str:
    return os.path.join(manager.shared_volume_path, "notebook", str(flow_id))


def forget_kernel(kernel_id: str, shared_dir: str) -> None:
    """Drop core's sessions on a kernel that stopped or was deleted, the canvas results only it used and its
    database copy. Never raises: it runs inside the kernel's stop."""
    try:
        with _lock:
            emptied = []
            for flow_id, kernel_ids in list(_sessions.items()):
                if kernel_id not in kernel_ids:
                    continue
                kernel_ids.discard(kernel_id)
                _fingerprints.pop((kernel_id, flow_id), None)
                if not kernel_ids:
                    del _sessions[flow_id]
                    _results.pop(flow_id, None)
                    emptied.append(flow_id)
            _verified.difference_update({key for key in _verified if key[0] == kernel_id})
        for flow_id in emptied:
            shutil.rmtree(os.path.join(shared_dir, "notebook", str(flow_id)), ignore_errors=True)
        notebook_db.remove(shared_dir, kernel_id)
    except Exception:
        logger.debug(f"notebook: could not forget the sessions on kernel {kernel_id}", exc_info=True)


def _has_result(flow, node) -> bool:
    """Whether ``node`` holds a current canvas result (performance mode never sets the run flag)."""
    if node.results.resulting_data is None or node.results.errors:
        return False
    if flow.flow_settings.execution_mode != "Performance":
        return bool(node.node_stats.has_run_with_current_setup)
    last = flow.latest_run_info
    return last is None or not any(r.node_id == node.node_id and r.skipped for r in last.node_step_result)


def _run_lineage(flow, node, kernel_id: str) -> None:
    """Run ``node`` and its ancestors on the canvas, gate-aware, as "Run and preview on canvas" does.

    409 while the flow runs, when the run would wait on this same kernel, or when a gate routes the node away;
    422 when the run fails.
    """
    from flowfile_core.routes.routes import _resolve_node_kernel_id

    lineage = {node.node_id, *flow._get_upstream_node_ids(node.node_id)}
    running = "The flow is running on the canvas; run the cell again when it has finished"
    if flow.flow_settings.is_running:
        raise HTTPException(409, running)
    waiting = sorted(
        node_id
        for node_id in lineage
        if (upstream := flow.get_node(node_id)) is not None
        and not _has_result(flow, upstream)
        and _resolve_node_kernel_id(upstream) == kernel_id
    )
    if waiting:
        raise HTTPException(
            409,
            f"Node {node.node_id} needs node(s) {', '.join(map(str, waiting))} to run on this notebook's own kernel, "
            f"which is busy with this cell: use Run and preview on canvas for node {node.node_id} first, "
            "or run the notebook on another kernel",
        )
    try:
        run_info = flow.run_graph(node_ids=lineage)
    except Exception as exc:
        if "already running" in str(exc):
            raise HTTPException(409, running) from exc
        raise HTTPException(422, f"Running node {node.node_id} on the canvas failed: {exc}") from exc
    results = {result.node_id: result for result in (run_info.node_step_result if run_info else [])}
    own = results.get(node.node_id)
    if own is not None and own.skipped:
        raise HTTPException(409, f"Node {node.node_id} was routed away by a gate in this run, so it has no rows")
    failed = [n for n in sorted(lineage) if n in results and results[n].success is False]
    problems = [f"  node {n}: {results[n].error}" for n in failed]
    if problems or not _has_result(flow, node):
        detail = "\n".join(problems) or f"  node {node.node_id}: did not run"
        raise HTTPException(422, f"Running node {node.node_id} on the canvas failed:\n{detail}")


def _write_result(flow, node_id: int, data, path: str) -> None:
    """Write a canvas result as parquet: through the worker when offloading, so core never collects it."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
        ExternalDfFetcher,
    )
    from flowfile_core.kernel.execution import _write_parquet_locally

    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not OFFLOAD_TO_WORKER or flow.flow_settings.execution_location == "local":
        _write_parquet_locally(data.data_frame, path)
        return
    fetcher = ExternalDfFetcher(
        flow_id=flow.flow_id,
        node_id=node_id,
        lf=data.data_frame,
        wait_on_completion=True,
        operation_type="write_parquet",
        kwargs={"output_path": path},
    )
    if fetcher.has_error:
        raise HTTPException(422, f"Could not hand node {node_id}'s rows to the kernel: {fetcher.error_description}")


def node_result(kernel_id: str, user, flow_id: int, node_id: int, output_handle: str | None) -> dict:
    """A canvas node's result for the flow's session on ``kernel_id``: ``{"path", "canvas_changed"}``.

    Answers only the owner of a kernel that holds an open session for the flow. The node and its
    ancestors run on the canvas first unless the node holds a current result; the result is written
    as parquet under the kernel's shared folder and reused while it is unchanged. ``path`` is the
    kernel's view of the file; ``canvas_changed`` tells whether the canvas changed since the session
    was seeded. 409 while the flow runs, when the run would need this same kernel, or for a node a
    gate routed away.
    """
    from flowfile_core import flow_file_handler
    from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
    from flowfile_core.notebook.render import code_fingerprint

    if not kernel_sessions_allowed(user):
        raise HTTPException(403, DISABLED_DETAIL)
    with _lock:
        open_here = kernel_id in _sessions.get(flow_id, set())
    if not open_here:
        raise HTTPException(403, f"Kernel '{kernel_id}' has no notebook session open for flow {flow_id}")
    manager = _manager()
    if manager.get_kernel_owner(kernel_id) != user.id:
        raise HTTPException(403, "Not authorized to access this kernel")
    flow = flow_file_handler.get_flow(flow_id, user.id)
    if flow is None:
        raise HTTPException(404, "Flow not found")
    node = flow.get_node(node_id)
    if node is None:
        raise HTTPException(404, f"Node {node_id} is no longer on the canvas: Reset session to pick up the canvas")
    handle = output_handle or DEFAULT_OUTPUT_HANDLE
    if not _has_result(flow, node):
        _run_lineage(flow, node, kernel_id)
    if handle in flow.last_closed_gate_handles.get(node_id, ()):
        raise HTTPException(409, f"Output {handle} of gate {node_id} was routed away in the last run: it has no rows")
    data = node.get_output(handle)
    if data is None:
        raise HTTPException(422, f"Node {node_id} has no result for output {handle}")
    with _lock:
        cached = _results.get(flow_id, {}).get((node_id, handle))
    if cached is not None and cached[0] is data and os.path.exists(cached[1]):
        path = cached[1]
    else:
        path = os.path.join(_results_dir(manager, flow_id), f"{node_id}_{handle}_{node.hash}.parquet")
        _write_result(flow, node_id, data, path)
        with _lock:
            _results.setdefault(flow_id, {})[(node_id, handle)] = (data, path)
    with _lock:
        seeded_with = _fingerprints.get((kernel_id, flow_id))
    return {"path": manager.to_kernel_path(path), "canvas_changed": code_fingerprint(flow) != seeded_with}


class KernelCleanRunner:
    """The ``bridge.CleanRunner`` of a push that names a kernel: the cells run as Python in the kernel."""

    def __init__(self, kernel_id: str, user) -> None:
        self.kernel_id = kernel_id
        self.user = user

    def clean_run(self, user_id: int, flow_id: int, request: CleanRunRequest) -> CleanRunResult:
        manager = _checked(self.kernel_id, self.user, flow_id)
        payload, raw = _call(
            manager, self.kernel_id, flow_id, "clean_run", user_id=user_id, request=request.model_dump(mode="json")
        )
        if payload is None or not payload.get("ok"):
            message = (payload or {}).get("error") or raw.error or "The notebook kernel returned no result"
            return CleanRunResult(error=message, kind="error", traceback=(payload or {}).get("traceback"))
        try:
            result = CleanRunResult.model_validate(payload.get("result"))
        except ValueError:
            return CleanRunResult(error="The notebook kernel returned a result core cannot read", kind="refused")
        result.traceback = payload.get("traceback")
        return result
