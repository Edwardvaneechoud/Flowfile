"""Core's side of the canvas notebook session that runs in a notebook kernel (one with ``flowfile`` installed).

Core never runs the cells. Every op is one constant :data:`SNIPPET` sent through ``KernelManager.execute_sync``
under the flow's own kernel namespace (:func:`kernel_flow_id`), where the editor's code intelligence also reads
the session's variables. The frame's ``notebook_kernel`` module runs the op and prints its JSON result on a
``shared.notebook_display.KERNEL_RESULT_MARKER`` line, which :func:`_call` cuts from the call's stdout; results
come back in the kernel routes' own shapes. Besides the session ops, :class:`KernelCleanRunner` runs a push that
names a kernel (its result then goes through ``notebook.validate``), and the kernel calls back for canvas rows it
cannot compute (:func:`node_result`) and for a fresh copy of the catalog database (:func:`refresh_database`).
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import threading
import weakref
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from flowfile_core.configs import logger
from flowfile_core.kernel import notebook_db
from flowfile_core.kernel.models import DisplayOutput, ExecuteRequest, ExecuteResult
from flowfile_core.kernel.notebook_support import is_notebook_kernel_config
from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult
from flowfile_core.notebook.gate import DISABLED_DETAIL, kernel_sessions_allowed
from flowfile_core.notebook.push import known_schemas, seed_snapshot
from shared.notebook_display import KERNEL_RESULT_MARKER

SNIPPET = "from flowfile_frame import notebook_kernel as _nb\n_nb.handle({request!r}, globals())\n"

_lock = threading.Lock()
_running: dict[tuple[str, int], str] = {}
_sessions: dict[int, dict[str, str | None]] = {}
_verified: set[tuple[str, str | None]] = set()
_fingerprints: dict[tuple[str, int], str] = {}
_schemas_sent: dict[tuple[str, int], dict[int, dict]] = {}
_results: dict[int, dict[tuple[int, str], tuple[weakref.ref, str, str]]] = {}


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
    """The kernel namespace of a flow's session, below every catalog notebook's and real flow's; mirrored by the
    notebook store's ``flowSessionId``."""
    return -(1 << 40) - flow_id


def _call(
    manager, kernel_id: str, flow_id: int, op: str, *, node_id: int = 0, track: bool = True, **fields: Any
) -> tuple[dict | None, ExecuteResult]:
    """Run one op in the kernel; its result (``None`` when none came back) and the call with that line cut out.

    ``node_id`` is the node the kernel runs the call as, whose artifacts it clears first: a cell's own for
    ``execute``, else 0. A ``track``ed call is the flow's running call on the kernel, for :func:`interrupt` and
    :func:`dataframe_schemas`.
    """
    request = ExecuteRequest(
        node_id=node_id,
        flow_id=kernel_flow_id(flow_id),
        code=SNIPPET.format(request=json.dumps({"op": op, "flow_id": flow_id, **fields}, default=str)),
        exec_token=uuid4().hex,
    )
    key = (kernel_id, flow_id)
    if track:
        with _lock:
            _running[key] = request.exec_token
    try:
        raw = manager.execute_sync(kernel_id, request)
    except Exception as exc:
        raise HTTPException(502, f"The notebook kernel call failed: {exc}") from exc
    finally:
        if track:
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


def _notebook_kernel(kernel_id: str, user):
    """The manager and ``kernel_id``'s kernel, after the mode gate and the kernel's owner and packages."""
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
    return manager, kernel


def _checked(kernel_id: str, user, flow_id: int):
    """The manager, after :func:`_notebook_kernel` and (once per container) the kernel's flowfile version and the
    Alembic head of its flowfile_core, which must be the catalog's revision (a dev image keeps the version string
    across migrations)."""
    from shared._version import get_version

    manager, kernel = _notebook_kernel(kernel_id, user)
    key = (kernel_id, kernel.container_id)
    if key not in _verified:
        hello = _succeeded(*_call(manager, kernel_id, flow_id, "hello"))
        if hello.get("version") != get_version():
            raise HTTPException(
                409,
                f"Kernel '{kernel_id}' has flowfile {hello.get('version')} and this app is {get_version()}: "
                "recreate the notebook kernel",
            )
        head, core_revision = hello.get("schema_head"), _schema_revision()
        if head and core_revision and head != core_revision:
            raise HTTPException(
                409,
                f"Kernel '{kernel_id}' has flowfile for database schema {head}, the catalog is at {core_revision}: "
                "recreate the notebook kernel",
            )
        _verified.add(key)
    return manager


def refresh_database(kernel_id: str, user) -> dict:
    """The kernel's own call before its first database connection in a call: its catalog copy brought up to date.

    Answers ``{"path"}``, the copy as the kernel sees it (``None`` without a SQLite file catalog): a new file
    whenever the catalog changed, which the kernel's next connections open. It never calls the kernel, so it is
    safe while a session call holds the kernel's execution lock.
    """
    manager, _ = _notebook_kernel(kernel_id, user)
    try:
        target = notebook_db.refresh(manager.shared_volume_path, kernel_id)
    except Exception as exc:
        raise HTTPException(502, f"Could not copy the catalog database for the notebook kernel: {exc}") from exc
    return {"path": None if target is None else manager.to_kernel_path(str(target))}


def _result_schemas(flow) -> dict[int, dict]:
    """The output schemas of every canvas node that holds a current result, as its run left them
    (``push.known_schemas``): nothing is predicted, so this is safe on every call and while the flow runs."""
    found: dict[int, dict] = {}
    for node in flow.nodes:
        try:
            schemas = known_schemas(node) if _has_result(flow, node) else None
        except Exception:
            continue
        if schemas:
            found[node.node_id] = schemas
    return found


def _session_call(manager, flow, kernel_id: str, op: str, **fields: Any) -> tuple[dict | None, ExecuteResult]:
    """An ``execute`` or ``schemas`` op, carrying the schemas of the canvas nodes that ran since the session last
    heard of them, so its frames know the columns the canvas found. A session is seeded before the canvas runs
    what it builds: a script's columns are known only once the canvas has run it."""
    key = (kernel_id, flow.flow_id)
    current = _result_schemas(flow)
    with _lock:
        sent = _schemas_sent.get(key, {})
        fresh = {node_id: schemas for node_id, schemas in current.items() if sent.get(node_id) != schemas}
    if fresh:
        fields["schemas"] = fresh
    payload, raw = _call(manager, kernel_id, flow.flow_id, op, **fields)
    if fresh and payload is not None and not payload.get("no_session"):
        with _lock:
            _schemas_sent.setdefault(key, {}).update(fresh)
    return payload, raw


def _open(manager, flow, user, kernel_id: str, op: str) -> None:
    from flowfile_core.notebook.render import code_fingerprint

    fingerprint = code_fingerprint(flow)
    seeded_with = _result_schemas(flow)
    opened = _succeeded(*_call(manager, kernel_id, flow.flow_id, op, user_id=user.id, snapshot=seed_snapshot(flow)))
    with _lock:
        _sessions.setdefault(flow.flow_id, {})[kernel_id] = opened.get("namespace_generation")
        _fingerprints[(kernel_id, flow.flow_id)] = fingerprint
        _schemas_sent[(kernel_id, flow.flow_id)] = seeded_with


def open_session(flow, user, kernel_id: str) -> None:
    """Seed the flow's session in the kernel from the canvas as it is now (replacing any previous one)."""
    _open(_checked(kernel_id, user, flow.flow_id), flow, user, kernel_id, "open")


def reset_session(flow, user, kernel_id: str) -> None:
    """Drop the session's variables and re-seed it from the canvas as it is now."""
    _open(_checked(kernel_id, user, flow.flow_id), flow, user, kernel_id, "reset")


def _rows_hint(lazy_safe: bool) -> str:
    canvas = "Run and preview on canvas in the ⋯ menu of the cell that builds it"
    if lazy_safe:
        return f"Rows are not computed here. Use {canvas}, or call display(...) to compute them in this session."
    return f"Rows are not computed here. Use {canvas}."


def _displays(payload: dict[str, Any], title: str = "") -> list[DisplayOutput]:
    """A session display payload as kernel display outputs: its MIME value, JSON-serialised unless text, under the
    payload's own ``title`` when it has one.

    A frame without rows becomes text: its schema, one column per line (none without columns), then where
    its rows come from.
    """
    if payload.get("kind") == "node":
        return [out for name, sub in (payload.get("outputs") or {}).items() for out in _displays(sub, name)]
    for key, value in payload.items():
        if "/" in key:
            data = value if isinstance(value, str) else json.dumps(value, default=str)
            return [DisplayOutput(mime_type=key, data=data, title=payload.get("title") or title)]
    columns = "".join(f"\n  {c['name']}: {c['data_type']}" for c in payload.get("schema") or [])
    hint = payload.get("rows_hint") or _rows_hint(bool(payload.get("lazy_safe")))
    text = f"Schema:{columns}\n\n{hint}" if columns else hint
    return [DisplayOutput(mime_type="text/plain", data=text, title=title)]


class SessionExecuteResult(ExecuteResult):
    """A session cell's result: the kernel's ``ExecuteResult`` plus the failing line (1-based within the cell)."""

    line: int | None = None


def run_cell(flow, user, kernel_id: str, cell_id: str, code: str, node_id: int = 0) -> SessionExecuteResult:
    """Run one cell as node ``node_id`` in the flow's session on the kernel.

    The session is opened first when core has none open there or the kernel lost it, so a cell never runs in a
    session a queued close is about to end. Displays come in the cell's order, then what the kernel shows by
    itself (end-of-cell figures, a direct ``flowfile_ctx.display``); a failure's ``error`` is the traceback from
    the cell down, with its ``line``.
    """
    manager = _checked(kernel_id, user, flow.flow_id)
    with _lock:
        registered = kernel_id in _sessions.get(flow.flow_id, {})
    if not registered:
        _open(manager, flow, user, kernel_id, "open")
    payload, raw = _session_call(manager, flow, kernel_id, "execute", node_id=node_id, cell_id=cell_id, code=code)
    if payload is not None and payload.get("no_session"):
        _open(manager, flow, user, kernel_id, "open")
        payload, raw = _session_call(manager, flow, kernel_id, "execute", node_id=node_id, cell_id=cell_id, code=code)
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
    payload, _ = _session_call(manager, flow, kernel_id, "schemas")
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
    """Interrupt the call running for this flow on the kernel, if any, and cancel the canvas run it waits on.

    The kernel's interrupt cannot reach a cell blocked reading core's answer to its canvas fallback, so that
    run, the one holding this kernel (:func:`_run_lineage`), is cancelled here; the fallback then answers and
    the cell stops.
    """
    if not kernel_sessions_allowed(user):
        raise HTTPException(403, DISABLED_DETAIL)
    manager = _manager()
    if manager.get_kernel_owner(kernel_id) != user.id:
        raise HTTPException(403, "Not authorized to access this kernel")
    with _lock:
        token = _running.get((kernel_id, flow.flow_id))
    interrupted = token is not None and manager.interrupt_execution_sync(kernel_id, token)
    hold = flow._kernel_hold
    if hold is not None and kernel_id in hold.kernel_ids:
        flow.cancel()
        interrupted = True
    return {"status": "interrupted" if interrupted else "no_execution_running"}


def close_flow_sessions(flow_id: int) -> None:
    """Close the flow's sessions on every kernel that holds one and remove its canvas results, in the background; a
    no-op when there are neither."""
    with _lock:
        sessions = _sessions.pop(flow_id, {})
        handed = _results.pop(flow_id, None)
        for kernel_id in sessions:
            _fingerprints.pop((kernel_id, flow_id), None)
            _schemas_sent.pop((kernel_id, flow_id), None)
    if not sessions and not handed:
        return
    threading.Thread(
        target=_close_in_kernels, args=(flow_id, sessions), name="notebook-session-close", daemon=True
    ).start()


def _close_in_kernels(flow_id: int, sessions: dict[str, str | None]) -> None:
    """Close each kernel's session of the flow by its generation, then remove the flow's canvas results.

    The flow may have been opened again by the time a kernel takes the call: that kernel keeps the newer session,
    and the results stay while any session of the flow is open.
    """
    for kernel_id, generation in sessions.items():
        try:
            _call(_manager(), kernel_id, flow_id, "close", track=False, generation=generation)
        except Exception:
            logger.debug(f"notebook: could not close the session of flow {flow_id} on {kernel_id}", exc_info=True)
    try:
        results = _results_dir(_manager(), flow_id)
        with _lock:
            if flow_id not in _sessions:
                shutil.rmtree(results, ignore_errors=True)
    except Exception:
        logger.debug(f"notebook: could not remove the canvas results of flow {flow_id}", exc_info=True)


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
                del kernel_ids[kernel_id]
                _fingerprints.pop((kernel_id, flow_id), None)
                _schemas_sent.pop((kernel_id, flow_id), None)
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


def lineage_commits(flow, node_ids) -> bool:
    """Whether a run of only ``node_ids`` commits its sources' progress (a change-feed cursor, a Kafka offset).

    Only when it writes, which is when it holds an output node: rows written without the commit would be written
    again by the flow's next run, and rows only looked at must leave the progress for that run.
    """
    templates = (node.node_template for node in map(flow.get_node, node_ids) if node is not None)
    return any(t.node_group == "output" or (t.custom_node and t.node_type == "output") for t in templates)


def _own_kernel_detail(node, kernel_id: str, needed: str) -> str:
    """The 409 detail when ``node``'s rows need ``needed`` to run on the session's own kernel, with the ways out.

    Run and preview on canvas runs them while the kernel is free; the node then holds a current result, which
    the fallback hands over without running anything.
    """
    return (
        f"Node {node.node_id} needs {needed} to run on this notebook's own kernel '{kernel_id}', which is busy "
        f"with this cell: use Run and preview on canvas for node {node.node_id} first, or run the notebook on "
        "another kernel"
    )


def _run_lineage(flow, node, kernel_id: str) -> None:
    """Run ``node`` and its ancestors on the canvas, gate-aware, as "Run and preview on canvas" does.

    The session's kernel call holds ``kernel_id`` until this returns, so nothing in the run may execute on it: a
    node on it without a current result is refused up front, and one that runs anyway (Performance mode, a missing
    cache, a changed source, a subflow, a virtual table's producer) is refused at once by the run's ``KernelHold``,
    which also marks the run for :func:`interrupt`. The run commits its sources' progress only when it writes
    (:func:`lineage_commits`): showing rows never moves a change-feed cursor or a Kafka offset.

    409 while the flow runs, when the run needs this kernel, when it was cancelled, or when a gate routes the node
    away; 422 when the run fails.
    """
    from flowfile_core.kernel.execution import KernelHold
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
        raise HTTPException(409, _own_kernel_detail(node, kernel_id, f"node(s) {', '.join(map(str, waiting))}"))
    hold = KernelHold({kernel_id})
    try:
        run_info = flow.run_graph(node_ids=lineage, kernel_hold=hold, commit_sources=lineage_commits(flow, lineage))
    except Exception as exc:
        if "already running" in str(exc):
            raise HTTPException(409, running) from exc
        raise HTTPException(422, f"Running node {node.node_id} on the canvas failed: {exc}") from exc
    if flow.flow_settings.is_canceled:
        raise HTTPException(409, f"The canvas run for node {node.node_id} was cancelled")
    if hold.refusals:
        here = sorted({node_id for flow_id, node_id in hold.refusals if flow_id == flow.flow_id})
        parts = [f"node(s) {', '.join(map(str, here))}"] if here else []
        if any(flow_id != flow.flow_id for flow_id, _ in hold.refusals):
            parts.append("nodes of a flow it runs (a subflow or a virtual table's producer)")
        raise HTTPException(409, _own_kernel_detail(node, kernel_id, " and ".join(parts)))
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
    from flowfile_core.kernel.execution import write_parquet_for_kernel

    local = flow.flow_settings.execution_location == "local"
    try:
        write_parquet_for_kernel(
            data.data_frame, path, flow_id=flow.flow_id, node_id=node_id, local=local, what=f"node {node_id}'s rows"
        )
    except RuntimeError as exc:
        raise HTTPException(422, f"Could not hand node {node_id}'s rows to the kernel: {exc}") from exc


def node_result(kernel_id: str, user, flow_id: int, node_id: int, output_handle: str | None) -> dict:
    """A canvas node's rows for the flow's session on ``kernel_id``: ``{"path", "canvas_changed"}``.

    Only for the kernel's owner while the kernel holds the flow's session or runs a call for it (a push's clean
    run). ``path`` is the kernel's view of a parquet in its shared folder, written after :func:`_run_lineage`
    when the node has no current result (409/422 as there, and 409 for a gate output routed away);
    ``canvas_changed`` is whether the canvas changed since the session was seeded. The cache holds the result
    weakly, so it never keeps one alive, and removes a superseded file only when this kernel was handed it:
    another kernel's session may still read it.
    """
    from flowfile_core import flow_file_handler
    from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
    from flowfile_core.notebook.render import code_fingerprint

    if not kernel_sessions_allowed(user):
        raise HTTPException(403, DISABLED_DETAIL)
    with _lock:
        open_here = kernel_id in _sessions.get(flow_id, {}) or (kernel_id, flow_id) in _running
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
    if cached is not None and cached[0]() is data and os.path.exists(cached[1]):
        path = cached[1]
    else:
        path = os.path.join(_results_dir(manager, flow_id), f"{node_id}_{handle}_{uuid4().hex}.parquet")
        _write_result(flow, node_id, data, path)
        with _lock:
            superseded = _results.setdefault(flow_id, {}).get((node_id, handle))
            _results[flow_id][(node_id, handle)] = (weakref.ref(data), path, kernel_id)
        if superseded is not None and superseded[2] == kernel_id:
            with contextlib.suppress(OSError):
                os.remove(superseded[1])
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

    def host_folders(self) -> dict[str, str]:
        """Kernel folder -> host folder of this kernel (``KernelManager.host_folders``), for ``notebook.validate``."""
        return _manager().host_folders(self.kernel_id)
