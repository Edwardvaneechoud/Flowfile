"""The canvas notebook's session subprocess behind the kernel routes, as the pseudo kernel ``flow-session:<flow_id>``.

The notebook UI already talks to kernels through ``/kernels/{kernel_id}/...``; ``kernel/routes.py`` hands
the pseudo ids here before any Docker lookup, so nothing on this path calls ``get_kernel_manager()``.
Sessions are gated like the old session routes: ``notebook_sessions_allowed`` and, in ``electron`` mode,
a loopback caller. The flow must be open in the caller's editor session.
"""

from __future__ import annotations

import ipaddress
import json
import os
import time
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, Request

from flowfile_core.kernel.models import DisplayOutput, ExecuteRequest, ExecuteResult, KernelInfo, KernelState
from flowfile_core.notebook import registry
from flowfile_core.notebook.gate import notebook_sessions_allowed

PREFIX = "flow-session:"
_STATES = {"ready": KernelState.IDLE, "busy": KernelState.EXECUTING, "dead": KernelState.ERROR}


def is_flow_session(kernel_id: str) -> bool:
    return kernel_id.startswith(PREFIX)


def _loopback(http: Request) -> bool:
    host = http.client.host if http.client is not None else ""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _flow(kernel_id: str, http: Request, user) -> Any:
    """The open flow behind ``kernel_id`` after the mode, loopback and ownership gates."""
    from flowfile_core import flow_file_handler

    if not notebook_sessions_allowed(user):
        raise HTTPException(status_code=403, detail="Notebook sessions are disabled on this server")
    if os.environ.get("FLOWFILE_MODE", "electron") == "electron" and not _loopback(http):
        raise HTTPException(status_code=403, detail="Notebook sessions only accept local connections")
    try:
        flow_id = int(kernel_id[len(PREFIX) :])
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=f"Kernel '{kernel_id}' not found") from exc
    flow = flow_file_handler.get_flow(flow_id, user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return flow


def _session(kernel_id: str, http: Request, user) -> registry.NotebookSession:
    flow = _flow(kernel_id, http, user)
    sessions = registry.get_registry()
    existing = sessions.find(user.id, flow.flow_id)
    return existing or sessions.open(user.id, flow.flow_id, registry.seed_snapshot(flow))


def _displays(payload: dict[str, Any], title: str = "") -> list[DisplayOutput]:
    """A session display payload as kernel display outputs: its MIME value, JSON-serialised unless text."""
    if payload.get("kind") == "node":
        return [out for name, sub in (payload.get("outputs") or {}).items() for out in _displays(sub, name)]
    for key, value in payload.items():
        if "/" in key:
            data = value if isinstance(value, str) else json.dumps(value, default=str)
            return [DisplayOutput(mime_type=key, data=data, title=title)]
    columns = ", ".join(f"{c['name']}: {c['data_type']}" for c in payload.get("schema") or [])
    return [DisplayOutput(mime_type="text/plain", data=f"Schema: {columns}", title=title)]


def info(kernel_id: str, http: Request, user) -> KernelInfo:
    """A synthetic kernel for the session; reading it starts the session, so it warms up while the notebook opens."""
    session = _session(kernel_id, http, user)
    return KernelInfo(id=kernel_id, name="Flow session", state=_STATES.get(session.state, KernelState.STARTING))


def execute(kernel_id: str, body: ExecuteRequest, http: Request, user) -> ExecuteResult:
    """Run one cell and wait for it; ``body.node_id`` is only provenance (0 for a cell without a node)."""
    session = _session(kernel_id, http, user)
    started = time.monotonic()
    provenance = [["canvas", body.node_id]] if body.node_id else None
    cell_id = f"node-{body.node_id}" if body.node_id else f"scratch-{uuid4().hex}"
    result = session.run_cell(cell_id, body.code, provenance)
    stderr = result["stderr"] + (result["traceback"] or "") if not result["ok"] else result["stderr"]
    return ExecuteResult(
        success=result["ok"],
        display_outputs=[out for payload in result["displays"] for out in _displays(payload)],
        stdout=result["stdout"],
        stderr=stderr,
        error=result["error"],
        execution_time_ms=round((time.monotonic() - started) * 1000, 1),
        namespace_generation=session.namespace_generation,
        revision=session.revision,
    )


def clear_namespace(kernel_id: str, http: Request, user) -> dict:
    """Drop the session's variables and re-seed it from the canvas as it is now."""
    flow = _flow(kernel_id, http, user)
    session = registry.get_registry().find(user.id, flow.flow_id)
    if session is not None:
        session.reset(registry.seed_snapshot(flow))
    return {"status": "cleared", "kernel_id": kernel_id, "flow_id": flow.flow_id}


def dataframe_schemas(kernel_id: str, user) -> dict:
    """The frames bound in a running session, in the kernel's ``dataframe_schemas`` shape; never starts one."""
    try:
        flow_id = int(kernel_id[len(PREFIX) :])
    except ValueError:
        return {}
    session = registry.get_registry().find(user.id, flow_id)
    if session is None or session.state == "starting":
        return {}
    stamp = {"namespace_generation": session.namespace_generation, "revision": session.revision}
    if session.busy:
        return {**stamp, "state": "busy"}
    frames = [
        {
            "name": name,
            "kind": "LazyFrame",
            "state": "ready",
            "columns": [{"name": c["name"], "dtype": c["data_type"]} for c in columns],
        }
        for name, columns in session.schemas().items()
    ]
    return {**stamp, "state": "ready", "dataframes": frames}
