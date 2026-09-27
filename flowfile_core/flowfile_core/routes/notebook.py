"""The ``/notebook/*`` router: every route is gated by ``FEATURE_FLAG_CANVAS_NOTEBOOK`` (503 when off).

Mounted at ``/notebook``. A 503 from ``GET /notebook/status`` is how the frontend learns the flag is off;
a 200 says it is on and whether this user may start a notebook session.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.notebook.gate import (
    is_canvas_notebook_enabled,
    notebook_sessions_allowed,
    require_canvas_notebook_enabled,
)
from flowfile_core.notebook.render import NotebookRendering, render

router = APIRouter(dependencies=[Depends(require_canvas_notebook_enabled)])


class NotebookStatus(BaseModel):
    canvas_notebook: bool
    sessions: bool


@router.get("/status", response_model=NotebookStatus)
def notebook_status(current_user=Depends(get_current_active_user)) -> NotebookStatus:
    return NotebookStatus(
        canvas_notebook=is_canvas_notebook_enabled(), sessions=notebook_sessions_allowed(current_user)
    )


@router.get("/render", response_model=NotebookRendering)
def render_notebook(flow_id: int = Query(...), current_user=Depends(get_current_active_user)) -> NotebookRendering:
    """Render an open flow as notebook cells.

    Open to every authenticated user for flows in their own editor session, in every mode; a flow that is
    not open, or open only by another user, is a 404.
    """
    flow = flow_file_handler.get_flow(flow_id, current_user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return render(flow)


# sessions
import ipaddress  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402

from fastapi import Request  # noqa: E402
from fastapi.concurrency import run_in_threadpool  # noqa: E402
from fastapi.responses import StreamingResponse  # noqa: E402

from flowfile_core.notebook import registry as session_registry  # noqa: E402

_LIVE_POLL_SECONDS = 1.0


class SessionOpen(BaseModel):
    flow_id: int


class SessionState(BaseModel):
    session_id: str
    state: str


class SessionExecute(BaseModel):
    cell_id: str
    code: str
    provenance: list[tuple[str, int]] | None = None


class SessionTicket(BaseModel):
    ticket: str


def _loopback(request: Request) -> bool:
    host = request.client.host if request.client is not None else ""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _require_session_caller(request: Request, current_user) -> None:
    """403 unless the mode lets this user run sessions; in electron mode the caller must also be on loopback."""
    if not notebook_sessions_allowed(current_user):
        raise HTTPException(status_code=403, detail="Notebook sessions are disabled on this server")
    if os.environ.get("FLOWFILE_MODE", "electron") == "electron" and not _loopback(request):
        raise HTTPException(status_code=403, detail="Notebook sessions only accept local connections")


def _owned_session(session_id: str, request: Request, current_user) -> session_registry.NotebookSession:
    _require_session_caller(request, current_user)
    session = session_registry.get_registry().get(session_id, current_user.id)
    if session is None:
        raise HTTPException(status_code=404, detail="Notebook session not found")
    return session


@router.post("/sessions", response_model=SessionState)
def open_session(body: SessionOpen, request: Request, current_user=Depends(get_current_active_user)) -> SessionState:
    """Start (or return) the caller's session for an open flow, seeded from the live canvas."""
    _require_session_caller(request, current_user)
    flow = flow_file_handler.get_flow(body.flow_id, current_user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    registry = session_registry.get_registry()
    existing = next(
        (s for s in registry.sessions() if s.user_id == current_user.id and s.flow_id == body.flow_id), None
    )
    snapshot = None if existing is not None and existing.alive() else session_registry.seed_snapshot(flow)
    session = registry.open(current_user.id, body.flow_id, snapshot)
    return SessionState(session_id=session.session_id, state=session.state)


@router.post("/sessions/{session_id}/execute", response_model=SessionTicket)
def execute_in_session(
    session_id: str, body: SessionExecute, request: Request, current_user=Depends(get_current_active_user)
) -> SessionTicket:
    """Queue a cell; its output and ``done`` arrive on the events stream under the returned ticket."""
    session = _owned_session(session_id, request, current_user)
    return SessionTicket(ticket=session.execute(body.cell_id, body.code, body.provenance))


@router.post("/sessions/{session_id}/interrupt", response_model=SessionState)
def interrupt_session(session_id: str, request: Request, current_user=Depends(get_current_active_user)) -> SessionState:
    session = _owned_session(session_id, request, current_user)
    session.interrupt()
    return SessionState(session_id=session.session_id, state=session.state)


@router.post("/sessions/{session_id}/reset", response_model=SessionTicket)
def reset_session(session_id: str, request: Request, current_user=Depends(get_current_active_user)) -> SessionTicket:
    """Drop every variable and re-seed from the canvas as it is now."""
    session = _owned_session(session_id, request, current_user)
    flow = flow_file_handler.get_flow(session.flow_id, current_user.id)
    snapshot = session_registry.seed_snapshot(flow) if flow is not None else None
    return SessionTicket(ticket=session.reset(snapshot))


@router.get("/sessions/{session_id}/schemas")
def session_schemas(session_id: str, request: Request, current_user=Depends(get_current_active_user)) -> dict:
    """Columns of every frame bound in the session, for completions."""
    session = _owned_session(session_id, request, current_user)
    return {"frames": session.schemas()}


@router.get("/sessions/{session_id}/events")
async def session_events(
    session_id: str,
    request: Request,
    after: int = Query(0, ge=0),
    follow: bool = Query(True),
    current_user=Depends(get_current_active_user),
) -> StreamingResponse:
    """Newline-delimited JSON events after ``after``: the buffered replay, then live until the session closes.

    Authenticated by the ``Authorization`` header only (a token in the URL would land in access logs).
    ``follow=false`` returns the replay and ends.
    """
    session = _owned_session(session_id, request, current_user)

    async def stream():
        cursor = after
        for event in session.events_after(cursor):
            cursor = event["seq"]
            yield json.dumps(event, default=str) + "\n"
        while follow:
            if await request.is_disconnected():
                return
            events = await run_in_threadpool(session.wait_events, cursor, _LIVE_POLL_SECONDS)
            for event in events:
                cursor = event["seq"]
                yield json.dumps(event, default=str) + "\n"
            if session.closed and not events:
                return

    return StreamingResponse(stream(), media_type="application/x-ndjson")


# push preview
from flowfile_core.notebook import bridge as _bridge  # noqa: E402
from flowfile_core.notebook.push import (  # noqa: E402
    NotebookPlanResponse,
    NotebookPushRequest,
    plan_push,
    plan_response,
)

if os.environ.get("FLOWFILE_NOTEBOOK_INPROCESS_CLEAN_RUN") == "1":
    _bridge.set_clean_runner(_bridge.InProcessCleanRunner())


@router.post("/plan", response_model=NotebookPlanResponse)
def plan_notebook_push(
    body: NotebookPushRequest, current_user=Depends(get_current_active_user)
) -> NotebookPlanResponse:
    """What ``POST /editor/notebook/push/`` would apply (ops and review warnings), without applying it."""
    flow = flow_file_handler.get_flow(body.flow_id, current_user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return plan_response(*plan_push(flow, current_user, body))
