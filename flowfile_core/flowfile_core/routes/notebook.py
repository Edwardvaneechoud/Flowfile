"""The ``/notebook/*`` router: status, render and push preview for the canvas notebook.

Mounted at ``/notebook``. ``GET /notebook/status`` tells the frontend whether this user may start a notebook
session. Sessions themselves are driven through the kernel routes as the pseudo kernel
``flow-session:<flow_id>`` (``notebook/kernel_adapter.py``).
"""

import os

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.notebook import bridge as _bridge
from flowfile_core.notebook.gate import notebook_sessions_allowed
from flowfile_core.notebook.push import NotebookPlanResponse, NotebookPushRequest, plan_push, plan_response
from flowfile_core.notebook.render import NotebookRendering, render

router = APIRouter()


class NotebookStatus(BaseModel):
    sessions: bool


@router.get("/status", response_model=NotebookStatus)
def notebook_status(current_user=Depends(get_current_active_user)) -> NotebookStatus:
    return NotebookStatus(sessions=notebook_sessions_allowed(current_user))


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
