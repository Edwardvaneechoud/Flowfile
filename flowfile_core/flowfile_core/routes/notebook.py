"""The ``/notebook/*`` router: render and push preview for the canvas notebook. Mounted at ``/notebook``."""

from fastapi import APIRouter, Depends, HTTPException, Query

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.notebook.push import NotebookPlanResponse, NotebookPushRequest, plan_push, plan_response
from flowfile_core.notebook.render import NotebookRendering, render

router = APIRouter()


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


@router.post("/plan", response_model=NotebookPlanResponse)
def plan_notebook_push(
    body: NotebookPushRequest, current_user=Depends(get_current_active_user)
) -> NotebookPlanResponse:
    """What ``POST /editor/notebook/push/`` would apply (ops and review warnings), without applying it."""
    flow = flow_file_handler.get_flow(body.flow_id, current_user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return plan_response(*plan_push(flow, current_user, body))
