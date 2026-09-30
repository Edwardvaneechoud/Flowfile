"""The ``/notebook/*`` router: render and push preview for the canvas notebook. Mounted at ``/notebook``."""

from fastapi import APIRouter, Depends, HTTPException, Query

from flowfile_core import flow_file_handler
from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.notebook.push import NotebookPlanResponse, NotebookPushRequest, plan_push, plan_response
from flowfile_core.notebook.render import NotebookRendering, render
from flowfile_core.routes.custom_node_mounts import require_admin

router = APIRouter()


def require_notebook_sync(current_user=Depends(get_current_active_user)):
    """Gate the routes that sync cells to the canvas (``POST /notebook/plan``, ``POST /editor/notebook/push/``).

    In the multi-user modes (``sharing.sharing_enabled()``: docker and package) only an admin may sync,
    because the frame's catalog lookups a cell can reach do not yet check the requesting user's grants;
    the 403 is ``require_admin``'s and comes before the flow lookup. Electron is never gated. Rendering
    and running stay open to every user.
    """
    if sharing.sharing_enabled():
        require_admin(current_user)
    return current_user


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
def plan_notebook_push(body: NotebookPushRequest, current_user=Depends(require_notebook_sync)) -> NotebookPlanResponse:
    """What ``POST /editor/notebook/push/`` would apply (ops and review warnings), without applying it."""
    flow = flow_file_handler.get_flow(body.flow_id, current_user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return plan_response(*plan_push(flow, current_user, body))
