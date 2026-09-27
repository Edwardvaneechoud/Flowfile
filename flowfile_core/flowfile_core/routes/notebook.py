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
