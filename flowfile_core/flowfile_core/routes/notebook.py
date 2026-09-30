"""The ``/notebook/*`` router: render, push preview and the kernel session of the canvas notebook.

Mounted at ``/notebook``. The ``/notebook/session/*`` routes run cells on one of the caller's notebook
kernels (``notebook.kernel_runner``); they are gated by ``notebook.gate`` (403) before the flow lookup (404).
``/notebook/session/node_result`` is the kernel's own call back for a canvas node's rows.
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

from flowfile_core import flow_file_handler
from flowfile_core.auth import sharing
from flowfile_core.auth.jwt import get_current_active_user, get_user_or_internal_service
from flowfile_core.notebook import kernel_runner
from flowfile_core.notebook.gate import kernel_sessions_allowed, require_kernel_sessions
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
def plan_notebook_push(
    body: NotebookPushRequest, http: Request, current_user=Depends(require_notebook_sync)
) -> NotebookPlanResponse:
    """What ``POST /editor/notebook/push/`` would apply (ops and review warnings), without applying it."""
    if body.kernel_id is not None:
        require_kernel_sessions(http, current_user)
    flow = flow_file_handler.get_flow(body.flow_id, current_user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return plan_response(*plan_push(flow, current_user, body))


class NotebookSessionRequest(BaseModel):
    """Body of the ``/notebook/session/*`` routes: the open flow and the notebook kernel its session runs on."""

    flow_id: int
    kernel_id: str


class NotebookSessionExecuteRequest(NotebookSessionRequest):
    """Body of ``POST /notebook/session/execute``; ``node_id`` is the cell's node, 0 for a cell without one."""

    cell_id: str = Field(pattern=r"^[\w.:\-]{1,128}$")
    code: str
    node_id: int = 0


def _session_flow(body: NotebookSessionRequest, http: Request, user):
    require_kernel_sessions(http, user)
    flow = flow_file_handler.get_flow(body.flow_id, user.id)
    if flow is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return flow


@router.get("/status")
def notebook_status(current_user=Depends(get_current_active_user)) -> dict:
    """Whether the caller may run the notebook on a kernel (``kernel_sessions``)."""
    return {"kernel_sessions": kernel_sessions_allowed(current_user)}


@router.post("/session/open")
def open_notebook_session(
    body: NotebookSessionRequest, http: Request, current_user=Depends(get_current_active_user)
) -> dict:
    """Seed the flow's session on the kernel from the canvas as it is now."""
    kernel_runner.open_session(_session_flow(body, http, current_user), current_user, body.kernel_id)
    return {"status": "ready"}


@router.post("/session/execute", response_model=kernel_runner.SessionExecuteResult)
def execute_notebook_cell(
    body: NotebookSessionExecuteRequest, http: Request, current_user=Depends(get_current_active_user)
) -> kernel_runner.SessionExecuteResult:
    """Run one cell as Python in the flow's session on the kernel (opened first when there is none)."""
    flow = _session_flow(body, http, current_user)
    return kernel_runner.run_cell(flow, current_user, body.kernel_id, body.cell_id, body.code)


@router.post("/session/reset")
def reset_notebook_session(
    body: NotebookSessionRequest, http: Request, current_user=Depends(get_current_active_user)
) -> dict:
    """Drop the session's variables and re-seed it from the canvas as it is now."""
    kernel_runner.reset_session(_session_flow(body, http, current_user), current_user, body.kernel_id)
    return {"status": "cleared"}


@router.post("/session/schemas")
def notebook_session_schemas(
    body: NotebookSessionRequest, http: Request, current_user=Depends(get_current_active_user)
) -> dict:
    """The frames bound in the flow's session, for column completions (the kernel's ``dataframe_schemas`` shape)."""
    flow = _session_flow(body, http, current_user)
    return kernel_runner.dataframe_schemas(flow, current_user, body.kernel_id)


@router.post("/session/interrupt")
def interrupt_notebook_session(
    body: NotebookSessionRequest, http: Request, current_user=Depends(get_current_active_user)
) -> dict:
    """Interrupt the cell the flow's session is running on the kernel."""
    return kernel_runner.interrupt(_session_flow(body, http, current_user), current_user, body.kernel_id)


class NotebookNodeResultRequest(BaseModel):
    """Body of ``POST /notebook/session/node_result``: a canvas node (and output) of the session's flow."""

    flow_id: int
    node_id: int
    output_handle: str | None = Field(default=None, pattern=r"^output-\d{1,4}$")


@router.post("/session/node_result")
def notebook_node_result(
    body: NotebookNodeResultRequest,
    x_kernel_id: str | None = Header(None, alias="X-Kernel-Id"),
    current_user=Depends(get_user_or_internal_service),
) -> dict:
    """A canvas node's rows for the notebook session on the calling kernel: ``{"path", "canvas_changed"}``.

    Called by the kernel (``X-Internal-Token`` + ``X-Kernel-Id``, resolving to the kernel's owner). Runs
    the node's lineage on the canvas when it has no current result, synchronously, then answers with
    the kernel's path to the result as parquet on its shared folder.
    """
    if not x_kernel_id:
        raise HTTPException(status_code=403, detail="Only a notebook kernel can ask for a node's rows")
    return kernel_runner.node_result(x_kernel_id, current_user, body.flow_id, body.node_id, body.output_handle)
