"""REST API for standalone (flow-independent) Python notebooks.

Notebooks are persisted as ``.ipynb`` files per user. Cell *execution* is not
handled here — the frontend runs cells through the existing ``/kernels`` API,
using each notebook's ``flow_id`` as the kernel namespace key and ``node_id=0``
(so the kernel skips upstream-path resolution).
"""

import logging

from fastapi import APIRouter, Depends, HTTPException

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.notebook import store
from flowfile_core.notebook.models import Notebook, NotebookCreate, NotebookSummary, NotebookUpdate

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/notebooks", dependencies=[Depends(get_current_active_user)])


def _require_notebook(user_id: int, notebook_id: str) -> Notebook:
    if not store.is_valid_id(notebook_id):
        raise HTTPException(status_code=400, detail="Invalid notebook id")
    notebook = store.get_notebook(user_id, notebook_id)
    if notebook is None:
        raise HTTPException(status_code=404, detail=f"Notebook '{notebook_id}' not found")
    return notebook


@router.get("/", response_model=list[NotebookSummary])
async def list_notebooks(current_user=Depends(get_current_active_user)):
    return store.list_notebooks(current_user.id)


@router.post("/", response_model=Notebook)
async def create_notebook(payload: NotebookCreate, current_user=Depends(get_current_active_user)):
    return store.create_notebook(current_user.id, payload.name, payload.kernel_id)


@router.get("/{notebook_id}", response_model=Notebook)
async def get_notebook(notebook_id: str, current_user=Depends(get_current_active_user)):
    return _require_notebook(current_user.id, notebook_id)


@router.put("/{notebook_id}", response_model=Notebook)
async def update_notebook(
    notebook_id: str,
    payload: NotebookUpdate,
    current_user=Depends(get_current_active_user),
):
    notebook = _require_notebook(current_user.id, notebook_id)
    return store.save_notebook(
        current_user.id,
        notebook,
        name=payload.name,
        kernel_id=payload.kernel_id,
        cells=payload.cells,
    )


@router.delete("/{notebook_id}")
async def delete_notebook(notebook_id: str, current_user=Depends(get_current_active_user)):
    if not store.is_valid_id(notebook_id):
        raise HTTPException(status_code=400, detail="Invalid notebook id")
    if not store.delete_notebook(current_user.id, notebook_id):
        raise HTTPException(status_code=404, detail=f"Notebook '{notebook_id}' not found")
    return {"status": "deleted", "id": notebook_id}
