"""Git-friendly Flowfile project endpoints (JWT-gated).

Projection (DB→files) happens automatically elsewhere; these endpoints cover the
explicit boundaries: create a project, open/rebuild one, check status, save a version.
There is deliberately no "export" or "apply" endpoint — the folder stays in sync on its own.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.project import project_sync
from flowfile_core.project.models import ActiveProject

router = APIRouter(dependencies=[Depends(get_current_active_user)])


class InitProjectRequest(BaseModel):
    folder_path: str
    name: str | None = None


class OpenProjectRequest(BaseModel):
    folder_path: str


class SaveVersionRequest(BaseModel):
    message: str


def _payload(project: ActiveProject) -> dict:
    return {"id": project.id, "name": project.name, "folder_path": str(project.root)}


@router.post("/init")
def init_project(req: InitProjectRequest, current_user=Depends(get_current_active_user)) -> dict:
    name = req.name or Path(req.folder_path).expanduser().name
    project = project_sync.init_project(req.folder_path, name, current_user.id)
    return {"project": _payload(project)}


@router.post("/open")
def open_project(req: OpenProjectRequest, current_user=Depends(get_current_active_user)) -> dict:
    try:
        project, result = project_sync.open_project(req.folder_path, current_user.id)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e
    return {"project": _payload(project), **result.to_dict()}


@router.get("/active")
def get_active(current_user=Depends(get_current_active_user)) -> dict:
    project = project_sync.get_active_project(current_user.id)
    if project is None:
        return {"project": None}
    return {
        "project": _payload(project),
        "has_external_changes": project_sync.has_external_changes(current_user.id),
    }


@router.post("/versions")
def save_version(req: SaveVersionRequest, current_user=Depends(get_current_active_user)) -> dict:
    try:
        sha = project_sync.save_version(current_user.id, req.message)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return {"sha": sha}
