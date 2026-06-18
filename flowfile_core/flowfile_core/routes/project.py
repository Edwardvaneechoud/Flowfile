"""Git-friendly Flowfile project endpoints (JWT-gated).

Projection (DB→files) happens automatically elsewhere; these endpoints cover the
explicit boundaries: create a project, open/rebuild one, check status, save a version.
There is deliberately no "export" or "apply" endpoint — the folder stays in sync on its own.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, SecretStr

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db_context
from flowfile_core.fileExplorer.funcs import validate_path_under_cwd
from flowfile_core.project import git_ops, project_sync
from flowfile_core.project.models import ActiveProject
from flowfile_core.secret_manager.secret_manager import SecretInput, upsert_secret

router = APIRouter(dependencies=[Depends(get_current_active_user)])


class InitProjectRequest(BaseModel):
    folder_path: str
    name: str | None = None
    track_data_artifacts: bool = True


class UpdateSettingsRequest(BaseModel):
    track_data_artifacts: bool


class OpenProjectRequest(BaseModel):
    folder_path: str


class SaveVersionRequest(BaseModel):
    message: str


class RestoreRequest(BaseModel):
    sha: str
    label: str | None = None


class PlaceholderSecret(BaseModel):
    name: str
    value: str


def _payload(project: ActiveProject) -> dict:
    return {
        "id": project.id,
        "name": project.name,
        "folder_path": str(project.root),
        "track_data_artifacts": project.track_data_artifacts,
    }


@router.post("/init")
def init_project(req: InitProjectRequest, current_user=Depends(get_current_active_user)) -> dict:
    folder_path = validate_path_under_cwd(req.folder_path)
    name = req.name or Path(folder_path).name
    project = project_sync.init_project(folder_path, name, current_user.id, req.track_data_artifacts)
    return {"project": _payload(project)}


@router.post("/open")
def open_project(req: OpenProjectRequest, current_user=Depends(get_current_active_user)) -> dict:
    folder_path = validate_path_under_cwd(req.folder_path)
    try:
        project, result = project_sync.open_project(folder_path, current_user.id)
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
        "dirty": project_sync.has_uncommitted_changes(current_user.id),
    }


@router.put("/settings")
def update_settings(req: UpdateSettingsRequest, current_user=Depends(get_current_active_user)) -> dict:
    try:
        value = project_sync.update_settings(current_user.id, req.track_data_artifacts)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return {"track_data_artifacts": value}


@router.post("/versions")
def save_version(req: SaveVersionRequest, current_user=Depends(get_current_active_user)) -> dict:
    try:
        sha = project_sync.save_version(current_user.id, req.message)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return {"sha": sha}


@router.get("/versions")
def list_versions(limit: int = 50, current_user=Depends(get_current_active_user)) -> dict:
    project = project_sync.get_active_project(current_user.id)
    if project is None:
        raise HTTPException(409, "No active project")
    return {"versions": git_ops.log(project.root, limit)}


@router.post("/restore")
def restore_version(req: RestoreRequest, current_user=Depends(get_current_active_user)) -> dict:
    try:
        result = project_sync.restore_version(current_user.id, req.sha, req.label)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return result.to_dict()


@router.get("/versions/{sha}/changes")
def version_changes(sha: str, current_user=Depends(get_current_active_user)) -> dict:
    try:
        changes = project_sync.changes_for_version(current_user.id, sha)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return {"changes": changes}


@router.get("/versions/{sha}/diff")
def version_diff(sha: str, current_user=Depends(get_current_active_user)) -> dict:
    try:
        changes = project_sync.version_diff(current_user.id, sha)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return {"changes": changes}


@router.get("/uncommitted")
def uncommitted(current_user=Depends(get_current_active_user)) -> dict:
    try:
        changes = project_sync.uncommitted_changes(current_user.id)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return {"changes": changes}


@router.post("/reload")
def reload_project(current_user=Depends(get_current_active_user)) -> dict:
    try:
        result = project_sync.reload_from_disk(current_user.id)
    except RuntimeError as e:
        raise HTTPException(409, str(e)) from e
    return result.to_dict()


@router.post("/close")
def close_project(current_user=Depends(get_current_active_user)) -> dict:
    project_sync.close_project(current_user.id)
    return {"ok": True}


@router.post("/secrets")
def fill_secrets(req: list[PlaceholderSecret], current_user=Depends(get_current_active_user)) -> dict:
    with get_db_context() as db:
        for item in req:
            upsert_secret(db, SecretInput(name=item.name, value=SecretStr(item.value)), current_user.id)
    project_sync.secret_changed(current_user.id)
    return {"updated": len(req)}
