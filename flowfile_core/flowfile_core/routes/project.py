"""Git-friendly Flowfile project endpoints (JWT-gated).

Projection (DB→files) happens automatically elsewhere; these endpoints cover the
explicit boundaries: create a project, open/rebuild one, check status, save a version.
There is deliberately no "export" or "apply" endpoint — the folder stays in sync on its own.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi import Path as FPath
from pydantic import BaseModel, Field, SecretStr, field_validator

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.fileExplorer.funcs import validate_path_under_cwd
from flowfile_core.project import git_ops, project_sync
from flowfile_core.project.git_ops import _SHA_RE
from flowfile_core.project.importer import ImportTooLargeError
from flowfile_core.project.models import ActiveProject, NoActiveProjectError
from flowfile_core.project.service import project_root_base
from flowfile_core.secret_manager.secret_manager import SecretInput, upsert_secret
from shared.storage_config import storage


def require_projects_enabled() -> None:
    """404 (not 403/503) in docker unless an operator opted in: in a multi-tenant deployment the
    git-tracking router does not exist by default. package/electron are always on."""
    if settings.is_docker_mode() and not settings.FLOWFILE_ENABLE_PROJECTS:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")


def require_projects_admin(current_user=Depends(get_current_active_user)) -> None:
    """In docker mode the git-tracking router is admin-only (404, matching require_projects_enabled,
    so the endpoints stay undisclosed to non-admins). electron/package are single-user and unrestricted."""
    if settings.is_docker_mode() and not getattr(current_user, "is_admin", False):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")


router = APIRouter(
    dependencies=[
        Depends(get_current_active_user),
        Depends(require_projects_enabled),
        Depends(require_projects_admin),
    ]
)


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
    force: bool = False

    @field_validator("sha")
    @classmethod
    def _validate_sha(cls, v: str) -> str:
        if not _SHA_RE.match(v):
            raise ValueError(f"sha must be 7-40 hex characters, got {v!r}")
        return v


class ReloadRequest(BaseModel):
    force: bool = False


class PlaceholderSecret(BaseModel):
    name: str
    value: str


class FillSecretsRequest(BaseModel):
    secrets: list[PlaceholderSecret] = Field(default_factory=list, max_length=500)


class ProjectPayload(BaseModel):
    id: int
    name: str
    folder_path: str
    track_data_artifacts: bool


class ImportedCounts(BaseModel):
    flows: int
    connections: int
    schedules: int


class SetupResultOut(BaseModel):
    imported: ImportedCounts
    placeholder_secrets: list[str]
    prune_errors: list[str]
    recovery_sha: str | None = None


class ProjectOut(BaseModel):
    project: ProjectPayload


class OpenProjectOut(SetupResultOut):
    project: ProjectPayload


class ActiveProjectOut(BaseModel):
    project: ProjectPayload | None = None
    has_external_changes: bool | None = None
    dirty: bool | None = None
    projection_failed: bool | None = None


class SettingsOut(BaseModel):
    track_data_artifacts: bool


class SaveVersionOut(BaseModel):
    sha: str | None


class VersionsOut(BaseModel):
    versions: list[dict]


class ChangesOut(BaseModel):
    changes: list[dict]


class ProjectRootOut(BaseModel):
    root: str


class OkOut(BaseModel):
    ok: bool


class UpdatedOut(BaseModel):
    updated: int


def _no_active_project(e: NoActiveProjectError) -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, str(e))


def _import_error(e: ValueError) -> HTTPException:
    """Cap breach → 413; any other malformed-manifest ValueError → 422."""
    if isinstance(e, ImportTooLargeError):
        return HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, str(e))
    return HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e))


def _display_folder_path(project: ActiveProject) -> str:
    """In docker mode return the path relative to the owner's project base so the response never leaks
    the internal storage layout or owner_id. electron/package own the filesystem → absolute path."""
    base = project_root_base(project.owner_id)
    if base is None:
        return str(project.root)
    try:
        return str(project.root.resolve().relative_to(base))
    except ValueError:
        return project.root.name


def _payload(project: ActiveProject) -> ProjectPayload:
    return ProjectPayload(
        id=project.id,
        name=project.name,
        folder_path=_display_folder_path(project),
        track_data_artifacts=project.track_data_artifacts,
    )


@router.post("/init", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
def init_project(req: InitProjectRequest, current_user=Depends(get_current_active_user)) -> ProjectOut:
    folder_path = validate_path_under_cwd(req.folder_path)
    name = req.name or Path(folder_path).name
    try:
        project = project_sync.init_project(folder_path, name, current_user.id, req.track_data_artifacts)
    except ValueError as e:
        raise _import_error(e) from e
    return ProjectOut(project=_payload(project))


@router.post("/open", response_model=OpenProjectOut, status_code=status.HTTP_201_CREATED)
def open_project(req: OpenProjectRequest, current_user=Depends(get_current_active_user)) -> OpenProjectOut:
    folder_path = validate_path_under_cwd(req.folder_path)
    try:
        project, result = project_sync.open_project(folder_path, current_user.id)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e
    except ValueError as e:
        raise _import_error(e) from e
    return OpenProjectOut(project=_payload(project), **result.to_dict())


@router.get("/root", response_model=ProjectRootOut)
def get_project_root(current_user=Depends(get_current_active_user)) -> ProjectRootOut:
    """Absolute base dir the folder picker is rooted at: the caller's own confined project subtree
    (created on demand) in docker/package, or the user-data root in unconfined electron."""
    base = project_root_base(current_user.id)
    if base is None:
        return ProjectRootOut(root=str(storage.user_data_directory))
    base.mkdir(parents=True, exist_ok=True)
    return ProjectRootOut(root=str(base))


@router.get("/active", response_model=ActiveProjectOut)
def get_active(current_user=Depends(get_current_active_user)) -> ActiveProjectOut:
    project = project_sync.get_active_project(current_user.id)
    if project is None:
        return ActiveProjectOut(project=None)
    return ActiveProjectOut(
        project=_payload(project),
        has_external_changes=project_sync.has_external_changes(current_user.id),
        dirty=project_sync.has_uncommitted_changes(current_user.id),
        projection_failed=project_sync.projection_failed(current_user.id),
    )


@router.put("/settings", response_model=SettingsOut)
def update_settings(req: UpdateSettingsRequest, current_user=Depends(get_current_active_user)) -> SettingsOut:
    try:
        value = project_sync.update_settings(current_user.id, req.track_data_artifacts)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    return SettingsOut(track_data_artifacts=value)


@router.post("/versions", response_model=SaveVersionOut)
def save_version(req: SaveVersionRequest, current_user=Depends(get_current_active_user)) -> SaveVersionOut:
    try:
        sha = project_sync.save_version(current_user.id, req.message)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    return SaveVersionOut(sha=sha)


@router.get("/versions", response_model=VersionsOut)
def list_versions(
    limit: int = Query(50, ge=1, le=500),
    current_user=Depends(get_current_active_user),
) -> VersionsOut:
    project = project_sync.get_active_project(current_user.id)
    if project is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "No active project")
    return VersionsOut(versions=git_ops.log(project.root, limit))


@router.post("/restore", response_model=SetupResultOut)
def restore_version(req: RestoreRequest, current_user=Depends(get_current_active_user)) -> SetupResultOut:
    try:
        result = project_sync.restore_version(current_user.id, req.sha, req.label, req.force)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except ValueError as e:
        raise _import_error(e) from e
    return SetupResultOut(**result.to_dict())


_SHA_PATH = FPath(..., pattern=r"^[0-9a-fA-F]{7,40}$")


@router.get("/versions/{sha}/changes", response_model=ChangesOut)
def version_changes(
    sha: str = _SHA_PATH,
    current_user=Depends(get_current_active_user),
) -> ChangesOut:
    try:
        changes = project_sync.changes_for_version(current_user.id, sha)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    return ChangesOut(changes=changes)


@router.get("/versions/{sha}/diff", response_model=ChangesOut)
def version_diff(
    sha: str = _SHA_PATH,
    current_user=Depends(get_current_active_user),
) -> ChangesOut:
    try:
        changes = project_sync.version_diff(current_user.id, sha)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    return ChangesOut(changes=changes)


@router.get("/uncommitted", response_model=ChangesOut)
def uncommitted(current_user=Depends(get_current_active_user)) -> ChangesOut:
    try:
        changes = project_sync.uncommitted_changes(current_user.id)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    return ChangesOut(changes=changes)


@router.post("/reload", response_model=SetupResultOut)
def reload_project(req: ReloadRequest | None = None, current_user=Depends(get_current_active_user)) -> SetupResultOut:
    force = req.force if req is not None else False
    try:
        result = project_sync.reload_from_disk(current_user.id, force)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except ValueError as e:
        raise _import_error(e) from e
    return SetupResultOut(**result.to_dict())


@router.post("/close", response_model=OkOut)
def close_project(current_user=Depends(get_current_active_user)) -> OkOut:
    project_sync.close_project(current_user.id)
    return OkOut(ok=True)


@router.post("/secrets", response_model=UpdatedOut)
def fill_secrets(req: FillSecretsRequest, current_user=Depends(get_current_active_user)) -> UpdatedOut:
    with get_db_context() as db:
        for item in req.secrets:
            upsert_secret(db, SecretInput(name=item.name, value=SecretStr(item.value)), current_user.id)
    project_sync.secret_changed(current_user.id)
    return UpdatedOut(updated=len(req.secrets))
