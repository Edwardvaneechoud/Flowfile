"""Git-friendly Flowfile project endpoints (JWT-gated).

These endpoints cover the explicit boundaries: create a project, open/rebuild one, check status,
save a version (the one write of the folder besides init, the settings toggle and a restore).
``POST /plan`` only previews what a restore or reload would change.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi import Path as FPath
from pydantic import BaseModel, Field, SecretStr, field_validator

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.fileExplorer.funcs import validate_path_under_cwd
from flowfile_core.flowfile.community_nodes import github_client
from flowfile_core.flowfile.community_nodes.github_client import (
    GithubApiError,
    GithubAuthError,
    GithubNotConfiguredError,
)
from flowfile_core.project import git_ops, project_sync
from flowfile_core.project.git_ops import _SHA_RE
from flowfile_core.project.importer import ImportTooLargeError
from flowfile_core.project.models import ActiveProject, NoActiveProjectError
from flowfile_core.project.service import project_root_base
from flowfile_core.routes.community_github import _api_http, _auth_http, _not_configured
from flowfile_core.secret_manager.secret_manager import SecretInput, upsert_secret
from shared.storage_config import storage


def _multi_user_server() -> bool:
    """Docker or a server run by hand, read where ``is_docker_mode`` reads it."""
    return settings.FLOWFILE_MODE in ("docker", "server")


def require_projects_enabled() -> None:
    """404 (not 403/503) on a multi-user server unless an operator opted in: in a multi-tenant deployment the
    git-tracking router does not exist by default. package/electron are always on."""
    if _multi_user_server() and not settings.FLOWFILE_ENABLE_PROJECTS:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")


def require_projects_admin(current_user=Depends(get_current_active_user)) -> None:
    """On a multi-user server the git-tracking router is admin-only (404, matching require_projects_enabled,
    so the endpoints stay undisclosed to non-admins). electron/package are single-user and unrestricted."""
    if _multi_user_server() and not getattr(current_user, "is_admin", False):
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
    overwrite: bool = False


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


class PlanRequest(BaseModel):
    sha: str | None = None

    @field_validator("sha")
    @classmethod
    def _validate_sha(cls, v: str | None) -> str | None:
        if v is not None and not _SHA_RE.match(v):
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
    by_kind: dict[str, int] = {}


class SetupResultOut(BaseModel):
    imported: ImportedCounts
    placeholder_secrets: list[str]
    unknown_kinds: list[str] = []
    not_applied: list[str] = []
    incomplete: list[str] = []
    recovery_sha: str | None = None


class ProjectOut(BaseModel):
    project: ProjectPayload


class OpenProjectOut(SetupResultOut):
    project: ProjectPayload


class ActiveProjectOut(BaseModel):
    project: ProjectPayload | None = None
    has_external_changes: bool | None = None
    dirty: bool | None = None
    external_edits: bool | None = None
    projection_failed: bool | None = Field(
        default=None, deprecated="Always false: no save writes into the project folder any more."
    )


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
        return HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, str(e))
    return HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))


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
        external_edits=project_sync.external_edits(current_user.id),
        projection_failed=False,
    )


@router.put("/settings", response_model=SettingsOut)
def update_settings(req: UpdateSettingsRequest, current_user=Depends(get_current_active_user)) -> SettingsOut:
    try:
        value = project_sync.update_settings(current_user.id, req.track_data_artifacts)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except ValueError as e:
        raise _import_error(e) from e
    return SettingsOut(track_data_artifacts=value)


@router.post("/versions", response_model=SaveVersionOut)
def save_version(req: SaveVersionRequest, current_user=Depends(get_current_active_user)) -> SaveVersionOut:
    try:
        sha = project_sync.save_version(current_user.id, req.message, req.overwrite)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except ValueError as e:
        raise _import_error(e) from e
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


@router.get("/versions/{sha}/changes", response_model=ChangesOut, deprecated=True)
def version_changes(
    sha: str = _SHA_PATH,
    current_user=Depends(get_current_active_user),
) -> ChangesOut:
    """What restoring ``sha`` would change: ``POST /project/plan`` with ``{"sha": ...}``, kept for older clients."""
    try:
        changes = project_sync.plan(current_user.id, sha)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except ValueError as e:
        raise _import_error(e) from e
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
        changes = project_sync.unsaved_changes(current_user.id)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except ValueError as e:
        raise _import_error(e) from e
    return ChangesOut(changes=changes)


@router.post("/plan", response_model=ChangesOut)
def plan(req: PlanRequest | None = None, current_user=Depends(get_current_active_user)) -> ChangesOut:
    try:
        changes = project_sync.plan(current_user.id, req.sha if req is not None else None)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except ValueError as e:
        raise _import_error(e) from e
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
    return UpdatedOut(updated=len(req.secrets))


class BranchRow(BaseModel):
    name: str
    sha: str
    current: bool
    ahead: int | None = None
    behind: int | None = None


class BranchStateOut(BaseModel):
    current: str | None
    branches: list[BranchRow]
    remote: str | None
    has_token: bool


class CreateBranchRequest(BaseModel):
    name: str
    switch: bool = True


class CreateBranchOut(BaseModel):
    name: str
    sha: str


class SwitchBranchRequest(BaseModel):
    name: str
    force: bool = False


class RemoteRequest(BaseModel):
    url: str = Field(max_length=2048)
    token: SecretStr | None = None
    username: str | None = Field(default=None, max_length=200)


class PushOut(BaseModel):
    branch: str
    sha: str | None
    unsaved: bool


class PullRequest(BaseModel):
    force: bool = False


class MergeItem(BaseModel):
    path: str
    key: str | None
    kind: str
    label: str
    message: str
    step_id: int | None = None
    ours_label: str
    theirs_label: str
    ours: list[str]
    theirs: list[str]


class MergeStateOut(BaseModel):
    branch: str
    ours: str
    theirs: str
    sides: dict[str, str]
    conflicts: list[MergeItem]
    flows: list[str]
    warnings: list[str]


class PullOut(BaseModel):
    state: str
    sha: str | None = None
    result: SetupResultOut | None = None
    conflicts: list[MergeItem] = []
    warnings: list[str] = []


class MergeStatusOut(BaseModel):
    merge: MergeStateOut | None


class SettleRequest(BaseModel):
    path: str = Field(max_length=1024)
    key: str | None = Field(default=None, max_length=200)
    take: Literal["ours", "theirs"]
    force: bool = False


class SettleOut(BaseModel):
    merge: MergeStateOut | None = None
    pull: PullOut | None = None


class OpenMergeFlowRequest(BaseModel):
    path: str = Field(max_length=1024)


class OpenMergeFlowOut(BaseModel):
    flow_id: int


class ConcludeRequest(BaseModel):
    force: bool = False


def _git_error(e: git_ops.GitRemoteError) -> HTTPException:
    detail = {"code": "GIT_REFUSED", "message": str(e)}
    if isinstance(e, git_ops.UnmergedFilesError):
        detail["paths"] = e.paths
    return HTTPException(status.HTTP_409_CONFLICT, detail)


def _pull_out(result) -> PullOut:
    return PullOut(
        state=result.state,
        sha=result.sha,
        result=SetupResultOut(**result.result.to_dict()) if result.result is not None else None,
        conflicts=[MergeItem(**row) for row in result.conflicts],
        warnings=result.warnings,
    )


def _project_call(call, *args):
    try:
        return call(*args)
    except NoActiveProjectError as e:
        raise _no_active_project(e) from e
    except git_ops.GitRemoteError as e:
        raise _git_error(e) from e
    except ValueError as e:
        raise _import_error(e) from e


@router.get("/branches", response_model=BranchStateOut)
def list_branches(current_user=Depends(get_current_active_user)) -> BranchStateOut:
    return BranchStateOut(**_project_call(project_sync.branch_state, current_user.id))


@router.post("/branches", response_model=CreateBranchOut, status_code=status.HTTP_201_CREATED)
def create_branch(req: CreateBranchRequest, current_user=Depends(get_current_active_user)) -> CreateBranchOut:
    return CreateBranchOut(**_project_call(project_sync.create_branch, current_user.id, req.name, req.switch))


@router.post("/branches/switch", response_model=SetupResultOut)
def switch_branch(req: SwitchBranchRequest, current_user=Depends(get_current_active_user)) -> SetupResultOut:
    result = _project_call(project_sync.switch_branch, current_user.id, req.name, req.force)
    return SetupResultOut(**result.to_dict())


@router.put("/remote", response_model=BranchStateOut)
def set_remote(req: RemoteRequest, current_user=Depends(get_current_active_user)) -> BranchStateOut:
    """Set the remote's address and, when given, this person's token for it (kept as a secret, never answered)."""
    token = req.token.get_secret_value() if req.token is not None else None
    state = _project_call(project_sync.set_remote, current_user.id, req.url, token, req.username)
    return BranchStateOut(**state)


# A project repository may be private, which an OAuth app reaches only with the ``repo`` scope.
GITHUB_REMOTE_SCOPE = "repo"


class GithubDevicePollRequest(BaseModel):
    device_code: str = Field(max_length=200)


class GithubDevicePollOut(BaseModel):
    status: Literal["pending", "slow_down", "connected"]
    interval: int | None = None
    login: str = ""


def _github_error(e: Exception) -> HTTPException:
    """The community publishing routes' typed answers (never 401, which the client reads as a lapsed session)."""
    if isinstance(e, GithubNotConfiguredError):
        return _not_configured(e)
    if isinstance(e, GithubAuthError):
        return _auth_http(e)
    return _api_http(e)


@router.post("/remote/github/device/start", response_model=github_client.DeviceStart)
def remote_github_device_start(current_user=Depends(get_current_active_user)) -> github_client.DeviceStart:
    """Begin a GitHub sign-in whose token becomes this person's remote token, as the node designer's publishing does."""
    try:
        return github_client.device_start(scope=GITHUB_REMOTE_SCOPE)
    except (GithubNotConfiguredError, GithubApiError) as e:
        raise _github_error(e) from e


@router.post("/remote/github/device/poll", response_model=GithubDevicePollOut)
def remote_github_device_poll(
    req: GithubDevicePollRequest, current_user=Depends(get_current_active_user)
) -> GithubDevicePollOut:
    """Poll the sign-in once; once GitHub grants it, the token is kept as the remote token and never answered."""
    try:
        result = github_client.device_poll(req.device_code)
        if result.status != "success":
            return GithubDevicePollOut(status=result.status, interval=result.interval)
        login = github_client.fetch_login(result.access_token)
    except (GithubNotConfiguredError, GithubAuthError, GithubApiError) as e:
        raise _github_error(e) from e
    _project_call(project_sync.keep_remote_token, current_user.id, result.access_token)
    return GithubDevicePollOut(status="connected", login=login)


@router.post("/push", response_model=PushOut)
def push(current_user=Depends(get_current_active_user)) -> PushOut:
    return PushOut(**_project_call(project_sync.push, current_user.id))


@router.post("/pull", response_model=PullOut)
def pull(req: PullRequest | None = None, current_user=Depends(get_current_active_user)) -> PullOut:
    force = req.force if req is not None else False
    return _pull_out(_project_call(project_sync.pull, current_user.id, force))


@router.get("/merge", response_model=MergeStatusOut)
def merge_status(current_user=Depends(get_current_active_user)) -> MergeStatusOut:
    state = _project_call(project_sync.merge_state, current_user.id)
    return MergeStatusOut(merge=MergeStateOut(**state) if state is not None else None)


@router.post("/merge/settle", response_model=SettleOut)
def settle_merge(req: SettleRequest, current_user=Depends(get_current_active_user)) -> SettleOut:
    """Settle one item; settling the last concludes the merge, answered as the pull it finishes."""
    answer = _project_call(project_sync.settle_merge, current_user.id, req.path, req.take, req.key, req.force)
    if isinstance(answer, dict) or answer is None:
        return SettleOut(merge=MergeStateOut(**answer) if answer else None)
    return SettleOut(pull=_pull_out(answer))


@router.post("/merge/conclude", response_model=PullOut)
def conclude_merge(req: ConcludeRequest | None = None, current_user=Depends(get_current_active_user)) -> PullOut:
    force = req.force if req is not None else False
    return _pull_out(_project_call(project_sync.conclude_merge, current_user.id, force))


@router.post("/merge/abort", response_model=OkOut)
def abort_merge(current_user=Depends(get_current_active_user)) -> OkOut:
    _project_call(project_sync.abort_merge, current_user.id)
    return OkOut(ok=True)


@router.post("/merge/open", response_model=OpenMergeFlowOut)
def open_merge_flow(req: OpenMergeFlowRequest, current_user=Depends(get_current_active_user)) -> OpenMergeFlowOut:
    """Open a conflicted flow of the waiting merge in the editor, where its steps are settled one by one."""
    return OpenMergeFlowOut(flow_id=_project_call(project_sync.open_merge_flow, current_user.id, req.path))
