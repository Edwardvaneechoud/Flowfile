"""REST router for the git-enabled project workspace (Phase 1).

JWT-gated. Endpoints are an explicit, opt-in projection layer over the runtime
DB:

* ``POST /workspace/init``             create/refresh a project tree + register it
* ``POST /workspace/export``           DB -> files (deterministic, idempotent)
* ``POST /workspace/apply``            files -> DB (clone bootstrap / rollback)
* ``GET  /workspace/status``           manifest + drift + secret requirements
* ``GET  /workspace/secrets/required`` secrets the project needs (names only)
* ``GET  /workspace/projects``         projects registered against this DB

Embedded git + rollback endpoints are Phase 2.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.workspace.git_backend import GitBackend, GitError
from flowfile_core.workspace.layout import ProjectLayout
from flowfile_core.workspace.manifest import init_project
from flowfile_core.workspace.models import (
    GitCommit,
    ProjectManifest,
    SecretRequirement,
    WorkspaceApplyResult,
    WorkspaceExportResult,
    WorkspaceGitHistory,
    WorkspaceStatus,
)
from flowfile_core.workspace.sync import WorkspaceSync

router = APIRouter(prefix="/workspace", tags=["workspace"])


class WorkspaceInitRequest(BaseModel):
    root_path: str
    name: str
    namespace_roots: list[str] = Field(default_factory=list)


class WorkspacePathRequest(BaseModel):
    # Optional: falls back to the caller's most recently registered project.
    root_path: str | None = None


class WorkspaceProjectInfo(BaseModel):
    project_id: str
    name: str
    root_path: str
    namespace_roots: list[str] = Field(default_factory=list)
    git_enabled: bool = False


def _resolve_root(db: Session, user_id: int, root_path: str | None) -> str:
    if root_path:
        return root_path
    row = (
        db.query(db_models.WorkspaceProject)
        .filter(db_models.WorkspaceProject.owner_id == user_id)
        .order_by(db_models.WorkspaceProject.updated_at.desc())
        .first()
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="No workspace project found. Provide root_path or call /workspace/init first.",
        )
    return row.root_path


def _register_project(
    db: Session, user_id: int, manifest: ProjectManifest, root_path: str
) -> db_models.WorkspaceProject:
    row = (
        db.query(db_models.WorkspaceProject)
        .filter(
            db_models.WorkspaceProject.owner_id == user_id,
            db_models.WorkspaceProject.root_path == root_path,
        )
        .first()
    )
    namespace_roots = json.dumps(manifest.namespace_roots)
    if row is None:
        row = db_models.WorkspaceProject(
            project_id=manifest.project_id,
            name=manifest.name,
            root_path=root_path,
            namespace_roots=namespace_roots,
            owner_id=user_id,
        )
        db.add(row)
    else:
        row.project_id = manifest.project_id
        row.name = manifest.name
        row.namespace_roots = namespace_roots
    db.commit()
    db.refresh(row)
    return row


@router.post("/init", response_model=ProjectManifest)
def init_workspace(
    body: WorkspaceInitRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Create (or refresh) a project tree and register it against this DB."""
    manifest = init_project(body.root_path, body.name, namespace_roots=body.namespace_roots)
    _register_project(db, current_user.id, manifest, body.root_path)
    return manifest


@router.post("/export", response_model=WorkspaceExportResult)
def export_workspace(
    body: WorkspacePathRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Project the DB onto the tree (writes only changed bytes; prunes stale)."""
    root = _resolve_root(db, current_user.id, body.root_path)
    return WorkspaceSync(db, current_user.id, root).export()


@router.post("/apply", response_model=WorkspaceApplyResult)
def apply_workspace(
    body: WorkspacePathRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Rebuild DB state from the tree (clone bootstrap / branch switch / rollback)."""
    root = _resolve_root(db, current_user.id, body.root_path)
    if not ProjectLayout(root).root.exists():
        raise HTTPException(status_code=404, detail=f"Project root does not exist: {root}")
    return WorkspaceSync(db, current_user.id, root).apply()


@router.get("/status", response_model=WorkspaceStatus)
def workspace_status(
    root_path: str | None = Query(default=None),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    root = _resolve_root(db, current_user.id, root_path)
    return WorkspaceSync(db, current_user.id, root).status()


@router.get("/secrets/required", response_model=list[SecretRequirement])
def workspace_required_secrets(
    root_path: str | None = Query(default=None),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    root = _resolve_root(db, current_user.id, root_path)
    return WorkspaceSync(db, current_user.id, root).required_secrets()


@router.get("/projects", response_model=list[WorkspaceProjectInfo])
def list_workspace_projects(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(db_models.WorkspaceProject)
        .filter(db_models.WorkspaceProject.owner_id == current_user.id)
        .order_by(db_models.WorkspaceProject.updated_at.desc())
        .all()
    )
    return [
        WorkspaceProjectInfo(
            project_id=row.project_id,
            name=row.name,
            root_path=row.root_path,
            namespace_roots=json.loads(row.namespace_roots) if row.namespace_roots else [],
            git_enabled=row.git_enabled,
        )
        for row in rows
    ]


# ----------------------------------------------------------------------------
# Embedded git: history, commit, diff, restore (Phase 2).
# Git runs only on these explicit actions — never automatically on export.
# ----------------------------------------------------------------------------


class WorkspaceCommitRequest(BaseModel):
    root_path: str | None = None
    message: str = "Update Flowfile project"


class WorkspaceRestoreRequest(BaseModel):
    root_path: str | None = None
    sha: str
    apply: bool = True  # also rebuild the runtime DB from the restored files


class CommitResponse(BaseModel):
    committed: bool
    sha: str | None = None
    branch: str | None = None


class DiffResponse(BaseModel):
    sha: str | None = None
    diff: str = ""


class RestoreResponse(BaseModel):
    commit_sha: str | None = None
    applied: WorkspaceApplyResult | None = None


@router.get("/history", response_model=WorkspaceGitHistory)
def workspace_history(
    root_path: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Git history + working-tree state for the project (empty if git is absent)."""
    root = _resolve_root(db, current_user.id, root_path)
    git = GitBackend(root)
    if not git.available():
        return WorkspaceGitHistory(git_available=False, is_repo=False)
    if not git.is_repo():
        return WorkspaceGitHistory(git_available=True, is_repo=False)
    status = git.status()
    return WorkspaceGitHistory(
        git_available=True,
        is_repo=True,
        branch=status["branch"],
        dirty=status["dirty"],
        uncommitted=status["uncommitted"],
        commits=[GitCommit(**c) for c in git.log(limit)],
    )


@router.post("/commit", response_model=CommitResponse)
def workspace_commit(
    body: WorkspaceCommitRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Snapshot the current project tree (initializes the repo on first commit)."""
    root = _resolve_root(db, current_user.id, body.root_path)
    git = GitBackend(root)
    if not git.available():
        raise HTTPException(status_code=400, detail="git is not available on the server")
    try:
        sha = git.commit(body.message)
    except GitError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return CommitResponse(committed=sha is not None, sha=sha, branch=git.current_branch())


@router.get("/diff", response_model=DiffResponse)
def workspace_diff(
    root_path: str | None = Query(default=None),
    sha: str | None = Query(default=None),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Diff for a commit (``sha`` given) or the uncommitted working-tree changes."""
    root = _resolve_root(db, current_user.id, root_path)
    git = GitBackend(root)
    if not git.available() or not git.is_repo():
        return DiffResponse(sha=sha, diff="")
    diff = git.commit_patch(sha) if sha else git.worktree_diff()
    return DiffResponse(sha=sha, diff=diff)


@router.post("/restore", response_model=RestoreResponse)
def workspace_restore(
    body: WorkspaceRestoreRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Roll the project back to a commit: restore files, snapshot, then re-apply.

    A new commit records the rollback (so it is itself undoable), and — when
    ``apply`` is set — the runtime catalog is rebuilt from the restored files.
    """
    root = _resolve_root(db, current_user.id, body.root_path)
    git = GitBackend(root)
    if not git.available() or not git.is_repo():
        raise HTTPException(status_code=400, detail="No git history for this project")
    try:
        git.restore(body.sha)
        new_sha = git.commit(f"Restore to {body.sha[:8]}")
    except GitError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    applied = WorkspaceSync(db, current_user.id, root).apply() if body.apply else None
    return RestoreResponse(commit_sha=new_sha, applied=applied)
