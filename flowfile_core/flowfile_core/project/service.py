"""ProjectSyncService: invisible DB→files projection + project lifecycle.

Hooks at the tail of the central save/store functions call the ``project_sync``
singleton. With no active project every call is a cheap no-op; with one active,
the affected file is re-projected as a side-effect — never blocking the primary
operation (projection failures are logged and swallowed).
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from flowfile_core import __version__
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration
from flowfile_core.project import git_ops, projection, repository
from flowfile_core.project.importer import import_project
from flowfile_core.project.manifest import (
    MANIFEST_NAME,
    ProjectManifest,
    read_manifest,
    write_gitignore,
    write_manifest,
)
from flowfile_core.project.models import ActiveProject, SetupResult

logger = logging.getLogger(__name__)


def _classify_path(path: str) -> tuple[str, str]:
    """Map a project file path to a (kind, label) for a friendly change summary."""
    name = path.split("/")[-1]
    if path.startswith("flows/") and name.endswith(".flow.yaml"):
        return "flow", name[: -len(".flow.yaml")]
    stem = name[: -len(".yaml")] if name.endswith(".yaml") else name
    if path.startswith("connections/database/"):
        return "database connection", stem
    if path.startswith("connections/cloud/"):
        return "cloud connection", stem
    if path.startswith("schedules/"):
        return "schedule", stem
    return "settings", name


def _friendly_changes(raw: list[dict]) -> list[dict]:
    """Turn `git diff --name-status` rows into `{change, kind, label, path}` for the restore preview."""
    out: list[dict] = []
    for item in raw:
        change = {"D": "removed", "A": "added"}.get(item["status"][0], "modified")
        kind, label = _classify_path(item["path"])
        out.append({"change": change, "kind": kind, "label": label, "path": item["path"]})
    return out


class ProjectSyncService:
    def __init__(self) -> None:
        self._by_owner: dict[int, ActiveProject] | None = None  # None = not loaded yet

    # --- active-project cache -------------------------------------------------

    def _load(self) -> dict[int, ActiveProject]:
        if self._by_owner is not None:
            return self._by_owner
        try:
            by_owner: dict[int, ActiveProject] = {}
            with get_db_context() as db:
                for p in repository.get_active_projects(db):
                    by_owner[p.owner_id] = ActiveProject(p.id, p.name, Path(p.folder_path), p.owner_id)
            self._by_owner = by_owner
            return by_owner
        except Exception:
            logger.warning("Failed to load active projects", exc_info=True)
            return {}

    def get_active_project(self, user_id: int | None) -> ActiveProject | None:
        if user_id is None:
            return None
        return self._load().get(user_id)

    # --- hooks (called after the primary commit; must never raise) ------------

    def flow_saved(self, flow_path: str, user_id: int | None) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with get_db_context() as db:
                reg = db.query(FlowRegistration).filter(FlowRegistration.flow_path == flow_path).first()
                if reg is not None and reg.owner_id == proj.owner_id:
                    target = projection.project_flow(proj.root, reg)
                    if target is not None:
                        projection.remove_stale_flow_files(proj.root, reg.flow_uuid, target)
        except Exception:
            logger.warning("Project flow projection failed for %s", flow_path, exc_info=True)

    def flow_deleted(self, flow_uuid: str, user_id: int | None) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            projection.remove_flow(proj.root, flow_uuid=flow_uuid)
        except Exception:
            logger.warning("Project flow-delete projection failed for %s", flow_uuid, exc_info=True)

    def connection_changed(self, kind: str, connection_name: str, user_id: int | None, deleted: bool = False) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            if deleted:
                projection.remove_connection(proj.root, kind, connection_name)
            else:
                with get_db_context() as db:
                    if kind == "database":
                        projection.project_database_connection(db, proj.root, connection_name, proj.owner_id)
                    elif kind == "cloud":
                        projection.project_cloud_connection(db, proj.root, connection_name, proj.owner_id)
            with get_db_context() as db:
                projection.regenerate_secret_manifest(db, proj.root, proj.owner_id)
        except Exception:
            logger.warning("Project connection projection failed for %s/%s", kind, connection_name, exc_info=True)

    def schedule_changed(self, registration_id: int, user_id: int | None, deleted: bool = False) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with get_db_context() as db:
                reg = db.query(FlowRegistration).filter(FlowRegistration.id == registration_id).first()
                if reg is not None and reg.owner_id == proj.owner_id:
                    projection.project_schedules_for_registration(db, proj.root, reg)
        except Exception:
            logger.warning("Project schedule projection failed for reg %s", registration_id, exc_info=True)

    def secret_changed(self, user_id: int | None) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with get_db_context() as db:
                projection.regenerate_secret_manifest(db, proj.root, proj.owner_id)
        except Exception:
            logger.warning("Project secret-manifest projection failed", exc_info=True)

    # --- lifecycle (explicit user actions via /project router + CLI) ----------

    def init_project(self, folder_path: str, name: str, owner_id: int) -> ActiveProject:
        root = Path(folder_path).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        existing = read_manifest(root)
        m = existing or ProjectManifest(name=name, project_id=str(uuid.uuid4()), created_with_version=__version__)
        write_manifest(root, m)
        write_gitignore(root)
        git_ops.init(root)
        with get_db_context() as db:
            row = repository.upsert_active(db, m.name, str(root), owner_id)
            project = ActiveProject(row.id, m.name, root, owner_id)
            projection.project_all(db, root, owner_id)
        sha = git_ops.commit_all(root, "Initialize Flowfile project")
        with get_db_context() as db:
            repository.set_head_sha(db, project.id, sha or git_ops.head_sha(root))
        self._load()[owner_id] = project
        return project

    def open_project(self, folder_path: str, owner_id: int) -> tuple[ActiveProject, SetupResult]:
        root = Path(folder_path).expanduser().resolve()
        m = read_manifest(root)
        if m is None:
            raise FileNotFoundError(f"No Flowfile project at {root} (missing {MANIFEST_NAME})")
        with get_db_context() as db:
            row = repository.upsert_active(db, m.name, str(root), owner_id)
            project = ActiveProject(row.id, m.name, root, owner_id)
        self._load()[owner_id] = project
        return project, import_project(root, owner_id)

    def save_version(self, user_id: int, message: str) -> str | None:
        proj = self.get_active_project(user_id)
        if proj is None:
            raise RuntimeError("No active project")
        with get_db_context() as db:
            projection.project_all(db, proj.root, proj.owner_id)
        sha = git_ops.commit_all(proj.root, message)
        with get_db_context() as db:
            repository.set_head_sha(db, proj.id, git_ops.head_sha(proj.root))
        return sha

    def restore_version(self, user_id: int, sha: str, label: str | None = None) -> SetupResult:
        """Reset files to ``sha``, rebuild + prune the DB to match, and record it as a new version.

        Restore is one complete action: the working tree is reset to ``sha`` (deletions included),
        the DB is rebuilt and pruned to match, and a "Restore: …" commit is made so history stays
        clean with no dangling unsaved state.
        """
        proj = self.get_active_project(user_id)
        if proj is None:
            raise RuntimeError("No active project")
        git_ops.restore(proj.root, sha)
        result = import_project(proj.root, proj.owner_id, prune=True)
        message = f"Restore: {label}" if label else f"Restore version {sha[:8]}"
        git_ops.commit_all(proj.root, message)
        with get_db_context() as db:
            repository.set_head_sha(db, proj.id, git_ops.head_sha(proj.root))
        return result

    def reload_from_disk(self, user_id: int) -> SetupResult:
        """Accept on-disk (external) changes by rebuilding + pruning the DB to match the files."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise RuntimeError("No active project")
        return import_project(proj.root, proj.owner_id, prune=True)

    def changes_for_version(self, user_id: int, sha: str) -> list[dict]:
        """Friendly summary of what restoring ``sha`` would change vs the latest saved version."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise RuntimeError("No active project")
        return _friendly_changes(git_ops.diff_name_status(proj.root, "HEAD", sha))

    def version_diff(self, user_id: int, sha: str) -> list[dict]:
        """Friendly changelog of what a specific version changed vs the one before it."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise RuntimeError("No active project")
        return _friendly_changes(git_ops.changes_in(proj.root, sha))

    def uncommitted_changes(self, user_id: int) -> list[dict]:
        """Friendly summary of the unsaved working-tree changes (what a Save version would record)."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise RuntimeError("No active project")
        return _friendly_changes(git_ops.uncommitted_changes(proj.root))

    def close_project(self, owner_id: int) -> None:
        with get_db_context() as db:
            repository.deactivate_owner(db, owner_id)
        self._load().pop(owner_id, None)

    def has_uncommitted_changes(self, user_id: int) -> bool:
        proj = self.get_active_project(user_id)
        if proj is None:
            return False
        return git_ops.is_dirty(proj.root)

    def has_external_changes(self, user_id: int) -> bool:
        proj = self.get_active_project(user_id)
        if proj is None:
            return False
        current = git_ops.head_sha(proj.root)
        with get_db_context() as db:
            row = repository.get_by_path(db, str(proj.root))
            stored = row.last_synced_head_sha if row else None
        return bool(current) and current != stored


project_sync = ProjectSyncService()
