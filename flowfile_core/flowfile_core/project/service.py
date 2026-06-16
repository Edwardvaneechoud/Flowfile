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
                    projection.project_flow(proj.root, reg)
        except Exception:
            logger.warning("Project flow projection failed for %s", flow_path, exc_info=True)

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

    def close_project(self, owner_id: int) -> None:
        with get_db_context() as db:
            repository.deactivate_owner(db, owner_id)
        self._load().pop(owner_id, None)

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
