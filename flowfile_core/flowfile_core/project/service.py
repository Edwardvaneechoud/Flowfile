"""ProjectSyncService: invisible DB→files projection + project lifecycle.

Hooks at the tail of the central save/store functions call the ``project_sync``
singleton. With no active project every call is a cheap no-op; with one active,
the affected file is re-projected as a side-effect — never blocking the primary
operation (projection failures are logged and swallowed).
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path

from fastapi import HTTPException

from flowfile_core import __version__
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration
from flowfile_core.fileExplorer.funcs import _is_contained, _local_filesystem_roots
from flowfile_core.project import git_ops, projection, repository
from flowfile_core.project.importer import import_project, preflight_import_caps, preflight_import_caps_worktree
from flowfile_core.project.manifest import (
    MANIFEST_NAME,
    ROOT_MANIFESTS,
    ProjectManifest,
    read_manifest,
    write_gitignore,
    write_manifest,
)
from flowfile_core.project.models import ActiveProject, NoActiveProjectError, SetupResult
from shared.storage_config import storage

logger = logging.getLogger(__name__)


def project_root_base(owner_id: int) -> Path | None:
    """The single legal project-root subtree for ``owner_id`` in multi-tenant mode, else ``None``.

    Electron is single-user and unconfined (``None`` ⇒ no confinement). Docker/package confine every
    project to ``<user_data>/projects/<owner_id>`` so one tenant cannot root a project in another's
    subtree or in a shared internal dir."""
    if settings.is_electron_mode():
        return None
    return (storage.user_data_directory / "projects" / str(owner_id)).resolve()


def _confine_project_root(folder_path: str, owner_id: int) -> Path:
    """Resolve the requested folder to the owner's confined project subtree (multi-tenant only).

    A bare name (no separator) is joined under the base; an absolute/relative path must already be
    contained in it. Roots equal to / ancestors of the shared internal roots are rejected.
    Electron is unconfined: the resolved path is allowed as long as it lands under a real
    filesystem root (the inline normalize + startswith barrier CodeQL recognizes)."""
    base = project_root_base(owner_id)
    if base is None:
        resolved = os.path.realpath(os.path.expanduser(folder_path))
        for fs_root in _local_filesystem_roots():
            root_real = os.path.realpath(fs_root)
            prefix = root_real if root_real.endswith(os.sep) else root_real + os.sep
            if resolved == root_real or resolved.startswith(prefix):
                return Path(resolved)
        raise HTTPException(status_code=403, detail="Access denied")
    candidate = folder_path.strip()
    if candidate and os.sep not in candidate and (os.altsep is None or os.altsep not in candidate):
        root = (base / candidate).resolve()
    else:
        root = Path(candidate).expanduser().resolve()
    for shared_root in (storage.base_directory, storage.database_directory, storage.user_data_directory):
        if _is_contained(str(root), str(shared_root)):
            raise HTTPException(status_code=403, detail="Access denied")
    if not _is_contained(str(base), str(root)):
        raise HTTPException(status_code=403, detail="Access denied")
    return root


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
    # Root-level manifests mirror a whole resource category; show the category rather than "settings".
    if "/" not in path and stem in ROOT_MANIFESTS:
        return ROOT_MANIFESTS[stem], stem
    return "settings", name


def _assert_not_foreign_owned(root: Path, owner_id: int) -> None:
    """In multi-tenant mode, reject opening/initing a folder whose existing WorkspaceProject is owned
    by another user (closes cross-tenant project takeover / metadata import). Electron is unconfined."""
    if settings.is_electron_mode():
        return
    with get_db_context() as db:
        existing = repository.get_by_path(db, str(root))
    if existing is not None and existing.owner_id != owner_id:
        raise HTTPException(status_code=403, detail="Project folder is owned by another user")


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
        self._suppressed: set[int] = set()  # owner_ids whose projection is paused (per-owner, not global)
        self._projection_failed: set[int] = set()  # owner_ids whose last projection hook errored
        self._cache_lock = threading.RLock()  # guards _by_owner/_suppressed/_projection_failed; separate from repo_lock

    def _mark_projection(self, owner_id: int, failed: bool) -> None:
        with self._cache_lock:
            if failed:
                self._projection_failed.add(owner_id)
            else:
                self._projection_failed.discard(owner_id)

    def projection_failed(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        with self._cache_lock:
            return user_id in self._projection_failed

    @contextmanager
    def suppress_projection(self, owner_id: int):
        """Disable projection hooks for ``owner_id`` for the duration.

        Only the importing owner's hooks no-op; every other user keeps projecting normally.
        """
        with self._cache_lock:
            self._suppressed.add(owner_id)
        try:
            yield
        finally:
            with self._cache_lock:
                self._suppressed.discard(owner_id)

    # --- active-project cache -------------------------------------------------

    def _load(self) -> dict[int, ActiveProject]:
        with self._cache_lock:
            if self._by_owner is not None:
                return self._by_owner
            try:
                by_owner: dict[int, ActiveProject] = {}
                with get_db_context() as db:
                    for p in repository.get_active_projects(db):
                        by_owner[p.owner_id] = ActiveProject(
                            p.id, p.name, Path(p.folder_path), p.owner_id, p.track_data_artifacts
                        )
                self._by_owner = by_owner
                return by_owner
            except Exception:
                logger.warning("Failed to load active projects", exc_info=True)
                return {}

    def get_active_project(self, user_id: int | None) -> ActiveProject | None:
        if user_id is None:
            return None
        with self._cache_lock:
            if user_id in self._suppressed:
                return None
        return self._load().get(user_id)

    # --- hooks (called after the primary commit; must never raise) ------------

    def flow_saved(self, flow_path: str, user_id: int | None) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with git_ops.repo_lock(proj.root):
                with get_db_context() as db:
                    reg = db.query(FlowRegistration).filter(FlowRegistration.flow_path == flow_path).first()
                    if reg is not None and reg.owner_id == proj.owner_id:
                        namespace = projection._namespace_path(db, reg.namespace_id)
                        target = projection.project_flow(proj.root, reg, namespace)
                        if target is not None:
                            projection.remove_stale_flow_files(proj.root, reg.flow_uuid, target)
            self._mark_projection(proj.owner_id, failed=False)
        except Exception:
            self._mark_projection(proj.owner_id, failed=True)
            logger.error("Project flow projection failed for %s", flow_path, exc_info=True)

    def flow_deleted(self, flow_uuid: str, user_id: int | None) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            projection.remove_flow(proj.root, flow_uuid=flow_uuid)
            self._mark_projection(proj.owner_id, failed=False)
        except Exception:
            self._mark_projection(proj.owner_id, failed=True)
            logger.error("Project flow-delete projection failed for %s", flow_uuid, exc_info=True)

    def connection_changed(self, kind: str, connection_name: str, user_id: int | None, deleted: bool = False) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with git_ops.repo_lock(proj.root):
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
            self._mark_projection(proj.owner_id, failed=False)
        except Exception:
            self._mark_projection(proj.owner_id, failed=True)
            logger.error("Project connection projection failed for %s/%s", kind, connection_name, exc_info=True)

    def schedule_changed(self, registration_id: int, user_id: int | None, deleted: bool = False) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with git_ops.repo_lock(proj.root):
                with get_db_context() as db:
                    reg = db.query(FlowRegistration).filter(FlowRegistration.id == registration_id).first()
                    if reg is not None and reg.owner_id == proj.owner_id:
                        projection.project_schedules_for_registration(db, proj.root, reg)
            self._mark_projection(proj.owner_id, failed=False)
        except Exception:
            self._mark_projection(proj.owner_id, failed=True)
            logger.error("Project schedule projection failed for reg %s", registration_id, exc_info=True)

    def _project_manifest(self, user_id: int | None, regen, label: str, require_track: bool = False) -> None:
        """Re-run one DB→file manifest regenerator under the repo lock; swallow+log on failure.

        ``regen(db, root, owner_id)`` is the projection writer; ``require_track`` skips when the
        project opts out of data-artifact tracking."""
        proj = self.get_active_project(user_id)
        if proj is None or (require_track and not proj.track_data_artifacts):
            return
        try:
            with git_ops.repo_lock(proj.root):
                with get_db_context() as db:
                    regen(db, proj.root, proj.owner_id)
            self._mark_projection(proj.owner_id, failed=False)
        except Exception:
            self._mark_projection(proj.owner_id, failed=True)
            logger.error("Project %s projection failed", label, exc_info=True)

    def secret_changed(self, user_id: int | None) -> None:
        self._project_manifest(user_id, projection.regenerate_secret_manifest, "secret-manifest")

    def namespace_changed(self, user_id: int | None) -> None:
        self._project_manifest(user_id, projection.regenerate_namespace_manifest, "namespace-manifest")

    def tables_changed(self, user_id: int | None) -> None:
        self._project_manifest(user_id, projection.regenerate_tables_manifest, "tables-manifest", require_track=True)

    def artifacts_changed(self, user_id: int | None) -> None:
        self._project_manifest(user_id, projection.regenerate_models_manifest, "models-manifest", require_track=True)

    def kernels_changed(self, user_id: int | None) -> None:
        self._project_manifest(user_id, projection.regenerate_kernels_manifest, "kernels-manifest")

    def visualizations_changed(self, user_id: int | None) -> None:
        self._project_manifest(user_id, projection.regenerate_visualizations_manifest, "visualizations-manifest")

    def dashboards_changed(self, user_id: int | None) -> None:
        self._project_manifest(user_id, projection.regenerate_dashboards_manifest, "dashboards-manifest")

    # --- lifecycle (explicit user actions via /project router + CLI) ----------

    def init_project(
        self, folder_path: str, name: str, owner_id: int, track_data_artifacts: bool = True
    ) -> ActiveProject:
        root = _confine_project_root(folder_path, owner_id)
        _assert_not_foreign_owned(root, owner_id)
        root.mkdir(parents=True, exist_ok=True)
        existing = read_manifest(root)
        # A new project takes the requested toggle; re-initializing an existing one keeps its manifest value.
        m = existing or ProjectManifest(
            name=name,
            project_id=str(uuid.uuid4()),
            created_with_version=__version__,
            track_data_artifacts=track_data_artifacts,
        )
        write_manifest(root, m)
        write_gitignore(root)
        git_ops.init(root)
        with git_ops.repo_lock(root):
            with get_db_context() as db:
                row = repository.upsert_active(db, m.name, str(root), owner_id, m.track_data_artifacts)
                project = ActiveProject(row.id, m.name, root, owner_id, m.track_data_artifacts)
                projection.project_all(db, root, owner_id)
            sha = git_ops.commit_all(root, "Initialize Flowfile project")
            with get_db_context() as db:
                repository.set_head_sha(db, project.id, sha or git_ops.head_sha(root))
        with self._cache_lock:
            self._load()[owner_id] = project
        return project

    def open_project(self, folder_path: str, owner_id: int) -> tuple[ActiveProject, SetupResult]:
        root = _confine_project_root(folder_path, owner_id)
        _assert_not_foreign_owned(root, owner_id)
        m = read_manifest(root)
        if m is None:
            raise FileNotFoundError(f"No Flowfile project found at the requested path (missing {MANIFEST_NAME})")
        write_gitignore(root)
        with get_db_context() as db:
            row = repository.upsert_active(db, m.name, str(root), owner_id, m.track_data_artifacts)
            project = ActiveProject(row.id, m.name, root, owner_id, m.track_data_artifacts)
        with self._cache_lock:
            self._load()[owner_id] = project
        return project, import_project(root, owner_id)

    def save_version(self, user_id: int, message: str) -> str | None:
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        with git_ops.repo_lock(proj.root):
            with get_db_context() as db:
                projection.project_all(db, proj.root, proj.owner_id)
            sha = git_ops.commit_all(proj.root, message)
            with get_db_context() as db:
                repository.set_head_sha(db, proj.id, sha or git_ops.head_sha(proj.root))
        return sha

    def _sync_track_setting_from_manifest(self, proj: ActiveProject) -> None:
        """Adopt the on-disk manifest's track-data-artifacts value after a files→DB rebuild.

        Restore/reload just reset the working tree (incl. ``project.yaml``) to an authoritative state,
        so the DB row and the cached project must match it — otherwise the Settings UI and the
        projection hooks (which read the cached flag) go stale. Mutates ``proj`` in place, mirroring
        :meth:`update_settings`.
        """
        m = read_manifest(proj.root)
        if m is None:
            return
        with get_db_context() as db:
            repository.set_track_data_artifacts(db, proj.id, m.track_data_artifacts)
        proj.track_data_artifacts = m.track_data_artifacts

    def _autosave_before_reset(self, proj: ActiveProject, force: bool, action: str) -> str | None:
        """Recovery point before a destructive reset/prune. Holds the caller's ``repo_lock``.

        ``restore``/``reload`` reset the working tree and hard-prune the DB to match the files, so any
        uncommitted work is gone afterwards. To never silently destroy work: when the tree is
        dirty and ``force`` is not set → 409 so the user explicitly confirms; otherwise snapshot the
        current state into git history first (always recoverable) and return its sha."""
        if not git_ops.is_dirty(proj.root):
            return None
        if not force:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Project has uncommitted changes that {action} would discard. "
                    "Save a version first, or retry with force=true to autosave and continue."
                ),
            )
        return git_ops.commit_all(proj.root, f"Autosave before {action}")

    def restore_version(self, user_id: int, sha: str, label: str | None = None, force: bool = False) -> SetupResult:
        """Reset files to ``sha``, rebuild + prune the DB to match, and record it as a new version.

        Restore is one complete action: the working tree is reset to ``sha`` (deletions included),
        the DB is rebuilt and pruned to match, and a "Restore: …" commit is made so history stays
        clean with no dangling unsaved state. Uncommitted work is never silently lost: a dirty tree
        is 409'd unless ``force``, and on ``force`` it is autosaved into history first.
        """
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        with git_ops.repo_lock(proj.root):
            recovery_sha = self._autosave_before_reset(proj, force, "restore")
            # Cap-check the target tree before resetting files or rebuilding the DB, so a breach
            # aborts cleanly instead of leaving files at sha with a half-rebuilt DB.
            preflight_import_caps(proj.root, sha)
            git_ops.restore(proj.root, sha)
            result = import_project(proj.root, proj.owner_id, prune=True)
            self._sync_track_setting_from_manifest(proj)
            message = f"Restore: {label}" if label else f"Restore version {sha[:8]}"
            new_sha = git_ops.commit_all(proj.root, message)
            with get_db_context() as db:
                repository.set_head_sha(db, proj.id, new_sha or git_ops.head_sha(proj.root))
        result.recovery_sha = recovery_sha
        return result

    def reload_from_disk(self, user_id: int, force: bool = False) -> SetupResult:
        """Accept on-disk (external) changes by rebuilding + pruning the DB to match the files.

        Uncommitted work is never silently lost: a dirty tree is 409'd unless ``force``, and on
        ``force`` it is autosaved into git history before the prune so it stays recoverable."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        with git_ops.repo_lock(proj.root):
            recovery_sha = self._autosave_before_reset(proj, force, "reload")
            # Cap-check the working tree before the rebuild mutates the DB, so a breach aborts cleanly.
            preflight_import_caps_worktree(proj.root)
            result = import_project(proj.root, proj.owner_id, prune=True)
            self._sync_track_setting_from_manifest(proj)
        result.recovery_sha = recovery_sha
        return result

    def changes_for_version(self, user_id: int, sha: str) -> list[dict]:
        """Friendly summary of what restoring ``sha`` would change vs the latest saved version."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        return _friendly_changes(git_ops.diff_name_status(proj.root, "HEAD", sha))

    def version_diff(self, user_id: int, sha: str) -> list[dict]:
        """Friendly changelog of what a specific version changed vs the one before it."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        return _friendly_changes(git_ops.changes_in(proj.root, sha))

    def uncommitted_changes(self, user_id: int) -> list[dict]:
        """Friendly summary of the unsaved working-tree changes (what a Save version would record)."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        return _friendly_changes(git_ops.uncommitted_changes(proj.root))

    def close_project(self, owner_id: int) -> None:
        with get_db_context() as db:
            repository.deactivate_owner(db, owner_id)
        with self._cache_lock:
            self._load().pop(owner_id, None)
            self._projection_failed.discard(owner_id)

    def update_settings(self, user_id: int, track_data_artifacts: bool) -> bool:
        """Flip the track-data-artifacts toggle: rewrite project.yaml + the DB mirror, then
        re-project so tables.yaml / models.yaml are dropped (off) or regenerated (on)."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        m = read_manifest(proj.root)
        if m is None:
            raise NoActiveProjectError("No active project")
        m.track_data_artifacts = track_data_artifacts
        write_manifest(proj.root, m)
        with get_db_context() as db:
            repository.set_track_data_artifacts(db, proj.id, track_data_artifacts)
        proj.track_data_artifacts = track_data_artifacts
        with get_db_context() as db:
            projection.project_all(db, proj.root, proj.owner_id)
        return track_data_artifacts

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
