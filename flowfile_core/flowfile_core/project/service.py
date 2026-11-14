"""ProjectSyncService: invisible DB→files projection + project lifecycle.

Hooks at the tail of the central save/store functions call the ``project_sync``
singleton. With no active project every call is a cheap no-op; with one active,
the affected file is re-projected as a side-effect — never blocking the primary
operation (projection failures are logged and swallowed).
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import yaml
from fastapi import HTTPException

from flowfile_core import __version__
from flowfile_core.configs import settings
from flowfile_core.database import backup
from flowfile_core.database.connection import get_database_path, get_db_context
from flowfile_core.database.models import FlowRegistration
from flowfile_core.fileExplorer.funcs import _is_contained, validate_path_under_cwd
from flowfile_core.project import git_ops, kinds, projection, repository
from flowfile_core.project.importer import (
    import_project,
    preflight_import_caps,
    preflight_import_caps_worktree,
    preflight_import_files,
    preflight_import_files_worktree,
)
from flowfile_core.project.manifest import (
    MANIFEST_NAME,
    PROJECT_FORMAT,
    ProjectManifest,
    kinds_to_save,
    parse_manifest,
    read_manifest,
    write_gitignore,
    write_manifest,
)
from flowfile_core.project.models import ActiveProject, NoActiveProjectError, SetupResult
from shared.storage_config import flow_path_spellings, storage

logger = logging.getLogger(__name__)


def _save_manifest(root: Path, m: ProjectManifest) -> None:
    """Write ``project.yaml`` in the current format, listing the kinds a save of it exports."""
    m.project_format = PROJECT_FORMAT
    m.kinds = kinds_to_save(m)
    write_manifest(root, m)


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
    Electron returns the resolved path unchanged. Keep both branches on ``Path.resolve()`` — the
    realpath + manual ``startswith`` alternative balloons CodeQL py/path-injection into ~33 downstream
    alerts. The two ``resolve()`` calls here stay flagged (downstream is confined / electron is the
    user's own machine); dismiss those two as false positives in the Code Scanning UI."""
    base = project_root_base(owner_id)
    if base is None:
        validated = validate_path_under_cwd(folder_path)
        return Path(validated).expanduser().resolve()
    candidate = folder_path.strip()
    if candidate and os.sep not in candidate and (os.altsep is None or os.altsep not in candidate):
        root = (base / candidate).resolve()
    else:
        root = base.joinpath(candidate).expanduser().resolve()
        if not _is_contained(str(base), str(root)):
            raise HTTPException(status_code=403, detail="Access denied")
    for shared_root in (storage.base_directory, storage.database_directory, storage.user_data_directory):
        if _is_contained(str(root), str(shared_root)):
            raise HTTPException(status_code=403, detail="Access denied")
    if not _is_contained(str(base), str(root)):
        raise HTTPException(status_code=403, detail="Access denied")
    return root


def _classify_path(path: str) -> tuple[str, str]:
    """Map a project file path to a (kind label, name) for a friendly change summary."""
    owned = kinds.kind_for_path(path)
    if owned is not None:
        kind, stem = owned
        return kind.label, stem
    return "settings", path.split("/")[-1]


def _snapshot(root: Path, owner_id: int, listed=None) -> dict[str, str]:
    """The export of the ``listed`` kinds when given, else of those the folder's ``project.yaml`` lists (every kind
    without one), nothing written."""
    if listed is None:
        m = read_manifest(root)
        listed = None if m is None else m.kinds
    with get_db_context() as db:
        ctx = kinds.ExportContext(db=db, owner_id=owner_id, kinds=None if listed is None else frozenset(listed))
        return kinds.snapshot(ctx)


def write_export(root: Path, owner_id: int) -> None:
    """Write the export of the kinds the folder lists into ``root``, removing the owned files it does not hold."""
    kinds.write_tree(root, _snapshot(root, owner_id))


def _plan_changes(root: Path, owner_id: int, sha: str | None) -> list[kinds.Change]:
    """``compare(snapshot, target)`` for the tree at ``sha``, or the folder when ``None``, over the kinds the
    target lists, the snapshot exporting those same kinds, so a kind the folder no longer lists is compared too.
    A target without a ``kinds`` list is compared over the kinds it holds a file for."""
    if sha is None:
        target = kinds.read_tree(root)
        manifest_file = root / MANIFEST_NAME
        manifest_text = manifest_file.read_text(encoding="utf-8") if manifest_file.is_file() else None
    else:
        target = git_ops.read_tree_at(root, sha)
        manifest_text = target.get(MANIFEST_NAME)
    listed = None
    if manifest_text is not None:
        data = yaml.safe_load(manifest_text) or {}
        listed = parse_manifest(data, root.name).kinds
        if "kinds" not in data:  # a version from before the list: a kind it has no file for is left alone
            held = {owned[0].name for owned in map(kinds.kind_for_path, target) if owned is not None}
            listed = [name for name in listed if name in held]
    snapshot = _snapshot(root, owner_id, [k.name for k in kinds.registered()] if listed is None else listed)
    return kinds.compare(snapshot, target, kinds=listed)


def _change_rows(changes: list[kinds.Change]) -> list[dict]:
    return [
        {"change": c.change, "kind": kinds.get(c.kind).label, "label": c.label, "path": c.path} for c in changes
    ]


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


def _named_changes(raw: list[dict]) -> list[dict]:
    """``_friendly_changes`` without ``project.yaml`` and ``.gitignore``, which name no object."""
    return _friendly_changes([r for r in raw if r["path"] not in (MANIFEST_NAME, ".gitignore")])


def _unsaved_detail(action: str, changes: list[dict]) -> dict:
    """The 409 body of a restore or reload that would lose what the last version does not hold."""
    if get_database_path() is not None:
        net = "A snapshot of the catalog database is also written to db_backups/ first."
    else:
        net = (
            "This catalog database is not snapshotted: the recovery point is the only copy, "
            "and it holds definitions, not stored passwords."
        )
    message = (
        f"{len(changes)} change(s) are not in the last version; a {action} may lose them. "
        f"Save a version first, or keep a recovery point and continue. {net}"
    )
    return {
        "code": "UNSAVED_CHANGES",
        "message": message,
        "changes": changes,
        "database_snapshot": get_database_path() is not None,
    }


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
                    reg = (
                        db.query(FlowRegistration)
                        .filter(FlowRegistration.flow_path.in_(flow_path_spellings(flow_path)))
                        .order_by(FlowRegistration.id)
                        .first()
                    )
                    if reg is not None and reg.owner_id == proj.owner_id:
                        namespace = projection._namespace_path(db, reg.namespace_id)
                        index = projection.flow_index(proj.root)
                        target = projection.project_flow(db, proj.root, reg, index, namespace)
                        if target is not None:
                            projection.remove_stale_flow_files(proj.root, reg.flow_uuid, target, index)
            self._mark_projection(proj.owner_id, failed=False)
        except Exception:
            self._mark_projection(proj.owner_id, failed=True)
            logger.error("Project flow projection failed for %s", flow_path, exc_info=True)

    def flow_deleted(self, flow_uuid: str, user_id: int | None) -> None:
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with git_ops.repo_lock(proj.root):
                projection.remove_flow(proj.root, projection.flow_index(proj.root), flow_uuid=flow_uuid)
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
                        index = projection.flow_index(proj.root)
                        projection.project_schedules_for_registration(db, proj.root, reg, index)
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

    def notebooks_changed(self, user_id: int | None) -> None:
        """Re-project all the owner's notebook files and prune orphaned ones (one hook covers
        create/update/delete/rename, like the manifest hooks). Notebooks are per-file (like flows),
        so this can't use ``_project_manifest`` — it regenerates the dir then prunes. Never raises."""
        proj = self.get_active_project(user_id)
        if proj is None:
            return
        try:
            with git_ops.repo_lock(proj.root):
                with get_db_context() as db:
                    written = projection.regenerate_notebooks(db, proj.root, proj.owner_id)
                projection.prune_orphan_notebooks(proj.root, written)
            self._mark_projection(proj.owner_id, failed=False)
        except Exception:
            self._mark_projection(proj.owner_id, failed=True)
            logger.error("Project notebooks projection failed", exc_info=True)

    # --- lifecycle (explicit user actions via /project router + CLI) ----------

    def init_project(
        self, folder_path: str, name: str, owner_id: int, track_data_artifacts: bool = True
    ) -> ActiveProject:
        root = _confine_project_root(folder_path, owner_id)
        _assert_not_foreign_owned(root, owner_id)
        root.mkdir(parents=True, exist_ok=True)
        existing = read_manifest(root)
        # A new project takes the requested toggle; re-initializing an existing one keeps its manifest value.
        m = existing
        if m is None:
            m = ProjectManifest(name=name, project_id=str(uuid.uuid4()), created_with_version=__version__)
            m.track_data_artifacts = track_data_artifacts
        _save_manifest(root, m)
        write_gitignore(root)
        if not git_ops.git_available():
            logger.warning("git is unavailable; project %r is created without version tracking", name)
        git_ops.init(root)
        with git_ops.repo_lock(root):
            with get_db_context() as db:
                row = repository.upsert_active(db, m.name, str(root), owner_id, m.track_data_artifacts)
                project = ActiveProject(row.id, m.name, root, owner_id, m.track_data_artifacts)
            write_export(root, owner_id)
            sha = git_ops.commit_all(root, "Initialize Flowfile project")
            with get_db_context() as db:
                repository.set_head_sha(db, project.id, sha or git_ops.head_sha(root))
        with self._cache_lock:
            self._load()[owner_id] = project
        return project

    def open_project(self, folder_path: str, owner_id: int) -> tuple[ActiveProject, SetupResult]:
        root = _confine_project_root(folder_path, owner_id)
        _assert_not_foreign_owned(root, owner_id)
        # Check sizes, then every file, before .gitignore or the database is touched.
        preflight_import_caps_worktree(root)
        preflight_import_files_worktree(root)
        m = read_manifest(root)
        if m is None:
            raise FileNotFoundError(f"No Flowfile project found at the requested path (missing {MANIFEST_NAME})")
        with git_ops.project_lock(root):
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
        git_ops.init(proj.root)  # heal a folder created while git was unavailable; no-op otherwise
        with git_ops.project_lock(proj.root):
            m = read_manifest(proj.root)
            if m is not None:
                _save_manifest(proj.root, m)
            write_export(proj.root, proj.owner_id)
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

    def _guard_before_apply(self, proj: ActiveProject, force: bool, action: str) -> str | None:
        """Name what a restore or reload would lose, and keep it first when ``force`` is set.

        The database is exported to a temp folder and compared with the last version, so a change
        no hook wrote to the folder is seen too. A restore also refuses a dirty folder, since its
        ``read-tree --reset`` discards hand edits; a reload applies them, so it refuses only unsaved
        database changes. With ``force`` a restore autosaves the folder and commits the export on the
        branch, and a reload commits the export under ``refs/flowfile/recovery/`` with the branch
        left alone. Returns the sha of the last recovery commit, if any. Holds the caller's lock."""
        dirty = action == "restore" and git_ops.is_dirty(proj.root)
        with tempfile.TemporaryDirectory(prefix="flowfile-project-") as tmp:
            folder = Path(tmp) / "tree"
            changes = self._database_changes(proj, folder)
            if not changes and not dirty:
                return None
            if not force:
                listed = {c["path"] for c in changes}
                if dirty:
                    folder_rows = [r for r in git_ops.uncommitted_changes(proj.root) if r["path"] not in listed]
                    changes += _named_changes(folder_rows)
                raise HTTPException(status_code=409, detail=_unsaved_detail(action, changes))
            recovery_sha = git_ops.commit_all(proj.root, "Autosave before restore") if dirty else None
            if changes:
                if action == "restore":
                    recovery_sha = git_ops.commit_folder(proj.root, folder, "Database before restore", "HEAD") or (
                        recovery_sha
                    )
                else:
                    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                    recovery_sha = git_ops.commit_folder(
                        proj.root, folder, "Database before reload", f"refs/flowfile/recovery/{stamp}"
                    )
            return recovery_sha

    def _database_changes(self, proj: ActiveProject, folder: Path) -> list[dict]:
        """Export the database into ``folder`` (seeded with the last version's files, so same-named
        flows keep the file names that version gave them) and name what differs from that version."""
        if not git_ops.seed_folder(proj.root, folder):
            return []
        manifest_file = proj.root / MANIFEST_NAME
        if manifest_file.exists():
            shutil.copyfile(manifest_file, folder / MANIFEST_NAME)
        write_export(folder, proj.owner_id)
        return _named_changes(git_ops.diff_against(proj.root, folder))

    @staticmethod
    def _snapshot_database() -> None:
        """Snapshot a SQLite catalog before a pruning import; a server catalog has no file to copy."""
        db_path = get_database_path()
        if db_path is not None:
            backup.create_snapshot(db_path, "pre_apply")

    def restore_version(self, user_id: int, sha: str, label: str | None = None, force: bool = False) -> SetupResult:
        """Reset files to ``sha``, apply its plan to the DB, and record the export after it as a new version.

        Restore is one complete action: the working tree is reset to ``sha`` (deletions included),
        the DB imports it and deletes exactly the plan's removals (``_plan_changes``, computed before
        the reset), and the export after the apply is committed as "Restore: …", so history stays
        clean with no dangling unsaved state. Unsaved work is never silently lost: a dirty tree or a
        database that differs from the last version is 409'd unless ``force``, and on ``force`` both
        are committed into history first (``_guard_before_apply``).
        """
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        with git_ops.project_lock(proj.root):
            # Check the target tree before the autosave, the file reset or the DB rebuild, so a bad
            # target aborts with nothing changed, history included.
            preflight_import_caps(proj.root, sha)
            preflight_import_files(proj.root, sha)
            recovery_sha = self._guard_before_apply(proj, force, "restore")
            removed = [c for c in _plan_changes(proj.root, proj.owner_id, sha) if c.change == "removed"]
            git_ops.restore(proj.root, sha)
            self._snapshot_database()
            result = import_project(proj.root, proj.owner_id, removed)
            self._sync_track_setting_from_manifest(proj)
            write_export(proj.root, proj.owner_id)
            message = f"Restore: {label}" if label else f"Restore version {sha[:8]}"
            new_sha = git_ops.commit_all(proj.root, message)
            with get_db_context() as db:
                repository.set_head_sha(db, proj.id, new_sha or git_ops.head_sha(proj.root))
        result.recovery_sha = recovery_sha
        return result

    def reload_from_disk(self, user_id: int, force: bool = False) -> SetupResult:
        """Accept on-disk (external) changes by applying the folder's plan to the DB; no export is written.

        A dirty tree is what gets applied, so it needs no ``force``. A database that differs from the
        last version is 409'd unless ``force``, and on ``force`` its export is kept under
        ``refs/flowfile/recovery/`` before the apply, with the branch left alone."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        with git_ops.project_lock(proj.root):
            # Check the working tree before the recovery point or the DB rebuild, so a bad file aborts cleanly.
            preflight_import_caps_worktree(proj.root)
            preflight_import_files_worktree(proj.root)
            recovery_sha = self._guard_before_apply(proj, force, "reload")
            removed = [c for c in _plan_changes(proj.root, proj.owner_id, None) if c.change == "removed"]
            self._snapshot_database()
            result = import_project(proj.root, proj.owner_id, removed)
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

    def unsaved_changes(self, user_id: int) -> list[dict]:
        """What the database holds that the last version does not, per object: ``compare(HEAD, snapshot)``.

        HEAD's owned files are taken unfiltered, so a kind the folder stopped listing shows as removed. A hand
        edit of the folder is not an unsaved change; a reload applies it."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        if not git_ops.is_repo(proj.root):
            return []
        head = git_ops.read_tree_at(proj.root, "HEAD")
        return _change_rows(kinds.compare(head, _snapshot(proj.root, proj.owner_id)))

    def plan(self, user_id: int, sha: str | None = None) -> list[dict]:
        """What a restore of ``sha``, or a reload of the folder when ``sha`` is ``None``, would create, change and
        remove: ``compare(snapshot, target)`` over the kinds the target lists, after the same preflight the restore
        or reload runs, so a bad target is the same error. A removal names its consequence."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        root = proj.root
        with git_ops.project_lock(root):
            if sha is None:
                preflight_import_caps_worktree(root)
                preflight_import_files_worktree(root)
            else:
                preflight_import_caps(root, sha)
                preflight_import_files(root, sha)
            changes = _plan_changes(root, proj.owner_id, sha)
        return [{**row, "consequence": c.consequence} for row, c in zip(_change_rows(changes), changes, strict=True)]

    def close_project(self, owner_id: int) -> None:
        with get_db_context() as db:
            repository.deactivate_owner(db, owner_id)
        with self._cache_lock:
            self._load().pop(owner_id, None)
            self._projection_failed.discard(owner_id)

    def update_settings(self, user_id: int, track_data_artifacts: bool) -> bool:
        """Flip the track-data-artifacts toggle: add or remove the data kinds in project.yaml's ``kinds``, update
        the DB mirror, then re-project so tables.yaml / models.yaml are dropped (off) or regenerated (on)."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        m = read_manifest(proj.root)
        if m is None:
            raise NoActiveProjectError("No active project")
        with git_ops.repo_lock(proj.root):
            m.track_data_artifacts = track_data_artifacts
            _save_manifest(proj.root, m)
            with get_db_context() as db:
                repository.set_track_data_artifacts(db, proj.id, track_data_artifacts)
            proj.track_data_artifacts = track_data_artifacts
            write_export(proj.root, proj.owner_id)
        return track_data_artifacts

    def has_uncommitted_changes(self, user_id: int) -> bool:
        proj = self.get_active_project(user_id)
        if proj is None:
            return False
        return bool(self.unsaved_changes(user_id))

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
