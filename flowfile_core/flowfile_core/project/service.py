"""ProjectSyncService: the project lifecycle.

The folder is written only by ``write_export`` (init, save, the settings toggle, after a restore);
status is read when asked, by comparing the last version with the database.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import threading
import uuid
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import UTC, datetime
from pathlib import Path

import yaml
from fastapi import HTTPException

from flowfile_core import __version__
from flowfile_core.configs import settings
from flowfile_core.configs.app_settings import (
    PROJECT_REMOTE_TOKEN_KEY,
    delete_user_secret,
    get_user_secret,
    set_user_secret,
)
from flowfile_core.database import backup
from flowfile_core.database.connection import get_database_path, get_db_context
from flowfile_core.fileExplorer.funcs import _is_contained, validate_path_under_cwd
from flowfile_core.project import git_ops, kinds, merging, repository
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
    write_gitattributes,
    write_gitignore,
    write_manifest,
)
from flowfile_core.project.models import ActiveProject, NoActiveProjectError, SetupResult
from shared.flow_merge.diff import diff as flow_diff
from shared.flow_merge.render import review_items
from shared.flow_merge.yaml_io import dump_yaml
from shared.storage_config import storage

logger = logging.getLogger(__name__)

REMOTE_USER_CONFIG = "flowfile.remoteuser"


@dataclass
class PullResult:
    """What a pull did: ``up_to_date`` or ``ahead`` (nothing to apply), ``fast_forward`` or ``merged`` (applied,
    ``result`` the import), or ``conflicted`` (nothing applied; ``conflicts`` are what to settle)."""

    state: str
    sha: str | None = None
    result: SetupResult | None = None
    conflicts: list[dict] = dc_field(default_factory=list)
    warnings: list[str] = dc_field(default_factory=list)


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


def _stamp_current_format(root: Path) -> None:
    """Raise the folder's ``project.yaml`` to the current format, keeping its kinds: an export is written in the
    current layout, so the folder must not tell an older Flowfile it can read it."""
    m = read_manifest(root)
    if m is not None and m.project_format != PROJECT_FORMAT:
        m.project_format = PROJECT_FORMAT
        write_manifest(root, m)


def write_export(root: Path, owner_id: int) -> None:
    """Write the export of the kinds the folder lists into ``root``, removing the owned files it does not hold, with
    ``project.yaml`` in the current format."""
    _stamp_current_format(root)
    kinds.write_tree(root, _snapshot(root, owner_id))


def _plan_changes(root: Path, owner_id: int, sha: str | None) -> list[kinds.Change]:
    """``compare(snapshot, target)`` for the tree at ``sha``, or the folder when ``None``, over the kinds the
    target lists, the snapshot exporting those same kinds, so a kind the folder no longer lists is compared too.
    A target without a ``kinds`` list, or without a ``project.yaml``, is compared over the kinds it holds a
    file for, so a kind it has no file for is left alone."""
    if sha is None:
        target = kinds.read_tree(root)
        manifest_file = root / MANIFEST_NAME
        manifest_text = manifest_file.read_text(encoding="utf-8") if manifest_file.is_file() else None
    else:
        target = git_ops.read_tree_at(root, sha)
        manifest_text = target.get(MANIFEST_NAME)
    data = (yaml.safe_load(manifest_text) or {}) if manifest_text is not None else {}
    listed = parse_manifest(data, root.name).kinds
    if "kinds" not in data:
        held = {owned[0].name for owned in map(kinds.kind_for_path, target) if owned is not None}
        listed = [name for name in listed if name in held]
    return kinds.compare(_snapshot(root, owner_id, listed), target, kinds=listed, strict=True)


def _removed(changes: list[kinds.Change]) -> list[kinds.Change]:
    return [c for c in changes if c.change == "removed"]


def _paths_at_head(root: Path, removed: list[kinds.Change]) -> set[str]:
    """The files that held the ``removed`` objects in the last version. A removal names the export's file, while the
    edit that removed the object can be in another, such as the root list an older folder keeps a kind in; a kind
    whose files there do not split into objects gives all of them."""
    if not removed:
        return set()
    head = git_ops.read_tree_at(root, "HEAD")
    paths: set[str] = set()
    for name in {c.kind for c in removed}:
        kind = kinds.get(name)
        files = {rel: text for rel, text in head.items() if (owned := kinds.kind_for_path(rel)) and owned[0] is kind}
        try:
            entries = kind.entries(files)
        except Exception:
            paths |= set(files)
            continue
        paths |= {entries[c.key].path for c in removed if c.kind == name and c.key in entries}
    return paths


def _change_rows(changes: list[kinds.Change]) -> list[dict]:
    return [{"change": c.change, "kind": kinds.get(c.kind).label, "label": c.label, "path": c.path} for c in changes]


def _unsaved_rows(proj: ActiveProject) -> list[dict]:
    """``compare(HEAD tree, snapshot)`` as rows: what the database holds that the last version does not."""
    head = git_ops.read_tree_at(proj.root, "HEAD")
    return _change_rows(kinds.compare(head, _snapshot(proj.root, proj.owner_id)))


def _edited_open_flows(owner_id: int, plan: list[kinds.Change]) -> list[tuple]:
    """The owner's open flows whose canvas holds edits no file has yet and whose file ``plan`` rewrites or removes,
    each with its plan change: an apply would leave such a canvas on a file from another version."""
    from flowfile_core import flow_file_handler
    from flowfile_core.database.models import FlowRegistration

    by_uuid = {c.key: c for c in plan if c.kind == "flow"}
    edited = [f for f in flow_file_handler.get_user_flows(owner_id) if f.flow_settings.path and f.has_unsaved_changes()]
    if not by_uuid or not edited:
        return []
    with get_db_context() as db:
        registered = (
            db.query(FlowRegistration.flow_path, FlowRegistration.flow_uuid)
            .filter(FlowRegistration.owner_id == owner_id, FlowRegistration.flow_uuid.in_(list(by_uuid)))
            .all()
        )
    change_of = {path: by_uuid[flow_uuid] for path, flow_uuid in registered}
    return [(f, change_of[f.flow_settings.path]) for f in edited if f.flow_settings.path in change_of]


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


def _with_step_changes(root: Path, sha: str, rows: list[dict]) -> list[dict]:
    """A changed flow's row gains ``steps``: what changed in it step by step, for the version's review."""
    flows = [
        row
        for row in rows
        if row["change"] == "modified" and (owned := kinds.kind_for_path(row["path"])) and owned[0].name == "flow"
    ]
    parent = git_ops.parent_of(root, sha) if flows else None
    if parent is None:
        return rows
    paths = [row["path"] for row in flows]
    befores, afters = git_ops.read_blobs(root, parent, paths), git_ops.read_blobs(root, sha, paths)
    for row in flows:
        before, after = befores.get(row["path"]), afters.get(row["path"])
        if before is None or after is None:
            continue
        try:
            row["steps"] = review_items(flow_diff(yaml.safe_load(before) or {}, yaml.safe_load(after) or {}))
        except (yaml.YAMLError, AttributeError, TypeError) as exc:
            logger.warning("No step changes for %s at %s: %s", row["path"], sha, exc)
    return rows


def _named_changes(raw: list[dict]) -> list[dict]:
    """``_friendly_changes`` without ``project.yaml``, ``.gitignore`` and ``.gitattributes``, which name no object."""
    return _friendly_changes([r for r in raw if r["path"] not in (MANIFEST_NAME, ".gitignore", ".gitattributes")])


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
        self._cache_lock = threading.RLock()  # guards _by_owner; separate from repo_lock

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
        return self._load().get(user_id)

    def _unapplied_edits(self, proj: ActiveProject) -> list[dict]:
        """The folder's uncommitted rows of files a kind owns that applying the folder would still change.

        Compared per entry, as the plan is, so an edit a reload already applied is none, however the export
        spells it. The plan is computed only when such rows exist, so a clean folder costs one ``git status``;
        a folder that cannot be read counts as edited."""
        rows = [r for r in git_ops.uncommitted_changes(proj.root) if kinds.kind_for_path(r["path"]) is not None]
        if not rows:
            return []
        try:
            plan = _plan_changes(proj.root, proj.owner_id, None)
            changed = {c.path for c in plan} | _paths_at_head(proj.root, _removed(plan))
        except (OSError, ValueError, yaml.YAMLError):
            return rows
        return [r for r in rows if r["path"] in changed]

    def external_edits(self, user_id: int) -> bool:
        """True when the folder holds edits of files a kind owns that were not applied to the database, or a
        ``project.yaml`` that cannot be read, so a reload names what is wrong with it."""
        proj = self.get_active_project(user_id)
        if proj is None:
            return False
        try:
            read_manifest(proj.root)
        except ValueError:
            return True
        return bool(self._unapplied_edits(proj))

    # --- lifecycle (explicit user actions via /project router + CLI) ----------

    def init_project(
        self, folder_path: str, name: str, owner_id: int, track_data_artifacts: bool = True
    ) -> ActiveProject:
        root = _confine_project_root(folder_path, owner_id)
        _assert_not_foreign_owned(root, owner_id)
        root.mkdir(parents=True, exist_ok=True)
        if not git_ops.git_available():
            logger.warning("git is unavailable; project %r is created without version tracking", name)
        git_ops.init(root)
        with git_ops.project_lock(root):
            # A new project takes the requested toggle; re-initializing an existing one keeps its manifest value.
            m = read_manifest(root)
            if m is None:
                m = ProjectManifest(name=name, project_id=str(uuid.uuid4()), created_with_version=__version__)
                m.track_data_artifacts = track_data_artifacts
            _save_manifest(root, m)
            write_gitignore(root)
            write_gitattributes(root)
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
            write_gitattributes(root)
            with get_db_context() as db:
                row = repository.upsert_active(db, m.name, str(root), owner_id, m.track_data_artifacts)
                project = ActiveProject(row.id, m.name, root, owner_id, m.track_data_artifacts)
            with self._cache_lock:
                self._load()[owner_id] = project
            return project, import_project(root, owner_id)

    def save_version(self, user_id: int, message: str, overwrite: bool = False) -> str | None:
        """Write the export and commit it; un-applied folder edits are a 409 ``FOLDER_EDITED`` unless ``overwrite``."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        git_ops.init(proj.root)  # heal a folder created while git was unavailable; no-op otherwise
        with git_ops.project_lock(proj.root):
            self._refuse_during_merge(proj)
            m = read_manifest(proj.root)
            snapshot = _snapshot(proj.root, proj.owner_id, None if m is None else kinds_to_save(m))
            edits = self._unapplied_edits(proj)
            if edits and not overwrite:
                refusal = (
                    f"{len(edits)} file(s) in the project folder were edited and not applied; "
                    "apply them first, or overwrite them with this save."
                )
                raise HTTPException(
                    status_code=409,
                    detail={"code": "FOLDER_EDITED", "message": refusal, "changes": _named_changes(edits)},
                )
            if m is not None:
                _save_manifest(proj.root, m)
            kinds.write_tree(proj.root, snapshot)
            sha = git_ops.commit_all(proj.root, message)
            with get_db_context() as db:
                repository.set_head_sha(db, proj.id, sha or git_ops.head_sha(proj.root))
        return sha

    def _sync_track_setting_from_manifest(self, proj: ActiveProject) -> None:
        """Adopt the on-disk manifest's track-data-artifacts value after a files→DB rebuild.

        Restore/reload just reset the working tree (incl. ``project.yaml``) to an authoritative state,
        so the DB row and the cached project must match it — otherwise the Settings UI and the
        export (which reads the cached flag) go stale. Mutates ``proj`` in place, mirroring
        :meth:`update_settings`.
        """
        m = read_manifest(proj.root)
        if m is None:
            return
        with get_db_context() as db:
            repository.set_track_data_artifacts(db, proj.id, m.track_data_artifacts)
        proj.track_data_artifacts = m.track_data_artifacts

    def _guard_before_apply(
        self, proj: ActiveProject, force: bool, action: str, plan: list[kinds.Change]
    ) -> str | None:
        """Name what a restore, reload, switch or pull would lose, and keep it first when ``force`` is set.

        Database changes are what status reports (``_unsaved_rows``); a repository with no commit yet has none. An
        open flow whose canvas holds unsaved edits and whose file ``plan`` rewrites or removes counts too: with
        ``force`` it is saved first, so it is a database change the recovery point keeps. All
        but a reload also refuse a dirty folder, since they reset it and discard hand edits; a reload applies them,
        so it refuses only unsaved database changes. With ``force`` a restore autosaves the folder and commits the
        export on the branch; the others keep the folder and the export under ``refs/flowfile/recovery/`` with the
        branch left alone. Returns the sha of the last recovery commit, if any. Holds the caller's lock."""
        edited = _edited_open_flows(proj.owner_id, plan)
        if force:
            for flow, _ in edited:
                flow.save_flow(flow.flow_settings.path)
        dirty = action != "reload" and git_ops.is_dirty(proj.root)
        changes = _unsaved_rows(proj) if git_ops.head_sha(proj.root) else []
        if not changes and not dirty and not edited:
            return None
        if not force:
            listed = {c["path"] for c in changes}
            canvas = [{**row, "change": "modified"} for row in _change_rows([c for _, c in edited])]
            changes += [row for row in canvas if row["path"] not in listed]
            if dirty:
                listed = {c["path"] for c in changes}
                folder_rows = [r for r in git_ops.uncommitted_changes(proj.root) if r["path"] not in listed]
                changes += _named_changes(folder_rows)
            raise HTTPException(status_code=409, detail=_unsaved_detail(action, changes))
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        recovery_sha = None
        if dirty and action == "restore":
            recovery_sha = git_ops.commit_all(proj.root, "Autosave before restore")
        elif dirty:
            recovery_sha = git_ops.commit_worktree(
                proj.root, f"Folder before {action}", f"refs/flowfile/recovery/{stamp}-folder"
            )
        if changes:
            if action == "restore":
                recovery_sha = self._commit_export(proj, "Database before restore", "HEAD") or recovery_sha
            else:
                ref = f"refs/flowfile/recovery/{stamp}"
                recovery_sha = self._commit_export(proj, f"Database before {action}", ref) or recovery_sha
        return recovery_sha

    @staticmethod
    def _commit_export(proj: ActiveProject, message: str, ref: str) -> str | None:
        """Commit the database's export on top of HEAD under ``ref``, built in a temp folder seeded with HEAD."""
        with tempfile.TemporaryDirectory(prefix="flowfile-project-") as tmp:
            folder = Path(tmp) / "tree"
            git_ops.seed_folder(proj.root, folder)
            manifest_file = proj.root / MANIFEST_NAME
            if manifest_file.exists():
                shutil.copyfile(manifest_file, folder / MANIFEST_NAME)
            write_export(folder, proj.owner_id)
            return git_ops.commit_folder(proj.root, folder, message, ref)

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
        are committed into history first (``_guard_before_apply``). The target is checked before any of
        that, and an import that fails rolls the database back and resets the folder to HEAD again. One left
        ``incomplete`` records no version, so the folder keeps the target's files for a reload to finish.
        """
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        with git_ops.project_lock(proj.root):
            self._refuse_during_merge(proj)
            preflight_import_caps(proj.root, sha)
            preflight_import_files(proj.root, sha)
            changes = _plan_changes(proj.root, proj.owner_id, sha)
            recovery_sha = self._guard_before_apply(proj, force, "restore", changes)
            head = git_ops.head_sha(proj.root)
            git_ops.restore(proj.root, sha)
            try:
                self._snapshot_database()
                result = import_project(proj.root, proj.owner_id, _removed(changes), changes)
            except BaseException:
                if head:  # the database rolled back, so the folder goes back to the last version too
                    git_ops.restore(proj.root, head)
                raise
            self._sync_track_setting_from_manifest(proj)
            if not result.incomplete:
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
            changes = _plan_changes(proj.root, proj.owner_id, None)
            recovery_sha = self._guard_before_apply(proj, force, "reload", changes)
            self._snapshot_database()
            result = import_project(proj.root, proj.owner_id, _removed(changes), changes)
            self._sync_track_setting_from_manifest(proj)
        result.recovery_sha = recovery_sha
        return result

    def _require_project(self, user_id: int) -> ActiveProject:
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        return proj

    def _apply_commit(self, proj: ActiveProject, sha: str, force: bool, action: str, move, undo) -> SetupResult:
        """Make the database what commit ``sha`` holds: the restore's preflight and plan, the guard, ``move()`` to
        put the folder on ``sha``, then the import with the plan's removals; a failed import runs ``undo()`` so the
        folder goes back with the database, and so does a ``move()`` git could not finish (on Windows a file another
        program holds open stops it halfway), refused as a ``GitRemoteError``. Holds the caller's lock."""
        preflight_import_caps(proj.root, sha)
        preflight_import_files(proj.root, sha)
        changes = _plan_changes(proj.root, proj.owner_id, sha)
        recovery_sha = self._guard_before_apply(proj, force, action, changes)
        try:
            move()
        except Exception as exc:
            logger.warning("git could not move the project folder for a %s: %s", action, exc)
            try:
                undo()
            except Exception:
                logger.exception("The project folder could not be put back after a failed %s", action)
                raise git_ops.GitRemoteError(
                    f"Git could not update the project folder for the {action}, nor put it back. A file in it may be "
                    "open in another program: close it, then reload the project from its folder."
                ) from None
            raise git_ops.GitRemoteError(
                f"Git could not update the project folder, so the {action} changed nothing. A file in it may be open "
                "in another program: close it and try again."
            ) from None
        try:
            self._snapshot_database()
            result = import_project(proj.root, proj.owner_id, _removed(changes), changes)
        except BaseException:
            undo()
            raise
        self._sync_track_setting_from_manifest(proj)
        with get_db_context() as db:
            repository.set_head_sha(db, proj.id, git_ops.head_sha(proj.root))
        result.recovery_sha = recovery_sha
        return result

    def branch_state(self, user_id: int) -> dict:
        """The branches, the one checked out, the remote's address and whether a token is kept for it."""
        proj = self._require_project(user_id)
        with get_db_context() as db:
            has_token = get_user_secret(db, PROJECT_REMOTE_TOKEN_KEY, user_id) is not None
        return {
            "current": git_ops.current_branch(proj.root),
            "branches": git_ops.branches(proj.root),
            "remote": git_ops.remote_url(proj.root),
            "has_token": has_token,
        }

    def create_branch(self, user_id: int, name: str, switch: bool = True) -> dict:
        """A branch at the current version; with ``switch`` it is checked out, which changes nothing in the
        database or the folder, since both point at the same version."""
        proj = self._require_project(user_id)
        with git_ops.project_lock(proj.root):
            sha = git_ops.create_branch(proj.root, name)
            if switch:
                git_ops.attach_head(proj.root, name)
        return {"name": name, "sha": sha}

    def switch_branch(self, user_id: int, name: str, force: bool = False) -> SetupResult:
        """Check out branch ``name`` and rebuild the database from it, as a restore of its last version would."""
        proj = self._require_project(user_id)
        git_ops.check_branch_name(name)
        with git_ops.project_lock(proj.root):
            self._refuse_during_merge(proj)
            target = git_ops.branch_sha(proj.root, f"refs/heads/{name}")
            if target is None:
                raise git_ops.GitRemoteError(f"No branch named {name!r}.")
            previous = git_ops.current_branch(proj.root)
            if previous == name:
                return SetupResult()
            if previous is None:
                raise git_ops.GitRemoteError("The project is on no branch; switch from a branch.")
            return self._apply_commit(
                proj,
                target,
                force,
                "switch",
                lambda: git_ops.checkout(proj.root, name),
                lambda: git_ops.checkout(proj.root, previous),
            )

    def set_remote(self, user_id: int, url: str, token: str | None = None, username: str | None = None) -> dict:
        """Point the project at a remote. ``token`` is kept as this person's secret (``""`` forgets it, ``None``
        keeps the one kept), never in the address or the folder; a folder remote only on a desktop."""
        proj = self._require_project(user_id)
        with git_ops.project_lock(proj.root):
            url = git_ops.set_remote(proj.root, url, local_allowed=settings.is_electron_mode())
            if username is not None:
                git_ops.set_config(proj.root, REMOTE_USER_CONFIG, username or None)
        if token is not None:
            with get_db_context() as db:
                if token:
                    set_user_secret(db, PROJECT_REMOTE_TOKEN_KEY, token, user_id)
                else:
                    delete_user_secret(db, PROJECT_REMOTE_TOKEN_KEY, user_id)
        return self.branch_state(user_id)

    def keep_remote_token(self, user_id: int, token: str) -> dict:
        """Keep ``token`` as this person's remote token, as ``set_remote`` does for a pasted one."""
        self._require_project(user_id)
        with get_db_context() as db:
            set_user_secret(db, PROJECT_REMOTE_TOKEN_KEY, token, user_id)
        return self.branch_state(user_id)

    @staticmethod
    def _credentials(proj: ActiveProject) -> tuple[str | None, str | None]:
        with get_db_context() as db:
            token = get_user_secret(db, PROJECT_REMOTE_TOKEN_KEY, proj.owner_id)
        return token, git_ops.get_config(proj.root, REMOTE_USER_CONFIG)

    def push(self, user_id: int) -> dict:
        """Push the checked-out branch's versions to the remote. What the database holds and no version does is
        not pushed: ``unsaved`` says so."""
        proj = self._require_project(user_id)
        with git_ops.project_lock(proj.root):
            branch = git_ops.current_branch(proj.root)
            if branch is None:
                raise git_ops.GitRemoteError("The project is on no branch.")
            if git_ops.head_sha(proj.root) is None:
                raise git_ops.GitRemoteError("Save a version first: there is nothing to push.")
            git_ops.push(proj.root, branch, *self._credentials(proj))
            unsaved = bool(_unsaved_rows(proj))
        return {"branch": branch, "sha": git_ops.head_sha(proj.root), "unsaved": unsaved}

    def pull(self, user_id: int, force: bool = False) -> PullResult:
        """Fetch, then bring the remote's copy of the checked-out branch in: nothing to do when this branch holds
        it, a fast-forward and its apply when the remote is only ahead, a merge when both moved."""
        proj = self._require_project(user_id)
        with git_ops.project_lock(proj.root):
            self._refuse_during_merge(proj)
            branch = git_ops.current_branch(proj.root)
            if branch is None:
                raise git_ops.GitRemoteError("The project is on no branch.")
            git_ops.fetch(proj.root, *self._credentials(proj))
            theirs = git_ops.branch_sha(proj.root, f"refs/remotes/{git_ops.REMOTE_NAME}/{branch}")
            if theirs is None:
                raise git_ops.GitRemoteError(f"The remote has no branch {branch!r} yet: push it first.")
            ours = git_ops.head_sha(proj.root)
            if ours == theirs or (ours and git_ops.is_ancestor(proj.root, theirs, ours)):
                return PullResult(state="up_to_date" if ours == theirs else "ahead", sha=ours)
            if ours is None or git_ops.is_ancestor(proj.root, ours, theirs):
                result = self._apply_commit(
                    proj,
                    theirs,
                    force,
                    "pull",
                    lambda: git_ops.move_branch(proj.root, theirs),
                    lambda: git_ops.move_branch(proj.root, ours) if ours else None,
                )
                return PullResult(state="fast_forward", sha=theirs, result=result)
            return self._merge_remote(proj, branch, ours, theirs, force)

    @staticmethod
    def _refuse_during_merge(proj: ActiveProject) -> None:
        """409 ``MERGE_IN_PROGRESS`` while a pull's merge waits to be settled: a version saved, a branch switched
        or a version restored now would leave the merge on a version that is no longer the branch's."""
        if merging.load_state(proj.root) is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "MERGE_IN_PROGRESS",
                    "message": "A pull is waiting for its conflicts to be settled: settle them, or abort the merge.",
                },
            )

    def _merge_remote(self, proj: ActiveProject, branch: str, ours: str, theirs: str, force: bool) -> PullResult:
        """Merge the remote's copy into this branch: the managed files through ``merging.merge_trees``, every other
        file by git's three-way rule (``git_ops.unmanaged_changes``, which refuses the pull before anything is written
        when both sides changed one differently). Clean, it concludes at once; else the state waits to be settled."""
        remote = f"{git_ops.REMOTE_NAME}/{branch}"
        base = git_ops.merge_base(proj.root, ours, theirs)
        unmanaged = git_ops.unmanaged_changes(proj.root, base, ours, theirs)
        sides = {"base": "base", "ours": f"{branch} (this copy)", "theirs": remote}
        merged = merging.merge_trees(
            git_ops.read_tree_at(proj.root, base) if base else {},
            git_ops.read_tree_at(proj.root, ours),
            git_ops.read_tree_at(proj.root, theirs),
            sides,
        )
        state = merging.new_state(branch, ours, theirs, merged, sides, unmanaged)
        if merged.clean:
            return self._conclude(proj, state, force)
        merging.save_state(proj.root, state)
        return PullResult(state="conflicted", sha=ours, conflicts=merging.open_items(state), warnings=merged.warnings)

    def _conclude(self, proj: ActiveProject, state: dict, force: bool) -> PullResult:
        """Commit the settled merge with both parents and apply it; the state goes once the import succeeded."""
        if git_ops.current_branch(proj.root) != state["branch"] or git_ops.head_sha(proj.root) != state["ours"]:
            merging.clear_state(proj.root)
            self._release_merge_flows(proj.owner_id)
            raise git_ops.GitRemoteError("The branch moved since the pull; pull again.")
        message = f"Merge {git_ops.REMOTE_NAME}/{state['branch']} into {state['branch']}"
        sha = git_ops.commit_tree_changes(
            proj.root,
            state["ours"],
            state["changes"],
            message,
            [state["ours"], state["theirs"]],
            state.get("unmanaged") or {},
        )
        result = self._apply_commit(
            proj,
            sha,
            force,
            "pull",
            lambda: git_ops.move_branch(proj.root, sha),
            lambda: git_ops.move_branch(proj.root, state["ours"]),
        )
        merging.clear_state(proj.root)
        self._release_merge_flows(proj.owner_id)
        return PullResult(state="merged", sha=sha, result=result, warnings=list(state.get("warnings") or []))

    @staticmethod
    def _release_merge_flows(user_id: int) -> None:
        """The waiting merge is over (concluded or dropped): a flow ``open_merge_flow`` opened from it stops holding
        its conflicts and its link to the merge, so the editor no longer refuses to run it over a merge that is gone."""
        from flowfile_core import flow_file_handler

        for flow in flow_file_handler.get_user_flows(user_id):
            marker = getattr(flow, "project_merge", None)
            if not marker or marker.get("owner_id") != user_id:
                continue
            flow.project_merge = None
            if flow.merge_conflicts:
                with flow.transaction("Merge finished", record=False):
                    flow.merge_conflicts = None

    def merge_state(self, user_id: int) -> dict | None:
        """The pull waiting to be settled: its branch, both sides and every item left, ``None`` when there is none."""
        proj = self._require_project(user_id)
        state = merging.load_state(proj.root)
        if state is None:
            return None
        return {
            "branch": state["branch"],
            "ours": state["ours"],
            "theirs": state["theirs"],
            "sides": state.get("sides") or {},
            "conflicts": merging.open_items(state),
            "flows": sorted(state.get("flows") or {}),
            "warnings": list(state.get("warnings") or []),
        }

    def settle_merge(self, user_id: int, path: str, take: str, key: str | None = None, force: bool = False):
        """Settle one item of the waiting merge; the last one concludes it (``PullResult``), else the state left.

        The settled state is saved before the conclusion, which the guard can refuse (409 ``UNSAVED_CHANGES``), so a
        state with nothing left to settle goes straight to the conclusion: the same request sent again with ``force``
        after that refusal concludes the merge instead of finding its item gone."""
        proj = self._require_project(user_id)
        with git_ops.project_lock(proj.root):
            state = merging.load_state(proj.root)
            if state is None:
                raise git_ops.GitRemoteError("No merge is waiting to be settled.")
            if merging.is_settled(state):
                return self._conclude(proj, state, force)
            try:
                state = merging.settle(state, path, take, key)
            except KeyError:
                detail = f"Nothing to settle at {path} {key or ''}".strip()
                raise HTTPException(status_code=404, detail=detail) from None
            merging.save_state(proj.root, state)
            if merging.is_settled(state):
                return self._conclude(proj, state, force)
        return self.merge_state(user_id)

    def conclude_merge(self, user_id: int, force: bool = False) -> PullResult:
        """Conclude a merge whose items are all settled, after a conclusion the guard refused."""
        proj = self._require_project(user_id)
        with git_ops.project_lock(proj.root):
            state = merging.load_state(proj.root)
            if state is None:
                raise git_ops.GitRemoteError("No merge is waiting to be settled.")
            if not merging.is_settled(state):
                raise git_ops.GitRemoteError(f"{len(merging.open_items(state))} item(s) still need settling.")
            return self._conclude(proj, state, force)

    def abort_merge(self, user_id: int) -> None:
        """Drop the waiting merge; nothing was applied, so the branch and the database stay as they are."""
        proj = self._require_project(user_id)
        with git_ops.project_lock(proj.root):
            merging.clear_state(proj.root)
        self._release_merge_flows(user_id)

    def open_merge_flow(self, user_id: int, path: str) -> int:
        """Open a conflicted flow of the waiting merge in the editor: the merged flow, the remote's side of each
        open step on the canvas, its conflicts held on the flow (``merge_conflicts``) for the settle screen, and
        ``project_merge`` naming the file, so what the editor settles is written back into the merge."""
        from flowfile_core import flow_file_handler

        proj = self._require_project(user_id)
        state = merging.load_state(proj.root)
        wrapper = (state or {}).get("flows", {}).get(path)
        if wrapper is None:
            raise HTTPException(status_code=404, detail=f"No conflicted flow at {path}.")
        folder = storage.temp_directory / "project_merge" / str(user_id)
        folder.mkdir(parents=True, exist_ok=True)
        scratch = folder / Path(path).name
        scratch.write_text(dump_yaml(wrapper["merged"]), encoding="utf-8")
        flow_id = flow_file_handler.import_flow(scratch, user_id)
        flow = flow_file_handler.get_flow(flow_id, user_id)
        flow.merge_conflicts = {k: v for k, v in wrapper.items() if k != "merged"}
        flow.project_merge = {"owner_id": user_id, "path": path}
        return flow_id

    def record_editor_settle(self, flow) -> PullResult | None:
        """Write what the editor settled on a flow opened by ``open_merge_flow`` back into the waiting merge,
        as the export would write the flow, so the merge commit holds names and no install's row ids. Settling the
        last item concludes the merge, unless the guard refuses (unsaved changes), which leaves it to
        ``conclude_merge``."""
        from flowfile_core.project.normalize import normalize_flow_data
        from flowfile_core.project.projection import _reference_lookup

        marker = getattr(flow, "project_merge", None)
        if not marker:
            return None
        proj = self.get_active_project(marker["owner_id"])
        if proj is None:
            logger.info("No active project to record the settle of %s in", marker["path"])
            return None
        with git_ops.project_lock(proj.root):
            state = merging.load_state(proj.root)
            wrapper = (state or {}).get("flows", {}).get(marker["path"])
            if wrapper is None:
                return None
            data = flow.get_flowfile_data().model_dump(mode="json")
            with get_db_context() as db:
                document = normalize_flow_data(
                    data,
                    wrapper["flow_uuid"],
                    wrapper.get("catalog_name") or "",
                    wrapper.get("namespace"),
                    lookup=_reference_lookup(db),
                )
            merging.settle_flow_document(state, marker["path"], document, flow.merge_conflicts)
            merging.save_state(proj.root, state)
            if not merging.is_settled(state):
                return None
            try:
                return self._conclude(proj, state, force=False)
            except HTTPException as exc:
                logger.info("The settled merge waits for its conclusion: %s", exc.detail)
                return None

    def version_diff(self, user_id: int, sha: str) -> list[dict]:
        """Friendly changelog of what a specific version changed vs the one before it."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        return _with_step_changes(proj.root, sha, _friendly_changes(git_ops.changes_in(proj.root, sha)))

    def unsaved_changes(self, user_id: int) -> list[dict]:
        """What the database holds that the last version does not, per object: ``compare(HEAD, snapshot)``.

        HEAD's owned files are taken unfiltered, so a kind the folder stopped listing shows as removed. A hand
        edit of the folder is not an unsaved change; a reload applies it."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        if not git_ops.is_repo(proj.root):
            return []
        return _unsaved_rows(proj)

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

    def update_settings(self, user_id: int, track_data_artifacts: bool) -> bool:
        """Flip the track-data-artifacts toggle: add or remove the data kinds in project.yaml's ``kinds``, update
        the DB mirror, then re-project so the table files and models.yaml are dropped (off) or regenerated (on)."""
        proj = self.get_active_project(user_id)
        if proj is None:
            raise NoActiveProjectError("No active project")
        with git_ops.project_lock(proj.root):
            m = read_manifest(proj.root)
            if m is None:
                raise NoActiveProjectError("No active project")
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
        try:
            return bool(self.unsaved_changes(user_id))
        except ValueError:
            return False

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
