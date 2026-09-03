"""Snapshots of the SQLite catalog database.

Taken automatically when pending Alembic migrations (or an unknown-revision
re-stamp) are about to mutate an existing database, and on demand via
:func:`create_snapshot` (the Backups tab, and before a desktop update).
Best-effort by design: failures are logged, never raised, so a backup
problem can never block startup.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

BACKUP_DIR_NAME = "db_backups"
DEFAULT_KEEP = 10
# sqlite's backup() retries SQLITE_BUSY forever; a peer holding an exclusive
# lock would otherwise hang startup. The deadline aborts via the progress hook.
_BACKUP_TIMEOUT_SECONDS = 30.0
_STAMP_FORMAT = "%Y%m%dT%H%M%SZ"


def keep_count() -> int:
    raw = os.environ.get("FLOWFILE_DB_BACKUP_KEEP")
    if not raw:
        return DEFAULT_KEEP
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid FLOWFILE_DB_BACKUP_KEEP=%r; using default %d", raw, DEFAULT_KEEP)
        return DEFAULT_KEEP


def _safe_revision(revision: str | None) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", revision) if revision else "none"


def _snapshot_target(backup_dir: Path, db_path: Path, tag: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime(_STAMP_FORMAT)
    base = f"{db_path.stem}.{tag}.{stamp}"
    target = backup_dir / f"{base}{db_path.suffix}"
    counter = 1
    while target.exists():
        target = backup_dir / f"{base}-{counter}{db_path.suffix}"
        counter += 1
    return target


def _prune(backup_dir: Path, stem: str, suffix: str, keep: int, protect: Path | None = None) -> None:
    candidates = [
        p for p in backup_dir.iterdir() if p.is_file() and p.name.startswith(f"{stem}.") and p.name.endswith(suffix)
    ]
    # the just-written snapshot always survives and fills one keep slot, even
    # when an mtime tie would sort it below an older file
    protected = sum(1 for p in candidates if p == protect)
    others = sorted(
        (p for p in candidates if p != protect),
        key=lambda p: (p.stat().st_mtime, p.name),
        reverse=True,
    )
    for old in others[keep - protected :]:
        try:
            old.unlink()
            logger.info("Pruned old catalog DB snapshot %s", old.name)
        except OSError:
            logger.warning("Could not prune old catalog DB snapshot %s", old, exc_info=True)


def _deadline_progress(deadline: float):
    def progress(status: int, remaining: int, total: int) -> None:
        if time.monotonic() > deadline:
            raise sqlite3.OperationalError("catalog DB snapshot timed out (database busy)")

    return progress


def _write_snapshot(db_path: Path, tag: str) -> Path | None:
    keep = keep_count()
    if keep <= 0:
        logger.info("Catalog DB snapshots disabled (FLOWFILE_DB_BACKUP_KEEP=%d)", keep)
        return None

    target: Path | None = None
    try:
        backup_dir = db_path.parent / BACKUP_DIR_NAME
        backup_dir.mkdir(parents=True, exist_ok=True)
        target = _snapshot_target(backup_dir, db_path, _safe_revision(tag))
        # sqlite's backup API is safe against concurrent writers (worker/scheduler
        # share this DB), unlike a plain file copy of db+WAL.
        source = sqlite3.connect(db_path, timeout=1.0)
        try:
            dest = sqlite3.connect(target)
            try:
                source.backup(dest, pages=64, progress=_deadline_progress(time.monotonic() + _BACKUP_TIMEOUT_SECONDS))
            finally:
                dest.close()
        finally:
            source.close()
    except Exception:
        logger.exception("Could not snapshot catalog DB; continuing without one")
        if target is not None:
            try:
                target.unlink(missing_ok=True)
            except OSError:
                pass
        return None

    try:
        _prune(backup_dir, db_path.stem, db_path.suffix, keep, protect=target)
    except Exception:
        logger.warning("Catalog DB snapshot pruning failed", exc_info=True)

    logger.info("Catalog DB snapshot written to %s", target)
    return target


def snapshot_database(db_path: Path, from_revision: str | None, to_revision: str | None) -> Path | None:
    """Snapshot *db_path* into its sibling ``db_backups/`` directory before a migration.

    Returns the snapshot path, or ``None`` when disabled or on failure.
    """
    return _write_snapshot(db_path, f"{_safe_revision(from_revision)}-to-{_safe_revision(to_revision)}")


def create_snapshot(db_path: Path, reason: Literal["manual", "pre_update"]) -> Path | None:
    """Snapshot *db_path* on demand. Same retention and best-effort contract as a migration snapshot."""
    return _write_snapshot(db_path, reason.replace("_", "-"))


def _name_pattern(stem: str, suffix: str) -> re.Pattern[str]:
    return re.compile(
        rf"^{re.escape(stem)}\.(?P<tag>.+)\.(?P<stamp>\d{{8}}T\d{{6}}Z)(?:-(?P<counter>\d+))?{re.escape(suffix)}$"
    )


def _parse_tag(tag: str) -> tuple[str, str | None, str | None] | None:
    if tag == "pre-update":
        return "pre_update", None, None
    if tag == "manual":
        return "manual", None, None
    revisions = re.fullmatch(r"(.+)-to-(.+)", tag)
    if revisions is None:
        return None
    return "migration", revisions.group(1), revisions.group(2)


def _created_at(stamp: str, mtime: float) -> str:
    try:
        return datetime.strptime(stamp, _STAMP_FORMAT).replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return datetime.fromtimestamp(mtime, timezone.utc).isoformat()


def _app_version(path: Path) -> str | None:
    try:
        # read-only URI so listing can never write into a snapshot (Windows-safe)
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        try:
            row = connection.execute("SELECT app_version FROM db_info LIMIT 1").fetchone()
        finally:
            connection.close()
    except Exception:
        return None
    return row[0] if row else None


def list_snapshots(db_path: Path) -> list[dict]:
    """Describe the snapshots of *db_path* in its ``db_backups/`` directory, newest first.

    Keys: ``file_name``, ``path``, ``size_bytes``, ``created_at``, ``kind``,
    ``from_revision``, ``to_revision``, ``app_version``. Files this module did
    not write are skipped; a missing directory yields an empty list.
    """
    backup_dir = db_path.parent / BACKUP_DIR_NAME
    if not backup_dir.is_dir():
        return []

    pattern = _name_pattern(db_path.stem, db_path.suffix)
    snapshots: list[dict] = []
    for path in backup_dir.iterdir():
        if not path.is_file():
            continue
        match = pattern.match(path.name)
        if match is None:
            continue
        parsed = _parse_tag(match.group("tag"))
        if parsed is None:
            continue
        kind, from_revision, to_revision = parsed
        try:
            stat = path.stat()
        except OSError:
            continue  # pruned between iterdir() and stat()
        snapshots.append(
            {
                "file_name": path.name,
                "path": str(path),
                "size_bytes": stat.st_size,
                "created_at": _created_at(match.group("stamp"), stat.st_mtime),
                "kind": kind,
                "from_revision": from_revision,
                "to_revision": to_revision,
                "app_version": _app_version(path),
                "_counter": int(match.group("counter") or 0),
            }
        )

    # same-second snapshots carry a collision counter that plain name order would invert
    snapshots.sort(key=lambda s: (s["created_at"], s["_counter"], s["file_name"]), reverse=True)
    for snapshot in snapshots:
        del snapshot["_counter"]
    return snapshots
