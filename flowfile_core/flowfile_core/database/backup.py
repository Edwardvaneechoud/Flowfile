"""Pre-migration snapshots of the SQLite catalog database.

A snapshot is taken only when pending Alembic migrations (or an
unknown-revision re-stamp) are about to mutate an existing database.
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

logger = logging.getLogger(__name__)

BACKUP_DIR_NAME = "db_backups"
DEFAULT_KEEP = 10
# sqlite's backup() retries SQLITE_BUSY forever; a peer holding an exclusive
# lock would otherwise hang startup. The deadline aborts via the progress hook.
_BACKUP_TIMEOUT_SECONDS = 30.0


def _keep_count() -> int:
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


def _snapshot_target(backup_dir: Path, db_path: Path, from_revision: str | None, to_revision: str | None) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"{db_path.stem}.{_safe_revision(from_revision)}-to-{_safe_revision(to_revision)}.{stamp}"
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


def snapshot_database(db_path: Path, from_revision: str | None, to_revision: str | None) -> Path | None:
    """Snapshot *db_path* into its sibling ``db_backups/`` directory.

    Returns the snapshot path, or ``None`` when disabled or on failure.
    """
    keep = _keep_count()
    if keep <= 0:
        logger.info("Catalog DB snapshots disabled (FLOWFILE_DB_BACKUP_KEEP=%d)", keep)
        return None

    target: Path | None = None
    try:
        backup_dir = db_path.parent / BACKUP_DIR_NAME
        backup_dir.mkdir(parents=True, exist_ok=True)
        target = _snapshot_target(backup_dir, db_path, from_revision, to_revision)
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
        logger.exception("Could not snapshot catalog DB before migration; continuing without one")
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
