"""The copy of the catalog database a notebook kernel reads, kept in the kernel's shared folder.

A read-only bind mount of the WAL-mode catalog goes stale inside a Docker Desktop VM (new rows show
up only after a checkpoint), so core hands each notebook kernel a copy instead:
``<shared>/notebook_db/<kernel_id>/flowfile_catalog.db``, written with the SQLite backup API from a
read-only connection, switched to ``journal_mode=DELETE`` (one plain file) and moved into place with
``os.replace``. :func:`refresh` runs when the kernel asks for it, before its first database connection in a
call (``notebook.kernel_runner.refresh_database``), and copies only when the source database or its ``-wal``
changed since the last copy. Each kernel's copy has its own lock. Only a SQLite file catalog can be copied.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from uuid import uuid4

from flowfile_core.database.backup import _BACKUP_TIMEOUT_SECONDS, _deadline_progress

COPY_DIR = "notebook_db"
DB_NAME = "flowfile_catalog.db"

_locks_lock = threading.Lock()
_locks: dict[str, threading.Lock] = {}
_stamps: dict[str, tuple] = {}


def source_path() -> Path | None:
    """Core's catalog database file, or ``None`` when the catalog is not a SQLite file."""
    from shared.database import sqlite_database_path

    return sqlite_database_path()


def copy_dir(shared_dir: str, kernel_id: str) -> Path:
    return Path(shared_dir) / COPY_DIR / kernel_id


def copy_path(shared_dir: str, kernel_id: str) -> Path:
    return copy_dir(shared_dir, kernel_id) / DB_NAME


def _lock(kernel_id: str) -> threading.Lock:
    with _locks_lock:
        return _locks.setdefault(kernel_id, threading.Lock())


def _stamp(source: Path) -> tuple:
    stamp = []
    for path in (source, source.with_name(source.name + "-wal")):
        try:
            stat = path.stat()
        except FileNotFoundError:
            stamp.append(None)
            continue
        stamp.append((stat.st_mtime_ns, stat.st_size))
    return tuple(stamp)


def _write_copy(source: Path, target: Path) -> None:
    tmp = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        src = sqlite3.connect(f"{source.resolve().as_uri()}?mode=ro", uri=True, timeout=1.0)
        try:
            dest = sqlite3.connect(tmp)
            try:
                src.backup(dest, pages=256, progress=_deadline_progress(time.monotonic() + _BACKUP_TIMEOUT_SECONDS))
                dest.execute("PRAGMA journal_mode=DELETE")
            finally:
                dest.close()
        finally:
            src.close()
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm", "-journal"):
        target.with_name(target.name + suffix).unlink(missing_ok=True)


def refresh(shared_dir: str, kernel_id: str) -> Path | None:
    """Bring the kernel's copy up to date with core's database; ``None`` when there is nothing to copy."""
    source = source_path()
    if source is None or not source.exists():
        return None
    target = copy_path(shared_dir, kernel_id)
    with _lock(kernel_id):
        stamp = _stamp(source)
        if _stamps.get(kernel_id) == stamp and target.exists():
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        _write_copy(source, target)
        _stamps[kernel_id] = stamp
    return target


def remove(shared_dir: str, kernel_id: str) -> None:
    """Delete the kernel's copy (on stop and delete); the kernel's next database read asks for a fresh one."""
    with _lock(kernel_id):
        _stamps.pop(kernel_id, None)
        shutil.rmtree(copy_dir(shared_dir, kernel_id), ignore_errors=True)
