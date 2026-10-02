"""The copy of the catalog database a notebook kernel reads, kept in the kernel's shared folder.

A read-only bind mount of the WAL-mode catalog goes stale inside a Docker Desktop VM (new rows show
up only after a checkpoint), so core hands each notebook kernel a copy instead:
``<shared>/notebook_db/<kernel_id>/flowfile_catalog.<uuid>.db``, written with the SQLite backup API from a
read-only connection and switched to ``journal_mode=DELETE`` (one plain file). Every copy gets a new name
and the older ones are deleted: a file replaced in place keeps its old entry in the VM for a few
milliseconds, in which the kernel sees it exist and cannot open it. :func:`refresh` runs when the kernel asks
for it, before its first database connection in a call (``notebook.kernel_runner.refresh_database``), copies
only when the source database or its ``-wal`` changed since the last copy and answers with the copy to open.
Each kernel's copies have their own lock. Only a SQLite file catalog can be copied.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import threading
import time
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from flowfile_core.database.backup import _BACKUP_TIMEOUT_SECONDS, _deadline_progress

COPY_DIR = "notebook_db"
DB_NAME = "flowfile_catalog.db"
_DB_STEM = "flowfile_catalog"

_locks_lock = threading.Lock()
_locks: dict[str, threading.Lock] = {}
_stamps: dict[str, tuple] = {}
_copies: dict[str, Path] = {}


def copy_dir(shared_dir: str, kernel_id: str) -> Path:
    """The folder of ``kernel_id``'s copies, which :func:`remove` deletes whole.

    ``ValueError`` for an id that does not name a folder inside the copies folder (``..``, a path): a kernel's
    id is whatever its creator typed.
    """
    root = os.path.normpath(os.path.join(shared_dir, COPY_DIR))
    directory = os.path.normpath(os.path.join(root, kernel_id))
    if not directory.startswith(root + os.sep):
        raise ValueError(f"Kernel id {kernel_id!r} cannot name the folder of its database copies")
    return Path(directory)


def copy_path(shared_dir: str, kernel_id: str) -> Path:
    """The path a notebook kernel's ``FLOWFILE_DB_PATH`` carries; nothing is written there (:func:`refresh`)."""
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


def _sweep(directory: Path, keep: Path) -> None:
    """Delete every other copy in ``directory``, with its sidecars and any temporary file left behind."""
    for entry in directory.iterdir():
        if entry != keep and _DB_STEM in entry.name:
            with suppress(OSError):
                entry.unlink()


def refresh(shared_dir: str, kernel_id: str) -> Path | None:
    """The kernel's up-to-date copy of core's database, a new file when it had to copy; ``None`` when there is
    nothing to copy.

    The kernel closed its connections before it asked and opens the answered path next, so the older copies
    can go.
    """
    from shared.database import sqlite_database_path

    source = sqlite_database_path()
    if source is None or not source.exists():
        return None
    directory = copy_dir(shared_dir, kernel_id)
    with _lock(kernel_id):
        stamp = _stamp(source)
        current = _copies.get(kernel_id)
        if _stamps.get(kernel_id) == stamp and current is not None and current.parent == directory and current.exists():
            return current
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"{_DB_STEM}.{uuid4().hex}.db"
        _write_copy(source, target)
        _stamps[kernel_id], _copies[kernel_id] = stamp, target
        _sweep(directory, target)
    return target


def remove(shared_dir: str, kernel_id: str) -> None:
    """Delete the kernel's copies (on stop and delete); the kernel's next database read asks for a fresh one."""
    with _lock(kernel_id):
        _stamps.pop(kernel_id, None)
        _copies.pop(kernel_id, None)
        shutil.rmtree(copy_dir(shared_dir, kernel_id), ignore_errors=True)
