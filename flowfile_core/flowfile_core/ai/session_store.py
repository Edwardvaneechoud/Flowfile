""":class:`AgentSession` persistence behind a repository ABC.

Two implementations:

* :class:`InMemorySessionRepository` — process-local dict shape;
  default for tests.
* :class:`DiskSessionRepository` — sidecar JSON files under
  ``{root}/{flow_id}/{session_id}.json`` with atomic
  ``tmp + os.replace`` writes, per-session ``filelock.FileLock``
  contention, in-process write-through LRU (default 32 entries), and
  a FIFO archive at ``{root}/{flow_id}/archive/`` capped at 50 per
  flow.

Both implementations share the same :class:`SessionRepository`
Protocol so ``sessions.py``'s public surface (``register_session`` /
``get_session`` / ``pop_session`` / ``clear_for_tests`` /
``list_sessions_for_user``) is backend-agnostic.

Schema versioning rides at the on-disk JSON layer (top-level
``"_schema": "ai_session.v1"``) rather than on the Pydantic model —
keeps :class:`flowfile_core.ai.sessions.AgentSession` free of an
alias dance and makes a future v2 a pure repository concern. Unknown
schema → ``None`` from :meth:`DiskSessionRepository.get` with one
WARN log; never raises.

The lazy-litellm contract is preserved: this module imports nothing
from ``litellm``; the only third-party dep is ``filelock``.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
from collections import OrderedDict
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

if TYPE_CHECKING:
    from flowfile_core.ai.sessions import AgentSession

logger = logging.getLogger(__name__)


SCHEMA_VERSION: str = "ai_session.v1"

DEFAULT_LRU_SIZE: int = 32

DEFAULT_ARCHIVE_CAP: int = 50

_LOCK_TIMEOUT_SECONDS: float = 2.0


# Protocol


class SessionRepository(Protocol):
    """Storage contract for :class:`flowfile_core.ai.sessions.AgentSession`.

    All public methods on the module-level surface in ``sessions.py``
    delegate here. Implementations must be thread-safe.
    """

    def get(self, session_id: str, *, user_id: int | None = None) -> AgentSession | None:
        """Look up a session, optionally enforcing ``user_id`` ownership."""

    def put(self, session: AgentSession) -> None:
        """Insert or replace ``session`` keyed by ``session_id``."""

    def pop(self, session_id: str, *, user_id: int | None = None) -> AgentSession | None:
        """Remove and return a session, archiving terminal ones for the
        disk backend. ``user_id`` mismatch returns ``None`` without popping."""

    def list_for_user(self, user_id: int) -> list[AgentSession]:
        """Snapshot of every active session belonging to ``user_id``."""

    def list_archived(self, *, user_id: int, flow_id: int) -> list[AgentSession]:
        """Hook — archived sessions filtered by ``(user_id, flow_id)``.
        In-memory implementation returns ``[]`` (no archive)."""

    def clear(self) -> None:
        """Tests-only — wipe in-memory cache and any disk subtree owned."""


# In-memory repository


class InMemorySessionRepository:
    """Process-local dict-backed repo."""

    def __init__(self) -> None:
        self._items: dict[str, AgentSession] = {}
        self._lock = threading.RLock()

    def get(self, session_id: str, *, user_id: int | None = None) -> AgentSession | None:
        with self._lock:
            session = self._items.get(session_id)
        if session is None:
            return None
        if user_id is not None and session.user_id != user_id:
            return None
        return session

    def put(self, session: AgentSession) -> None:
        with self._lock:
            self._items[session.session_id] = session

    def pop(self, session_id: str, *, user_id: int | None = None) -> AgentSession | None:
        with self._lock:
            session = self._items.get(session_id)
            if session is None:
                return None
            if user_id is not None and session.user_id != user_id:
                return None
            return self._items.pop(session_id, None)

    def list_for_user(self, user_id: int) -> list[AgentSession]:
        with self._lock:
            return [s for s in self._items.values() if s.user_id == user_id]

    def list_archived(self, *, user_id: int, flow_id: int) -> list[AgentSession]:
        return []

    def clear(self) -> None:
        with self._lock:
            self._items.clear()


# Disk repository


_TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "aborted", "failed"})


class DiskSessionRepository:
    """JSON sidecar repository at ``{root}/{flow_id}/{session_id}.json``.

    Atomic-write invariant: every persisted file is written to ``{name}.tmp.{pid}``
    then ``os.replace``-d into place under a per-session ``FileLock``. Concurrent
    writes serialize cleanly; a ``kill -9`` mid-write leaves the previous file
    intact (or absent for a first write).

    LRU mirror: hot reads (e.g. per-step planner ticks against the same session)
    don't hit the filesystem. The mirror is also the cold-start cache shape —
    a ``put`` immediately after restart doesn't double-read.

    FIFO archive: terminal sessions (``completed`` / ``aborted`` / ``failed``)
    move to ``{root}/{flow_id}/archive/`` on ``pop``; oldest archive entries
    are pruned when ``archive_cap`` is exceeded. Active-session pops (e.g.
    ``discard`` on a paused session) delete the file outright.
    """

    def __init__(
        self,
        root: Path,
        *,
        lru_size: int = DEFAULT_LRU_SIZE,
        archive_cap: int = DEFAULT_ARCHIVE_CAP,
    ) -> None:
        self._root = Path(root)
        self._lru_size = max(1, int(lru_size))
        self._archive_cap = max(1, int(archive_cap))
        self._lru: OrderedDict[str, AgentSession] = OrderedDict()
        self._lock = threading.RLock()

    # ---- Path helpers -----------------------------------------------------

    def _flow_dir(self, flow_id: int) -> Path:
        return self._root / str(int(flow_id))

    def _session_path(self, flow_id: int, session_id: str) -> Path:
        return self._flow_dir(flow_id) / f"{session_id}.json"

    def _archive_dir(self, flow_id: int) -> Path:
        return self._flow_dir(flow_id) / "archive"

    def _archive_path(self, flow_id: int, session_id: str) -> Path:
        return self._archive_dir(flow_id) / f"{session_id}.json"

    def _lock_path(self, target: Path) -> str:
        return str(target) + ".lock"

    def _find_active_path(self, session_id: str) -> Path | None:
        """Walk every flow_id directory looking for ``{session_id}.json``.

        Used only when the LRU is cold and the caller didn't supply
        ``flow_id`` (the default ``get`` / ``pop`` signatures don't). Cheap:
        we list the immediate children of ``root`` and stat one file per
        flow; for typical deployments (single-digit flow ids per user) the
        sweep is microseconds.
        """
        if not self._root.exists():
            return None
        for child in self._root.iterdir():
            if not child.is_dir():
                continue
            candidate = child / f"{session_id}.json"
            if candidate.is_file():
                return candidate
        return None

    # ---- Serialization ----------------------------------------------------

    @staticmethod
    def _to_disk(session: AgentSession) -> dict:
        payload = session.model_dump(mode="json")
        payload["_schema"] = SCHEMA_VERSION
        return payload

    @staticmethod
    def _from_disk(payload: dict) -> AgentSession | None:
        from flowfile_core.ai.sessions import AgentSession

        schema_tag = payload.get("_schema")
        if schema_tag != SCHEMA_VERSION:
            logger.warning(
                "DiskSessionRepository: unknown schema tag %r — dropping; expected %r",
                schema_tag,
                SCHEMA_VERSION,
            )
            return None
        clean = {k: v for k, v in payload.items() if k != "_schema"}
        try:
            return AgentSession.model_validate(clean)
        except Exception:
            logger.exception("DiskSessionRepository: AgentSession.model_validate failed")
            return None

    def _read_file(self, path: Path) -> AgentSession | None:
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError:
            logger.exception("DiskSessionRepository: read failed at %s", path)
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("DiskSessionRepository: JSON decode failed at %s", path)
            return None
        if not isinstance(payload, dict):
            logger.warning("DiskSessionRepository: non-object payload at %s", path)
            return None
        return self._from_disk(payload)

    def _atomic_write(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / f"{path.name}.tmp.{os.getpid()}"
        text = json.dumps(payload, ensure_ascii=False, sort_keys=False)
        tmp.write_text(text, encoding="utf-8")
        try:
            os.replace(tmp, path)
        except OSError:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    # ---- LRU mirror -------------------------------------------------------

    def _lru_get(self, session_id: str) -> AgentSession | None:
        with self._lock:
            session = self._lru.get(session_id)
            if session is not None:
                self._lru.move_to_end(session_id)
            return session

    def _lru_put(self, session: AgentSession) -> None:
        with self._lock:
            self._lru[session.session_id] = session
            self._lru.move_to_end(session.session_id)
            while len(self._lru) > self._lru_size:
                self._lru.popitem(last=False)

    def _lru_drop(self, session_id: str) -> None:
        with self._lock:
            self._lru.pop(session_id, None)

    # ---- Repository API ---------------------------------------------------

    def get(self, session_id: str, *, user_id: int | None = None) -> AgentSession | None:
        cached = self._lru_get(session_id)
        if cached is not None:
            if user_id is not None and cached.user_id != user_id:
                return None
            return cached

        path = self._find_active_path(session_id)
        if path is None:
            return None

        try:
            with FileLock(self._lock_path(path), timeout=_LOCK_TIMEOUT_SECONDS):
                session = self._read_file(path)
        except FileLockTimeout:
            logger.warning(
                "DiskSessionRepository: get() timed out waiting for lock on %s",
                path,
            )
            return None

        if session is None:
            return None
        if user_id is not None and session.user_id != user_id:
            return None

        self._lru_put(session)
        return session

    def put(self, session: AgentSession) -> None:
        path = self._session_path(session.flow_id, session.session_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with FileLock(self._lock_path(path), timeout=_LOCK_TIMEOUT_SECONDS):
                self._atomic_write(path, self._to_disk(session))
        except FileLockTimeout:
            logger.warning(
                "DiskSessionRepository: put() timed out waiting for lock on %s",
                path,
            )
            raise
        self._lru_put(session)

    def pop(self, session_id: str, *, user_id: int | None = None) -> AgentSession | None:
        # Read first to gate cross-user pops without touching the file.
        session = self.get(session_id, user_id=user_id)
        if session is None:
            return None

        path = self._session_path(session.flow_id, session_id)

        try:
            with FileLock(self._lock_path(path), timeout=_LOCK_TIMEOUT_SECONDS):
                if session.status in _TERMINAL_STATUSES:
                    self._archive(session)
                if path.exists():
                    try:
                        path.unlink()
                    except OSError:
                        logger.exception(
                            "DiskSessionRepository: failed to unlink active file %s",
                            path,
                        )
        except FileLockTimeout:
            logger.warning(
                "DiskSessionRepository: pop() timed out waiting for lock on %s",
                path,
            )
            return None

        self._lru_drop(session_id)
        return session

    def list_for_user(self, user_id: int) -> list[AgentSession]:
        out: list[AgentSession] = []
        if not self._root.exists():
            return out
        for flow_dir in self._root.iterdir():
            if not flow_dir.is_dir():
                continue
            for entry in flow_dir.iterdir():
                if not entry.is_file() or not entry.name.endswith(".json"):
                    continue
                if entry.name.endswith(".tmp") or ".tmp." in entry.name:
                    continue
                session = self._read_file(entry)
                if session is None:
                    continue
                if session.user_id == user_id:
                    out.append(session)
        return out

    def list_archived(self, *, user_id: int, flow_id: int) -> list[AgentSession]:
        archive_dir = self._archive_dir(flow_id)
        if not archive_dir.exists():
            return []
        out: list[AgentSession] = []
        for entry in archive_dir.iterdir():
            if not entry.is_file() or not entry.name.endswith(".json"):
                continue
            session = self._read_file(entry)
            if session is None:
                continue
            if session.user_id == user_id:
                out.append(session)
        out.sort(key=lambda s: s.updated_at, reverse=True)
        return out

    def clear(self) -> None:
        """Wipe the LRU mirror and remove the on-disk subtree."""
        with self._lock:
            self._lru.clear()
        if self._root.exists():
            try:
                shutil.rmtree(self._root)
            except OSError:
                logger.exception("DiskSessionRepository: clear() rmtree failed at %s", self._root)

    # ---- Archive helpers --------------------------------------------------

    def _archive(self, session: AgentSession) -> None:
        archive_path = self._archive_path(session.flow_id, session.session_id)
        try:
            self._atomic_write(archive_path, self._to_disk(session))
        except OSError:
            logger.exception(
                "DiskSessionRepository: archive write failed for %s",
                session.session_id,
            )
            return
        self._enforce_archive_cap(session.flow_id)

    def _enforce_archive_cap(self, flow_id: int) -> None:
        archive_dir = self._archive_dir(flow_id)
        if not archive_dir.exists():
            return
        entries = [e for e in archive_dir.iterdir() if e.is_file() and e.name.endswith(".json")]
        if len(entries) <= self._archive_cap:
            return
        entries.sort(key=lambda p: p.stat().st_mtime)
        to_drop = len(entries) - self._archive_cap
        for stale in entries[:to_drop]:
            try:
                stale.unlink()
            except OSError:
                logger.exception(
                    "DiskSessionRepository: archive cap prune failed at %s",
                    stale,
                )


# Buffered iterator helper


def iter_recent_files(directory: Path, *, limit: int | None = None) -> Iterator[Path]:
    """Yield ``directory``'s files sorted by mtime descending, capped at ``limit``."""
    if not directory.exists():
        return
    entries = [e for e in directory.iterdir() if e.is_file()]
    entries.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    if limit is not None:
        entries = entries[:limit]
    yield from entries


__all__ = [
    "DEFAULT_ARCHIVE_CAP",
    "DEFAULT_LRU_SIZE",
    "DiskSessionRepository",
    "InMemorySessionRepository",
    "SCHEMA_VERSION",
    "SessionRepository",
    "iter_recent_files",
]
