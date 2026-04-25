"""Per-source LazyFrame cache for catalog visualization compute.

The worker FastAPI process keeps a small in-memory pool of "viz sessions",
each holding a Polars ``LazyFrame`` for one catalog source (a physical Delta
table, an ad-hoc SQL query, or a flow-virtual table delivered as IPC bytes).

A session is created on first request for a given ``session_key`` and reused
by every subsequent request with the same key.  The session is evicted after
``IDLE_TTL_SECONDS`` of inactivity, or under LRU pressure once
``MAX_SESSIONS`` is exceeded.

This is what lets successive Graphic Walker tweaks share the same scan/SQL
plan without re-loading the source — the user's "select an existing worker,
don't pay startup" requirement.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class VizSession:
    key: str
    lf: pl.LazyFrame
    fields: list[dict[str, Any]] | None = None
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)


class VizSessionManager:
    """Thread-safe in-process LRU + idle-TTL cache of LazyFrames keyed by session_key."""

    IDLE_TTL_SECONDS = 300
    MAX_SESSIONS = 32
    REAP_INTERVAL_SECONDS = 30

    def __init__(self) -> None:
        self._sessions: dict[str, VizSession] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._reaper = threading.Thread(target=self._reap_loop, daemon=True, name="viz-session-reaper")
        self._reaper.start()

    # -- public API ---------------------------------------------------------

    def execute(
        self,
        session_key: str,
        loader: Callable[[], pl.LazyFrame],
        runner: Callable[[pl.LazyFrame], Any],
    ) -> tuple[Any, bool]:
        """Look up (or load) the session and run ``runner(lf)`` under its lock.

        ``runner`` receives the cached LazyFrame; the manager handles
        concurrency on the same key and timestamp updates.  Returns a tuple
        ``(result, cache_hit)`` so callers can surface cache stats to clients.
        """
        session, cache_hit = self._get_or_create(session_key, loader)
        with session.lock:
            session.last_used_at = time.time()
            return runner(session.lf), cache_hit

    def fields(
        self,
        session_key: str,
        loader: Callable[[], pl.LazyFrame],
        compute_fields: Callable[[pl.LazyFrame], list[dict[str, Any]]],
    ) -> tuple[list[dict[str, Any]], bool]:
        """Return cached GW field list, computing on first call."""
        session, cache_hit = self._get_or_create(session_key, loader)
        with session.lock:
            session.last_used_at = time.time()
            if session.fields is None:
                session.fields = compute_fields(session.lf)
                cache_hit = False
            return session.fields, cache_hit

    def evict(self, session_key: str) -> None:
        with self._lock:
            self._sessions.pop(session_key, None)

    def evict_all(self) -> None:
        with self._lock:
            self._sessions.clear()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "sessions": len(self._sessions),
                "keys": list(self._sessions.keys()),
                "idle_ttl_seconds": self.IDLE_TTL_SECONDS,
                "max_sessions": self.MAX_SESSIONS,
            }

    def shutdown(self) -> None:
        self._stop.set()

    # -- internals ----------------------------------------------------------

    def _get_or_create(
        self, session_key: str, loader: Callable[[], pl.LazyFrame]
    ) -> tuple[VizSession, bool]:
        with self._lock:
            existing = self._sessions.get(session_key)
            if existing is not None:
                return existing, True

        # Build outside the manager lock so the (potentially slow) loader
        # doesn't serialise other sessions' lookups.
        lf = loader()

        with self._lock:
            existing = self._sessions.get(session_key)
            if existing is not None:
                # Lost the race; discard our build and use the winner.
                return existing, True
            session = VizSession(key=session_key, lf=lf)
            self._sessions[session_key] = session
            self._enforce_lru_limit()
            return session, False

    def _enforce_lru_limit(self) -> None:
        # Caller must hold ``self._lock``.
        if len(self._sessions) <= self.MAX_SESSIONS:
            return
        ordered = sorted(self._sessions.items(), key=lambda kv: kv[1].last_used_at)
        excess = len(self._sessions) - self.MAX_SESSIONS
        for key, _ in ordered[:excess]:
            self._sessions.pop(key, None)

    def _reap_loop(self) -> None:
        while not self._stop.wait(self.REAP_INTERVAL_SECONDS):
            cutoff = time.time() - self.IDLE_TTL_SECONDS
            with self._lock:
                stale = [k for k, s in self._sessions.items() if s.last_used_at < cutoff]
                for k in stale:
                    self._sessions.pop(k, None)
            if stale:
                logger.debug("viz-session-reaper: evicted %d idle session(s)", len(stale))


viz_session_manager = VizSessionManager()
