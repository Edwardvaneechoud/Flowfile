"""W42 — SSE replay buffer for ``Last-Event-ID`` reconnects.

W13 already emits ``id: {session_id}.{step_count}`` on every ``tool_call`` /
``planner_event`` SSE frame, and :func:`flowfile_core.ai.streaming.resumable_sse_stream`
already cursor-skips a live provider stream past a known ``last_event_id``.
What's been missing is the *server-side ring* that those cursors read from
when the live stream is gone (process restart, network drop, kill -9, etc.):

* :class:`ReplayBuffer` keeps the last ``cap`` SSE frames per
  ``(flow_id, session_id)`` in an in-memory mirror **and** in an
  append-only NDJSON file at
  ``{root}/{flow_id}/replay/{session_id}.ndjson``.
* :meth:`append` writes the frame to disk (append-only, ``os.O_APPEND``)
  and pushes into the ``deque(maxlen=cap)`` mirror in O(1).
* :meth:`read_after` returns frames whose event-id is **strictly greater
  than** the cursor (or every cached frame when ``event_id is None``).
  Used by ``planner_events_sse`` to flush buffered frames to the resuming
  client before live streaming resumes.

The on-disk file is allowed to grow up to ``2 * cap`` lines before being
rewritten from the in-memory mirror — keeps the disk file bounded without
paying a rewrite cost on every append. Corrupt-tail recovery on read:
malformed JSON lines are skipped silently; the next valid line picks up
where the read left off.

Cap is intentionally small (default 64) — the buffer is for SSE reconnect,
NOT for arbitrary message-level undo. Keeping it bounded keeps the disk
footprint predictable across long-running agents.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import threading
from collections import deque
from collections.abc import Iterator
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


DEFAULT_CAP: int = 64
"""Plan §5.6 default — last 64 frames per session."""

_REWRITE_THRESHOLD_FACTOR: int = 2
"""Rewrite the on-disk NDJSON when its line count exceeds ``cap * factor``.
Set to 2 so the rewrite amortises across half the cap's worth of appends."""

_BufferKey = tuple[int, str]


def _parse_event_id(event_id: str) -> tuple[str, int]:
    """Return ``(session_part, step_part)`` from ``"{sid}.{step}"``.

    The step is parsed as an int when possible; raises ``ValueError`` for
    malformed cursors so the route layer can surface 400. ``session_part``
    is everything left of the **last** dot to keep session_ids that contain
    dots themselves (the spec doesn't forbid them) addressable.
    """
    if "." not in event_id:
        raise ValueError(f"replay cursor missing '.' separator: {event_id!r}")
    sid, _, step_str = event_id.rpartition(".")
    if not sid or not step_str:
        raise ValueError(f"replay cursor malformed: {event_id!r}")
    try:
        step = int(step_str)
    except ValueError as exc:
        raise ValueError(f"replay cursor step is not an int: {event_id!r}") from exc
    return sid, step


class ReplayBuffer:
    """Per-session ring buffer of SSE frames with a sidecar NDJSON file.

    Thread-safe: every public method acquires a per-session ``RLock`` so
    concurrent ``append`` / ``read_after`` calls don't tear the deque or
    the on-disk file. The locks are keyed on ``(flow_id, session_id)``;
    different sessions never contend.
    """

    def __init__(self, root: Path, *, cap: int = DEFAULT_CAP) -> None:
        self._root = Path(root)
        self._cap = max(1, int(cap))
        self._buffers: dict[_BufferKey, deque[tuple[str, bytes]]] = {}
        self._buffer_locks: dict[_BufferKey, threading.RLock] = {}
        self._dict_lock = threading.RLock()

    # ---- Path helpers -----------------------------------------------------

    def _replay_path(self, flow_id: int, session_id: str) -> Path:
        return self._root / str(int(flow_id)) / "replay" / f"{session_id}.ndjson"

    def _key(self, flow_id: int, session_id: str) -> _BufferKey:
        return (int(flow_id), session_id)

    def _lock_for(self, key: _BufferKey) -> threading.RLock:
        with self._dict_lock:
            lock = self._buffer_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._buffer_locks[key] = lock
            return lock

    # ---- Mirror cache -----------------------------------------------------

    def _ensure_loaded(self, key: _BufferKey) -> deque[tuple[str, bytes]]:
        """Return the in-memory deque for ``key``, hydrating from disk on miss."""
        with self._dict_lock:
            buf = self._buffers.get(key)
            if buf is not None:
                return buf
            buf = deque(maxlen=self._cap)
            self._buffers[key] = buf
        flow_id, session_id = key
        path = self._replay_path(flow_id, session_id)
        if path.is_file():
            for parsed in self._iter_disk_lines(path):
                buf.append(parsed)
        return buf

    def _iter_disk_lines(self, path: Path) -> Iterator[tuple[str, bytes]]:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            logger.exception("ReplayBuffer: read failed at %s", path)
            return
        for raw in text.splitlines():
            if not raw.strip():
                continue
            try:
                obj: Any = json.loads(raw)
            except json.JSONDecodeError:
                # Corrupt-tail recovery — skip the malformed line and continue.
                continue
            if not isinstance(obj, dict):
                continue
            event_id = obj.get("event_id")
            data_b64 = obj.get("data")
            if not isinstance(event_id, str) or not isinstance(data_b64, str):
                continue
            try:
                payload = base64.b64decode(data_b64.encode("ascii"), validate=True)
            except (ValueError, binascii.Error):
                continue
            yield event_id, payload

    # ---- Append -----------------------------------------------------------

    def append(
        self,
        *,
        flow_id: int,
        session_id: str,
        event_id: str,
        payload: bytes,
    ) -> None:
        """Add ``(event_id, payload)`` to the buffer; also append to the NDJSON."""
        if not isinstance(payload, bytes | bytearray):
            raise TypeError(f"payload must be bytes; got {type(payload).__name__}")
        key = self._key(flow_id, session_id)
        with self._lock_for(key):
            buf = self._ensure_loaded(key)
            buf.append((event_id, bytes(payload)))
            self._write_disk_append(flow_id, session_id, event_id, bytes(payload))
            self._maybe_rewrite(flow_id, session_id, buf)

    def _write_disk_append(self, flow_id: int, session_id: str, event_id: str, payload: bytes) -> None:
        path = self._replay_path(flow_id, session_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            {"event_id": event_id, "data": base64.b64encode(payload).decode("ascii")},
            ensure_ascii=False,
        )
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            logger.exception("ReplayBuffer: append write failed at %s", path)

    def _maybe_rewrite(
        self,
        flow_id: int,
        session_id: str,
        buf: deque[tuple[str, bytes]],
    ) -> None:
        path = self._replay_path(flow_id, session_id)
        if not path.is_file():
            return
        try:
            with open(path, encoding="utf-8") as fh:
                line_count = sum(1 for _ in fh)
        except OSError:
            return
        if line_count <= self._cap * _REWRITE_THRESHOLD_FACTOR:
            return
        # Rewrite atomically from the deque mirror.
        tmp = path.parent / f"{path.name}.tmp.{os.getpid()}"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                for event_id, payload in buf:
                    fh.write(
                        json.dumps(
                            {
                                "event_id": event_id,
                                "data": base64.b64encode(payload).decode("ascii"),
                            },
                            ensure_ascii=False,
                        )
                        + "\n",
                    )
            os.replace(tmp, path)
        except OSError:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            logger.exception("ReplayBuffer: rewrite failed at %s", path)

    # ---- Read -------------------------------------------------------------

    def read_after(
        self,
        *,
        flow_id: int,
        session_id: str,
        event_id: str | None,
    ) -> Iterator[tuple[str, bytes]]:
        """Yield buffered ``(event_id, payload)`` strictly after ``event_id``.

        ``event_id=None`` → every cached frame (most-recent ``cap`` of them).
        Cursor matched on the integer step suffix (``"{sid}.<step>"``); the
        session-id portion is intentionally **ignored** for cross-restart
        resumes where the cursor's session id is identical-by-construction
        but a future workstream might want a stricter match.
        """
        key = self._key(flow_id, session_id)
        with self._lock_for(key):
            buf = self._ensure_loaded(key)
            snapshot = list(buf)

        if event_id is None:
            yield from snapshot
            return

        try:
            _, target_step = _parse_event_id(event_id)
        except ValueError:
            logger.warning("ReplayBuffer.read_after: malformed cursor %r", event_id)
            yield from snapshot
            return

        for entry_id, payload in snapshot:
            try:
                _, step = _parse_event_id(entry_id)
            except ValueError:
                continue
            if step > target_step:
                yield entry_id, payload

    # ---- Lifecycle --------------------------------------------------------

    def drop(self, *, flow_id: int, session_id: str) -> None:
        """Forget the in-memory mirror and delete the on-disk file."""
        key = self._key(flow_id, session_id)
        with self._lock_for(key):
            self._buffers.pop(key, None)
            path = self._replay_path(flow_id, session_id)
            if path.exists():
                try:
                    path.unlink()
                except OSError:
                    logger.exception("ReplayBuffer.drop unlink failed at %s", path)

    def clear(self) -> None:
        """Wipe every cached buffer; on-disk files left in place."""
        with self._dict_lock:
            self._buffers.clear()


# --------------------------------------------------------------------------- #
# Process-wide default                                                         #
# --------------------------------------------------------------------------- #


_DEFAULT_BUFFER: ReplayBuffer | None = None
_DEFAULT_LOCK = threading.Lock()


def default_replay_buffer() -> ReplayBuffer:
    """Process-wide singleton rooted at ``storage.ai_sessions_directory``.

    Mirrors :func:`flowfile_core.ai.scheduler.default_scheduler` — lazy on
    first call so importing this module doesn't construct a path against
    the storage helper before tests have had a chance to monkeypatch it.
    """
    global _DEFAULT_BUFFER
    with _DEFAULT_LOCK:
        if _DEFAULT_BUFFER is None:
            from shared.storage_config import storage

            _DEFAULT_BUFFER = ReplayBuffer(storage.ai_sessions_directory)
        return _DEFAULT_BUFFER


def set_default_replay_buffer(buffer: ReplayBuffer | None) -> None:
    """Tests-only — swap the process-wide singleton (or reset with ``None``)."""
    global _DEFAULT_BUFFER
    with _DEFAULT_LOCK:
        _DEFAULT_BUFFER = buffer


__all__ = [
    "DEFAULT_CAP",
    "ReplayBuffer",
    "default_replay_buffer",
    "set_default_replay_buffer",
]
