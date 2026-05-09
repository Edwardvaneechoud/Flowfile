""":class:`flowfile_core.ai.diff.GraphDiff` persistence behind a repo ABC.

Mirror of :mod:`flowfile_core.ai.session_store` for staged diffs:

* :class:`InMemoryDiffRepository` — process-local dict shape;
  default for tests.
* :class:`DiskDiffRepository` — sidecar JSON at
  ``{root}/{flow_id}/{session_id}.diff.json`` colocated with the
  session file. ``register_diff`` / ``get_diff`` / ``pop_diff``
  delegate to the active repo via :mod:`flowfile_core.ai.diff`.

Schema versioning rides on the on-disk JSON layer (top-level
``"_schema": "ai_diff.v1"``); unknown tag → ``None`` from
:meth:`get` with a single WARN log. Atomic writes via
``tmp + os.replace`` under a per-diff ``filelock.FileLock``.

Diff lookup is keyed on ``diff_id`` (not ``session_id``), so the
disk repo maintains a shadow index of
``diff_id → (flow_id, session_id)`` pairs so ``get_diff`` doesn't
have to walk every flow on every call. The index itself is
per-instance and rebuilt lazily on a cold-start scan triggered by a
missed lookup.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from filelock import FileLock
from filelock import Timeout as FileLockTimeout

if TYPE_CHECKING:
    from flowfile_core.ai.diff import GraphDiff

logger = logging.getLogger(__name__)


SCHEMA_VERSION: str = "ai_diff.v1"
DEFAULT_LRU_SIZE: int = 32
_LOCK_TIMEOUT_SECONDS: float = 2.0


# --------------------------------------------------------------------------- #
# Protocol                                                                     #
# --------------------------------------------------------------------------- #


class DiffRepository(Protocol):
    """Storage contract for :class:`flowfile_core.ai.diff.GraphDiff`.

    Module-level ``register_diff`` / ``get_diff`` / ``pop_diff`` /
    ``clear_for_tests`` in ``diff.py`` delegate here. Implementations must
    be thread-safe.
    """

    def get(self, diff_id: str) -> GraphDiff | None: ...

    def put(self, diff: GraphDiff) -> None: ...

    def pop(self, diff_id: str) -> GraphDiff | None: ...

    def clear(self) -> None: ...


# --------------------------------------------------------------------------- #
# In-memory                                                                    #
# --------------------------------------------------------------------------- #


class InMemoryDiffRepository:
    """Process-local dict-backed repo."""

    def __init__(self) -> None:
        self._items: dict[str, GraphDiff] = {}
        self._lock = threading.RLock()

    def get(self, diff_id: str) -> GraphDiff | None:
        with self._lock:
            return self._items.get(diff_id)

    def put(self, diff: GraphDiff) -> None:
        with self._lock:
            self._items[diff.diff_id] = diff

    def pop(self, diff_id: str) -> GraphDiff | None:
        with self._lock:
            return self._items.pop(diff_id, None)

    def clear(self) -> None:
        with self._lock:
            self._items.clear()


# --------------------------------------------------------------------------- #
# Disk                                                                         #
# --------------------------------------------------------------------------- #


class DiskDiffRepository:
    """JSON sidecar repository at ``{root}/{flow_id}/{session_id}.diff.json``.

    Layout chosen to colocate the diff with the session file per plan §5.6.
    Diff lookup by ``diff_id`` walks the in-memory shadow index; on miss,
    we sweep the root once to rebuild the index. The sweep is bounded by
    the number of staged diffs across all flows in the user's data — small.
    """

    def __init__(self, root: Path, *, lru_size: int = DEFAULT_LRU_SIZE) -> None:
        self._root = Path(root)
        self._lru_size = max(1, int(lru_size))
        self._lru: OrderedDict[str, GraphDiff] = OrderedDict()
        # Shadow index: diff_id → on-disk path. Rebuilt lazily on miss.
        self._index: dict[str, Path] = {}
        self._lock = threading.RLock()

    # ---- Path helpers -----------------------------------------------------

    def _flow_dir(self, flow_id: int) -> Path:
        return self._root / str(int(flow_id))

    def _diff_path(self, flow_id: int, session_id: str) -> Path:
        return self._flow_dir(flow_id) / f"{session_id}.diff.json"

    @staticmethod
    def _lock_path(target: Path) -> str:
        return str(target) + ".lock"

    # ---- Serialization ----------------------------------------------------

    @staticmethod
    def _to_disk(diff: GraphDiff) -> dict:
        payload = diff.model_dump(mode="json")
        payload["_schema"] = SCHEMA_VERSION
        return payload

    @staticmethod
    def _from_disk(payload: dict) -> GraphDiff | None:
        from flowfile_core.ai.diff import GraphDiff

        schema_tag = payload.get("_schema")
        if schema_tag != SCHEMA_VERSION:
            logger.warning(
                "DiskDiffRepository: unknown schema tag %r — dropping; expected %r",
                schema_tag,
                SCHEMA_VERSION,
            )
            return None
        clean = {k: v for k, v in payload.items() if k != "_schema"}
        try:
            return GraphDiff.model_validate(clean)
        except Exception:
            logger.exception("DiskDiffRepository: GraphDiff.model_validate failed")
            return None

    def _read_file(self, path: Path) -> GraphDiff | None:
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError:
            logger.exception("DiskDiffRepository: read failed at %s", path)
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("DiskDiffRepository: JSON decode failed at %s", path)
            return None
        if not isinstance(payload, dict):
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

    # ---- Index ------------------------------------------------------------

    def _rebuild_index(self) -> None:
        """Sweep ``root`` and (re)populate the diff_id → path index."""
        index: dict[str, Path] = {}
        if not self._root.exists():
            with self._lock:
                self._index = index
            return
        for flow_dir in self._root.iterdir():
            if not flow_dir.is_dir():
                continue
            for entry in flow_dir.iterdir():
                if not entry.is_file() or not entry.name.endswith(".diff.json"):
                    continue
                diff = self._read_file(entry)
                if diff is None:
                    continue
                index[diff.diff_id] = entry
        with self._lock:
            self._index = index

    def _index_lookup(self, diff_id: str) -> Path | None:
        with self._lock:
            path = self._index.get(diff_id)
        if path is not None and path.exists():
            return path
        # Cold or stale entry — rebuild and try again.
        self._rebuild_index()
        with self._lock:
            return self._index.get(diff_id)

    # ---- LRU -------------------------------------------------------------

    def _lru_get(self, diff_id: str) -> GraphDiff | None:
        with self._lock:
            diff = self._lru.get(diff_id)
            if diff is not None:
                self._lru.move_to_end(diff_id)
            return diff

    def _lru_put(self, diff: GraphDiff) -> None:
        with self._lock:
            self._lru[diff.diff_id] = diff
            self._lru.move_to_end(diff.diff_id)
            while len(self._lru) > self._lru_size:
                self._lru.popitem(last=False)

    def _lru_drop(self, diff_id: str) -> None:
        with self._lock:
            self._lru.pop(diff_id, None)

    # ---- API --------------------------------------------------------------

    def get(self, diff_id: str) -> GraphDiff | None:
        cached = self._lru_get(diff_id)
        if cached is not None:
            return cached
        path = self._index_lookup(diff_id)
        if path is None:
            return None
        try:
            with FileLock(self._lock_path(path), timeout=_LOCK_TIMEOUT_SECONDS):
                diff = self._read_file(path)
        except FileLockTimeout:
            logger.warning("DiskDiffRepository: get() lock timeout at %s", path)
            return None
        if diff is None:
            return None
        self._lru_put(diff)
        return diff

    def put(self, diff: GraphDiff) -> None:
        path = self._diff_path(diff.flow_id, diff.session_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with FileLock(self._lock_path(path), timeout=_LOCK_TIMEOUT_SECONDS):
                self._atomic_write(path, self._to_disk(diff))
        except FileLockTimeout:
            logger.warning("DiskDiffRepository: put() lock timeout at %s", path)
            raise
        with self._lock:
            self._index[diff.diff_id] = path
        self._lru_put(diff)

    def pop(self, diff_id: str) -> GraphDiff | None:
        diff = self.get(diff_id)
        if diff is None:
            return None
        path = self._diff_path(diff.flow_id, diff.session_id)
        try:
            with FileLock(self._lock_path(path), timeout=_LOCK_TIMEOUT_SECONDS):
                if path.exists():
                    try:
                        path.unlink()
                    except OSError:
                        logger.exception("DiskDiffRepository: pop unlink failed at %s", path)
        except FileLockTimeout:
            logger.warning("DiskDiffRepository: pop() lock timeout at %s", path)
            return None
        self._lru_drop(diff_id)
        with self._lock:
            self._index.pop(diff_id, None)
        return diff

    def clear(self) -> None:
        with self._lock:
            self._lru.clear()
            self._index.clear()
        if self._root.exists():
            try:
                shutil.rmtree(self._root)
            except OSError:
                logger.exception("DiskDiffRepository: clear() rmtree failed at %s", self._root)


__all__ = [
    "DEFAULT_LRU_SIZE",
    "DiffRepository",
    "DiskDiffRepository",
    "InMemoryDiffRepository",
    "SCHEMA_VERSION",
]
