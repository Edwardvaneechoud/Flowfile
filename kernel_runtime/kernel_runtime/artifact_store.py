from __future__ import annotations

import logging
import sys
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class ArtifactStore:
    """Thread-safe in-memory store for Python artifacts produced during kernel execution.

    Artifacts are scoped by ``flow_id`` so that multiple flows sharing the
    same kernel container cannot collide on artifact names.

    When an :class:`~kernel_runtime.artifact_persistence.ArtifactPersistence`
    backend is attached, artifacts are automatically saved to disk on
    ``publish()`` and removed on ``delete()`` / ``clear()``.  In *lazy*
    recovery mode, ``get()`` transparently loads from disk when the
    artifact is not yet in memory.

    .. note:: **Tech Debt / Future Improvement**

       Currently stores the entire object in memory via ``self._artifacts``.
       For very large artifacts (e.g., ML models >1GB), this causes memory
       pressure and potential OOM. A future improvement would be to:

       1. Implement a spill-to-disk mechanism (e.g., pickle to temp file when
          size exceeds threshold, keep only metadata in memory).
       2. Or integrate with an external object store (S3, MinIO) for truly
          large artifacts, storing only a reference here.
       3. For blob uploads, consider a streaming/chunked approach rather than
          reading the entire file into memory before storage.

       See: https://github.com/Edwardvaneechoud/Flowfile/issues/XXX (placeholder)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Keyed by (flow_id, name) so each flow has its own namespace.
        self._artifacts: dict[tuple[int, str], dict[str, Any]] = {}

        # Optional persistence backend — set via ``enable_persistence()``.
        self._persistence: Any | None = None  # ArtifactPersistence
        # Index of artifacts known to be on disk but not yet loaded.
        # Only used in lazy-recovery mode.
        self._lazy_index: dict[tuple[int, str], dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Persistence integration
    # ------------------------------------------------------------------

    def enable_persistence(self, persistence: Any) -> None:
        """Attach a persistence backend to this store.

        Parameters
        ----------
        persistence:
            An :class:`~kernel_runtime.artifact_persistence.ArtifactPersistence`
            instance.
        """
        self._persistence = persistence

    def recover_all(self) -> list[str]:
        """Eagerly load **all** persisted artifacts into memory.

        Returns the names of recovered artifacts.
        """
        if self._persistence is None:
            return []

        recovered: list[str] = []
        for (flow_id, name), meta in self._persistence.list_persisted().items():
            key = (flow_id, name)
            if key in self._artifacts:
                continue  # already in memory
            try:
                obj = self._persistence.load(name, flow_id=flow_id)
                with self._lock:
                    self._artifacts[key] = {
                        "object": obj,
                        "name": name,
                        "type_name": meta.get("type_name", type(obj).__name__),
                        "module": meta.get("module", type(obj).__module__),
                        "node_id": meta.get("node_id", -1),
                        "flow_id": flow_id,
                        "created_at": meta.get("created_at", datetime.now(timezone.utc).isoformat()),
                        "size_bytes": meta.get("size_bytes", sys.getsizeof(obj)),
                        "persisted": True,
                        "recovered": True,
                    }
                recovered.append(name)
                logger.info("Recovered artifact '%s' (flow_id=%d)", name, flow_id)
            except Exception as exc:
                logger.warning("Failed to recover artifact '%s' (flow_id=%d): %s", name, flow_id, exc)
        return recovered

    def build_lazy_index(self) -> int:
        """Scan persisted artifacts and build the lazy-load index.

        Returns the number of artifacts indexed.
        """
        if self._persistence is None:
            return 0
        persisted = self._persistence.list_persisted()
        with self._lock:
            for key, meta in persisted.items():
                if key not in self._artifacts:
                    self._lazy_index[key] = meta
        return len(self._lazy_index)

    def _try_lazy_load(self, key: tuple[int, str]) -> bool:
        """Attempt to load an artifact from disk into memory (lazy mode).

        Must be called **inside** ``self._lock``.  Returns True if
        the artifact was loaded.
        """
        if self._persistence is None or key not in self._lazy_index:
            return False

        flow_id, name = key
        meta = self._lazy_index.pop(key)
        try:
            obj = self._persistence.load(name, flow_id=flow_id)
            self._artifacts[key] = {
                "object": obj,
                "name": name,
                "type_name": meta.get("type_name", type(obj).__name__),
                "module": meta.get("module", type(obj).__module__),
                "node_id": meta.get("node_id", -1),
                "flow_id": flow_id,
                "created_at": meta.get("created_at", datetime.now(timezone.utc).isoformat()),
                "size_bytes": meta.get("size_bytes", sys.getsizeof(obj)),
                "persisted": True,
                "recovered": True,
            }
            logger.info("Lazy-loaded artifact '%s' (flow_id=%d)", name, flow_id)
            return True
        except Exception as exc:
            logger.warning("Failed to lazy-load artifact '%s' (flow_id=%d): %s", name, flow_id, exc)
            return False

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def publish(self, name: str, obj: Any, node_id: int, flow_id: int = 0) -> None:
        key = (flow_id, name)
        with self._lock:
            if key in self._artifacts:
                raise ValueError(
                    f"Artifact '{name}' already exists (published by node "
                    f"{self._artifacts[key]['node_id']}). "
                    f"Delete it first with flowfile.delete_artifact('{name}') "
                    f"before publishing a new one with the same name."
                )
            metadata = {
                "object": obj,
                "name": name,
                "type_name": type(obj).__name__,
                "module": type(obj).__module__,
                "node_id": node_id,
                "flow_id": flow_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "size_bytes": sys.getsizeof(obj),
                "persisted": self._persistence is not None,
            }
            self._artifacts[key] = metadata

            # Remove from lazy index if present (we now have it in memory)
            self._lazy_index.pop(key, None)

        # Persist to disk outside the lock (I/O can be slow)
        if self._persistence is not None:
            try:
                self._persistence.save(name, obj, metadata, flow_id=flow_id)
            except Exception as exc:
                logger.warning("Failed to persist artifact '%s': %s", name, exc)
                # Update persisted flag
                with self._lock:
                    if key in self._artifacts:
                        self._artifacts[key]["persisted"] = False

    def delete(self, name: str, flow_id: int = 0) -> None:
        key = (flow_id, name)
        with self._lock:
            if key not in self._artifacts and key not in self._lazy_index:
                raise KeyError(f"Artifact '{name}' not found")
            self._artifacts.pop(key, None)
            self._lazy_index.pop(key, None)

        if self._persistence is not None:
            try:
                self._persistence.delete(name, flow_id=flow_id)
            except Exception as exc:
                logger.warning("Failed to delete persisted artifact '%s': %s", name, exc)

    def get(self, name: str, flow_id: int = 0) -> Any:
        key = (flow_id, name)
        with self._lock:
            if key in self._artifacts:
                return self._artifacts[key]["object"]
            # Attempt lazy load from disk
            if self._try_lazy_load(key):
                return self._artifacts[key]["object"]
            raise KeyError(f"Artifact '{name}' not found")

    def list_all(self, flow_id: int | None = None) -> dict[str, dict[str, Any]]:
        """Return metadata for all artifacts, optionally filtered by *flow_id*.

        Includes both in-memory artifacts and artifacts known to be
        persisted on disk (lazy index).
        """
        with self._lock:
            result: dict[str, dict[str, Any]] = {}
            # In-memory artifacts
            for (_fid, _name), meta in self._artifacts.items():
                if flow_id is None or _fid == flow_id:
                    result[meta["name"]] = {k: v for k, v in meta.items() if k != "object"}
            # Lazy-indexed (on disk, not yet loaded)
            for (_fid, _name), meta in self._lazy_index.items():
                if flow_id is None or _fid == flow_id:
                    name = meta.get("name", _name)
                    if name not in result:
                        entry = dict(meta)
                        entry["persisted"] = True
                        entry["in_memory"] = False
                        result[name] = entry
            return result

    def clear(self, flow_id: int | None = None) -> None:
        """Clear all artifacts, or only those belonging to *flow_id*."""
        with self._lock:
            if flow_id is None:
                self._artifacts.clear()
                self._lazy_index.clear()
            else:
                to_remove = [
                    key for key in self._artifacts if key[0] == flow_id
                ]
                for key in to_remove:
                    del self._artifacts[key]
                lazy_remove = [
                    key for key in self._lazy_index if key[0] == flow_id
                ]
                for key in lazy_remove:
                    del self._lazy_index[key]

        if self._persistence is not None:
            try:
                self._persistence.clear(flow_id=flow_id)
            except Exception as exc:
                logger.warning("Failed to clear persisted artifacts: %s", exc)

    def clear_by_node_ids(
        self, node_ids: set[int], flow_id: int | None = None,
    ) -> list[str]:
        """Remove all artifacts published by the given *node_ids*.

        When *flow_id* is provided, only artifacts in that flow are
        considered.  Returns the names of deleted artifacts.
        """
        with self._lock:
            to_remove = [
                key
                for key, meta in self._artifacts.items()
                if meta["node_id"] in node_ids
                and (flow_id is None or key[0] == flow_id)
            ]
            removed_names = [self._artifacts[key]["name"] for key in to_remove]
            for key in to_remove:
                del self._artifacts[key]
            # Also clear from lazy index
            lazy_remove = [
                key
                for key, meta in self._lazy_index.items()
                if meta.get("node_id") in node_ids
                and (flow_id is None or key[0] == flow_id)
            ]
            for key in lazy_remove:
                name = self._lazy_index[key].get("name", key[1])
                if name not in removed_names:
                    removed_names.append(name)
                del self._lazy_index[key]

        # Also remove from disk
        if self._persistence is not None:
            for key in to_remove + lazy_remove:
                fid, name = key
                try:
                    self._persistence.delete(name, flow_id=fid)
                except Exception as exc:
                    logger.warning("Failed to delete persisted artifact '%s': %s", name, exc)

        return removed_names

    def list_by_node_id(
        self, node_id: int, flow_id: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Return metadata for artifacts published by *node_id*."""
        with self._lock:
            result: dict[str, dict[str, Any]] = {}
            for (_fid, _name), meta in self._artifacts.items():
                if meta["node_id"] == node_id and (flow_id is None or _fid == flow_id):
                    result[meta["name"]] = {k: v for k, v in meta.items() if k != "object"}
            for (_fid, _name), meta in self._lazy_index.items():
                if meta.get("node_id") == node_id and (flow_id is None or _fid == flow_id):
                    name = meta.get("name", _name)
                    if name not in result:
                        entry = dict(meta)
                        entry["persisted"] = True
                        entry["in_memory"] = False
                        result[name] = entry
            return result
