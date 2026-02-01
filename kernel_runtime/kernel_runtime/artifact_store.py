from __future__ import annotations

import logging
import sys
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kernel_runtime.persistence import ArtifactPersistence

logger = logging.getLogger(__name__)


class ArtifactStore:
    """Thread-safe in-memory store for Python artifacts produced during kernel execution.

    When initialized with a persistence backend, artifacts are automatically
    saved to disk on publish and can be recovered from disk on access (lazy mode).
    """

    def __init__(self, persistence: ArtifactPersistence | None = None):
        self._lock = threading.Lock()
        self._artifacts: dict[str, dict[str, Any]] = {}
        self._persistence = persistence
        # Track artifacts that were lazy-loaded from disk (not explicitly published
        # in this session) so that list_all() excludes them from delta tracking.
        self._lazy_loaded: set[str] = set()

    def publish(self, name: str, obj: Any, node_id: int) -> None:
        with self._lock:
            if name in self._artifacts:
                raise ValueError(
                    f"Artifact '{name}' already exists (published by node "
                    f"{self._artifacts[name]['node_id']}). "
                    f"Delete it first with flowfile.delete_artifact('{name}') "
                    f"before publishing a new one with the same name."
                )
            metadata = {
                "object": obj,
                "name": name,
                "type_name": type(obj).__name__,
                "module": type(obj).__module__,
                "node_id": node_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "size_bytes": sys.getsizeof(obj),
                "persisted": False,
            }
            self._artifacts[name] = metadata
            # If this name was previously lazy-loaded, it is now explicitly
            # published so remove it from the lazy set.
            self._lazy_loaded.discard(name)

            if self._persistence is not None:
                try:
                    disk_meta = {k: v for k, v in metadata.items() if k != "object"}
                    self._persistence.save(name, obj, disk_meta)
                    self._artifacts[name]["persisted"] = True
                except Exception:
                    logger.warning(
                        "Failed to persist artifact '%s'", name, exc_info=True
                    )

    def delete(self, name: str) -> None:
        with self._lock:
            if name not in self._artifacts:
                raise KeyError(f"Artifact '{name}' not found")
            del self._artifacts[name]
            self._lazy_loaded.discard(name)

            if self._persistence is not None:
                try:
                    self._persistence.delete(name)
                except Exception:
                    logger.warning(
                        "Failed to delete persisted artifact '%s'",
                        name,
                        exc_info=True,
                    )

    def get(self, name: str) -> Any:
        with self._lock:
            if name in self._artifacts:
                return self._artifacts[name]["object"]

            # Lazy load from disk if not in memory
            if self._persistence is not None:
                try:
                    obj, metadata = self._persistence.load(name)
                    self._artifacts[name] = {
                        "object": obj,
                        "name": name,
                        "type_name": metadata.get("type_name", type(obj).__name__),
                        "module": metadata.get("module", type(obj).__module__),
                        "node_id": metadata.get("node_id", -1),
                        "created_at": metadata.get(
                            "created_at",
                            datetime.now(timezone.utc).isoformat(),
                        ),
                        "size_bytes": sys.getsizeof(obj),
                        "persisted": True,
                        "recovered": True,
                    }
                    self._lazy_loaded.add(name)
                    return obj
                except FileNotFoundError:
                    pass
                except Exception:
                    logger.warning(
                        "Failed to load persisted artifact '%s'",
                        name,
                        exc_info=True,
                    )

            raise KeyError(f"Artifact '{name}' not found")

    def list_all(self) -> dict[str, dict[str, Any]]:
        """List in-memory artifacts that were explicitly published (not lazy-loaded).

        This is used by the /execute endpoint for delta tracking, so it must
        not include artifacts that were merely loaded from disk during access.
        """
        with self._lock:
            return {
                name: {k: v for k, v in meta.items() if k != "object"}
                for name, meta in self._artifacts.items()
                if name not in self._lazy_loaded
            }

    def list_available(self) -> dict[str, dict[str, Any]]:
        """List all available artifacts: in-memory plus persisted-but-not-yet-loaded.

        Used by user-facing endpoints and the flowfile client's list_artifacts().
        """
        with self._lock:
            result: dict[str, dict[str, Any]] = {}

            for name, meta in self._artifacts.items():
                result[name] = {k: v for k, v in meta.items() if k != "object"}

            if self._persistence is not None:
                persisted = self._persistence.list_persisted()
                for name, disk_meta in persisted.items():
                    if name not in result:
                        result[name] = {
                            "name": name,
                            "type_name": disk_meta.get("type_name", "unknown"),
                            "module": disk_meta.get("module", "unknown"),
                            "node_id": disk_meta.get("node_id", -1),
                            "created_at": disk_meta.get("created_at", ""),
                            "size_bytes": disk_meta.get("size_bytes", 0),
                            "persisted": True,
                            "recovered": False,
                        }

            return result

    def clear_for_node(self, node_id: int) -> list[str]:
        """Clear only in-memory artifacts owned by a specific node.

        Returns the list of artifact names that were removed.
        Persisted artifacts on disk are preserved for crash recovery.
        """
        with self._lock:
            to_remove = [
                name
                for name, meta in self._artifacts.items()
                if meta.get("node_id") == node_id
            ]
            for name in to_remove:
                del self._artifacts[name]
                self._lazy_loaded.discard(name)
            return to_remove

    def clear(self) -> None:
        """Clear all in-memory artifacts. Persisted artifacts on disk are preserved."""
        with self._lock:
            self._artifacts.clear()
            self._lazy_loaded.clear()

    def recover_all(self) -> list[str]:
        """Eagerly load all persisted artifacts into memory.

        Returns list of recovered artifact names.
        """
        recovered: list[str] = []
        if self._persistence is None:
            return recovered

        with self._lock:
            persisted = self._persistence.list_persisted()
            for name in persisted:
                if name in self._artifacts:
                    continue
                try:
                    obj, metadata = self._persistence.load(name)
                    self._artifacts[name] = {
                        "object": obj,
                        "name": name,
                        "type_name": metadata.get("type_name", type(obj).__name__),
                        "module": metadata.get("module", type(obj).__module__),
                        "node_id": metadata.get("node_id", -1),
                        "created_at": metadata.get(
                            "created_at",
                            datetime.now(timezone.utc).isoformat(),
                        ),
                        "size_bytes": sys.getsizeof(obj),
                        "persisted": True,
                        "recovered": True,
                    }
                    self._lazy_loaded.add(name)
                    recovered.append(name)
                except Exception:
                    logger.warning(
                        "Failed to recover artifact '%s'", name, exc_info=True
                    )

        return recovered
