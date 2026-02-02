from __future__ import annotations

import logging
import sys
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kernel_runtime.persistence import PersistenceManager

logger = logging.getLogger(__name__)


class RecoveryMode(str, Enum):
    LAZY = "lazy"
    EAGER = "eager"
    NONE = "none"


class ArtifactStore:
    """Thread-safe store for Python artifacts with optional disk persistence.

    When a *PersistenceManager* is provided, artifacts are transparently
    written to disk on :meth:`publish` and can be recovered after a kernel
    restart according to the chosen :class:`RecoveryMode`:

    * **lazy** (default) – artifacts are loaded from disk on first
      :meth:`get` access.
    * **eager** – all persisted artifacts are pre-loaded into memory when the
      store is created.
    * **none** – persisted data is cleared on startup (clean slate).
    """

    def __init__(
        self,
        persistence: PersistenceManager | None = None,
        recovery_mode: RecoveryMode = RecoveryMode.LAZY,
    ):
        self._lock = threading.Lock()
        self._artifacts: dict[str, dict[str, Any]] = {}
        self._persistence = persistence
        self._recovery_mode = recovery_mode
        self._recovered: dict[str, bool] = {}

        if persistence and recovery_mode == RecoveryMode.EAGER:
            self._eager_load()
        elif persistence and recovery_mode == RecoveryMode.NONE:
            persistence.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _eager_load(self) -> None:
        """Load all persisted artifacts into memory at startup."""
        if not self._persistence:
            return
        for name, meta in self._persistence.list_persisted().items():
            try:
                obj, loaded_meta = self._persistence.load(name)
                self._artifacts[name] = {
                    "object": obj,
                    "name": name,
                    "type_name": loaded_meta.get("type_name", type(obj).__name__),
                    "module": loaded_meta.get("module", type(obj).__module__),
                    "node_id": loaded_meta.get("node_id", -1),
                    "created_at": loaded_meta.get(
                        "created_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "size_bytes": sys.getsizeof(obj),
                    "persisted": True,
                }
                self._recovered[name] = True
            except Exception as exc:
                logger.warning("Failed to eagerly load artifact '%s': %s", name, exc)

    def _load_from_disk(self, name: str) -> Any:
        """Attempt to lazy-load an artifact from disk into memory.

        Must be called while holding ``self._lock``.
        """
        if not self._persistence:
            raise KeyError(f"Artifact '{name}' not found")
        obj, meta = self._persistence.load(name)
        self._artifacts[name] = {
            "object": obj,
            "name": name,
            "type_name": meta.get("type_name", type(obj).__name__),
            "module": meta.get("module", type(obj).__module__),
            "node_id": meta.get("node_id", -1),
            "created_at": meta.get(
                "created_at", datetime.now(timezone.utc).isoformat()
            ),
            "size_bytes": sys.getsizeof(obj),
            "persisted": True,
        }
        self._recovered[name] = True
        return obj

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, name: str, obj: Any, node_id: int) -> None:
        with self._lock:
            if name in self._artifacts:
                raise ValueError(
                    f"Artifact '{name}' already exists (published by node "
                    f"{self._artifacts[name]['node_id']}). "
                    f"Delete it first with flowfile.delete_artifact('{name}') "
                    f"before publishing a new one with the same name."
                )
            metadata: dict[str, Any] = {
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

            if self._persistence:
                try:
                    self._persistence.persist(name, obj, metadata)
                    metadata["persisted"] = True
                except Exception:
                    logger.warning(
                        "Failed to persist artifact '%s'", name, exc_info=True
                    )

    def delete(self, name: str) -> None:
        with self._lock:
            if name not in self._artifacts:
                # Check disk for lazy-mode artifacts
                if self._persistence and self._persistence.has_persisted(name):
                    self._persistence.delete(name)
                    self._recovered.pop(name, None)
                    return
                raise KeyError(f"Artifact '{name}' not found")
            del self._artifacts[name]
            self._recovered.pop(name, None)

            if self._persistence:
                try:
                    self._persistence.delete(name)
                except Exception:
                    pass

    def get(self, name: str) -> Any:
        with self._lock:
            if name in self._artifacts:
                return self._artifacts[name]["object"]

            # Lazy load from disk when available
            if (
                self._persistence
                and self._recovery_mode == RecoveryMode.LAZY
                and self._persistence.has_persisted(name)
            ):
                try:
                    return self._load_from_disk(name)
                except Exception as exc:
                    logger.warning(
                        "Failed to lazy-load artifact '%s': %s", name, exc
                    )

            raise KeyError(f"Artifact '{name}' not found")

    def list_all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            result = {
                name: {k: v for k, v in meta.items() if k != "object"}
                for name, meta in self._artifacts.items()
            }

            # Include persisted-but-not-loaded artifacts in lazy mode
            if self._persistence and self._recovery_mode == RecoveryMode.LAZY:
                for name, meta in self._persistence.list_persisted().items():
                    if name not in result:
                        result[name] = {
                            **meta,
                            "persisted": True,
                            "loaded": False,
                        }

            return result

    def clear(self) -> None:
        """Clear in-memory artifacts.  Persisted data on disk is kept."""
        with self._lock:
            self._artifacts.clear()
            self._recovered.clear()

    # ------------------------------------------------------------------
    # Recovery & persistence queries
    # ------------------------------------------------------------------

    def recover_all(self) -> dict[str, str]:
        """Manually load all persisted artifacts into memory.

        Returns:
            Dict mapping artifact name to status
            (``"recovered"``, ``"already_loaded"``, or an error message).
        """
        results: dict[str, str] = {}
        if not self._persistence:
            return results

        with self._lock:
            for name in self._persistence.list_persisted():
                if name in self._artifacts:
                    results[name] = "already_loaded"
                    continue
                try:
                    self._load_from_disk(name)
                    results[name] = "recovered"
                except Exception as exc:
                    results[name] = f"error: {exc}"

        return results

    def recovery_status(self) -> dict[str, Any]:
        """Return recovery status information."""
        with self._lock:
            in_memory = set(self._artifacts.keys())

        persisted: set[str] = set()
        if self._persistence:
            persisted = set(self._persistence.list_persisted().keys())

        return {
            "recovery_mode": self._recovery_mode.value,
            "persistence_enabled": self._persistence is not None,
            "artifacts_in_memory": len(in_memory),
            "artifacts_persisted": len(persisted),
            "artifacts_recovered": dict(self._recovered),
            "not_yet_loaded": sorted(persisted - in_memory),
        }

    def persistence_info(self) -> dict[str, Any]:
        """Return detailed persistence information."""
        persisted_artifacts: dict[str, dict[str, Any]] = {}
        disk_usage = 0
        if self._persistence:
            persisted_artifacts = self._persistence.list_persisted()
            disk_usage = self._persistence.disk_usage()

        with self._lock:
            artifact_statuses: dict[str, dict[str, Any]] = {}
            for name, meta in self._artifacts.items():
                artifact_statuses[name] = {
                    "in_memory": True,
                    "persisted": meta.get("persisted", False),
                    "recovered": self._recovered.get(name, False),
                }

            for name in persisted_artifacts:
                if name not in artifact_statuses:
                    artifact_statuses[name] = {
                        "in_memory": False,
                        "persisted": True,
                        "recovered": False,
                    }

        return {
            "persistence_enabled": self._persistence is not None,
            "recovery_mode": self._recovery_mode.value,
            "disk_usage_bytes": disk_usage,
            "total_artifacts": len(artifact_statuses),
            "persisted_count": sum(
                1 for s in artifact_statuses.values() if s["persisted"]
            ),
            "memory_only_count": sum(
                1
                for s in artifact_statuses.values()
                if s["in_memory"] and not s["persisted"]
            ),
            "artifacts": artifact_statuses,
        }

    def cleanup(
        self,
        max_age_hours: float | None = None,
        names: list[str] | None = None,
    ) -> list[str]:
        """Clean up persisted artifacts from disk (and memory).

        Returns:
            List of artifact names that were deleted.
        """
        if not self._persistence:
            return []
        deleted = self._persistence.cleanup(
            max_age_hours=max_age_hours, names=names
        )
        with self._lock:
            for name in deleted:
                self._artifacts.pop(name, None)
                self._recovered.pop(name, None)
        return deleted
