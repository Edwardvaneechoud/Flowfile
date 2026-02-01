import sys
import threading
from datetime import datetime, timezone
from typing import Any


class ArtifactStore:
    """Thread-safe in-memory store for Python artifacts produced during kernel execution."""

    def __init__(self):
        self._lock = threading.Lock()
        self._artifacts: dict[str, dict[str, Any]] = {}

    def publish(self, name: str, obj: Any, node_id: int) -> None:
        with self._lock:
            if name in self._artifacts:
                raise ValueError(
                    f"Artifact '{name}' already exists (published by node "
                    f"{self._artifacts[name]['node_id']}). "
                    f"Delete it first with flowfile.delete_artifact('{name}') "
                    f"before publishing a new one with the same name."
                )
            self._artifacts[name] = {
                "object": obj,
                "name": name,
                "type_name": type(obj).__name__,
                "module": type(obj).__module__,
                "node_id": node_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "size_bytes": sys.getsizeof(obj),
            }

    def delete(self, name: str) -> None:
        with self._lock:
            if name not in self._artifacts:
                raise KeyError(f"Artifact '{name}' not found")
            del self._artifacts[name]

    def get(self, name: str) -> Any:
        with self._lock:
            if name not in self._artifacts:
                raise KeyError(f"Artifact '{name}' not found")
            return self._artifacts[name]["object"]

    def list_all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {
                name: {k: v for k, v in meta.items() if k != "object"}
                for name, meta in self._artifacts.items()
            }

    def clear(self) -> None:
        with self._lock:
            self._artifacts.clear()

    def clear_by_node_ids(self, node_ids: set[int]) -> list[str]:
        """Remove all artifacts published by the given *node_ids*.

        Returns the names of deleted artifacts.
        """
        with self._lock:
            to_remove = [
                name
                for name, meta in self._artifacts.items()
                if meta["node_id"] in node_ids
            ]
            for name in to_remove:
                del self._artifacts[name]
            return to_remove

    def list_by_node_id(self, node_id: int) -> dict[str, dict[str, Any]]:
        """Return metadata for artifacts published by *node_id*."""
        with self._lock:
            return {
                name: {k: v for k, v in meta.items() if k != "object"}
                for name, meta in self._artifacts.items()
                if meta["node_id"] == node_id
            }
