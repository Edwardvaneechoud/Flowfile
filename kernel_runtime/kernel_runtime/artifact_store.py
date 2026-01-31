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
            self._artifacts[name] = {
                "object": obj,
                "name": name,
                "type_name": type(obj).__name__,
                "module": type(obj).__module__,
                "node_id": node_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "size_bytes": sys.getsizeof(obj),
            }

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
