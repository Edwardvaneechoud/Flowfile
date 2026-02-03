import sys
import threading
from datetime import datetime, timezone
from typing import Any


class ArtifactStore:
    """Thread-safe in-memory store for Python artifacts produced during kernel execution.

    Artifacts are scoped by ``flow_id`` so that multiple flows sharing the
    same kernel container cannot collide on artifact names.

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

    def __init__(self):
        self._lock = threading.Lock()
        # Keyed by (flow_id, name) so each flow has its own namespace.
        self._artifacts: dict[tuple[int, str], dict[str, Any]] = {}

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
            self._artifacts[key] = {
                "object": obj,
                "name": name,
                "type_name": type(obj).__name__,
                "module": type(obj).__module__,
                "node_id": node_id,
                "flow_id": flow_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "size_bytes": sys.getsizeof(obj),
            }

    def delete(self, name: str, flow_id: int = 0) -> None:
        key = (flow_id, name)
        with self._lock:
            if key not in self._artifacts:
                raise KeyError(f"Artifact '{name}' not found")
            del self._artifacts[key]

    def get(self, name: str, flow_id: int = 0) -> Any:
        key = (flow_id, name)
        with self._lock:
            if key not in self._artifacts:
                raise KeyError(f"Artifact '{name}' not found")
            return self._artifacts[key]["object"]

    def list_all(self, flow_id: int | None = None) -> dict[str, dict[str, Any]]:
        """Return metadata for all artifacts, optionally filtered by *flow_id*."""
        with self._lock:
            return {
                meta["name"]: {k: v for k, v in meta.items() if k != "object"}
                for (_fid, _name), meta in self._artifacts.items()
                if flow_id is None or _fid == flow_id
            }

    def clear(self, flow_id: int | None = None) -> None:
        """Clear all artifacts, or only those belonging to *flow_id*."""
        with self._lock:
            if flow_id is None:
                self._artifacts.clear()
            else:
                to_remove = [
                    key for key in self._artifacts if key[0] == flow_id
                ]
                for key in to_remove:
                    del self._artifacts[key]

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
            return removed_names

    def list_by_node_id(
        self, node_id: int, flow_id: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Return metadata for artifacts published by *node_id*."""
        with self._lock:
            return {
                meta["name"]: {k: v for k, v in meta.items() if k != "object"}
                for (_fid, _name), meta in self._artifacts.items()
                if meta["node_id"] == node_id
                and (flow_id is None or _fid == flow_id)
            }
