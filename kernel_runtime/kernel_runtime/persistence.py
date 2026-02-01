"""Artifact persistence layer for kernel runtime.

Provides transparent serialization/deserialization of Python artifacts
to disk using cloudpickle, enabling recovery after kernel restarts.

Storage layout:
    {base_path}/{kernel_id}/{artifact_name}/
        data.artifact   - cloudpickle serialized object
        meta.json       - metadata (type, source node, timestamp, checksum)
"""

import hashlib
import json
import logging
import shutil
import time
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class RecoveryMode(str, Enum):
    LAZY = "lazy"
    EAGER = "eager"
    NONE = "none"


class ArtifactPersistence:
    """Handles artifact serialization/deserialization to disk."""

    def __init__(self, base_path: str, kernel_id: str):
        self._base_path = Path(base_path)
        self._kernel_id = kernel_id
        self._artifacts_dir = self._base_path / kernel_id
        self._artifacts_dir.mkdir(parents=True, exist_ok=True)

    @property
    def artifacts_dir(self) -> Path:
        return self._artifacts_dir

    @property
    def kernel_id(self) -> str:
        return self._kernel_id

    def save(self, name: str, obj: Any, metadata: dict[str, Any]) -> None:
        """Persist an artifact to disk."""
        import cloudpickle

        artifact_dir = self._artifacts_dir / name
        artifact_dir.mkdir(parents=True, exist_ok=True)

        data_path = artifact_dir / "data.artifact"
        meta_path = artifact_dir / "meta.json"

        serialized = cloudpickle.dumps(obj)
        checksum = hashlib.sha256(serialized).hexdigest()

        data_path.write_bytes(serialized)

        meta = {
            **metadata,
            "checksum": f"sha256:{checksum}",
            "persisted_at": datetime.now(timezone.utc).isoformat(),
            "data_size_bytes": len(serialized),
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        logger.info(
            "Persisted artifact '%s' for kernel '%s' (%d bytes)",
            name,
            self._kernel_id,
            len(serialized),
        )

    def load(self, name: str) -> tuple[Any, dict[str, Any]]:
        """Load a persisted artifact from disk.

        Returns (object, metadata) tuple.
        Raises FileNotFoundError if artifact doesn't exist on disk.
        Raises ValueError if checksum validation fails.
        """
        import cloudpickle

        artifact_dir = self._artifacts_dir / name
        data_path = artifact_dir / "data.artifact"
        meta_path = artifact_dir / "meta.json"

        if not data_path.exists():
            raise FileNotFoundError(f"Persisted artifact '{name}' not found")

        metadata: dict[str, Any] = {}
        if meta_path.exists():
            metadata = json.loads(meta_path.read_text())

        serialized = data_path.read_bytes()

        if "checksum" in metadata:
            expected = metadata["checksum"]
            actual = f"sha256:{hashlib.sha256(serialized).hexdigest()}"
            if expected != actual:
                raise ValueError(
                    f"Checksum mismatch for artifact '{name}': "
                    f"expected {expected}, got {actual}"
                )

        obj = cloudpickle.loads(serialized)  # noqa: S301
        logger.info(
            "Loaded persisted artifact '%s' for kernel '%s'",
            name,
            self._kernel_id,
        )
        return obj, metadata

    def delete(self, name: str) -> None:
        """Remove a persisted artifact from disk."""
        artifact_dir = self._artifacts_dir / name
        if artifact_dir.exists():
            shutil.rmtree(artifact_dir)
            logger.info(
                "Deleted persisted artifact '%s' for kernel '%s'",
                name,
                self._kernel_id,
            )

    def list_persisted(self) -> dict[str, dict[str, Any]]:
        """List all persisted artifacts with their metadata."""
        result: dict[str, dict[str, Any]] = {}
        if not self._artifacts_dir.exists():
            return result

        for artifact_dir in sorted(self._artifacts_dir.iterdir()):
            if not artifact_dir.is_dir():
                continue
            data_path = artifact_dir / "data.artifact"
            if not data_path.exists():
                continue

            meta_path = artifact_dir / "meta.json"
            metadata: dict[str, Any] = {}
            if meta_path.exists():
                try:
                    metadata = json.loads(meta_path.read_text())
                except (json.JSONDecodeError, OSError):
                    pass

            result[artifact_dir.name] = metadata

        return result

    def clear(self) -> None:
        """Remove all persisted artifacts for this kernel."""
        if self._artifacts_dir.exists():
            shutil.rmtree(self._artifacts_dir)
            self._artifacts_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Cleared all persisted artifacts for kernel '%s'",
                self._kernel_id,
            )

    def cleanup(self, max_age_hours: int = 24) -> list[str]:
        """Remove artifacts older than max_age_hours.

        Returns list of removed artifact names.
        """
        removed: list[str] = []
        if not self._artifacts_dir.exists():
            return removed

        cutoff = time.time() - (max_age_hours * 3600)

        for artifact_dir in sorted(self._artifacts_dir.iterdir()):
            if not artifact_dir.is_dir():
                continue

            data_path = artifact_dir / "data.artifact"
            if not data_path.exists():
                continue

            should_remove = False
            meta_path = artifact_dir / "meta.json"
            if meta_path.exists():
                try:
                    metadata = json.loads(meta_path.read_text())
                    persisted_at = metadata.get("persisted_at", "")
                    if persisted_at:
                        dt = datetime.fromisoformat(persisted_at)
                        if dt.timestamp() < cutoff:
                            should_remove = True
                except (json.JSONDecodeError, ValueError, OSError):
                    pass

            if not should_remove and data_path.stat().st_mtime < cutoff:
                should_remove = True

            if should_remove:
                name = artifact_dir.name
                shutil.rmtree(artifact_dir)
                removed.append(name)
                logger.info(
                    "Cleaned up old artifact '%s' for kernel '%s'",
                    name,
                    self._kernel_id,
                )

        return removed

    def get_stats(self) -> dict[str, Any]:
        """Get persistence statistics."""
        persisted = self.list_persisted()
        total_size = 0

        for name in persisted:
            data_path = self._artifacts_dir / name / "data.artifact"
            if data_path.exists():
                total_size += data_path.stat().st_size

        return {
            "kernel_id": self._kernel_id,
            "persistence_path": str(self._artifacts_dir),
            "artifact_count": len(persisted),
            "total_disk_bytes": total_size,
            "artifacts": {
                name: {
                    "persisted_at": meta.get("persisted_at"),
                    "data_size_bytes": meta.get("data_size_bytes", 0),
                    "type_name": meta.get("type_name"),
                }
                for name, meta in persisted.items()
            },
        }
