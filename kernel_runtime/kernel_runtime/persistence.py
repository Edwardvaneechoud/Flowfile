"""Disk persistence for kernel artifacts using cloudpickle.

Storage layout::

    {base_path}/{kernel_id}/{artifact_name}/
        data.artifact   # cloudpickle-serialized object
        meta.json       # metadata (type, source node, timestamp, checksum)
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import cloudpickle

logger = logging.getLogger(__name__)


class PersistenceManager:
    """Handles serializing/deserializing artifacts to/from disk."""

    def __init__(self, base_path: str, kernel_id: str):
        self._base_path = Path(base_path) / kernel_id
        self._kernel_id = kernel_id
        self._base_path.mkdir(parents=True, exist_ok=True)

    @property
    def storage_path(self) -> Path:
        return self._base_path

    def persist(self, name: str, obj: Any, metadata: dict[str, Any]) -> None:
        """Serialize an artifact to disk."""
        artifact_dir = self._base_path / name
        artifact_dir.mkdir(parents=True, exist_ok=True)

        data_path = artifact_dir / "data.artifact"
        meta_path = artifact_dir / "meta.json"

        pickled = cloudpickle.dumps(obj)
        checksum = hashlib.sha256(pickled).hexdigest()

        data_path.write_bytes(pickled)

        meta = {
            **{k: v for k, v in metadata.items() if k != "object"},
            "checksum": checksum,
            "persisted_at": datetime.now(timezone.utc).isoformat(),
            "size_on_disk": len(pickled),
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        logger.info("Persisted artifact '%s' (%d bytes)", name, len(pickled))

    def load(self, name: str) -> tuple[Any, dict[str, Any]]:
        """Load an artifact from disk.

        Returns:
            Tuple of (deserialized object, metadata dict).
        """
        artifact_dir = self._base_path / name
        data_path = artifact_dir / "data.artifact"
        meta_path = artifact_dir / "meta.json"

        if not data_path.exists():
            raise FileNotFoundError(f"No persisted artifact '{name}' found")

        meta: dict[str, Any] = {}
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())

        pickled = data_path.read_bytes()
        expected_checksum = meta.get("checksum")
        if expected_checksum:
            actual_checksum = hashlib.sha256(pickled).hexdigest()
            if actual_checksum != expected_checksum:
                raise ValueError(
                    f"Checksum mismatch for artifact '{name}': "
                    f"expected {expected_checksum}, got {actual_checksum}"
                )

        obj = cloudpickle.loads(pickled)
        logger.info("Loaded artifact '%s' from disk", name)
        return obj, meta

    def delete(self, name: str) -> None:
        """Remove a persisted artifact from disk."""
        artifact_dir = self._base_path / name
        if artifact_dir.exists():
            shutil.rmtree(artifact_dir)
            logger.info("Deleted persisted artifact '%s'", name)

    def clear(self) -> None:
        """Remove all persisted artifacts for this kernel."""
        if self._base_path.exists():
            shutil.rmtree(self._base_path)
            self._base_path.mkdir(parents=True, exist_ok=True)
            logger.info("Cleared all persisted artifacts for kernel '%s'", self._kernel_id)

    def list_persisted(self) -> dict[str, dict[str, Any]]:
        """List all persisted artifacts with their metadata."""
        result: dict[str, dict[str, Any]] = {}
        if not self._base_path.exists():
            return result

        for artifact_dir in sorted(self._base_path.iterdir()):
            if not artifact_dir.is_dir():
                continue
            data_path = artifact_dir / "data.artifact"
            if not data_path.exists():
                continue
            meta_path = artifact_dir / "meta.json"
            meta: dict[str, Any] = {}
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text())
                except (json.JSONDecodeError, OSError):
                    meta = {"error": "could not read metadata"}
            result[artifact_dir.name] = meta

        return result

    def has_persisted(self, name: str) -> bool:
        """Check if an artifact exists on disk."""
        return (self._base_path / name / "data.artifact").exists()

    def cleanup(
        self,
        max_age_hours: float | None = None,
        names: list[str] | None = None,
    ) -> list[str]:
        """Clean up persisted artifacts.

        Args:
            max_age_hours: Delete artifacts older than this many hours.
            names: Delete specific artifacts by name.

        Returns:
            List of artifact names that were deleted.
        """
        deleted: list[str] = []

        if names:
            for name in names:
                if self.has_persisted(name):
                    self.delete(name)
                    deleted.append(name)

        if max_age_hours is not None:
            cutoff = time.time() - (max_age_hours * 3600)
            for artifact_dir in list(self._base_path.iterdir()):
                if not artifact_dir.is_dir():
                    continue
                name = artifact_dir.name
                if name in deleted:
                    continue
                meta_path = artifact_dir / "meta.json"
                if not meta_path.exists():
                    continue
                try:
                    meta = json.loads(meta_path.read_text())
                    persisted_at = meta.get("persisted_at", "")
                    if persisted_at:
                        ts = datetime.fromisoformat(persisted_at).timestamp()
                        if ts < cutoff:
                            shutil.rmtree(artifact_dir)
                            deleted.append(name)
                            logger.info(
                                "Cleaned up artifact '%s' (older than %.1f hours)",
                                name,
                                max_age_hours,
                            )
                except (json.JSONDecodeError, OSError, ValueError):
                    pass

        return deleted

    def disk_usage(self) -> int:
        """Total disk usage in bytes for this kernel's persisted artifacts."""
        total = 0
        if not self._base_path.exists():
            return total
        for f in self._base_path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
        return total
