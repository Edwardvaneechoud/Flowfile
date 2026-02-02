"""Disk-backed persistence layer for kernel artifacts.

Uses ``cloudpickle`` for serialisation — it handles lambdas, closures,
sklearn models, torch modules, and most arbitrary Python objects out of
the box.  Each artifact is stored as a pair of files:

    {base_path}/{flow_id}/{artifact_name}/data.artifact   # cloudpickle bytes
    {base_path}/{flow_id}/{artifact_name}/meta.json        # JSON metadata

A SHA-256 checksum is written into the metadata so corruption can be
detected on load.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import time
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import cloudpickle

logger = logging.getLogger(__name__)


class RecoveryMode(str, Enum):
    LAZY = "lazy"
    EAGER = "eager"
    NONE = "none"


def _safe_dirname(name: str) -> str:
    """Convert an artifact name to a filesystem-safe directory name."""
    return re.sub(r"[^\w\-.]", "_", name)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ArtifactPersistence:
    """Saves and loads artifacts to/from local disk using cloudpickle.

    Parameters
    ----------
    base_path:
        Root directory for persisted artifacts (e.g. ``/shared/artifacts/{kernel_id}``).
    """

    def __init__(self, base_path: str | Path) -> None:
        self._base = Path(base_path)
        self._base.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    def _artifact_dir(self, flow_id: int, name: str) -> Path:
        return self._base / str(flow_id) / _safe_dirname(name)

    def _data_path(self, flow_id: int, name: str) -> Path:
        return self._artifact_dir(flow_id, name) / "data.artifact"

    def _meta_path(self, flow_id: int, name: str) -> Path:
        return self._artifact_dir(flow_id, name) / "meta.json"

    # ------------------------------------------------------------------
    # Save / Load / Delete
    # ------------------------------------------------------------------

    def save(self, name: str, obj: Any, metadata: dict[str, Any], flow_id: int = 0) -> None:
        """Persist *obj* to disk alongside its *metadata*."""
        artifact_dir = self._artifact_dir(flow_id, name)
        artifact_dir.mkdir(parents=True, exist_ok=True)

        data = cloudpickle.dumps(obj)
        checksum = _sha256(data)

        data_path = self._data_path(flow_id, name)
        data_path.write_bytes(data)

        meta = {
            k: v for k, v in metadata.items() if k != "object"
        }
        meta["checksum"] = checksum
        meta["persisted_at"] = datetime.now(timezone.utc).isoformat()
        meta["data_size_bytes"] = len(data)

        self._meta_path(flow_id, name).write_text(json.dumps(meta, indent=2))
        logger.debug("Persisted artifact '%s' (flow_id=%d, %d bytes)", name, flow_id, len(data))

    def load(self, name: str, flow_id: int = 0) -> Any:
        """Load an artifact from disk.  Raises ``FileNotFoundError`` if
        the artifact has not been persisted or ``ValueError`` on
        checksum mismatch.
        """
        data_path = self._data_path(flow_id, name)
        meta_path = self._meta_path(flow_id, name)

        if not data_path.exists():
            raise FileNotFoundError(f"No persisted artifact '{name}' for flow_id={flow_id}")

        data = data_path.read_bytes()

        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            expected = meta.get("checksum")
            if expected and _sha256(data) != expected:
                raise ValueError(
                    f"Checksum mismatch for artifact '{name}' — the persisted file may be corrupt"
                )

        return cloudpickle.loads(data)

    def load_metadata(self, name: str, flow_id: int = 0) -> dict[str, Any] | None:
        """Load only the JSON metadata for a persisted artifact."""
        meta_path = self._meta_path(flow_id, name)
        if not meta_path.exists():
            return None
        return json.loads(meta_path.read_text())

    def delete(self, name: str, flow_id: int = 0) -> None:
        """Remove a persisted artifact from disk."""
        artifact_dir = self._artifact_dir(flow_id, name)
        if artifact_dir.exists():
            shutil.rmtree(artifact_dir)
            logger.debug("Deleted persisted artifact '%s' (flow_id=%d)", name, flow_id)

    def clear(self, flow_id: int | None = None) -> None:
        """Remove all persisted artifacts, optionally scoped to *flow_id*."""
        if flow_id is not None:
            flow_dir = self._base / str(flow_id)
            if flow_dir.exists():
                shutil.rmtree(flow_dir)
                logger.debug("Cleared persisted artifacts for flow_id=%d", flow_id)
        else:
            for child in self._base.iterdir():
                if child.is_dir():
                    shutil.rmtree(child)
            logger.debug("Cleared all persisted artifacts")

    # ------------------------------------------------------------------
    # Index / Discovery
    # ------------------------------------------------------------------

    def list_persisted(self, flow_id: int | None = None) -> dict[tuple[int, str], dict[str, Any]]:
        """Scan disk and return ``{(flow_id, name): metadata}`` for all
        persisted artifacts.
        """
        result: dict[tuple[int, str], dict[str, Any]] = {}
        flow_dirs = (
            [self._base / str(flow_id)] if flow_id is not None
            else [d for d in self._base.iterdir() if d.is_dir()]
        )
        for flow_dir in flow_dirs:
            if not flow_dir.exists():
                continue
            try:
                fid = int(flow_dir.name)
            except ValueError:
                continue
            for artifact_dir in flow_dir.iterdir():
                if not artifact_dir.is_dir():
                    continue
                meta_path = artifact_dir / "meta.json"
                if not meta_path.exists():
                    continue
                try:
                    meta = json.loads(meta_path.read_text())
                    name = meta.get("name", artifact_dir.name)
                    result[(fid, name)] = meta
                except (json.JSONDecodeError, OSError) as exc:
                    logger.warning("Skipping corrupt metadata in %s: %s", meta_path, exc)
        return result

    def disk_usage_bytes(self) -> int:
        """Return total bytes used by all persisted artifacts."""
        total = 0
        for path in self._base.rglob("*"):
            if path.is_file():
                total += path.stat().st_size
        return total

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(
        self,
        max_age_hours: float | None = None,
        names: list[tuple[int, str]] | None = None,
    ) -> int:
        """Remove old or specific persisted artifacts.

        Parameters
        ----------
        max_age_hours:
            If set, delete artifacts persisted more than this many hours ago.
        names:
            If set, delete these specific ``(flow_id, name)`` pairs.

        Returns the number of artifacts removed.
        """
        removed = 0

        if names:
            for flow_id, name in names:
                self.delete(name, flow_id=flow_id)
                removed += 1

        if max_age_hours is not None:
            cutoff = time.time() - (max_age_hours * 3600)
            for (fid, name), meta in self.list_persisted().items():
                persisted_at = meta.get("persisted_at")
                if persisted_at:
                    try:
                        ts = datetime.fromisoformat(persisted_at).timestamp()
                        if ts < cutoff:
                            self.delete(name, flow_id=fid)
                            removed += 1
                    except (ValueError, OSError):
                        pass

        return removed
