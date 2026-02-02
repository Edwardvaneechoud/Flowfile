"""
Artifact persistence store.

Provides transparent disk-backed storage for Python objects (trained models,
encoders, computed objects) using cloudpickle.  Artifacts survive kernel
restarts and can be recovered lazily or eagerly.

Storage layout
--------------
{artifacts_dir}/{flow_id}/{artifact_name}/
    data.artifact   – cloudpickle-serialised object
    meta.json       – human-readable metadata (type, timestamp, checksum …)
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import cloudpickle

from flowfile_core.artifacts.models import (
    ArtifactInfo,
    ArtifactMetadata,
    CleanupResult,
    PersistenceStats,
    RecoveryMode,
    RecoveryStatus,
)
from shared.storage_config import storage

logger = logging.getLogger(__name__)

DATA_FILENAME = "data.artifact"
META_FILENAME = "meta.json"


class ArtifactStore:
    """Per-flow, thread-safe artifact store backed by the shared filesystem.

    Each *flow_id* gets its own namespace so artifacts from different flows
    never collide.  All writes are immediately persisted to disk; reads check
    the in-memory cache first, falling back to disk (lazy recovery).
    """

    def __init__(self, flow_id: int, recovery_mode: RecoveryMode = RecoveryMode.LAZY):
        self.flow_id = flow_id
        self.recovery_mode = recovery_mode
        self._memory: dict[str, Any] = {}
        self._metadata: dict[str, ArtifactMetadata] = {}
        self._lock = threading.Lock()
        self._flow_dir.mkdir(parents=True, exist_ok=True)

        # Eager mode: pre-load everything from disk on init.
        if self.recovery_mode == RecoveryMode.EAGER:
            self._recover_all()
        elif self.recovery_mode == RecoveryMode.NONE:
            self._clear_persisted()

        # Always load metadata index from disk (cheap).
        self._load_metadata_index()

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    @property
    def _flow_dir(self) -> Path:
        return storage.artifacts_directory / str(self.flow_id)

    def _artifact_dir(self, name: str) -> Path:
        return self._flow_dir / name

    def _data_path(self, name: str) -> Path:
        return self._artifact_dir(name) / DATA_FILENAME

    def _meta_path(self, name: str) -> Path:
        return self._artifact_dir(name) / META_FILENAME

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(
        self,
        name: str,
        obj: Any,
        *,
        source_node_id: int | None = None,
    ) -> ArtifactMetadata:
        """Persist *obj* under *name*, overwriting any previous version."""
        with self._lock:
            artifact_dir = self._artifact_dir(name)
            artifact_dir.mkdir(parents=True, exist_ok=True)

            # Serialise
            data = cloudpickle.dumps(obj)
            checksum = hashlib.sha256(data).hexdigest()

            # Write data
            data_path = self._data_path(name)
            data_path.write_bytes(data)

            # Build metadata
            now = datetime.utcnow()
            obj_type = f"{type(obj).__module__}.{type(obj).__qualname__}"
            meta = ArtifactMetadata(
                name=name,
                flow_id=self.flow_id,
                object_type=obj_type,
                source_node_id=source_node_id,
                created_at=self._metadata[name].created_at if name in self._metadata else now,
                updated_at=now,
                size_bytes=len(data),
                checksum=checksum,
                persisted=True,
            )

            # Write metadata
            self._meta_path(name).write_text(
                json.dumps(meta.model_dump(mode="json"), indent=2, default=str),
                encoding="utf-8",
            )

            # Update in-memory cache
            self._memory[name] = obj
            self._metadata[name] = meta

            logger.info("Artifact '%s' published for flow %s (%d bytes)", name, self.flow_id, len(data))
            return meta

    def read(self, name: str) -> Any:
        """Return the artifact object, loading from disk if necessary (lazy)."""
        with self._lock:
            # Fast path: already in memory.
            if name in self._memory:
                return self._memory[name]

            # Lazy load from disk.
            data_path = self._data_path(name)
            if not data_path.exists():
                raise KeyError(f"Artifact '{name}' not found for flow {self.flow_id}")

            obj = cloudpickle.loads(data_path.read_bytes())
            self._memory[name] = obj

            # Ensure metadata is loaded too.
            if name not in self._metadata:
                self._load_single_metadata(name)

            logger.info("Artifact '%s' lazily loaded for flow %s", name, self.flow_id)
            return obj

    def delete(self, name: str) -> bool:
        """Delete a single artifact from memory and disk."""
        with self._lock:
            self._memory.pop(name, None)
            self._metadata.pop(name, None)
            artifact_dir = self._artifact_dir(name)
            if artifact_dir.exists():
                shutil.rmtree(artifact_dir)
                logger.info("Artifact '%s' deleted for flow %s", name, self.flow_id)
                return True
            return False

    def list_artifacts(self) -> list[ArtifactInfo]:
        """Return metadata for every known artifact (persisted + memory-only)."""
        with self._lock:
            self._load_metadata_index()  # refresh from disk
            result: list[ArtifactInfo] = []
            seen: set[str] = set()

            for name, meta in self._metadata.items():
                seen.add(name)
                result.append(
                    ArtifactInfo(
                        name=meta.name,
                        flow_id=meta.flow_id,
                        object_type=meta.object_type,
                        source_node_id=meta.source_node_id,
                        created_at=meta.created_at,
                        updated_at=meta.updated_at,
                        size_bytes=meta.size_bytes,
                        persisted=meta.persisted,
                        in_memory=name in self._memory,
                    )
                )

            # Include memory-only artifacts that haven't been persisted.
            for name in self._memory:
                if name not in seen:
                    now = datetime.utcnow()
                    result.append(
                        ArtifactInfo(
                            name=name,
                            flow_id=self.flow_id,
                            object_type=f"{type(self._memory[name]).__module__}.{type(self._memory[name]).__qualname__}",
                            created_at=now,
                            updated_at=now,
                            persisted=False,
                            in_memory=True,
                        )
                    )
            return result

    def get_persistence_stats(self) -> PersistenceStats:
        """Aggregate persistence statistics."""
        artifacts = self.list_artifacts()
        persisted = [a for a in artifacts if a.persisted]
        memory_only = [a for a in artifacts if not a.persisted]
        return PersistenceStats(
            flow_id=self.flow_id,
            total_artifacts=len(artifacts),
            persisted_count=len(persisted),
            memory_only_count=len(memory_only),
            total_disk_bytes=sum(a.size_bytes for a in persisted),
            recovery_mode=self.recovery_mode,
        )

    def exists(self, name: str) -> bool:
        """Check whether an artifact exists (in memory or on disk)."""
        if name in self._memory:
            return True
        return self._data_path(name).exists()

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def recover(self, mode: RecoveryMode | None = None) -> RecoveryStatus:
        """Trigger recovery according to *mode* (defaults to store's mode)."""
        mode = mode or self.recovery_mode
        if mode == RecoveryMode.NONE:
            self._clear_persisted()
            return RecoveryStatus(flow_id=self.flow_id, recovery_mode=mode)

        if mode == RecoveryMode.EAGER:
            return self._recover_all()

        # LAZY: just confirm what's available, don't load into memory.
        return self._scan_recoverable()

    def _recover_all(self) -> RecoveryStatus:
        """Load every persisted artifact into memory."""
        status = RecoveryStatus(flow_id=self.flow_id, recovery_mode=RecoveryMode.EAGER)
        if not self._flow_dir.exists():
            return status

        for artifact_dir in self._flow_dir.iterdir():
            if not artifact_dir.is_dir():
                continue
            name = artifact_dir.name
            data_path = artifact_dir / DATA_FILENAME
            if not data_path.exists():
                continue
            try:
                obj = cloudpickle.loads(data_path.read_bytes())
                self._memory[name] = obj
                self._load_single_metadata(name)
                status.recovered_count += 1
                status.recovered_artifacts.append(name)
            except Exception:
                logger.exception("Failed to recover artifact '%s' for flow %s", name, self.flow_id)
                status.failed_count += 1
                status.failed_artifacts.append(name)
        return status

    def _scan_recoverable(self) -> RecoveryStatus:
        """Scan disk for recoverable artifacts without loading them."""
        status = RecoveryStatus(flow_id=self.flow_id, recovery_mode=RecoveryMode.LAZY)
        if not self._flow_dir.exists():
            return status

        for artifact_dir in self._flow_dir.iterdir():
            if not artifact_dir.is_dir():
                continue
            name = artifact_dir.name
            data_path = artifact_dir / DATA_FILENAME
            if data_path.exists():
                if name in self._memory:
                    status.skipped_count += 1
                else:
                    status.recovered_count += 1
                    status.recovered_artifacts.append(name)
        return status

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self, max_age_hours: float | None = None) -> CleanupResult:
        """Remove old artifacts.  If *max_age_hours* is ``None``, remove all."""
        result = CleanupResult(flow_id=self.flow_id)
        if not self._flow_dir.exists():
            return result

        cutoff = time.time() - (max_age_hours * 3600) if max_age_hours is not None else None

        for artifact_dir in list(self._flow_dir.iterdir()):
            if not artifact_dir.is_dir():
                continue
            name = artifact_dir.name
            data_path = artifact_dir / DATA_FILENAME
            if not data_path.exists():
                continue

            if cutoff is not None:
                mtime = data_path.stat().st_mtime
                if mtime >= cutoff:
                    continue  # too recent, skip

            size = data_path.stat().st_size
            shutil.rmtree(artifact_dir)
            self._memory.pop(name, None)
            self._metadata.pop(name, None)
            result.deleted_count += 1
            result.deleted_artifacts.append(name)
            result.freed_bytes += size

        logger.info(
            "Cleanup for flow %s: deleted %d artifacts, freed %d bytes",
            self.flow_id,
            result.deleted_count,
            result.freed_bytes,
        )
        return result

    def clear(self) -> CleanupResult:
        """Delete all artifacts (memory + disk) for this flow."""
        return self.cleanup(max_age_hours=None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_metadata_index(self) -> None:
        """Scan the flow directory and load all meta.json files."""
        if not self._flow_dir.exists():
            return
        for artifact_dir in self._flow_dir.iterdir():
            if artifact_dir.is_dir():
                self._load_single_metadata(artifact_dir.name)

    def _load_single_metadata(self, name: str) -> None:
        meta_path = self._meta_path(name)
        if meta_path.exists():
            try:
                raw = json.loads(meta_path.read_text(encoding="utf-8"))
                self._metadata[name] = ArtifactMetadata.model_validate(raw)
            except Exception:
                logger.warning("Could not load metadata for artifact '%s'", name)

    def _clear_persisted(self) -> None:
        """Remove all persisted files for this flow (NONE mode)."""
        if self._flow_dir.exists():
            shutil.rmtree(self._flow_dir)
            self._flow_dir.mkdir(parents=True, exist_ok=True)
        self._memory.clear()
        self._metadata.clear()


class ArtifactStoreManager:
    """Manages per-flow ArtifactStore instances.

    Acts as a registry so callers can ``get_store(flow_id)`` without caring
    about instantiation or recovery mode.
    """

    def __init__(self, default_recovery_mode: RecoveryMode = RecoveryMode.LAZY):
        self.default_recovery_mode = default_recovery_mode
        self._stores: dict[int, ArtifactStore] = {}
        self._lock = threading.Lock()

    def get_store(
        self,
        flow_id: int,
        recovery_mode: RecoveryMode | None = None,
    ) -> ArtifactStore:
        """Return (or create) the ArtifactStore for *flow_id*."""
        with self._lock:
            if flow_id not in self._stores:
                mode = recovery_mode or self.default_recovery_mode
                self._stores[flow_id] = ArtifactStore(flow_id, recovery_mode=mode)
            return self._stores[flow_id]

    def remove_store(self, flow_id: int) -> None:
        """Evict a store from the manager (does NOT delete persisted data)."""
        with self._lock:
            self._stores.pop(flow_id, None)

    def list_flow_ids(self) -> list[int]:
        """Return flow IDs that have persisted artifacts on disk."""
        result: list[int] = []
        artifacts_dir = storage.artifacts_directory
        if not artifacts_dir.exists():
            return result
        for child in artifacts_dir.iterdir():
            if child.is_dir():
                try:
                    result.append(int(child.name))
                except ValueError:
                    continue
        return result


# Global singleton – imported by routes and other modules.
artifact_manager = ArtifactStoreManager()
