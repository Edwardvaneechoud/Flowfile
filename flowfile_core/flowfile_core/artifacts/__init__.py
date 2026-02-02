"""Artifact persistence and recovery for FlowFile.

Quick start
-----------
>>> from flowfile_core.artifacts import artifact_manager
>>> store = artifact_manager.get_store(flow_id=1)
>>> store.publish("model", my_trained_model)
>>> recovered = store.read("model")
"""

from flowfile_core.artifacts.models import (
    ArtifactInfo,
    ArtifactMetadata,
    CleanupResult,
    PersistenceStats,
    RecoveryMode,
    RecoveryStatus,
)
from flowfile_core.artifacts.store import ArtifactStore, ArtifactStoreManager, artifact_manager

__all__ = [
    "ArtifactInfo",
    "ArtifactMetadata",
    "ArtifactStore",
    "ArtifactStoreManager",
    "CleanupResult",
    "PersistenceStats",
    "RecoveryMode",
    "RecoveryStatus",
    "artifact_manager",
]
