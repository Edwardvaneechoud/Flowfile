"""Pydantic models for the artifact persistence system."""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, Field


class RecoveryMode(str, enum.Enum):
    """Controls how artifact recovery behaves on startup."""

    LAZY = "lazy"  # Load from disk on first access (default)
    EAGER = "eager"  # Pre-load all artifacts into memory on start
    NONE = "none"  # No recovery, start fresh


class ArtifactMetadata(BaseModel):
    """Metadata stored alongside each persisted artifact."""

    name: str
    flow_id: int
    object_type: str = ""  # e.g. "sklearn.linear_model.LogisticRegression"
    source_node_id: int | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    size_bytes: int = 0
    checksum: str = ""  # SHA-256 hex digest of serialized data
    persisted: bool = True


class ArtifactInfo(BaseModel):
    """Public-facing artifact information returned by the API."""

    name: str
    flow_id: int
    object_type: str = ""
    source_node_id: int | None = None
    created_at: datetime
    updated_at: datetime
    size_bytes: int = 0
    persisted: bool = True
    in_memory: bool = False


class PersistenceStats(BaseModel):
    """Aggregate persistence statistics for a flow's artifacts."""

    flow_id: int
    total_artifacts: int = 0
    persisted_count: int = 0
    memory_only_count: int = 0
    total_disk_bytes: int = 0
    recovery_mode: RecoveryMode = RecoveryMode.LAZY


class RecoveryStatus(BaseModel):
    """Status of an artifact recovery operation."""

    flow_id: int
    recovered_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    recovered_artifacts: list[str] = Field(default_factory=list)
    failed_artifacts: list[str] = Field(default_factory=list)
    recovery_mode: RecoveryMode = RecoveryMode.LAZY


class CleanupResult(BaseModel):
    """Result of an artifact cleanup operation."""

    flow_id: int
    deleted_count: int = 0
    deleted_artifacts: list[str] = Field(default_factory=list)
    freed_bytes: int = 0
