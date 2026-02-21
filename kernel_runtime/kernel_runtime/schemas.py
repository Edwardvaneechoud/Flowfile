"""Pydantic schemas for kernel runtime artifact responses."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ArtifactInfo(BaseModel):
    """Metadata for a flow-scoped (local) artifact."""

    name: str
    type_name: str
    module: str
    node_id: int
    flow_id: int
    created_at: datetime
    size_bytes: int
    persisted: bool
    persist_pending: bool = False


class GlobalArtifactInfo(BaseModel):
    """Metadata for a globally persisted artifact (from the catalog)."""

    id: int
    name: str
    namespace_id: int | None = None
    version: int
    status: str
    source_registration_id: int
    python_type: str | None = None
    serialization_format: str
    size_bytes: int | None = None
    created_at: datetime
    tags: list[str] = Field(default_factory=list)
    owner_id: int
