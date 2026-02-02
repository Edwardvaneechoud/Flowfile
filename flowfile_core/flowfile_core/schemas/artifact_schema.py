"""Pydantic schemas for the Global Artifact Registry.

Covers creation (publish), retrieval, listing, search, and deletion of
persisted artifacts in the FlowFile Catalog.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


# ==================== Publish (upload) ====================


class ArtifactPublishRequest(BaseModel):
    """Metadata sent alongside the binary blob when publishing a global artifact.

    The blob itself is uploaded as a multipart file — this model describes the
    JSON metadata part of the multipart request.
    """
    name: str
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    namespace_id: int | None = None

    # Provenance (populated by the kernel / Core automatically)
    source_flow_id: int | None = None
    source_node_id: int | None = None
    source_kernel_id: str | None = None

    # Python type information (set by the kernel serializer)
    python_type: str = ""
    python_module: str = ""
    serialization_format: str = "pickle"


# ==================== Output ====================


class ArtifactOut(BaseModel):
    """Full artifact metadata returned from list / get / search endpoints."""
    id: int
    name: str
    namespace_id: int | None = None
    owner_id: int
    source_flow_id: int | None = None
    source_node_id: int | None = None
    source_kernel_id: str | None = None
    python_type: str
    python_module: str
    serialization_format: str
    size_bytes: int
    sha256: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    version: int = 1
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ArtifactSummary(BaseModel):
    """Lightweight artifact reference for list views."""
    id: int
    name: str
    python_type: str
    serialization_format: str
    size_bytes: int
    version: int = 1
    owner_id: int
    created_at: datetime

    model_config = {"from_attributes": True}


# ==================== Update ====================


class ArtifactUpdate(BaseModel):
    """Mutable fields the user can change after publish."""
    name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    namespace_id: int | None = None


# ==================== Search / Filter ====================


class ArtifactSearchParams(BaseModel):
    """Query parameters for artifact search.  All fields are optional filters."""
    name_contains: str | None = None
    python_type: str | None = None
    serialization_format: str | None = None
    tag: str | None = None
    namespace_id: int | None = None
    owner_id: int | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


# ==================== Catalog Stats ====================


class ArtifactStats(BaseModel):
    """Summary statistics for the artifact catalog."""
    total_artifacts: int = 0
    total_size_bytes: int = 0
    format_counts: dict[str, int] = Field(default_factory=dict)
    recent_artifacts: list[ArtifactSummary] = Field(default_factory=list)
