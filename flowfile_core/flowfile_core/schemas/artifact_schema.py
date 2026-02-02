"""Pydantic schemas for the Global Artifacts system.

Covers prepare/finalize upload workflow, artifact metadata responses,
and list/search endpoints.
"""

from datetime import datetime

from pydantic import BaseModel, Field


# ==================== Upload Workflow Schemas ====================


class PrepareUploadRequest(BaseModel):
    """Request to initiate an artifact upload."""
    name: str
    serialization_format: str  # parquet, joblib, pickle
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    namespace_id: int | None = None

    # Lineage (set by kernel)
    source_flow_id: int | None = None
    source_node_id: int | None = None
    source_kernel_id: str | None = None

    # Type info (set by kernel after serialization)
    python_type: str | None = None
    python_module: str | None = None


class PrepareUploadResponse(BaseModel):
    """Response with upload target information."""
    artifact_id: int
    version: int
    method: str           # "file" or "s3_presigned"
    path: str             # Where to write
    storage_key: str      # For finalize call


class FinalizeUploadRequest(BaseModel):
    """Request to finalize an upload after blob is written."""
    artifact_id: int
    storage_key: str
    sha256: str
    size_bytes: int


class FinalizeUploadResponse(BaseModel):
    """Response after successful finalization."""
    status: str = "ok"
    artifact_id: int
    version: int


# ==================== Artifact Output Schemas ====================


class ArtifactOut(BaseModel):
    """Full artifact metadata for API responses."""
    id: int
    name: str
    namespace_id: int | None = None
    version: int
    status: str

    owner_id: int
    source_flow_id: int | None = None
    source_node_id: int | None = None
    source_kernel_id: str | None = None

    python_type: str | None = None
    python_module: str | None = None
    serialization_format: str

    storage_key: str | None = None
    size_bytes: int | None = None
    sha256: str | None = None

    description: str | None = None
    tags: list[str] = Field(default_factory=list)

    created_at: datetime
    updated_at: datetime

    # Added for download
    download_source: dict | None = None  # {method, path}

    model_config = {"from_attributes": True}


class ArtifactListItem(BaseModel):
    """Lightweight artifact info for list endpoints."""
    id: int
    name: str
    version: int
    status: str
    python_type: str | None = None
    serialization_format: str
    size_bytes: int | None = None
    description: str | None = None
    created_at: datetime
    tags: list[str] = Field(default_factory=list)

    model_config = {"from_attributes": True}
