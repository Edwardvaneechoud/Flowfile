"""Pydantic schemas for the Global Artifacts system.

Covers artifact upload/download workflows, metadata, and list responses.
"""

from datetime import datetime

from pydantic import BaseModel, Field


# ==================== Upload Workflow Schemas ====================


class PrepareUploadRequest(BaseModel):
    """Request to initiate an artifact upload.

    Sent by kernel to Core to get upload target information.
    """
    name: str = Field(..., description="Artifact name (required)")
    source_registration_id: int = Field(
        ...,
        description="ID of the registered catalog flow that produces this artifact"
    )
    serialization_format: str = Field(
        ...,
        description="Serialization format: parquet, joblib, or pickle"
    )
    description: str | None = Field(
        None,
        description="Human-readable description of the artifact"
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Tags for categorization and search"
    )
    namespace_id: int | None = Field(
        None,
        description="Namespace (schema) ID. Defaults from source registration if not provided."
    )

    # Lineage information (set by kernel)
    source_flow_id: int | None = Field(
        None,
        description="ID of the flow that created this artifact"
    )
    source_node_id: int | None = Field(
        None,
        description="ID of the node that created this artifact"
    )
    source_kernel_id: str | None = Field(
        None,
        description="ID of the kernel that created this artifact"
    )

    # Type information (set by kernel after serialization)
    python_type: str | None = Field(
        None,
        description="Full Python type name (e.g., 'sklearn.ensemble.RandomForestClassifier')"
    )
    python_module: str | None = Field(
        None,
        description="Python module name (e.g., 'sklearn.ensemble')"
    )


class PrepareUploadResponse(BaseModel):
    """Response with upload target information.

    Returned by Core to kernel with details on where to write the blob.
    """
    artifact_id: int = Field(..., description="Database ID of the created artifact")
    version: int = Field(..., description="Version number for this artifact")
    method: str = Field(
        ...,
        description="Upload method: 'file' for local filesystem or 's3_presigned' for S3"
    )
    path: str = Field(
        ...,
        description="Local path or presigned URL where kernel should write"
    )
    storage_key: str = Field(
        ...,
        description="Storage key to include in finalize request"
    )


class FinalizeUploadRequest(BaseModel):
    """Request to finalize an upload after blob is written.

    Sent by kernel to Core after successfully writing the blob.
    """
    artifact_id: int = Field(..., description="Artifact ID from prepare response")
    storage_key: str = Field(..., description="Storage key from prepare response")
    sha256: str = Field(..., description="SHA-256 hash of the uploaded blob")
    size_bytes: int = Field(..., description="Size of the uploaded blob in bytes")


class FinalizeUploadResponse(BaseModel):
    """Response confirming upload finalization."""
    status: str = Field(default="ok", description="Status of the finalization")
    artifact_id: int = Field(..., description="Database ID of the artifact")
    version: int = Field(..., description="Version number of the artifact")


# ==================== Download Workflow Schemas ====================


class DownloadSource(BaseModel):
    """Download source information for kernel to fetch blob."""
    method: str = Field(
        ...,
        description="Download method: 'file' for local filesystem or 's3_presigned' for S3"
    )
    path: str = Field(
        ...,
        description="Local path or presigned URL where kernel should read from"
    )


# ==================== Artifact Metadata Schemas ====================


class ArtifactOut(BaseModel):
    """Full artifact metadata for API responses.

    Includes all fields for detailed artifact information and download source.
    """
    id: int
    name: str
    namespace_id: int | None = None
    version: int
    status: str

    # Ownership & Lineage
    owner_id: int
    source_registration_id: int
    source_flow_id: int | None = None
    source_node_id: int | None = None
    source_kernel_id: str | None = None

    # Serialization info
    python_type: str | None = None
    python_module: str | None = None
    serialization_format: str

    # Storage info
    storage_key: str | None = None
    size_bytes: int | None = None
    sha256: str | None = None

    # Metadata
    description: str | None = None
    tags: list[str] = Field(default_factory=list)

    # Timestamps
    created_at: datetime
    updated_at: datetime

    # Download information (populated when requested)
    download_source: DownloadSource | None = None

    model_config = {"from_attributes": True}


class ArtifactListItem(BaseModel):
    """Lightweight artifact info for list endpoints.

    Includes essential fields for browsing artifacts without full details.
    """
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

    model_config = {"from_attributes": True}


class ArtifactVersionInfo(BaseModel):
    """Version information for an artifact."""
    version: int
    id: int
    created_at: datetime
    size_bytes: int | None = None
    sha256: str | None = None


class ArtifactWithVersions(ArtifactOut):
    """Artifact with list of all available versions."""
    all_versions: list[ArtifactVersionInfo] = Field(default_factory=list)


# ==================== Search and Filter Schemas ====================


class ArtifactSearchParams(BaseModel):
    """Parameters for searching artifacts."""
    namespace_id: int | None = Field(None, description="Filter by namespace")
    tags: list[str] | None = Field(None, description="Filter by tags (AND logic)")
    name_contains: str | None = Field(None, description="Filter by name substring")
    python_type_contains: str | None = Field(None, description="Filter by Python type")
    limit: int = Field(100, ge=1, le=500, description="Maximum results to return")
    offset: int = Field(0, ge=0, description="Offset for pagination")


# ==================== Delete Response ====================


class ArtifactDeleteResponse(BaseModel):
    """Response confirming artifact deletion."""
    status: str = Field(default="deleted", description="Status of the deletion")
    artifact_id: int = Field(..., description="ID of the deleted artifact")
    versions_deleted: int = Field(
        default=1,
        description="Number of versions deleted (>1 if all versions deleted)"
    )
