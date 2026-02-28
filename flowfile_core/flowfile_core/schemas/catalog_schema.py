"""Pydantic schemas for the Flow Catalog system.

Covers namespaces (Unity Catalog-style hierarchy), flow registrations,
run history, favorites and follows.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

# ==================== Namespace Schemas ====================


class NamespaceCreate(BaseModel):
    name: str
    parent_id: int | None = None
    description: str | None = None


class NamespaceUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


class NamespaceOut(BaseModel):
    id: int
    name: str
    parent_id: int | None = None
    level: int
    description: str | None = None
    owner_id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class NamespaceTree(NamespaceOut):
    """Recursive tree node – children are nested schemas of the same hierarchy."""

    children: list["NamespaceTree"] = Field(default_factory=list)
    flows: list["FlowRegistrationOut"] = Field(default_factory=list)
    artifacts: list["GlobalArtifactOut"] = Field(default_factory=list)
    tables: list["CatalogTableOut"] = Field(default_factory=list)


# ==================== Flow Registration Schemas ====================


class FlowRegistrationCreate(BaseModel):
    name: str
    description: str | None = None
    flow_path: str
    namespace_id: int | None = None


class FlowRegistrationUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    namespace_id: int | None = None


class FlowRegistrationOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    flow_path: str
    namespace_id: int | None = None
    owner_id: int
    created_at: datetime
    updated_at: datetime
    is_favorite: bool = False
    is_following: bool = False
    run_count: int = 0
    last_run_at: datetime | None = None
    last_run_success: bool | None = None
    file_exists: bool = True
    artifact_count: int = 0

    model_config = {"from_attributes": True}


# ==================== Flow Run Schemas ====================


class FlowRunOut(BaseModel):
    id: int
    registration_id: int | None = None
    flow_name: str
    flow_path: str | None = None
    user_id: int
    started_at: datetime
    ended_at: datetime | None = None
    success: bool | None = None
    nodes_completed: int = 0
    number_of_nodes: int = 0
    duration_seconds: float | None = None
    run_type: str = "full_run"
    has_snapshot: bool = False

    model_config = {"from_attributes": True}


class FlowRunDetail(FlowRunOut):
    """Extended run detail that includes the YAML flow snapshot and node results."""

    flow_snapshot: str | None = None
    node_results_json: str | None = None


# ==================== Favorite / Follow Schemas ====================


class FavoriteOut(BaseModel):
    id: int
    user_id: int
    registration_id: int
    created_at: datetime

    model_config = {"from_attributes": True}


class FollowOut(BaseModel):
    id: int
    user_id: int
    registration_id: int
    created_at: datetime

    model_config = {"from_attributes": True}


# ==================== Global Artifact Schemas ====================


class GlobalArtifactOut(BaseModel):
    """Read-only representation of a global artifact for catalog display."""

    id: int
    name: str
    version: int
    status: str  # "active", "deleted"
    description: str | None = None
    python_type: str | None = None
    python_module: str | None = None
    serialization_format: str | None = None  # "pickle", "joblib", "parquet"
    size_bytes: int | None = None
    sha256: str | None = None
    tags: list[str] = Field(default_factory=list)
    namespace_id: int | None = None
    source_registration_id: int | None = None
    source_flow_id: int | None = None
    source_node_id: int | None = None
    owner_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


# ==================== Catalog Table Schemas ====================


class CatalogTableCreate(BaseModel):
    name: str
    namespace_id: int | None = None
    description: str | None = None


class CatalogTableUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    namespace_id: int | None = None


class ColumnSchema(BaseModel):
    name: str
    dtype: str


class CatalogTableOut(BaseModel):
    id: int
    name: str
    namespace_id: int | None = None
    description: str | None = None
    owner_id: int
    file_path: str
    schema_columns: list[ColumnSchema] = Field(default_factory=list)
    row_count: int | None = None
    column_count: int | None = None
    size_bytes: int | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class CatalogTablePreview(BaseModel):
    """Preview data: column names + rows of values."""

    columns: list[str]
    dtypes: list[str]
    rows: list[list] = Field(default_factory=list)
    total_rows: int = 0


# ==================== Catalog Overview ====================


class CatalogStats(BaseModel):
    total_namespaces: int = 0
    total_flows: int = 0
    total_runs: int = 0
    total_favorites: int = 0
    total_artifacts: int = 0
    total_tables: int = 0
    recent_runs: list[FlowRunOut] = Field(default_factory=list)
    favorite_flows: list[FlowRegistrationOut] = Field(default_factory=list)


# Rebuild forward-referenced models
NamespaceTree.model_rebuild()
