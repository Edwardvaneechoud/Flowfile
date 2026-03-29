"""Pydantic schemas for the Catalog system.

Covers namespaces (Unity Catalog-style hierarchy), flow registrations,
run history, favorites and follows.
"""

from datetime import datetime
from typing import Literal

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
    tables_produced: list["CatalogTableSummary"] = Field(default_factory=list)
    tables_read: list["CatalogTableSummary"] = Field(default_factory=list)

    model_config = {"from_attributes": True}


class CatalogTableSummary(BaseModel):
    """Lightweight reference to a catalog table (used in flow detail views)."""

    id: int
    name: str
    namespace_id: int | None = None

    model_config = ConfigDict(from_attributes=True)


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
    run_type: Literal["in_designer_run", "scheduled", "manual", "on_demand"] = "in_designer_run"
    schedule_id: int | None = None
    has_snapshot: bool = False
    has_log: bool = False

    model_config = {"from_attributes": True}


class PaginatedFlowRuns(BaseModel):
    """Paginated list of flow runs with total count for pagination controls."""

    items: list[FlowRunOut] = Field(default_factory=list)
    total: int = 0
    total_success: int = 0
    total_failed: int = 0
    total_running: int = 0


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
    file_path: str
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
    file_exists: bool = True
    is_favorite: bool = False
    schema_columns: list[ColumnSchema] = Field(default_factory=list)
    row_count: int | None = None
    column_count: int | None = None
    size_bytes: int | None = None
    source_registration_id: int | None = None
    source_registration_name: str | None = None
    source_run_id: int | None = None
    read_by_flows: list["FlowSummary"] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FlowSummary(BaseModel):
    """Lightweight reference to a registered flow."""

    id: int
    name: str

    model_config = ConfigDict(from_attributes=True)


class CatalogTablePreview(BaseModel):
    """Preview data: column names + rows of values."""

    columns: list[str]
    dtypes: list[str]
    rows: list[list] = Field(default_factory=list)
    total_rows: int = 0


class DeltaVersionCommit(BaseModel):
    """A single version entry from a Delta table's transaction log."""

    version: int
    timestamp: str | None = None
    operation: str | None = None
    parameters: dict | None = None


class DeltaTableHistory(BaseModel):
    """Version history of a Delta table."""

    current_version: int
    history: list[DeltaVersionCommit] = Field(default_factory=list)


# ==================== Catalog Overview ====================


class TableFavoriteOut(BaseModel):
    id: int
    user_id: int
    table_id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ==================== Schedule Schemas ====================


class FlowScheduleCreate(BaseModel):
    registration_id: int
    schedule_type: Literal["interval", "table_trigger", "table_set_trigger"]
    interval_seconds: int | None = None
    trigger_table_id: int | None = None
    trigger_table_ids: list[int] | None = None
    enabled: bool = True
    name: str | None = None
    description: str | None = None


class FlowScheduleUpdate(BaseModel):
    enabled: bool | None = None
    interval_seconds: int | None = None
    name: str | None = None
    description: str | None = None


class FlowScheduleOut(BaseModel):
    id: int
    registration_id: int
    owner_id: int
    enabled: bool
    name: str | None = None
    description: str | None = None
    schedule_type: Literal["interval", "table_trigger", "table_set_trigger"]
    interval_seconds: int | None = None
    trigger_table_id: int | None = None
    trigger_table_name: str | None = None
    trigger_table_ids: list[int] = Field(default_factory=list)
    trigger_table_names: list[str] = Field(default_factory=list)
    last_triggered_at: datetime | None = None
    last_trigger_table_updated_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ==================== Active Run Schemas ====================


class ActiveFlowRun(BaseModel):
    id: int
    registration_id: int | None = None
    flow_name: str
    flow_path: str | None = None
    user_id: int
    started_at: datetime
    nodes_completed: int = 0
    number_of_nodes: int = 0
    run_type: Literal["in_designer_run", "scheduled", "manual", "on_demand"] = "in_designer_run"

    model_config = ConfigDict(from_attributes=True)


# ==================== Catalog Overview ====================


class CatalogStats(BaseModel):
    total_namespaces: int = 0
    total_flows: int = 0
    total_runs: int = 0
    total_favorites: int = 0
    total_table_favorites: int = 0
    total_artifacts: int = 0
    total_tables: int = 0
    total_schedules: int = 0
    recent_runs: list[FlowRunOut] = Field(default_factory=list)
    favorite_flows: list[FlowRegistrationOut] = Field(default_factory=list)
    favorite_tables: list[CatalogTableOut] = Field(default_factory=list)
    active_runs: list[ActiveFlowRun] = Field(default_factory=list)


# ==================== Scheduler Status ====================


class SchedulerStatusOut(BaseModel):
    active: bool
    holder_id: str | None = None
    started_at: datetime | None = None
    heartbeat_at: datetime | None = None
    is_embedded: bool | None = None


# Rebuild forward-referenced models
CatalogTableOut.model_rebuild()
FlowRegistrationOut.model_rebuild()
NamespaceTree.model_rebuild()
CatalogStats.model_rebuild()
PaginatedFlowRuns.model_rebuild()
