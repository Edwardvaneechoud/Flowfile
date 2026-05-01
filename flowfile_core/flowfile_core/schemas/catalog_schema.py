"""Pydantic schemas for the Catalog system.

Covers namespaces (Unity Catalog-style hierarchy), flow registrations,
run history, favorites and follows.
"""

from datetime import datetime
from typing import Any, Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field

from shared.delta_models import DeltaVersionCommit as DeltaVersionCommit  # noqa: F401

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
    visualizations: list["VisualizationOut"] = Field(default_factory=list)


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
    flow_uuid: str | None = None
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


class VirtualFlowTableCreate(BaseModel):
    name: str
    namespace_id: int | None = None
    description: str | None = None
    producer_registration_id: int


class VirtualFlowTableUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    namespace_id: int | None = None
    producer_registration_id: int | None = None


class QueryVirtualTableCreate(BaseModel):
    name: str
    namespace_id: int | None = None
    description: str | None = None
    sql_query: str


class QueryVirtualTableUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    namespace_id: int | None = None
    sql_query: str | None = None


class ColumnSchema(BaseModel):
    name: str
    dtype: str


class TableWriteMetadata(TypedDict, total=False):
    schema: list[dict[str, str]]
    row_count: int
    column_count: int
    size_bytes: int


class CatalogTableOut(BaseModel):
    id: int
    name: str
    namespace_id: int | None = None
    namespace_name: str | None = None
    full_table_name: str | None = None
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
    table_type: str = "physical"
    producer_registration_id: int | None = None
    producer_registration_name: str | None = None
    is_optimized: bool | None = None
    laziness_blockers: list[str] | None = None
    sql_query: str | None = None
    polars_plan: str | None = None
    source_table_versions: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FlowSummary(BaseModel):
    """Lightweight reference to a registered flow."""

    id: int
    name: str

    model_config = ConfigDict(from_attributes=True)


class ResolveTableCandidate(BaseModel):
    id: int
    name: str
    namespace_id: int | None = None
    namespace_name: str | None = None


class ResolveTableResult(BaseModel):
    """Response body for ``GET /catalog/tables/resolve``.

    When a bare reference matches multiple rows the resolver soft-picks the
    deterministic candidate and reports the full list under ``warnings``."""

    table: CatalogTableOut
    warnings: list[ResolveTableCandidate] = Field(default_factory=list)


class CatalogTablePreview(BaseModel):
    """Preview data: column names + rows of values."""

    columns: list[str]
    dtypes: list[str]
    rows: list[list] = Field(default_factory=list)
    total_rows: int = 0


class DeltaTableHistory(BaseModel):
    """Version history of a Delta table."""

    current_version: int
    history: list[DeltaVersionCommit] = Field(default_factory=list)


# ==================== SQL Query Schemas ====================


class SqlQueryRequest(BaseModel):
    """Request to execute a SQL query against catalog Delta tables."""

    query: str
    max_rows: int = 10_000


class SqlQueryResult(BaseModel):
    """Result of a SQL query execution."""

    columns: list[str] = Field(default_factory=list)
    dtypes: list[str] = Field(default_factory=list)
    rows: list[list] = Field(default_factory=list)
    total_rows: int = 0
    truncated: bool = False
    execution_time_ms: float = 0.0
    used_tables: list[str] = Field(default_factory=list)
    error: str | None = None


class SaveQueryAsFlowRequest(BaseModel):
    """Request to save a SQL query as a registered flow."""

    query: str
    name: str
    namespace_id: int | None = None
    description: str | None = None
    used_tables: list[str] = Field(default_factory=list)


# ==================== Visualization Schemas ====================


class VisualizationCreate(BaseModel):
    """Create a viz. The source must be either a catalog table or an inline
    SQL query — set ``source_type`` accordingly. ``namespace_id`` defaults to
    the parent table's namespace (when ``source_type="table"``) but can be
    overridden so a viz can live anywhere in the catalog tree."""

    name: str
    description: str | None = None
    chart_type: str | None = None
    # ``spec`` is the array of GraphicWalker IChart entries that
    # ``exportCode()`` returns — one per chart tab. A single-entry list is
    # the common case but multi-tab specs round-trip too.
    spec: list[dict]
    spec_gw_version: str | None = None
    source_type: Literal["table", "sql"] = "table"
    catalog_table_id: int | None = None
    sql_query: str | None = None
    namespace_id: int | None = None
    thumbnail_data_url: str | None = None


class VisualizationUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    chart_type: str | None = None
    spec: list[dict] | None = None
    spec_gw_version: str | None = None
    namespace_id: int | None = None
    sql_query: str | None = None
    catalog_table_id: int | None = None
    thumbnail_data_url: str | None = None


class VisualizationOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    chart_type: str | None = None
    spec: list[dict] = Field(default_factory=list)
    spec_gw_version: str | None = None
    source_type: Literal["table", "sql"]
    catalog_table_id: int | None = None
    sql_query: str | None = None
    namespace_id: int | None = None
    thumbnail_data_url: str | None = None
    created_by: int | None = None
    created_at: datetime
    updated_at: datetime
    # Enrichment so the viewer can show "ns.tablename" without a second
    # round-trip. None for SQL-source viz or when the parent table was deleted.
    table_name: str | None = None
    table_namespace_name: str | None = None
    table_full_name: str | None = None
    table_type: str | None = None
    namespace_name: str | None = None

    model_config = ConfigDict(from_attributes=True)


class VizSourceDescriptor(BaseModel):
    """Frontend-supplied identifier for the viz source.

    For saved viz this is implied by the URL (table_id); ad-hoc compute
    explicitly carries the descriptor so the editor can preview before saving.
    """

    source_type: Literal["table", "sql"]
    table_id: int | None = None
    sql_query: str | None = None


class VisualizationComputeRequest(BaseModel):
    payload: dict
    max_rows: int = 100_000


class VisualizationSavedComputeRequest(BaseModel):
    """Body for the saved-viz compute route.

    When ``payload`` is set, the worker runs ``polars_gw.execute_workflow``
    with that GraphicWalker IDataQueryPayload against the viz's stored
    source — this is the path GW's ``computation`` callback drives so every
    aggregation pushes down to the worker. When ``payload`` is omitted the
    server falls back to a "raw select all" so legacy callers (and the
    initial sample-fetch path) keep working.
    """

    payload: dict | None = None
    max_rows: int | None = None


class VisualizationAdHocComputeRequest(BaseModel):
    source: VizSourceDescriptor
    payload: dict
    max_rows: int = 100_000


class VisualizationFieldsRequest(BaseModel):
    source: VizSourceDescriptor


class VisualizationComputeResponse(BaseModel):
    rows: list[dict] = Field(default_factory=list)
    total_rows: int = 0
    truncated: bool = False
    elapsed_ms: float = 0.0
    cache_hit: bool = False
    error: str | None = None


class VisualizationFieldsResponse(BaseModel):
    fields: list[dict] = Field(default_factory=list)
    cache_hit: bool = False
    error: str | None = None


class ColumnStatsResponse(BaseModel):
    """Distinct values (capped) plus min/max for a single column on a catalog table.

    Used by the dashboard filter UI to pre-populate categorical option
    lists and numeric range inputs. ``truncated`` is true when there are
    more distinct values than ``values`` returns; ``min`` / ``max`` are
    only set for numeric and temporal columns.
    """

    dtype: str = ""
    values: list = Field(default_factory=list)
    truncated: bool = False
    distinct_count: int | None = None
    min: object | None = None
    max: object | None = None
    cache_hit: bool = False
    error: str | None = None


# ==================== Dashboard Schemas ====================


class DashboardTile(BaseModel):
    """One tile on a dashboard canvas.

    ``id`` is a client-generated UUID, stable across saves so the frontend
    can preserve component state on layout updates. ``type`` discriminates
    tile kinds: ``"viz"`` renders a saved visualization (uses ``viz_id`` /
    ``chart_index``); ``"text"`` renders user-authored Markdown from
    ``text_md``. Type-irrelevant fields are simply ignored.
    """

    id: str
    type: Literal["viz", "text"] = "viz"
    viz_id: int | None = None
    chart_index: int = 0
    text_md: str | None = None
    x: int
    y: int
    w: int
    h: int


class DashboardGrid(BaseModel):
    cols: int = 12
    row_height: int = 40
    version: int = 1


class DashboardFilter(BaseModel):
    """Dashboard-level filter applied to one or more tiles.

    ``datasource_id`` (optional) binds the filter to a ``CatalogTable``;
    only tiles whose visualization reads from the same table are eligible
    targets, and the field picker is populated from that table's
    ``schema_columns``. Filters without ``datasource_id`` are legacy /
    "untied" — they apply on field-name match alone.

    ``state`` carries widget-specific values (selected categorical values,
    a date range, a numeric range). The frontend injects these as a
    ``filter`` workflow step at the front of every targeted tile's GW
    payload before forwarding to ``/visualizations/{viz_id}/compute``.
    """

    id: str
    field_name: str
    label: str | None = None
    kind: Literal["categorical", "date_range", "numeric_range"]
    state: dict[str, Any] = Field(default_factory=dict)
    target: Literal["all", "tiles"] = "all"
    target_tile_ids: list[str] = Field(default_factory=list)
    datasource_id: int | None = None


class DashboardLayout(BaseModel):
    tiles: list[DashboardTile] = Field(default_factory=list)
    grid: DashboardGrid = Field(default_factory=DashboardGrid)
    filters: list[DashboardFilter] = Field(default_factory=list)


class DashboardCreate(BaseModel):
    name: str
    description: str | None = None
    namespace_id: int | None = None
    layout: DashboardLayout = Field(default_factory=DashboardLayout)


class DashboardUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    namespace_id: int | None = None
    layout: DashboardLayout | None = None


class DashboardOut(BaseModel):
    id: int
    name: str
    description: str | None = None
    layout: DashboardLayout
    layout_version: int
    namespace_id: int | None = None
    namespace_name: str | None = None
    created_by: int | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


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
    trigger_namespace_id: int | None = None
    trigger_namespace_name: str | None = None
    trigger_full_table_name: str | None = None
    trigger_table_ids: list[int] = Field(default_factory=list)
    trigger_table_names: list[str] = Field(default_factory=list)
    trigger_full_table_names: list[str] = Field(default_factory=list)
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
    total_virtual_tables: int = 0
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
