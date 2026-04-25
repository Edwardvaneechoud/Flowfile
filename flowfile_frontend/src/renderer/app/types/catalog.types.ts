// Catalog TypeScript interfaces
// Maps to backend schemas in catalog_schema.py

// ============================================================================
// Namespace (Unity Catalog-style hierarchy)
// ============================================================================

export interface CatalogNamespace {
  id: number;
  name: string;
  parent_id: number | null;
  level: number; // 0=catalog, 1=schema
  description: string | null;
  owner_id: number;
  created_at: string;
  updated_at: string;
}

export interface NamespaceTree extends CatalogNamespace {
  children: NamespaceTree[];
  flows: FlowRegistration[];
  artifacts: GlobalArtifact[];
  tables: CatalogTable[];
}

export interface NamespaceCreate {
  name: string;
  parent_id?: number | null;
  description?: string | null;
}

export interface NamespaceUpdate {
  name?: string;
  description?: string;
}

// ============================================================================
// Flow Registration
// ============================================================================

export interface FlowRegistration {
  id: number;
  name: string;
  description: string | null;
  flow_path: string;
  namespace_id: number | null;
  owner_id: number;
  created_at: string;
  updated_at: string;
  is_favorite: boolean;
  is_following: boolean;
  run_count: number;
  last_run_at: string | null;
  last_run_success: boolean | null;
  file_exists: boolean;
  artifact_count: number;
  tables_produced: CatalogTableSummary[];
  tables_read: CatalogTableSummary[];
}

export interface FlowRegistrationCreate {
  name: string;
  description?: string | null;
  flow_path: string;
  namespace_id?: number | null;
}

export interface FlowRegistrationUpdate {
  name?: string;
  description?: string;
  namespace_id?: number | null;
}

// ============================================================================
// Flow Run
// ============================================================================

export interface FlowRun {
  id: number;
  registration_id: number | null;
  flow_name: string;
  flow_path: string | null;
  user_id: number;
  started_at: string;
  ended_at: string | null;
  success: boolean | null;
  nodes_completed: number;
  number_of_nodes: number;
  duration_seconds: number | null;
  run_type: "in_designer_run" | "scheduled" | "manual" | "on_demand";
  schedule_id: number | null;
  has_snapshot: boolean;
  has_log: boolean;
}

export interface PaginatedFlowRuns {
  items: FlowRun[];
  total: number;
  total_success: number;
  total_failed: number;
  total_running: number;
}

export interface FlowRunDetail extends FlowRun {
  flow_snapshot: string | null;
  node_results_json: string | null;
}

// ============================================================================
// Global Artifact
// ============================================================================

/** A global artifact stored in the catalog. */
export interface GlobalArtifact {
  id: number;
  name: string;
  version: number;
  status: string; // "active" | "deleted"
  description: string | null;
  python_type: string | null;
  python_module: string | null;
  serialization_format: string | null; // "pickle" | "joblib" | "parquet"
  size_bytes: number | null;
  sha256: string | null;
  tags: string[];
  namespace_id: number | null;
  source_registration_id: number | null;
  source_flow_id: number | null;
  source_node_id: number | null;
  owner_id: number | null;
  created_at: string | null;
  updated_at: string | null;
}

// ============================================================================
// Catalog Table
// ============================================================================

export interface ColumnSchema {
  name: string;
  dtype: string;
}

export interface CatalogTableSummary {
  id: number;
  name: string;
  namespace_id: number | null;
}

export interface FlowSummary {
  id: number;
  name: string;
}

export interface CatalogTable {
  id: number;
  name: string;
  namespace_id: number | null;
  namespace_name: string | null;
  full_table_name: string | null;
  description: string | null;
  owner_id: number;
  file_exists: boolean;
  is_favorite: boolean;
  schema_columns: ColumnSchema[];
  row_count: number | null;
  column_count: number | null;
  size_bytes: number | null;
  source_registration_id: number | null;
  source_registration_name: string | null;
  source_run_id: number | null;
  read_by_flows: FlowSummary[];
  table_type: "physical" | "virtual";
  producer_registration_id: number | null;
  producer_registration_name: string | null;
  is_optimized: boolean | null;
  laziness_blockers: string[] | null;
  sql_query: string | null;
  polars_plan: string | null;
  source_table_versions: string | null;
  created_at: string;
  updated_at: string;
}

export interface CatalogTableCreate {
  name: string;
  file_path: string;
  namespace_id?: number | null;
  description?: string | null;
}

export interface CatalogTableUpdate {
  name?: string;
  description?: string;
  namespace_id?: number | null;
}

export interface VirtualFlowTableCreate {
  name: string;
  namespace_id?: number | null;
  description?: string | null;
  producer_registration_id: number;
}

export interface VirtualFlowTableUpdate {
  name?: string;
  description?: string;
  namespace_id?: number | null;
  producer_registration_id?: number | null;
}

export interface QueryVirtualTableCreate {
  name: string;
  namespace_id?: number | null;
  description?: string | null;
  sql_query: string;
}

export interface CatalogTablePreview {
  columns: string[];
  dtypes: string[];
  rows: any[][];
  total_rows: number;
}

// ============================================================================
// Delta Version History
// ============================================================================

export interface DeltaVersionCommit {
  version: number;
  timestamp: string | null;
  operation: string | null;
  parameters: Record<string, any> | null;
}

export interface DeltaTableHistory {
  current_version: number;
  history: DeltaVersionCommit[];
}

// ============================================================================
// Schedule
// ============================================================================

export interface FlowSchedule {
  id: number;
  registration_id: number;
  owner_id: number;
  enabled: boolean;
  name: string | null;
  description: string | null;
  schedule_type: "interval" | "table_trigger" | "table_set_trigger";
  interval_seconds: number | null;
  trigger_table_id: number | null;
  trigger_table_name: string | null;
  trigger_namespace_id: number | null;
  trigger_namespace_name: string | null;
  trigger_full_table_name: string | null;
  trigger_table_ids: number[];
  trigger_table_names: string[];
  trigger_full_table_names: string[];
  last_triggered_at: string | null;
  last_trigger_table_updated_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface FlowScheduleCreate {
  registration_id: number;
  schedule_type: "interval" | "table_trigger" | "table_set_trigger";
  interval_seconds?: number | null;
  trigger_table_id?: number | null;
  trigger_table_ids?: number[] | null;
  enabled?: boolean;
  name?: string | null;
  description?: string | null;
}

export interface FlowScheduleUpdate {
  enabled?: boolean;
  interval_seconds?: number | null;
  name?: string | null;
  description?: string | null;
}

// ============================================================================
// Active Flow Run
// ============================================================================

export interface ActiveFlowRun {
  id: number;
  registration_id: number | null;
  flow_name: string;
  flow_path: string | null;
  user_id: number;
  started_at: string;
  nodes_completed: number;
  number_of_nodes: number;
  run_type: "in_designer_run" | "scheduled" | "manual" | "on_demand";
}

// ============================================================================
// Catalog Stats
// ============================================================================

export interface CatalogStats {
  total_namespaces: number;
  total_flows: number;
  total_runs: number;
  total_favorites: number;
  total_table_favorites: number;
  total_artifacts: number;
  total_tables: number;
  total_virtual_tables: number;
  total_schedules: number;
  recent_runs: FlowRun[];
  favorite_flows: FlowRegistration[];
  favorite_tables: CatalogTable[];
  active_runs: ActiveFlowRun[];
}

// ============================================================================
// Scheduler Status
// ============================================================================

export interface SchedulerStatus {
  active: boolean;
  holder_id?: string;
  started_at?: string;
  heartbeat_at?: string;
  is_embedded?: boolean;
}

// ============================================================================
// View state helpers
// ============================================================================

export type CatalogTab =
  | "catalog"
  | "favorites"
  | "following"
  | "runs"
  | "schedules"
  | "sql"
  | "visualizations";

// ============================================================================
// SQL Query types
// ============================================================================

export interface SqlQueryResult {
  columns: string[];
  dtypes: string[];
  rows: any[][];
  total_rows: number;
  truncated: boolean;
  execution_time_ms: number;
  used_tables: string[];
  error: string | null;
}

// ============================================================================
// Visualizations
// ============================================================================

export interface CatalogVisualization {
  id: number;
  catalog_table_id: number;
  name: string;
  chart_type: string | null;
  spec: Record<string, any>;
  spec_gw_version: string | null;
  created_by: number | null;
  created_at: string;
  updated_at: string;
}

export interface VisualizationCreatePayload {
  name: string;
  chart_type?: string | null;
  spec: Record<string, any>;
  spec_gw_version?: string | null;
}

export interface VisualizationUpdatePayload {
  name?: string;
  chart_type?: string | null;
  spec?: Record<string, any>;
  spec_gw_version?: string | null;
}

/** Source descriptor sent to the ad-hoc compute and fields endpoints. */
export interface VizSourceDescriptor {
  source_type: "table" | "sql";
  table_id?: number | null;
  sql_query?: string | null;
}

export interface VisualizationComputeResponse {
  rows: Record<string, any>[];
  total_rows: number;
  truncated: boolean;
  elapsed_ms: number;
  cache_hit: boolean;
  error: string | null;
}

export interface VisualizationFieldsResponse {
  fields: Record<string, any>[];
  cache_hit: boolean;
  error: string | null;
}

export interface VisualizationLibraryItem {
  id: number;
  catalog_table_id: number;
  name: string;
  chart_type: string | null;
  spec_gw_version: string | null;
  created_by: number | null;
  created_at: string;
  updated_at: string;
  table_name: string;
  table_namespace_name: string | null;
  table_full_name: string;
  table_type: string;
}
