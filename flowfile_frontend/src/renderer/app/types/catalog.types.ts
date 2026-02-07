// Flow Catalog TypeScript interfaces
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
  run_type: string;
  has_snapshot: boolean;
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
// Catalog Stats
// ============================================================================

export interface CatalogStats {
  total_namespaces: number;
  total_flows: number;
  total_runs: number;
  total_favorites: number;
  total_artifacts: number;
  recent_runs: FlowRun[];
  favorite_flows: FlowRegistration[];
}

// ============================================================================
// View state helpers
// ============================================================================

export type CatalogTab = "catalog" | "favorites" | "following" | "runs";
