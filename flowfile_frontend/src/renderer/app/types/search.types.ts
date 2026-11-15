// Mirror of core's flowfile_core/search/models.py (GET /search).

export type SearchKind =
  | "namespace"
  | "table"
  | "column"
  | "flow"
  | "schedule"
  | "notebook"
  | "visualization"
  | "dashboard"
  | "artifact"
  | "kernel"
  | "custom_node";

export type SearchDetailValue = string | number | boolean | null;

export interface SearchRef {
  kind: SearchKind;
  id: number | string;
  name: string;
  uri: string;
}

export interface SearchMatch {
  field: string;
  value: string;
}

export interface SearchHit {
  kind: SearchKind;
  id: number | string;
  name: string;
  qualified_name: string | null;
  namespace_id: number | null;
  path: string[];
  description: string | null;
  subtitle: string | null;
  parent: SearchRef | null;
  matches: SearchMatch[];
  score: number;
  uri: string;
  updated_at: string | null;
  details: Record<string, SearchDetailValue>;
}

export interface SearchResponse {
  query: string;
  tokens: string[];
  kinds: SearchKind[];
  hits: SearchHit[];
  counts: Partial<Record<SearchKind, number>>;
  truncated: boolean;
  /** The kinds whose candidates were cut, so some of their matches may be missing. */
  truncated_kinds: SearchKind[];
}
