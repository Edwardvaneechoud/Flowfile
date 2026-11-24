// Mirrors flowfile_core/schemas/semantic_schema.py

export type SemanticFactStatus =
  | "proposed"
  | "reviewed"
  | "accepted"
  | "rejected"
  | "superseded"
  | "stale"
  | "retracted";

export type SemanticSourceKind =
  | "human"
  | "ai"
  | "inferred"
  | "catalog"
  | "import"
  | "system"
  | "other";

export interface SemanticFact {
  id: string;
  attribute: string;
  value: unknown;
  status: SemanticFactStatus;
  source: string;
  source_kind: SemanticSourceKind;
  via: string | null;
  confidence: number | null;
  rationale: string | null;
  created_by: string;
  updated_at: string;
  group: string | null;
}

export interface SemanticField {
  name: string;
  dtype: string | null;
  facts: Record<string, SemanticFact>;
  pending: SemanticFact[];
}

export interface SemanticMetric {
  key: string;
  name: string;
  table: string;
  table_id: number | null;
  expression: string | null;
  description: string | null;
  aggregate: string | null;
  filter: string | null;
  time_column: string | null;
  dimensions: string[];
  format: string | null;
  status: SemanticFactStatus;
  version: number | null;
  approved_by: string | null;
  approved_at: string | null;
  group: string | null;
  pending_group: string | null;
}

export interface SemanticJoin {
  key: string;
  name: string;
  from_table: string;
  to_table: string;
  from_table_id: number | null;
  to_table_id: number | null;
  from_columns: string[];
  to_columns: string[];
  description: string | null;
  status: SemanticFactStatus;
  group: string | null;
  layer: "schema" | "catalog";
}

export interface SemanticTable {
  table_id: number;
  key: string;
  name: string;
  description: string | null;
  fields: SemanticField[];
  metrics: SemanticMetric[];
  facts: Record<string, SemanticFact>;
  pending: SemanticFact[];
}

export interface SemanticProgress {
  accepted: number;
  pending: number;
  stale: number;
}

export type SemanticReviewMode = "solo" | "team";

export interface SemanticModel {
  namespace_id: number;
  level: number;
  key: string;
  name: string | null;
  description: string | null;
  ai_instructions: string | null;
  tables: SemanticTable[];
  joins: SemanticJoin[];
  progress: SemanticProgress;
  can_manage: boolean;
  review_mode: SemanticReviewMode;
}

export interface SemanticModelUpdate {
  name?: string | null;
  description?: string | null;
  ai_instructions?: string | null;
}

export interface FieldProposal {
  kind: "field";
  table_id: number;
  column: string;
  attribute: string;
  value: unknown;
}

export interface TableProposal {
  kind: "table";
  table_id: number;
  attribute: string;
  value: unknown;
}

export interface JoinProposal {
  kind: "join";
  name: string;
  from_table_id: number;
  to_table_id: number;
  from_columns: string[];
  to_columns: string[];
  description?: string | null;
}

export interface MetricProposal {
  kind: "metric";
  table_id: number;
  name: string;
  expression: string;
  description?: string | null;
  aggregate?: string | null;
  filter?: string | null;
  time_column?: string | null;
  dimensions?: string[] | null;
  format?: string | null;
  edit?: boolean;
}

export type SemanticProposal = FieldProposal | TableProposal | JoinProposal | MetricProposal;

export type SemanticReviewActionType = "accept" | "reject" | "retract" | "reopen";

export interface SemanticReviewAction {
  action: SemanticReviewActionType;
  ids?: string[] | null;
  group?: string | null;
  comment?: string | null;
}

export interface SemanticReviewResult {
  changed: SemanticFact[];
  errors: { id: string; message: string }[];
}

export interface SemanticReviewItem extends SemanticFact {
  subject: string;
  subject_kind: "model" | "dataset" | "field" | "relationship" | "metric" | "type";
  subject_label: string;
  table: string | null;
  table_id: number | null;
  column: string | null;
  group_facts: SemanticFact[];
}

export interface SemanticEvent {
  seq: number | null;
  id: string;
  ts: string;
  type: string;
  assertion_id: string;
  attribute: string | null;
  actor: string;
  via: string | null;
  comment: string | null;
  value: unknown;
}

export interface SemanticSyncResult {
  new: string[];
  changed: string[];
  moved: Record<string, string>;
  gone: string[];
  proposed: number;
  accepted: number;
  staled: number;
}

export interface SemanticModelSummary {
  accepted: number;
  pending: number;
  stale: number;
}
