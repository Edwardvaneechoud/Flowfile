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

/** A join measured against the data (POST …/semantic-model/joins/check). */
export type JoinCheckVerdict = "ok" | "reversed" | "many_to_many" | "unchecked";

export interface JoinCheck {
  key: string;
  name: string;
  verdict: JoinCheckVerdict;
  message: string;
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

export type MetricFilterOp =
  | "="
  | "!="
  | "<"
  | "<="
  | ">"
  | ">="
  | "in"
  | "not in"
  | "between"
  | "is null"
  | "is not null";

export type MetricGrain = "day" | "week" | "month" | "quarter" | "year";

export interface MetricFilter {
  field: string;
  op: MetricFilterOp;
  value?: unknown;
}

export interface MetricOrder {
  field: string;
  direction: "asc" | "desc";
}

export interface MetricQuery {
  metric: string;
  dimensions: string[];
  filters: MetricFilter[];
  grain: MetricGrain | null;
  order: MetricOrder | null;
  limit: number;
  preview: boolean;
}

export interface MetricQueryResult {
  columns: string[];
  dtypes: string[];
  rows: unknown[][];
  total_rows: number;
  truncated: boolean;
  sql: string;
  metric: {
    name: string;
    version: number | null;
    status: string;
    approved_by: string | null;
  };
  error: string | null;
}

export interface MetricQuerySaveAsFlow {
  query: MetricQuery;
  name: string;
  namespace_id?: number | null;
  description?: string | null;
}

export interface MetricQuerySaveAsFlowOut {
  flow_id: number;
  sql: string;
}

export interface SuggestCounts {
  roles: number;
  descriptions: number;
  joins: number;
  metrics: number;
}

export interface SuggestOut {
  namespace_id: number;
  tables: number;
  added: SuggestCounts;
  skipped: SuggestCounts;
  stats_available: boolean;
  stats_message: string | null;
}

export interface SemanticDraftRequest {
  namespace_id: number;
  table_ids?: number[];
  provider?: string | null;
  model?: string | null;
}

export interface SemanticDraftTable {
  table_id: number;
  table: string;
  calls: number;
  ok: boolean;
  reason: string | null;
  message: string | null;
}

export interface SemanticDraftOut extends SuggestOut {
  provider: string;
  model: string | null;
  table_outcomes: SemanticDraftTable[];
  degraded: boolean;
  reason: string | null;
}

export interface SemanticAskRequest {
  namespace_id: number;
  question: string;
  provider?: string | null;
  model?: string | null;
}

export interface SemanticAnsweredWith {
  name: string;
  version: number | null;
  status: string;
  approved_by: string | null;
}

export type SemanticAskReason =
  | "no_accepted_metrics"
  | "parse_error"
  | "unknown_metric"
  | "invalid_query"
  | "query_failed"
  | "timeout"
  | "provider_error";

export type SemanticAskIntent = "value" | "top" | "bottom" | "trend" | "breakdown";

/** One group or period of an answer, as core computed it from the result rows. */
export interface SemanticAnswerItem {
  label: string;
  value: number | null;
  text: string;
}

/** The facts the answer sentence was rendered from (core's `SemanticAnswerFacts`). */
export interface SemanticAnswerFacts {
  intent: SemanticAskIntent | string;
  value: SemanticAnswerItem | null;
  leaders: SemanticAnswerItem[];
  runners_up: SemanticAnswerItem[];
  groups: number | null;
  share: number | null;
  highest: SemanticAnswerItem | null;
  lowest: SemanticAnswerItem | null;
  latest: SemanticAnswerItem | null;
  previous: SemanticAnswerItem | null;
  change: number | null;
  direction: "up" | "down" | "flat" | null;
  truncated: boolean;
}

export interface SemanticAskOut {
  question: string;
  provider: string;
  model: string | null;
  query: MetricQuery | null;
  result: MetricQueryResult | null;
  answered_with: SemanticAnsweredWith | null;
  notes: string[];
  partial?: boolean;
  degraded: boolean;
  reason: SemanticAskReason | null;
  no_metric_fits: boolean;
  attempts: number;
  /** The answer in words, rendered by core from the result rows; absent from an older core. */
  answer?: string | null;
  intent?: SemanticAskIntent | string | null;
  facts?: SemanticAnswerFacts | null;
}

export interface AiNotSetUp {
  error_code: "AI_NOT_SET_UP";
  message: string;
  local_available: boolean;
}

export const isAiNotSetUp = (e: unknown): boolean =>
  (e as { response?: { status?: number; data?: { error_code?: string } } })?.response?.data
    ?.error_code === "AI_NOT_SET_UP";
