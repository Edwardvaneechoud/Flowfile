// The parts of flowfile_core/schemas/semantic_schema.py the app reads
import { detailOf } from "../utils/apiDetail";

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
  /** Other words for the metric; absent from an older core. */
  synonyms?: string[];
  status: SemanticFactStatus;
  version: number | null;
  approved_by: string | null;
  approved_at: string | null;
  group: string | null;
  pending_group: string | null;
  /** Whether the review policy lets this person remove the metric. */
  can_remove: boolean;
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
  /** Whether the review policy lets this person remove the join. */
  can_remove: boolean;
}

/** An accepted metric that stops working without a join (GET …/semantic-model/joins/dependents). */
export interface JoinDependent {
  metric: string;
  /** "formula", "time column" or "dimension <name>". */
  uses: string[];
}

/** A join measured against the data (POST …/semantic-model/joins/check). */
export interface JoinCheck {
  key: string;
  name: string;
  verdict: "ok" | "reversed" | "many_to_many" | "unchecked";
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

/** Fact counts of a model, a schema or a table. */
export interface SemanticModelSummary {
  accepted: number;
  pending: number;
  stale: number;
}

export interface SemanticModel {
  namespace_id: number;
  level: number;
  key: string;
  name: string | null;
  description: string | null;
  ai_instructions: string | null;
  tables: SemanticTable[];
  joins: SemanticJoin[];
  progress: SemanticModelSummary;
  can_manage: boolean;
  review_mode: "solo" | "team";
}

export type SemanticModelUpdate = Partial<
  Pick<SemanticModel, "name" | "description" | "ai_instructions">
>;

export type SemanticProposal =
  | { kind: "field"; table_id: number; column: string; attribute: string; value: unknown }
  | { kind: "table"; table_id: number; attribute: string; value: unknown }
  | {
      kind: "join";
      name: string;
      from_table_id: number;
      to_table_id: number;
      from_columns: string[];
      to_columns: string[];
      description?: string | null;
    }
  | {
      kind: "metric";
      table_id: number;
      name: string;
      expression: string;
      description?: string | null;
      filter?: string | null;
      time_column?: string | null;
      dimensions?: string[] | null;
      format?: string | null;
      synonyms?: string[] | null;
      edit?: boolean;
    };

export interface SemanticReviewAction {
  action: "accept" | "reject" | "retract" | "reopen";
  ids?: string[] | null;
  group?: string | null;
  /** With "retract": remove a whole metric or join by its key. */
  subject?: string | null;
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

export interface MetricQuery {
  metric: string;
  dimensions: string[];
  filters: { field: string; op: MetricFilterOp; value?: unknown }[];
  grain: MetricGrain | null;
  order: { field: string; direction: "asc" | "desc" } | null;
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
  /** The qualified names of the tables the query reads; absent from an older core. */
  tables?: string[];
  metric: {
    name: string;
    version: number | null;
    status: string;
    approved_by: string | null;
  };
  error: string | null;
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

/** What a suggestion can be; the `kinds` scope of both suggest routes. */
export type SemanticSuggestKind = "join" | "metric" | "description" | "role" | "synonyms";

/** The provider and model a request names; neither lets core choose. */
export interface SemanticModelPick {
  provider?: string;
  model?: string;
}

export interface SemanticSuggestionProposed {
  kind: SemanticSuggestKind;
  summary: string;
  table: string | null;
  name: string | null;
  fact_ids: string[];
  group: string | null;
}

export interface SemanticSuggestionSkipped {
  what: string;
  why: string;
}

/** What the AI suggest route stored: every `proposed` entry waits for review. */
export interface SemanticSuggestRequestOut {
  request: string | null;
  kinds: SemanticSuggestKind[] | null;
  provider: string;
  model: string | null;
  proposed: SemanticSuggestionProposed[];
  skipped: SemanticSuggestionSkipped[];
  notes: string[];
  nothing_to_suggest: boolean;
  why: string | null;
  degraded: boolean;
  reason: string | null;
  attempts: number;
  withheld_columns: number;
}

export interface SemanticDraftOut extends SuggestOut {
  provider: string;
  model: string | null;
  table_outcomes: { table: string; ok: boolean }[];
  degraded: boolean;
  reason: string | null;
}

export type SemanticAskReason =
  | "no_accepted_metrics"
  | "parse_error"
  | "unknown_metric"
  | "invalid_query"
  | "query_failed"
  | "timeout"
  | "provider_error";

export interface SemanticAskOut {
  question: string;
  provider: string;
  model: string | null;
  query: MetricQuery | null;
  result: MetricQueryResult | null;
  notes: string[];
  partial?: boolean;
  degraded: boolean;
  reason: SemanticAskReason | null;
  no_metric_fits: boolean;
  attempts: number;
  /** The answer in words, rendered by core from the result rows; absent from an older core. */
  answer?: string | null;
}

const codeOf = (e: unknown): string | undefined =>
  (e as { response?: { data?: { error_code?: string } } })?.response?.data?.error_code;

export const isAiNotSetUp = (e: unknown): boolean => codeOf(e) === "AI_NOT_SET_UP";

/** A Suggest route's 409 while another suggestion runs for the same model. */
export const isSuggestRunning = (e: unknown): boolean => codeOf(e) === "SUGGEST_RUNNING";

/** What a failed request says: its detail, a typed error's own message, else the error's. */
export const errorText = (e: unknown, fallback: string): string => {
  const { response, message } = (e ?? {}) as {
    response?: { data?: { message?: unknown } };
    message?: string;
  };
  const own = response?.data?.message;
  return detailOf(e, typeof own === "string" ? own : (message ?? fallback));
};

/** One Suggest run (core's `catalog/semantic/activity.py`); running while `finished_at` is absent. */
export interface SemanticActivity {
  kind: "suggest" | "draft" | "request";
  label: string;
  started_at: string;
  user_id: number | null;
  mine: boolean;
  step: string | null;
  finished_at?: string | null;
  ok?: boolean | null;
  summary?: string | null;
  error?: string | null;
  counts?: Record<string, number>;
}

/** `GET …/semantic-model/activity`: the run going on now and the last finished one. */
export interface SemanticActivityOut {
  running: SemanticActivity | null;
  last: SemanticActivity | null;
}
