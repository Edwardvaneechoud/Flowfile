/**
 * Pure logic for the catalog table edit surface: the edit session buffer,
 * dtype editability rules, key-column suggestion/validation, and payload
 * assembly for POST /catalog/tables/{id}/edits.
 *
 * No Vue/axios imports — unit-tested in tableEditing.test.ts.
 */
import type { CatalogTableEditsRequest, NewColumnSpec } from "../../types";

export interface EditColumn {
  name: string;
  /** Arrow dtype string from the preview endpoint (e.g. "int64", "large_string"). */
  dtype: string;
  isNew: boolean;
  editable: boolean;
}

export interface EditRow {
  /** Stable id within the session (not persisted). */
  uid: number;
  cells: Record<string, unknown>;
  /** Snapshot at load time; null for rows added in this session. */
  original: Record<string, unknown> | null;
  deleted: boolean;
  /** Column names whose value changed since load (always all-filled cells for added rows). */
  dirty: Set<string>;
}

export interface EditSession {
  columns: EditColumn[];
  rows: EditRow[];
  keyColumns: string[];
  expectedVersion: number | null;
  newColumns: NewColumnSpec[];
}

/** Arrow dtypes the grid lets users edit: scalars that round-trip through JSON + a server cast. */
const EDITABLE_DTYPE_PATTERN =
  /^(u?int(8|16|32|64)|float(16|32|64)?|double|string|large_string|utf8|bool|boolean|date32.*|date64.*|timestamp.*|decimal.*)$/i;

export function isEditableDtype(arrowDtype: string): boolean {
  return EDITABLE_DTYPE_PATTERN.test(arrowDtype.trim());
}

const KEY_NAME_PATTERN = /^(id|.*_id|record_id|row_id|uuid|key|pk)$/i;

/** True when every loaded value in the column is unique and non-null. */
function isUniqueInSlice(rows: unknown[][], columnIndex: number): boolean {
  const seen = new Set<string>();
  for (const row of rows) {
    const value = row[columnIndex];
    if (value === null || value === undefined) return false;
    const token = typeof value === "string" ? `s:${value}` : `v:${String(value)}`;
    if (seen.has(token)) return false;
    seen.add(token);
  }
  return true;
}

/**
 * Suggest key columns for the edit session: id-shaped names first, then any
 * column whose loaded values are unique. Uniqueness over the loaded slice is
 * a heuristic only — the server re-validates over the whole table.
 */
export function suggestKeyColumns(columns: string[], rows: unknown[][]): string[] {
  const nameMatches = columns.filter((c) => KEY_NAME_PATTERN.test(c));
  for (const name of nameMatches) {
    if (isUniqueInSlice(rows, columns.indexOf(name))) return [name];
  }
  for (const name of columns) {
    if (isUniqueInSlice(rows, columns.indexOf(name))) return [name];
  }
  return [];
}

/**
 * Client-side key sanity check before a save. JS numbers lose integer
 * precision above 2^53, so a bigint-ish key would silently mis-target rows —
 * that is blocked here rather than corrupted quietly.
 */
export function validateKeyValues(session: EditSession): string | null {
  const keys = session.keyColumns;
  if (!keys.length) return "Pick at least one key column.";
  const seen = new Set<string>();
  for (const row of session.rows) {
    if (row.deleted && row.original === null) continue;
    const token = keys.map((k) => keyToken(row.cells[k])).join("\u0000");
    for (const k of keys) {
      const value = row.cells[k];
      if (value === null || value === undefined || value === "") {
        return row.original === null
          ? `New rows need a value in key column "${k}".`
          : `Key column "${k}" contains empty values.`;
      }
      if (typeof value === "number" && !Number.isSafeInteger(value) && Number.isInteger(value)) {
        return `Key column "${k}" holds integers beyond JavaScript's safe range; edits could attach to the wrong rows. Pick another key column.`;
      }
    }
    if (seen.has(token)) return `Key columns (${keys.join(", ")}) are not unique in the grid.`;
    seen.add(token);
  }

  // Collateral-cell guard: a column that is dirty anywhere ships for every
  // upserted row, and an UNEDITED integer cell beyond 2^53 already arrived
  // JSON-rounded — refuse rather than silently write the rounded value back.
  const dirtyColumns = new Set<string>();
  const upserted: EditRow[] = [];
  for (const row of session.rows) {
    if (row.deleted) continue;
    if (row.original === null || row.dirty.size > 0) {
      upserted.push(row);
      for (const col of row.dirty) dirtyColumns.add(col);
    }
  }
  for (const col of dirtyColumns) {
    for (const row of upserted) {
      if (row.dirty.has(col)) continue;
      const value = row.cells[col];
      if (typeof value === "number" && Number.isInteger(value) && !Number.isSafeInteger(value)) {
        return `Column "${col}" holds integers beyond JavaScript's safe range on rows you did not edit; saving would write rounded values back. Edit those cells explicitly or avoid editing this column.`;
      }
    }
  }
  return null;
}

function keyToken(value: unknown): string {
  return typeof value === "string" ? `s:${value}` : `v:${String(value)}`;
}

function valuesDiffer(a: unknown, b: unknown): boolean {
  if (a === b) return false;
  if ((a === null || a === undefined || a === "") && (b === null || b === undefined || b === "")) {
    return false;
  }
  // Grid editors return strings; treat "42" == 42 as unchanged.
  return String(a ?? "") !== String(b ?? "");
}

/** Record a cell change on a row, keeping the dirty set truthful on revert. */
export function applyCellEdit(row: EditRow, column: string, value: unknown): void {
  row.cells[column] = value;
  if (row.original === null) {
    if (value !== null && value !== undefined && value !== "") row.dirty.add(column);
    else row.dirty.delete(column);
    return;
  }
  if (valuesDiffer(value, row.original[column])) row.dirty.add(column);
  else row.dirty.delete(column);
}

export interface SessionCounts {
  edited: number;
  added: number;
  deleted: number;
}

export function sessionCounts(session: EditSession): SessionCounts {
  let edited = 0;
  let added = 0;
  let deleted = 0;
  for (const row of session.rows) {
    if (row.deleted) {
      if (row.original !== null) deleted += 1;
    } else if (row.original === null) {
      added += 1;
    } else if (row.dirty.size > 0) {
      edited += 1;
    }
  }
  return { edited, added, deleted };
}

export function hasPendingChanges(session: EditSession): boolean {
  const counts = sessionCounts(session);
  return counts.edited + counts.added + counts.deleted > 0 || session.newColumns.length > 0;
}

/**
 * Assemble the request payload. Upserted rows ship only the key columns plus
 * columns actually edited somewhere in the session — unsent columns are
 * preserved server-side, so untouched values never round-trip through JSON
 * (protects e.g. float precision and big integers in unedited columns).
 */
export function buildEditsPayload(session: EditSession): CatalogTableEditsRequest {
  const dirtyColumns = new Set<string>();
  const upsertRowsSource: EditRow[] = [];
  const deleteRows: EditRow[] = [];

  for (const row of session.rows) {
    if (row.deleted) {
      if (row.original !== null) deleteRows.push(row);
      continue;
    }
    if (row.original === null || row.dirty.size > 0) {
      upsertRowsSource.push(row);
      for (const col of row.dirty) dirtyColumns.add(col);
    }
  }

  const knownColumns = new Set(session.columns.map((c) => c.name));
  const upsertColumns = [
    ...session.keyColumns,
    ...[...dirtyColumns].filter((c) => !session.keyColumns.includes(c) && knownColumns.has(c)),
  ];

  const normalize = (value: unknown): unknown =>
    value === undefined || value === "" ? null : value;

  const payload: CatalogTableEditsRequest = {
    key_columns: session.keyColumns,
    expected_version: session.expectedVersion,
    new_columns: session.newColumns,
  };
  if (upsertRowsSource.length > 0) {
    payload.upsert_columns = upsertColumns;
    payload.upsert_rows = upsertRowsSource.map((row) =>
      upsertColumns.map((col) => normalize(row.cells[col])),
    );
  }
  if (deleteRows.length > 0) {
    // Deletes target the row as loaded: original key values, in case a key cell was touched.
    payload.delete_keys = deleteRows.map((row) =>
      session.keyColumns.map((col) => normalize((row.original ?? row.cells)[col])),
    );
  }
  return payload;
}

// Labelling session helpers

export interface LabelClass {
  value: string;
  /** 1-based keyboard shortcut; only the first 9 classes get one. */
  shortcut: number | null;
}

export function buildLabelClasses(values: string[]): LabelClass[] {
  return values.map((value, i) => ({ value, shortcut: i < 9 ? i + 1 : null }));
}

/** Distinct non-empty values of a column across the buffer, insertion-ordered. */
export function distinctColumnValues(rows: EditRow[], column: string, cap = 50): string[] {
  const seen = new Set<string>();
  for (const row of rows) {
    if (row.deleted) continue;
    const value = row.cells[column];
    if (value === null || value === undefined || value === "") continue;
    seen.add(String(value));
    if (seen.size >= cap) break;
  }
  return [...seen];
}

/**
 * Row visit order for a labelling pass: unlabelled rows first (stable),
 * then already-labelled ones for review. Deleted rows are skipped.
 *
 * With `confidenceOf` the unlabelled bucket is split: rows carrying a
 * confidence come first, least confident first (active-learning order),
 * then the unlabelled rows without one in their original order.
 */
export function labelingOrder(
  rows: EditRow[],
  targetColumn: string,
  confidenceOf?: (row: EditRow) => number | null,
): number[] {
  const scored: { index: number; confidence: number }[] = [];
  const unlabelled: number[] = [];
  const labelled: number[] = [];
  rows.forEach((row, index) => {
    if (row.deleted) return;
    const value = row.cells[targetColumn];
    if (value !== null && value !== undefined && value !== "") {
      labelled.push(index);
      return;
    }
    const confidence = confidenceOf ? confidenceOf(row) : null;
    if (confidence === null || !Number.isFinite(confidence)) unlabelled.push(index);
    else scored.push({ index, confidence });
  });
  scored.sort((a, b) => a.confidence - b.confidence || a.index - b.index);
  return [...scored.map((entry) => entry.index), ...unlabelled, ...labelled];
}

export function labelingProgress(
  rows: EditRow[],
  targetColumn: string,
): { labelled: number; total: number; perClass: Record<string, number> } {
  let labelled = 0;
  let total = 0;
  const perClass: Record<string, number> = {};
  for (const row of rows) {
    if (row.deleted) continue;
    total += 1;
    const value = row.cells[targetColumn];
    if (value === null || value === undefined || value === "") continue;
    labelled += 1;
    const key = String(value);
    perClass[key] = (perClass[key] ?? 0) + 1;
  }
  return { labelled, total, perClass };
}

// Cross-table suggestion join

export interface JoinKeySpec {
  column: string;
  coerceNumeric: boolean;
}

export interface SuggestionEntry {
  suggested: string;
  confidence: number | null;
  probabilities: Record<string, number> | null;
}

export interface SuggestionMap {
  byKey: Map<string, SuggestionEntry>;
  duplicateKeys: number;
  unjoinableRows: number;
  /** Rows whose probability was not in 0..1 — a melted key or a count, not a score. */
  outOfRangeRows: number;
  /** Rows naming a class that was not declared — typically a column swept into a melt. */
  unknownClassRows: number;
}

const NUMERIC_DTYPE_PATTERN = /^(u?int(8|16|32|64)|float(16|32|64)?|double|decimal.*)$/i;

export function isNumericDtype(arrowDtype: string): boolean {
  return NUMERIC_DTYPE_PATTERN.test(arrowDtype.trim());
}

/**
 * Join spec between the edit session and a prediction table; null when a key
 * column is missing there. Only a numeric *session* dtype coerces: the grid
 * hands back strings so `42` must match `"42"`, but coercing a string-typed key
 * would collapse genuinely distinct values like `"007"` and `"7"`.
 */
export function buildJoinKeySpec(
  keyColumns: string[],
  sessionColumns: EditColumn[],
  predColumns: string[],
): JoinKeySpec[] | null {
  if (!keyColumns.length) return null;
  const spec: JoinKeySpec[] = [];
  for (const column of keyColumns) {
    const predIndex = predColumns.indexOf(column);
    if (predIndex === -1) return null;
    const sessionDtype = sessionColumns.find((c) => c.name === column)?.dtype ?? "";
    spec.push({
      column,
      coerceNumeric: isNumericDtype(sessionDtype),
    });
  }
  return spec;
}

const CONTINUOUS_DTYPE_PATTERN = /^(float(16|32|64)?|double|decimal.*)$/i;

/** Float-ish dtypes hold probabilities, never class labels — used to keep them out of the class picker. */
export function isContinuousDtype(arrowDtype: string): boolean {
  return CONTINUOUS_DTYPE_PATTERN.test(arrowDtype.trim());
}

const INTEGER_TEXT_PATTERN = /^[+-]?\d+$/;

/**
 * Textual canonicalisation, so a *text-valued* key beyond 2^53 stays exact.
 * A number-valued one cannot — the preview sends integer columns as JSON
 * numbers, so those digits are already lost before this runs.
 */
function canonicalIntegerText(text: string): string {
  const negative = text.startsWith("-");
  const digits = text.replace(/^[+-]/, "").replace(/^0+(?=\d)/, "");
  return negative && digits !== "0" ? `-${digits}` : digits;
}

/**
 * Join token for one row's key values (parallel to `spec`), or null when any
 * part is blank — a blank must not canonicalise to `n:0` and collide with a
 * real `0`. Parts join on NUL, which no cell can contain, so two distinct
 * composite keys can never render as the same token.
 */
export function joinKeyToken(values: unknown[], spec: JoinKeySpec[]): string | null {
  const parts: string[] = [];
  for (let i = 0; i < spec.length; i += 1) {
    const value = values[i];
    if (value === null || value === undefined) return null;
    const text = String(value).trim();
    if (text === "") return null;
    if (!spec[i].coerceNumeric) {
      parts.push(`s:${String(value)}`);
      continue;
    }
    if (INTEGER_TEXT_PATTERN.test(text)) {
      parts.push(`n:${canonicalIntegerText(text)}`);
      continue;
    }
    const numeric = Number(text);
    parts.push(Number.isNaN(numeric) ? `s:${String(value)}` : `n:${String(numeric)}`);
  }
  return parts.join("\u0000");
}

/** Columns the prediction read must project, or null when the pick is incomplete.
 *  The probability column is optional — without it confidence is simply null. */
export function predictionProjectionColumns(
  keyColumns: string[],
  classColumn: string,
  probabilityColumn: string | null,
): string[] | null {
  if (!classColumn) return null;
  return [
    ...new Set([...keyColumns, classColumn, ...(probabilityColumn ? [probabilityColumn] : [])]),
  ];
}

export const TRUNCATED_SUGGESTION_NOTE =
  "The prediction table was only read in part, so suggestions are off for this pass.";
export const SCHEMA_CHANGED_SUGGESTION_NOTE =
  "The prediction table's schema changed, so suggestions are off for this pass.";

/** Why the current prediction read cannot back suggestions, or null when it can.
 *  Truncation must be re-checked on every rebuild: rows past the cap lose their
 *  suggestion and tail-only classes drop out of the argmax. */
export function suggestionBlockReason(read: {
  truncated: boolean;
  joinSpec: JoinKeySpec[] | null;
  columns: string[];
  classColumn: string;
}): string | null {
  if (read.truncated) return TRUNCATED_SUGGESTION_NOTE;
  if (!read.joinSpec || !read.classColumn || !read.columns.includes(read.classColumn)) {
    return SCHEMA_CHANGED_SUGGESTION_NOTE;
  }
  return null;
}

/**
 * Index a prediction table by join token. A prediction table is exploded: one row
 * per (record, class) carrying that class's probability. Per key the
 * highest-probability row wins the suggestion, its probability is the confidence,
 * and all of the key's rows form the distribution.
 *
 * Only classes present in `allowedClasses` are considered (an empty list accepts all).
 *
 * A repeat of the same class for one key is a data error, not a second candidate:
 * it is counted and resolved to the higher probability so the result stays
 * independent of row order. Empty keys count as unjoinable; rows with no class are
 * skipped silently.
 */
export function buildSuggestionMap(
  predColumns: string[],
  predRows: unknown[][],
  keySpec: JoinKeySpec[],
  classColumn: string,
  probabilityColumn: string,
  allowedClasses: readonly string[] = [],
): SuggestionMap {
  const byKey = new Map<string, SuggestionEntry>();
  let duplicateKeys = 0;
  let unjoinableRows = 0;
  let outOfRangeRows = 0;
  let unknownClassRows = 0;
  const allowed = allowedClasses.length > 0 ? new Set(allowedClasses) : null;

  interface Candidate {
    best: string;
    bestProbability: number | null;
    observed: Record<string, number>;
    seen: Set<string>;
  }
  const accumulated = new Map<string, Candidate>();

  const keyIndexes = keySpec.map((s) => predColumns.indexOf(s.column));
  const classIndex = predColumns.indexOf(classColumn);
  const probabilityIndex = predColumns.indexOf(probabilityColumn);

  const cellOf = (row: unknown[], index: number): unknown => (index === -1 ? null : row[index]);

  for (const row of predRows) {
    const token = joinKeyToken(
      keyIndexes.map((index) => cellOf(row, index)),
      keySpec,
    );
    if (token === null) {
      unjoinableRows += 1;
      continue;
    }
    const rawClass = cellOf(row, classIndex);
    if (rawClass === null || rawClass === undefined || rawClass === "") continue;
    const className = String(rawClass);
    // Only a declared class can be a candidate. A melt that swept in a key or a
    // confidence column otherwise contributes a pseudo-class that wins the argmax.
    if (allowed !== null && !allowed.has(className)) {
      unknownClassRows += 1;
      continue;
    }

    let probability: number | null = null;
    const rawProbability = cellOf(row, probabilityIndex);
    if (rawProbability !== null && rawProbability !== undefined && rawProbability !== "") {
      const parsed = Number(rawProbability);
      if (!Number.isFinite(parsed)) probability = null;
      else if (parsed < 0 || parsed > 1) outOfRangeRows += 1;
      else probability = parsed;
    }

    let candidate = accumulated.get(token);
    if (candidate === undefined) {
      candidate = { best: className, bestProbability: null, observed: {}, seen: new Set() };
      accumulated.set(token, candidate);
    }
    if (candidate.seen.has(className)) duplicateKeys += 1;
    candidate.seen.add(className);

    const known = candidate.observed[className];
    if (probability !== null && (known === undefined || probability > known)) {
      candidate.observed[className] = probability;
    }
    if (
      probability !== null &&
      (candidate.bestProbability === null || probability > candidate.bestProbability)
    ) {
      candidate.best = className;
      candidate.bestProbability = probability;
    }
  }

  for (const [token, candidate] of accumulated) {
    byKey.set(token, {
      suggested: candidate.best,
      confidence: candidate.bestProbability,
      probabilities: Object.keys(candidate.observed).length > 0 ? candidate.observed : null,
    });
  }
  return { byKey, duplicateKeys, unjoinableRows, outOfRangeRows, unknownClassRows };
}

export function rowSuggestion(
  row: EditRow,
  spec: JoinKeySpec[],
  byKey: Map<string, SuggestionEntry>,
): SuggestionEntry | null {
  const token = joinKeyToken(
    spec.map((s) => row.cells[s.column]),
    spec,
  );
  if (token === null) return null;
  return byKey.get(token) ?? null;
}

export function suggestionCoverage(
  rows: EditRow[],
  spec: JoinKeySpec[],
  byKey: Map<string, SuggestionEntry>,
): { covered: number; total: number } {
  let covered = 0;
  let total = 0;
  for (const row of rows) {
    if (row.deleted) continue;
    total += 1;
    if (rowSuggestion(row, spec, byKey) !== null) covered += 1;
  }
  return { covered, total };
}

/** Rows where the human label and the suggestion disagree — the review shortlist. */
export function suggestionDisagreement(
  rows: EditRow[],
  targetColumn: string,
  spec: JoinKeySpec[],
  byKey: Map<string, SuggestionEntry>,
): number {
  let disagreeing = 0;
  for (const row of rows) {
    if (row.deleted) continue;
    const label = row.cells[targetColumn];
    if (label === null || label === undefined || label === "") continue;
    const suggestion = rowSuggestion(row, spec, byKey);
    if (suggestion === null) continue;
    if (String(label) !== suggestion.suggested) disagreeing += 1;
  }
  return disagreeing;
}

/** Only a 0..1 fraction is a probability; anything else renders as nothing rather than a bogus percent. */
export function formatConfidence(value: number | null): string {
  if (value === null || !Number.isFinite(value) || value < 0 || value > 1) return "";
  return `${Math.round(value * 100)}%`;
}

/** Distinct values of the prediction table's class column, in first-seen order. */
export function distinctPredictionClasses(
  predColumns: string[],
  predRows: unknown[][],
  classColumn: string,
  cap = 50,
): string[] {
  const index = predColumns.indexOf(classColumn);
  if (index === -1) return [];
  const seen = new Set<string>();
  for (const row of predRows) {
    const raw = row[index];
    if (raw === null || raw === undefined || raw === "") continue;
    seen.add(String(raw));
    if (seen.size >= cap) break;
  }
  return [...seen];
}
