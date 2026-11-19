import type { DashboardFilter } from "../../types";

/**
 * A categorical filter's chosen values and the value search behind its input.
 *
 * Single-select is "at most one value": the state keeps its `selected` list either way, so the
 * query builder, the `?f=` URL state and the KPI code read one shape.
 */

/** How many values one value search returns; one more is asked for to tell a capped list. */
export const VALUE_SEARCH_LIMIT = 100;

export interface ValueSearchResult {
  values: string[];
  /** More values match than the list holds. */
  capped: boolean;
}

/** False only when the author turned "allow several values" off; absent on older layouts. */
export const allowsSeveral = (filter: DashboardFilter): boolean => filter.multiple !== false;

/** A value as the input shows and stores it. */
export const formatFilterValue = (v: unknown): string => {
  if (v == null) return "";
  if (v instanceof Date) return v.toISOString();
  return String(v);
};

/** The chosen values, as strings; at most one for a single-select filter. */
export const selectedValues = (filter: DashboardFilter): string[] => {
  const raw = filter.state.selected;
  const all = Array.isArray(raw) ? raw.map(formatFilterValue) : [];
  return allowsSeveral(filter) ? all : all.slice(0, 1);
};

/** The state after the input changed: a list in multi mode, one value or nothing in single mode. */
export const selectionState = (
  filter: DashboardFilter,
  value: unknown,
): Record<string, unknown> => {
  if (Array.isArray(value)) {
    const values = value.map(formatFilterValue);
    return { ...filter.state, selected: allowsSeveral(filter) ? values : values.slice(0, 1) };
  }
  const one = value == null ? "" : formatFilterValue(value);
  return { ...filter.state, selected: one === "" ? [] : [one] };
};

/** Turn "allow several values" on or off; going single keeps the first chosen value. */
export const withMultiple = (filter: DashboardFilter, multiple: boolean): DashboardFilter => {
  if (multiple) return { ...filter, multiple: true };
  const next: DashboardFilter = { ...filter, multiple: false };
  if (filter.kind === "categorical" && Array.isArray(filter.state.selected)) {
    next.state = { ...filter.state, selected: filter.state.selected.slice(0, 1) };
  }
  return next;
};

/** Escape text for the worker's regex engine (Rust `regex`), so it is matched literally. */
export const escapeRegExp = (text: string): string =>
  text.replace(/[\\.+*?()|[\]{}^$#&\-~]/g, "\\$&");

/**
 * The Graphic Walker workflow for the distinct values of `field` that contain `text`, ignoring
 * case, sorted, at most `limit` (plus one, to tell a capped list). An empty text lists the first
 * values. polars-gw runs a `regexp` rule as `str.contains` on the column cast to text.
 */
export const valueSearchPayload = (
  field: string,
  text: string,
  limit: number = VALUE_SEARCH_LIMIT,
): Record<string, unknown> => {
  const workflow: Record<string, unknown>[] = [];
  if (text !== "") {
    workflow.push({
      type: "filter",
      filters: [
        { fid: field, rule: { type: "regexp", value: escapeRegExp(text), caseSensitive: false } },
      ],
    });
  }
  workflow.push(
    { type: "view", query: [{ op: "aggregate", groupBy: [field], measures: [] }] },
    { type: "sort", by: [field], sort: "ascending" },
  );
  return { workflow, limit: limit + 1 };
};

/** Read a value search's rows; a missing value (null) is left out, as a filter cannot pick it. */
export const valueSearchResult = (
  rows: Record<string, unknown>[],
  field: string,
  limit: number = VALUE_SEARCH_LIMIT,
): ValueSearchResult => {
  const values = rows
    .slice(0, limit)
    .map((r) => r[field])
    .filter((v) => v != null)
    .map(formatFilterValue);
  return { values, capped: rows.length > limit };
};

/** Case-insensitive "contains", the local twin of the value search. */
export const matchesQuery = (value: string, query: string): boolean =>
  query === "" || value.toLowerCase().includes(query.toLowerCase());

/** The listed values, then every chosen value the list lacks, so a choice always shows as chosen. */
export const valueOptions = (listed: string[], selected: string[]): string[] => {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const v of [...listed, ...selected]) {
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
};
