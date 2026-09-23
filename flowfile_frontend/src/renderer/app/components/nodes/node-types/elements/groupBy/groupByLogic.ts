/**
 * Pure helpers behind the Group By drawer: which aggregation a column gets by
 * default, how rows are added without duplicates, and the derived feedback
 * (usage per column, clashing output names).
 *
 * No Vue or DOM imports so it runs under vitest's `node` environment.
 */
import type { AggColl, AggOption, GroupByOption } from "../../../../../types/node.types";
import { AGGREGATE_OPTIONS, aggLabel } from "../../../baseNode/aggregations";

export { AGGREGATE_OPTIONS, aggLabel };

export type AggKind = AggOption | GroupByOption;

export const AGG_OPTIONS: readonly AggKind[] = [
  "groupby",
  ...AGGREGATE_OPTIONS.map((option) => option.value),
];

const NUMERIC_TYPE = /^(u?int|float|decimal)/i;

export const isNumericType = (dataType: string | null | undefined): boolean =>
  NUMERIC_TYPE.test(dataType ?? "");

/** Sum is what most people want from a number; anything else gets a count. */
export const defaultAggFor = (dataType: string | null | undefined): AggOption =>
  isNumericType(dataType) ? "sum" : "count";

/** The naming the context menu always used: `col_sum`; a key keeps its name. */
export const outputNameFor = (column: string, agg: string): string =>
  agg === "groupby" ? column : `${column}_${agg}`;

export interface AddRowsResult {
  rows: AggColl[];
  /** Indices of rows appended by this call. */
  added: number[];
  /** Indices of rows that already covered a requested (column, aggregation) pair. */
  existing: number[];
}

/** Appends one row per column; a pair already present is reported rather than duplicated. */
export const addAggRows = (
  rows: readonly AggColl[],
  columns: readonly string[],
  agg: AggKind,
): AddRowsResult => {
  const next = [...rows];
  const added: number[] = [];
  const existing: number[] = [];
  for (const column of columns) {
    const index = next.findIndex((row) => row.old_name === column && row.agg === agg);
    if (index !== -1) {
      existing.push(index);
      continue;
    }
    next.push({ old_name: column, agg, new_name: outputNameFor(column, agg) });
    added.push(next.length - 1);
  }
  return { rows: next, added, existing };
};

/** Retargets an auto-generated output name when the aggregation changes; a hand-typed one is kept. */
export const renamedForAgg = (row: AggColl, nextAgg: string): string | undefined =>
  row.new_name === outputNameFor(row.old_name, row.agg)
    ? outputNameFor(row.old_name, nextAgg)
    : row.new_name;

/** Output names used by more than one row. Case-sensitive, like Polars column names. */
export const duplicateOutputNames = (rows: readonly AggColl[]): Set<string> => {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const row of rows) {
    const name = row.new_name ?? "";
    if (!name) continue;
    if (seen.has(name)) duplicates.add(name);
    seen.add(name);
  }
  return duplicates;
};

export interface ColumnUsage {
  agg: string;
  /** Row index in the settings table, so a chip can reveal its row. */
  index: number;
}

export const usageByColumn = (rows: readonly AggColl[]): Map<string, ColumnUsage[]> => {
  const usage = new Map<string, ColumnUsage[]>();
  rows.forEach((row, index) => {
    const uses = usage.get(row.old_name) ?? [];
    uses.push({ agg: row.agg, index });
    usage.set(row.old_name, uses);
  });
  return usage;
};
