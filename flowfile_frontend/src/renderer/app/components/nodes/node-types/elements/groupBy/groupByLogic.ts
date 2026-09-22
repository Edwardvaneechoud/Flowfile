/**
 * Pure helpers behind the Group By drawer: which aggregation a column gets by
 * default, how rows are added without duplicates, and the derived feedback
 * (usage per column, clashing output names, drop-zone hit-testing).
 *
 * No Vue or DOM imports so it runs under vitest's `node` environment.
 */
import type { AggColl, AggOption, GroupByOption } from "../../../../../types/node.types";

export type AggKind = AggOption | GroupByOption;

export const AGGREGATE_OPTIONS: readonly { value: AggOption; label: string }[] = [
  { value: "count", label: "Count" },
  { value: "sum", label: "Sum" },
  { value: "mean", label: "Mean" },
  { value: "median", label: "Median" },
  { value: "min", label: "Min" },
  { value: "max", label: "Max" },
  { value: "n_unique", label: "N unique" },
  { value: "first", label: "First" },
  { value: "last", label: "Last" },
  { value: "concat", label: "Concat" },
];

export const AGG_OPTIONS: readonly AggKind[] = [
  "groupby",
  ...AGGREGATE_OPTIONS.map((option) => option.value),
];

export const aggLabel = (agg: string): string =>
  agg === "groupby"
    ? "Group by"
    : (AGGREGATE_OPTIONS.find((option) => option.value === agg)?.label ?? agg);

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

export type DropZone = "groupby" | "aggregate";

/** Left half of the settings pane adds keys, right half adds aggregations. */
export const dropZoneAt = (rect: { left: number; width: number }, clientX: number): DropZone =>
  clientX < rect.left + rect.width / 2 ? "groupby" : "aggregate";

export const pluralize = (count: number, singular: string, plural = `${singular}s`): string =>
  `${count} ${count === 1 ? singular : plural}`;

/** The rows left after dropping the given indices, in their original order. */
export const withoutRows = <T>(rows: readonly T[], indices: readonly number[]): T[] => {
  const drop = new Set(indices);
  return rows.filter((_, index) => !drop.has(index));
};

/** Selected indices that survive a removal, shifted down past the rows that went. */
export const shiftAfterRemoval = (
  selected: readonly number[],
  removed: readonly number[],
): number[] => {
  const drop = new Set(removed);
  return selected
    .filter((index) => !drop.has(index))
    .map((index) => index - removed.filter((gone) => gone < index).length);
};

/** The settings strip on its own, and the column list's toolbar-plus-one-row floor. */
export const SETTINGS_STRIP_PX = 30;
export const COLUMNS_MIN_PX = 84;

/** Keeps a dragged settings height between the bare strip and the column list's floor. */
export const clampSettingsHeight = (height: number, available: number): number => {
  const max = Math.max(SETTINGS_STRIP_PX, available - COLUMNS_MIN_PX);
  return Math.round(Math.min(Math.max(height, SETTINGS_STRIP_PX), max));
};
