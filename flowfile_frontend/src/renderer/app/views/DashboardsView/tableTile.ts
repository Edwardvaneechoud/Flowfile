import { formatCellValue } from "../../utils/cellFormat";
import type { DashboardTableColumn, DashboardTableConfig, TableSortDirection } from "../../types";
import { formatKpiValue } from "./kpi";

/** Key the count query carries the row count under. */
const ROW_COUNT_KEY = "table_row_count";

/** Half the 48-column grid, tall enough for a page of rows. */
export const TABLE_DEFAULT_SIZE = { w: 24, h: 8 } as const;

export const TABLE_PAGE_SIZES = [10, 25, 50, 100, 250];

/** Most rows one CSV download carries. */
export const TABLE_CSV_ROW_CAP = 50_000;

export const DEFAULT_TABLE_CONFIG: DashboardTableConfig = {
  columns: [],
  sort_by: [],
  sort_direction: "ascending",
  page_size: 25,
};

export interface TableSort {
  by: string[];
  direction: TableSortDirection;
}

/** A field as the source describes it; ``semanticType`` absent means unclassified. */
export interface TableField {
  fid: string;
  semanticType?: string;
}

export const tableConfigOf = (config: DashboardTableConfig | null | undefined) => ({
  ...DEFAULT_TABLE_CONFIG,
  ...(config ?? {}),
});

/** The columns to show, in order: the configured ones the source still has, else all of them. */
export const visibleColumns = (
  config: DashboardTableConfig | null | undefined,
  fields: TableField[],
): DashboardTableColumn[] => {
  const configured = config?.columns ?? [];
  if (!configured.length) return fields.map((f) => ({ field: f.fid }));
  const known = new Set(fields.map((f) => f.fid));
  return configured.filter((c) => known.has(c.field));
};

export const columnHeader = (column: DashboardTableColumn): string =>
  column.label?.trim() || column.field;

/** The saved default sort, limited to columns the source still has; null when there is none. */
export const defaultSort = (
  config: DashboardTableConfig | null | undefined,
  fields: TableField[],
): TableSort | null => {
  const known = new Set(fields.map((f) => f.fid));
  const by = (config?.sort_by ?? []).filter((f) => known.has(f));
  return by.length ? { by, direction: config?.sort_direction ?? "ascending" } : null;
};

/** A header click: ascending, then descending, then back to the default sort (null). */
export const nextHeaderSort = (current: TableSort | null, field: string): TableSort | null => {
  const onThis = current && current.by.length === 1 && current.by[0] === field;
  if (!onThis) return { by: [field], direction: "ascending" };
  return current.direction === "ascending" ? { by: [field], direction: "descending" } : null;
};

/** The direction a header shows, or null when the rows are not sorted by that column first. */
export const headerSortDirection = (
  sort: TableSort | null,
  field: string,
): TableSortDirection | null => (sort?.by[0] === field ? sort.direction : null);

/** Column dtypes by name, as the catalog records them; a column missing from it counts as sortable. */
export type ColumnDtypes = Record<string, string>;

/** Polars refuses to sort by an Object column (row encoding); lists, structs, arrays, binary,
 * categoricals and decimals sort fine on the pinned Polars range. */
export const isSortableDtype = (dtype: string | undefined): boolean =>
  !dtype || !/^Object\b/.test(dtype);

/**
 * The order the worker sorts by: the chosen columns, then every other shown column that can be
 * sorted. polars-gw's sort keeps no order on ties, so without these a row could move between pages;
 * rows that still tie are identical in every shown column. The header shows the chosen sort only.
 */
export const totalSort = (
  sort: TableSort | null,
  columns: string[],
  dtypes?: ColumnDtypes | null,
): TableSort | null => {
  if (!sort?.by.length) return null;
  const chosen = new Set(sort.by);
  const tiebreakers = columns.filter((c) => !chosen.has(c) && isSortableDtype(dtypes?.[c]));
  return { by: [...sort.by, ...tiebreakers], direction: sort.direction };
};

const rowSteps = (
  columns: string[],
  chosen: TableSort | null,
  dtypes?: ColumnDtypes | null,
): Record<string, unknown>[] => {
  const steps: Record<string, unknown>[] = [];
  const sort = totalSort(chosen, columns, dtypes);
  if (sort) steps.push({ type: "sort", by: sort.by, sort: sort.direction });
  // polars-gw's raw view selects the listed fields; none listed keeps every column.
  if (columns.length) steps.push({ type: "view", query: [{ op: "raw", fields: columns }] });
  return steps;
};

/** One page of rows. The dashboard's filter step is prepended by the caller. */
export const buildTablePagePayload = (opts: {
  columns: string[];
  sort: TableSort | null;
  dtypes?: ColumnDtypes | null;
  page: number;
  pageSize: number;
}): Record<string, unknown> => ({
  workflow: rowSteps(opts.columns, opts.sort, opts.dtypes),
  limit: opts.pageSize,
  offset: Math.max(0, opts.page) * opts.pageSize,
});

/** The filtered row count, one aggregate row. */
export const buildTableCountPayload = (): Record<string, unknown> => ({
  workflow: [
    {
      type: "view",
      query: [
        {
          op: "aggregate",
          groupBy: [],
          measures: [{ field: "*", agg: "count", asFieldKey: ROW_COUNT_KEY }],
        },
      ],
    },
  ],
  limit: 2,
});

export const readRowCount = (rows: Record<string, unknown>[]): number | null => {
  const raw = rows.length === 1 ? rows[0][ROW_COUNT_KEY] : undefined;
  const value = typeof raw === "number" ? raw : Number(raw);
  return raw != null && Number.isFinite(value) ? value : null;
};

/** The rows of a CSV download: one past the cap, so the caller can tell the cap was hit. */
export const buildTableCsvPayload = (opts: {
  columns: string[];
  sort: TableSort | null;
  dtypes?: ColumnDtypes | null;
  cap?: number;
}): Record<string, unknown> => ({
  workflow: rowSteps(opts.columns, opts.sort, opts.dtypes),
  limit: (opts.cap ?? TABLE_CSV_ROW_CAP) + 1,
  offset: 0,
});

export const capRows = <T>(rows: T[], cap = TABLE_CSV_ROW_CAP): { rows: T[]; capped: boolean } =>
  rows.length > cap ? { rows: rows.slice(0, cap), capped: true } : { rows, capped: false };

const csvCell = (value: unknown): string => {
  const raw = formatCellValue(value);
  return /[",\n\r]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
};

/** CSV with the columns' headers; cells keep their stored values, not the display format. */
export const toCsv = (columns: DashboardTableColumn[], rows: Record<string, unknown>[]): string => {
  const line = (values: unknown[]) => values.map(csvCell).join(",");
  return [
    line(columns.map(columnHeader)),
    ...rows.map((row) => line(columns.map((c) => row[c.field]))),
  ].join("\n");
};

/** File name for a download: the title made safe, else "table". */
export const csvFileName = (title: string | null | undefined): string => {
  const base = (title ?? "")
    .trim()
    .replace(/[^\w\- ]+/g, "")
    .replace(/\s+/g, "_");
  return `${base || "table"}.csv`;
};

/** A cell as the table shows it: numbers in the column's format, everything else as text. */
export const formatTableCell = (
  value: unknown,
  column: DashboardTableColumn,
  locale?: string,
): string => {
  if (typeof value !== "number" || !Number.isFinite(value)) return formatCellValue(value);
  const decimals = column.decimals ?? null;
  if (column.thousands === false) {
    return new Intl.NumberFormat(locale, {
      useGrouping: false,
      maximumFractionDigits: decimals ?? 2,
      minimumFractionDigits: decimals ?? 0,
    }).format(value);
  }
  return formatKpiValue(value, { compact: false, decimals }, locale);
};

export const pageCount = (total: number | null, pageSize: number): number =>
  total == null ? 1 : Math.max(1, Math.ceil(total / Math.max(1, pageSize)));
