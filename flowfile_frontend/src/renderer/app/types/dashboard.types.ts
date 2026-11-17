// Dashboards — TS mirror of flowfile_core/schemas/catalog_schema.py

import type { AccessInfo } from "./sharing.types";

export type DashboardTileType = "viz" | "text" | "separator" | "kpi" | "table" | "filter";
export type TableSortDirection = "ascending" | "descending";

export interface DashboardTableColumn {
  field: string;
  /** Header text; absent or null shows the column name. */
  label?: string | null;
  /** Number format; text cells ignore both. null = auto. */
  decimals?: number | null;
  thousands?: boolean;
}

export interface DashboardTableConfig {
  /** Display order; empty shows every column. */
  columns: DashboardTableColumn[];
  sort_by: string[];
  sort_direction: TableSortDirection;
  page_size: number;
}
export type SeparatorOrientation = "horizontal" | "vertical";
export type KpiAgg = "sum" | "mean" | "median" | "min" | "max" | "count" | "distinctCount";
export type KpiComparison = "none" | "target" | "previous_period";
export type KpiValueSize = "auto" | "sm" | "md" | "lg" | "xl";

export interface DashboardKpi {
  /** null with agg "count" = row count. */
  field: string | null;
  agg: KpiAgg;
  label?: string | null;
  prefix?: string | null;
  suffix?: string | null;
  /** null = auto. */
  decimals?: number | null;
  compact?: boolean;
  comparison?: KpiComparison;
  target?: number | null;
  higher_is_better?: boolean;
  /** Fixed number size; "auto" fits the tile. */
  value_size?: KpiValueSize;
}

/** How a chart tile presents its chart; the saved chart is never changed. null keeps the chart's own. */
export interface DashboardChartFormat {
  /** d3-format string, e.g. ",.2f" or ".3~s". */
  number_format?: string | null;
  /** d3-time-format string, e.g. "%Y-%m-%d". */
  time_format?: string | null;
  /** Display name of the field ending the columns shelf (an aggregated measure reads sum(<title>)). */
  x_title?: string | null;
  /** The same for the rows shelf. */
  y_title?: string | null;
}

export interface DashboardTile {
  /** Client-generated UUID, stable across saves so component state survives. */
  id: string;
  type: DashboardTileType;
  /** Required when type === "viz"; a KPI's or table's chart source (never with table_id). */
  viz_id: number | null;
  /** A KPI's or table's catalog table source, read without a chart (never with viz_id). */
  table_id?: number | null;
  /** Which entry of CatalogVisualization.spec[] to render. */
  chart_index: number;
  /** A chart tile's own title; absent or null shows the chart's name. */
  title?: string | null;
  /** False hides a chart tile's title from viewers. */
  show_title?: boolean;
  /** A chart tile's number, date and axis-title overrides; absent or null draws the chart as saved. */
  chart_format?: DashboardChartFormat | null;
  /** Markdown source when type === "text". */
  text_md?: string | null;
  /** CSS colors for text tiles. */
  bg_color?: string | null;
  text_color?: string | null;
  /** Separator styling (type === "separator"); absent means a 1px default-colour horizontal line. */
  orientation?: SeparatorOrientation | null;
  thickness?: number | null;
  line_color?: string | null;
  /** Headline-number config when type === "kpi"; the source is table_id or viz_id. */
  kpi?: DashboardKpi | null;
  /** Rows config when type === "table"; null = every column, default page size. */
  table?: DashboardTableConfig | null;
  /** The layout filter a `type === "filter"` tile shows; that filter then leaves the strip. */
  filter_id?: string | null;
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface DashboardGrid {
  cols: number;
  row_height: number;
  version: number;
}

export type DashboardFilterKind = "categorical" | "date_range" | "numeric_range";

export interface DashboardFilter {
  id: string;
  field_name: string;
  label?: string | null;
  kind: DashboardFilterKind;
  /** Widget-specific state; shape depends on `kind`. */
  state: Record<string, unknown>;
  target: "all" | "tiles";
  target_tile_ids: string[];
  /**
   * Optional FK to CatalogTable.id. When set, the filter only applies to
   * tiles whose viz reads from this table, and the field picker is
   * populated from that table's schema_columns. Null = legacy / untied.
   */
  datasource_id: number | null;
  /**
   * Optional FK to a SQL-backed CatalogVisualization.id, never with
   * `datasource_id`: the filter's fields and values come from that chart's
   * source and scope "datasource" reaches the tiles reading that chart.
   */
  datasource_viz_id?: number | null;
  /**
   * "datasource" (default, also when absent on legacy layouts) applies only
   * to tiles reading from the filter's source; "all" applies to every targeted
   * tile whose data has a column named `field_name` of a compatible type.
   */
  scope?: DashboardFilterScope;
  /** False makes a categorical filter single-select (`state.selected` holds at most one value). */
  multiple?: boolean;
}

export type DashboardFilterScope = "datasource" | "all";

/** A value of a chart's colour field as the data holds it. */
export type DashboardColorValue = string | number | boolean | null;

/**
 * The colour one value of a colour field has on every chart tile of the dashboard: the `slot`-th
 * colour of the chart palette (`vizTheme.ts`), the same slot in light and dark.
 */
export interface DashboardColorAssignment {
  field: string;
  value: DashboardColorValue;
  slot: number;
}

export interface DashboardLayout {
  tiles: DashboardTile[];
  grid: DashboardGrid;
  filters: DashboardFilter[];
  /** Absent on layouts saved before value colours existed. */
  color_assignments?: DashboardColorAssignment[];
}

export interface Dashboard {
  id: number;
  name: string;
  description: string | null;
  layout: DashboardLayout;
  layout_version: number;
  namespace_id: number | null;
  namespace_name: string | null;
  created_by: number | null;
  created_at: string;
  updated_at: string;
  access?: AccessInfo | null;
}

export interface DashboardCreatePayload {
  name: string;
  description?: string | null;
  namespace_id?: number | null;
  layout?: DashboardLayout;
}

export interface DashboardUpdatePayload {
  name?: string;
  description?: string | null;
  namespace_id?: number | null;
  layout?: DashboardLayout;
  /** CAS guard: server 409s with detail.error "stale_write" when layout_version has moved on. */
  expected_layout_version?: number;
}

/** Current grid resolution. v1 layouts used 12 columns; see gridVersion.ts for the upgrade. */
export const DASHBOARD_GRID_COLS = 48;
export const DASHBOARD_GRID_VERSION = 2;
export const DASHBOARD_GRID_ROW_HEIGHT = 40;

export const EMPTY_DASHBOARD_LAYOUT: DashboardLayout = {
  tiles: [],
  grid: {
    cols: DASHBOARD_GRID_COLS,
    row_height: DASHBOARD_GRID_ROW_HEIGHT,
    version: DASHBOARD_GRID_VERSION,
  },
  filters: [],
};
