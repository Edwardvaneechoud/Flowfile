// Dashboards — TS mirror of flowfile_core/schemas/catalog_schema.py

import type { AccessInfo } from "./sharing.types";

export type DashboardTileType = "viz" | "text" | "separator";
export type SeparatorOrientation = "horizontal" | "vertical";

export interface DashboardTile {
  /** Client-generated UUID, stable across saves so component state survives. */
  id: string;
  type: DashboardTileType;
  /** Required when type === "viz". */
  viz_id: number | null;
  /** Which entry of CatalogVisualization.spec[] to render. */
  chart_index: number;
  /** Markdown source when type === "text". */
  text_md?: string | null;
  /** CSS colors for text tiles. */
  bg_color?: string | null;
  text_color?: string | null;
  /** Separator styling (type === "separator"); absent means a 1px default-colour horizontal line. */
  orientation?: SeparatorOrientation | null;
  thickness?: number | null;
  line_color?: string | null;
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
   * "datasource" (default, also when absent on legacy layouts) applies only
   * to tiles reading from `datasource_id`; "all" applies to every targeted
   * tile whose data has a column named `field_name` of a compatible type.
   */
  scope?: DashboardFilterScope;
}

export type DashboardFilterScope = "datasource" | "all";

export interface DashboardLayout {
  tiles: DashboardTile[];
  grid: DashboardGrid;
  filters: DashboardFilter[];
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
