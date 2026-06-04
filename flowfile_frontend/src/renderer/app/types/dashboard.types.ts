// Dashboards — TS mirror of flowfile_core/schemas/catalog_schema.py
// (DashboardTile / DashboardGrid / DashboardFilter / DashboardLayout / Dashboard).

export type DashboardTileType = "viz" | "text";

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
}

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
}

export const EMPTY_DASHBOARD_LAYOUT: DashboardLayout = {
  tiles: [],
  grid: { cols: 12, row_height: 40, version: 1 },
  filters: [],
};
