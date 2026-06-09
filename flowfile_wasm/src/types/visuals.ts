/**
 * Visuals + Dashboards — browser-only port of flowfile_frontend's catalog
 * visualizations and dashboards. IDs are client UUIDs (no backend rows) and a
 * visual binds to a catalog dataset by NAME (the WASM "table"). Persisted in
 * localStorage (see visuals-store.ts / dashboards-store.ts).
 *
 * Mirrors flowfile_frontend/src/renderer/app/types/{catalog,dashboard}.types.ts,
 * trimmed to the table-source + client-compute path the WASM build supports.
 */

/** Where a visual's rows come from. SQL is deferred (no worker in-browser). */
export type VizSourceKind = 'table'

/** Which catalog map backs the dataset name. */
export type DatasetKind = 'catalog' | 'external'

/** A saved chart — GraphicWalker spec(s) bound to a catalog dataset. */
export interface SavedVisual {
  id: string
  name: string
  description: string | null
  chart_type: string | null
  /** GraphicWalker IChart[] — one entry per chart tab. */
  spec: Record<string, any>[]
  spec_gw_version: string | null
  source_type: VizSourceKind
  /** Catalog/external dataset name (↔ frontend's catalog_table_id). */
  dataset_name: string
  source_kind: DatasetKind
  /** Base64 PNG data URL captured on save (small, downscaled). */
  thumbnail_data_url: string | null
  createdAt: number
  updatedAt: number
}

/** Create/update payloads (the editor never sets ids/timestamps). */
export interface VisualizationCreatePayload {
  name: string
  description?: string | null
  chart_type?: string | null
  spec: Record<string, any>[]
  spec_gw_version?: string | null
  source_type: VizSourceKind
  dataset_name: string
  source_kind: DatasetKind
  thumbnail_data_url?: string | null
}

export interface VisualizationUpdatePayload {
  name?: string
  description?: string | null
  chart_type?: string | null
  spec?: Record<string, any>[]
  spec_gw_version?: string | null
  thumbnail_data_url?: string | null
}

/** What the source picker hands the editor. */
export interface VizSourceDescriptor {
  source_type: VizSourceKind
  dataset_name: string
  source_kind: DatasetKind
}

// Dashboards — TS mirror of dashboard.types.ts (viz_id is a string here).

export type DashboardTileType = 'viz' | 'text'

export interface DashboardTile {
  /** Client UUID, stable across saves so component state survives. */
  id: string
  type: DashboardTileType
  /** Required when type === "viz". References SavedVisual.id. */
  viz_id: string | null
  /** Which entry of SavedVisual.spec[] to render. */
  chart_index: number
  /** Markdown source when type === "text". */
  text_md?: string | null
  bg_color?: string | null
  text_color?: string | null
  x: number
  y: number
  w: number
  h: number
}

export interface DashboardGrid {
  cols: number
  row_height: number
  version: number
}

export type DashboardFilterKind = 'categorical' | 'date_range' | 'numeric_range'

export interface DashboardFilter {
  id: string
  field_name: string
  label?: string | null
  kind: DashboardFilterKind
  /** Widget-specific state; shape depends on `kind`. */
  state: Record<string, unknown>
  target: 'all' | 'tiles'
  target_tile_ids: string[]
  /** When set, the filter only applies to tiles whose viz reads this dataset. */
  datasource_name: string | null
}

export interface DashboardLayout {
  tiles: DashboardTile[]
  grid: DashboardGrid
  filters: DashboardFilter[]
}

export interface Dashboard {
  id: string
  name: string
  layout: DashboardLayout
  createdAt: number
  updatedAt: number
}

export const EMPTY_DASHBOARD_LAYOUT: DashboardLayout = {
  tiles: [],
  grid: { cols: 12, row_height: 40, version: 1 },
  filters: [],
}
