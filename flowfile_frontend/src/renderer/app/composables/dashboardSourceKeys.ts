import type { DashboardFilter } from "../types";

/** A tile's or a filter's data source: a catalog table, or a chart with no table (a SQL chart). */
export type TileSource = { kind: "table"; tableId: number } | { kind: "viz"; vizId: number };

/**
 * Which source a tile reads, as filters compare it: `table:<id>` for a catalog table (read
 * directly or through a chart on it), `viz:<id>` for a chart with no table (a SQL chart).
 */
export type DatasourceKey = string;

export const tableSourceKey = (tableId: number): DatasourceKey => `table:${tableId}`;
export const vizSourceKey = (vizId: number): DatasourceKey => `viz:${vizId}`;

/** The source a filter is bound to, or null for a legacy untied filter. */
export const filterSourceOf = (f: DashboardFilter): TileSource | null => {
  if (f.datasource_id != null) return { kind: "table", tableId: f.datasource_id };
  if (f.datasource_viz_id != null) return { kind: "viz", vizId: f.datasource_viz_id };
  return null;
};

export const filterSourceKey = (f: DashboardFilter): DatasourceKey | null => {
  const source = filterSourceOf(f);
  if (source == null) return null;
  return source.kind === "table" ? tableSourceKey(source.tableId) : vizSourceKey(source.vizId);
};
