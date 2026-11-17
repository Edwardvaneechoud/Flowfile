import type { DashboardTableColumn, DashboardTileType } from "../../types";

/** What a viewer can do with one tile, from its "⋯" menu. */
export type TileAction = "expand" | "see-rows" | "download-csv" | "download-image";

export const TILE_ACTION_LABELS: Record<TileAction, string> = {
  expand: "Expand",
  "see-rows": "See rows",
  "download-csv": "Download CSV",
  "download-image": "Download image",
};

export interface TileActionContext {
  type: DashboardTileType;
  /** The tile names a source and, for a KPI, a complete measure. */
  configured: boolean;
  /** Core answered that the tile's chart or table is gone. */
  missing: boolean;
  /** A chart tile has a chart drawn at its index. */
  hasChart?: boolean;
}

/**
 * The entries a tile's menu offers, in menu order. Text, separator and filter tiles get none, nor
 * does a tile with nothing to show. A chart's CSV is the data it draws; a KPI's rows are in See rows.
 */
export const tileActions = (ctx: TileActionContext): TileAction[] => {
  if (!ctx.configured || ctx.missing) return [];
  if (ctx.type === "viz") {
    return ctx.hasChart ? ["expand", "see-rows", "download-csv", "download-image"] : [];
  }
  if (ctx.type === "kpi") return ["expand", "see-rows"];
  if (ctx.type === "table") return ["expand", "download-csv"];
  return [];
};

/** A download's file name: the title made safe, else ``fallback``. */
export const downloadName = (
  title: string | null | undefined,
  extension: string,
  fallback: string,
): string => {
  const base = (title ?? "")
    .trim()
    .replace(/[^\w\- ]+/g, "")
    .replace(/\s+/g, "_");
  return `${base || fallback}.${extension}`;
};

/** The columns of a query result in the order they first appear, for its CSV. */
export const resultColumns = (rows: Record<string, unknown>[]): DashboardTableColumn[] => {
  const seen = new Set<string>();
  for (const row of rows) for (const key of Object.keys(row)) seen.add(key);
  return Array.from(seen, (field) => ({ field }));
};
