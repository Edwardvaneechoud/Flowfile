import {
  DASHBOARD_GRID_COLS,
  type DashboardLayout,
  type DashboardTileType,
  type SeparatorOrientation,
} from "../../types";
import { upgradeLayoutGrid } from "./gridVersion";
import { resolveTabId, tilesOfTab } from "./dashboardTabs";

export interface PreviewRect {
  id: string;
  type: DashboardTileType;
  vizId: number | null;
  orientation: SeparatorOrientation;
  /** Percentages of the preview box, so the wireframe scales with the card. */
  left: number;
  top: number;
  width: number;
  height: number;
}

/** Rows shown even for a short layout, so a single small tile does not fill the whole box. */
const MIN_PREVIEW_ROWS = 8;

/**
 * Turn a dashboard layout into normalised rectangles for a miniature wireframe.
 * Legacy grids are upgraded first so old and new layouts land on the same columns. A dashboard
 * with tabs previews its first tab.
 */
export function buildLayoutPreview(layout: DashboardLayout): PreviewRect[] {
  const current = upgradeLayoutGrid(layout);
  const tiles = tilesOfTab(current, resolveTabId(current, null));
  if (!tiles.length) return [];
  const stored = current.grid?.cols;
  const cols = stored && stored > 0 ? stored : DASHBOARD_GRID_COLS;
  const rows = Math.max(MIN_PREVIEW_ROWS, ...tiles.map((t) => t.y + t.h));
  return tiles.map((t) => ({
    id: t.id,
    type: t.type,
    vizId: t.viz_id ?? null,
    orientation: t.orientation ?? "horizontal",
    left: (t.x / cols) * 100,
    top: (t.y / rows) * 100,
    width: (t.w / cols) * 100,
    height: (t.h / rows) * 100,
  }));
}
