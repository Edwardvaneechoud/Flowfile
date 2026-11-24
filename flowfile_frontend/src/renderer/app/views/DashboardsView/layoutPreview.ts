import {
  DASHBOARD_GRID_COLS,
  DASHBOARD_GRID_ROW_HEIGHT,
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
  /** An image tile that holds a picture; the library list carries no image bytes, so a card shows an icon. */
  hasImage: boolean;
  /** A KPI that draws a trend line under its number. */
  hasTrend: boolean;
  /** A text or KPI tile's own colours. */
  bgColor: string | null;
  textColor: string | null;
  /** Percentages of the preview box, so the miniature scales with the card. */
  left: number;
  top: number;
  width: number;
  height: number;
}

/** What a card knows about a chart tile's chart, from the catalog's chart library. */
export interface PreviewChart {
  /** The saved chart picture; null draws a sketch of the chart type instead. */
  thumbnail: string | null;
  chartType: string | null;
}

export interface LayoutPreview {
  rects: PreviewRect[];
  /** Tiles reach below the box, so the card fades its bottom edge. */
  clipped: boolean;
}

/** Width and height of the preview box; the card's CSS uses the same ratio. */
export const PREVIEW_ASPECT = 16 / 10;
/** The canvas width the preview stands for, so tiles keep the proportions they have on screen. */
const REFERENCE_CANVAS_PX = 1200;

/** Grid rows that fit in the box at the reference width: the dashboard's first screen. */
export function previewRows(rowHeight: number): number {
  const height = rowHeight > 0 ? rowHeight : DASHBOARD_GRID_ROW_HEIGHT;
  return REFERENCE_CANVAS_PX / PREVIEW_ASPECT / height;
}

/**
 * Turn a dashboard layout into normalised rectangles for a miniature of its first screen.
 * Every card draws at the same scale, so a tile keeps its on-screen shape and a long dashboard
 * is cut at the box's bottom rather than squeezed into it. Legacy grids are upgraded first, and a
 * dashboard with tabs previews its first tab.
 */
export function buildLayoutPreview(layout: DashboardLayout): LayoutPreview {
  const current = upgradeLayoutGrid(layout);
  const tiles = tilesOfTab(current, resolveTabId(current, null));
  if (!tiles.length) return { rects: [], clipped: false };
  const stored = current.grid?.cols;
  const cols = stored && stored > 0 ? stored : DASHBOARD_GRID_COLS;
  const rows = previewRows(current.grid?.row_height ?? DASHBOARD_GRID_ROW_HEIGHT);
  const rects = tiles
    .filter((t) => t.y < rows)
    .map((t) => ({
      id: t.id,
      type: t.type,
      vizId: t.viz_id ?? null,
      orientation: t.orientation ?? "horizontal",
      hasImage: t.type === "image" && t.image != null,
      hasTrend: t.type === "kpi" && t.kpi?.trend != null,
      bgColor: t.type === "text" || t.type === "kpi" ? (t.bg_color ?? null) : null,
      textColor: t.type === "text" || t.type === "kpi" ? (t.text_color ?? null) : null,
      left: (t.x / cols) * 100,
      top: (t.y / rows) * 100,
      width: (t.w / cols) * 100,
      height: (t.h / rows) * 100,
    }));
  const clipped = tiles.some((t) => t.y + t.h > rows);
  return { rects, clipped };
}
