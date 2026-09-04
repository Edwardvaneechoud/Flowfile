import { DASHBOARD_GRID_COLS, DASHBOARD_GRID_VERSION, type DashboardLayout } from "../../types";

/**
 * Bring a stored layout up to the current grid resolution. Version-1 layouts
 * were placed on 12 columns; scaling x and w by the column ratio keeps every
 * tile where it was while allowing finer placement (a vertical separator can
 * now be one narrow column). Already-current layouts are returned as-is.
 */
export function upgradeLayoutGrid(layout: DashboardLayout): DashboardLayout {
  const cols = layout.grid?.cols ?? 12;
  const version = layout.grid?.version ?? 1;
  if (version >= DASHBOARD_GRID_VERSION && cols === DASHBOARD_GRID_COLS) return layout;
  const factor = DASHBOARD_GRID_COLS / cols;
  if (!Number.isFinite(factor) || factor <= 0) return layout;
  return {
    ...layout,
    grid: { ...layout.grid, cols: DASHBOARD_GRID_COLS, version: DASHBOARD_GRID_VERSION },
    tiles: layout.tiles.map((t) => ({
      ...t,
      x: Math.round(t.x * factor),
      w: Math.max(1, Math.round(t.w * factor)),
    })),
  };
}
