import type { DashboardFilter, DashboardLayout, DashboardTile } from "../../types";

/** A filter tile's footprint when it is dropped or placed: a label over one control. */
export const FILTER_TILE_DEFAULT_SIZE = { w: 12, h: 2 } as const;

export const isFilterTile = (tile: DashboardTile): boolean => tile.type === "filter";

/** The filter a filter tile shows, or null when the tile names none of the layout's filters. */
export const filterOfTile = (
  tile: DashboardTile,
  filters: DashboardFilter[],
): DashboardFilter | null =>
  isFilterTile(tile) ? (filters.find((f) => f.id === tile.filter_id) ?? null) : null;

/** Ids of the filters a tile shows on the canvas; those filters leave the strip. */
export const placedFilterIds = (layout: DashboardLayout): Set<string> => {
  const known = new Set(layout.filters.map((f) => f.id));
  const out = new Set<string>();
  for (const tile of layout.tiles) {
    if (isFilterTile(tile) && tile.filter_id != null && known.has(tile.filter_id)) {
      out.add(tile.filter_id);
    }
  }
  return out;
};

/** The filters the strip shows, in the layout's order. */
export const unplacedFilters = (layout: DashboardLayout): DashboardFilter[] => {
  const placed = placedFilterIds(layout);
  return layout.filters.filter((f) => !placed.has(f.id));
};

const nextFreeRow = (tiles: DashboardTile[]): number =>
  tiles.length ? Math.max(...tiles.map((t) => t.y + t.h)) : 0;

/**
 * Put a filter on the canvas at a grid cell (a negative `y` takes the next free row). A filter
 * that is unknown or already on the canvas leaves the layout as it is: one tile per filter.
 */
export const placeFilter = (
  layout: DashboardLayout,
  filterId: string,
  cell: { x: number; y: number },
  tileId: string,
): DashboardLayout => {
  if (!layout.filters.some((f) => f.id === filterId)) return layout;
  if (placedFilterIds(layout).has(filterId)) return layout;
  const { w, h } = FILTER_TILE_DEFAULT_SIZE;
  const cols = layout.grid.cols;
  const tile: DashboardTile = {
    id: tileId,
    type: "filter",
    viz_id: null,
    chart_index: 0,
    filter_id: filterId,
    x: Math.max(0, Math.min(cell.x, cols - w)),
    y: cell.y < 0 ? nextFreeRow(layout.tiles) : cell.y,
    w,
    h,
  };
  return { ...layout, tiles: [...layout.tiles, tile] };
};

/** Take a filter off the canvas; it keeps its state and shows in the strip again. */
export const moveFilterToBar = (layout: DashboardLayout, filterId: string): DashboardLayout => {
  const tiles = layout.tiles.filter((t) => !(isFilterTile(t) && t.filter_id === filterId));
  return tiles.length === layout.tiles.length ? layout : { ...layout, tiles };
};

/** Replace the layout's filters; a filter tile whose filter is gone goes with it. */
export const withFilters = (
  layout: DashboardLayout,
  filters: DashboardFilter[],
): DashboardLayout => {
  const known = new Set(filters.map((f) => f.id));
  const tiles = layout.tiles.filter(
    (t) => !isFilterTile(t) || (t.filter_id != null && known.has(t.filter_id)),
  );
  return { ...layout, filters, tiles: tiles.length === layout.tiles.length ? layout.tiles : tiles };
};

/** Delete a filter from the dashboard, and its tile when it is on the canvas. */
export const deleteFilter = (layout: DashboardLayout, filterId: string): DashboardLayout =>
  withFilters(
    layout,
    layout.filters.filter((f) => f.id !== filterId),
  );

/** True for a filter whose explicit target list is empty: it filters no tile. */
export const targetsNoTile = (filter: DashboardFilter): boolean =>
  filter.target === "tiles" && filter.target_tile_ids.length === 0;

/**
 * Drop every filter target that names a tile the layout no longer holds, the rule the server
 * enforces at save. A filter whose last target goes stays `target: "tiles"` with an empty list,
 * so it filters no tile rather than all of them. The same layout comes back when nothing changed.
 */
export const pruneFilterTargets = (layout: DashboardLayout): DashboardLayout => {
  const tileIds = new Set(layout.tiles.map((t) => t.id));
  let changed = false;
  const filters = layout.filters.map((f) => {
    const kept = f.target_tile_ids.filter((id) => tileIds.has(id));
    if (kept.length === f.target_tile_ids.length) return f;
    changed = true;
    return { ...f, target_tile_ids: kept };
  });
  return changed ? { ...layout, filters } : layout;
};
