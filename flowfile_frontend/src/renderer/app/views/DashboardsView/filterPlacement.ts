import type { DashboardFilter, DashboardLayout, DashboardTile } from "../../types";
import { filterSourceKey, type DatasourceKey } from "../../composables/dashboardSourceKeys";
import { firstFreeRow } from "./gridDrop";

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
    y: cell.y < 0 ? firstFreeRow(layout.tiles) : cell.y,
    w,
    h,
  };
  return { ...layout, tiles: [...layout.tiles, tile] };
};

const overlaps = (
  a: { x: number; y: number; w: number; h: number },
  b: { x: number; y: number; w: number; h: number },
): boolean => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;

/**
 * Add a new filter and its tile in one layout change, the Insert list's Filter. A cell that a tile
 * already covers moves the new tile down its own columns to the first row under them, so it never
 * pushes the author's tiles; a negative `y` takes the next free row.
 */
export const addPlacedFilter = (
  layout: DashboardLayout,
  filter: DashboardFilter,
  cell: { x: number; y: number },
  tileId: string,
): DashboardLayout => {
  if (layout.filters.some((f) => f.id === filter.id)) return layout;
  const { w, h } = FILTER_TILE_DEFAULT_SIZE;
  const x = Math.max(0, Math.min(cell.x, layout.grid.cols - w));
  let y = cell.y;
  if (y >= 0 && layout.tiles.some((t) => overlaps(t, { x, y, w, h }))) {
    const under = layout.tiles.filter((t) => t.x < x + w && x < t.x + t.w);
    y = Math.max(...under.map((t) => t.y + t.h));
  }
  return placeFilter(
    { ...layout, filters: [...layout.filters, filter] },
    filter.id,
    { x, y },
    tileId,
  );
};

/**
 * The edit-mode hint for each filter that filters nothing because no tile on the dashboard reads
 * its source (scope "all": because no tile reads data at all), keyed by filter id. `tileKeys` are
 * the source keys of every tile that reads data, null for one not resolved yet, which keeps the
 * hint off while it may still match. Untied filters and those whose chosen tiles are gone have
 * their own hints and are left out.
 */
export const filtersWithoutSourceTiles = (
  filters: DashboardFilter[],
  tileKeys: (DatasourceKey | null)[],
  sourceName: (key: DatasourceKey) => string,
): Record<string, string> => {
  const out: Record<string, string> = {};
  for (const f of filters) {
    const key = filterSourceKey(f);
    if (key == null || targetsNoTile(f)) continue;
    if (f.scope === "all") {
      if (!tileKeys.length) {
        out[f.id] = "No tile on this dashboard reads data yet, so this filter filters nothing.";
      }
      continue;
    }
    if (tileKeys.includes(key) || tileKeys.includes(null)) continue;
    out[f.id] =
      `No tile on this dashboard reads ${sourceName(key)} yet, so this filter filters nothing. ` +
      "A tile you add on it is filtered like the others.";
  }
  return out;
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
