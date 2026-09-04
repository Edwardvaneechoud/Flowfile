import { ref, type ComputedRef, type Ref } from "vue";
import type { DashboardLayout, DashboardTile } from "../types";

/** Increment the refresh nonce of every viz on the dashboard, once per
 * distinct viz id (text tiles skipped). Returns a new object so the views
 * can immutably replace their nonce map, matching the editor's per-viz bump. */
export function bumpVizNonces(
  nonces: Record<number, number>,
  tiles: DashboardTile[],
): Record<number, number> {
  const next = { ...nonces };
  const seen = new Set<number>();
  for (const tile of tiles) {
    if (tile.viz_id == null || seen.has(tile.viz_id)) continue;
    seen.add(tile.viz_id);
    next[tile.viz_id] = (next[tile.viz_id] ?? 0) + 1;
  }
  return next;
}

export interface DashboardRefreshOptions {
  layout: Ref<DashboardLayout> | ComputedRef<DashboardLayout>;
  vizRefreshNonces: Ref<Record<number, number>>;
  refreshDatasources: (force?: boolean) => Promise<void>;
}

/** Manual dashboard refresh: force-refetch the datasource metadata, then bump the tile + filter-stats nonces so every
 * mounted tile and filter re-queries. Nonces bump even when the datasource
 * refetch rejects (tiles surface their own errors); the rejection still
 * propagates so the view can toast. */
export function useDashboardRefresh(opts: DashboardRefreshOptions) {
  const refreshing = ref(false);
  const statsRefreshNonce = ref(0);

  const refreshAll = async (): Promise<void> => {
    refreshing.value = true;
    try {
      await opts.refreshDatasources(true);
    } finally {
      statsRefreshNonce.value += 1;
      opts.vizRefreshNonces.value = bumpVizNonces(
        opts.vizRefreshNonces.value,
        opts.layout.value.tiles,
      );
      refreshing.value = false;
    }
  };

  return { refreshing, statsRefreshNonce, refreshAll };
}
