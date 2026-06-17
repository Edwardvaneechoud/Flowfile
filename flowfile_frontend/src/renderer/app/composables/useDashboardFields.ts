import { computed, ref, watch, type ComputedRef, type Ref } from "vue";
import { CatalogApi } from "../api/catalog.api";
import type { DashboardLayout } from "../types";

export type SemanticType = "quantitative" | "nominal" | "ordinal" | "temporal";

export interface DashboardField {
  fid: string;
  name: string;
  semanticType: SemanticType;
  /** IDs of dashboard tiles whose source contains this field. */
  tileIds: string[];
  /** IDs of viz that contain this field (used to dedupe identical sources). */
  vizIds: number[];
}

interface RawField {
  fid: string;
  name?: string;
  semanticType?: SemanticType | "?";
  analyticType?: string;
}

const isSemantic = (s: unknown): s is SemanticType =>
  s === "quantitative" || s === "nominal" || s === "ordinal" || s === "temporal";

/** Aggregate the IMutField list across every viz referenced on the
 * dashboard. Keys by ``fid`` so columns with the same id across multiple
 * tiles collapse into one entry; the entry tracks which tiles/viz it
 * came from so the filter UI can scope by membership.
 *
 * Per-viz fetches are cached locally — the same viz_id only hits the
 * worker once. */
export function useDashboardFields(
  layout: Ref<DashboardLayout> | ComputedRef<DashboardLayout>,
) {
  const fieldsByVizId = ref<Record<number, RawField[]>>({});
  const loading = ref(false);

  const fetchVizFields = async (vizId: number) => {
    if (fieldsByVizId.value[vizId]) return;
    try {
      const resp = await CatalogApi.getSavedVisualizationFields(vizId);
      fieldsByVizId.value[vizId] = (resp.fields as RawField[]) ?? [];
    } catch (err) {
      console.warn(`[dashboard] could not fetch fields for viz ${vizId}:`, err);
      fieldsByVizId.value[vizId] = [];
    }
  };

  const refresh = async () => {
    const ids = Array.from(
      new Set(
        layout.value.tiles
          .map((t) => t.viz_id)
          .filter((id): id is number => id != null),
      ),
    );
    loading.value = true;
    try {
      await Promise.all(ids.map(fetchVizFields));
    } finally {
      loading.value = false;
    }
  };

  watch(
    () => layout.value.tiles.map((t) => t.viz_id).filter((id): id is number => id != null),
    () => {
      refresh();
    },
    { immediate: true },
  );

  const availableFields = computed<DashboardField[]>(() => {
    const acc = new Map<string, DashboardField>();
    for (const tile of layout.value.tiles) {
      if (tile.viz_id == null) continue;
      const fields = fieldsByVizId.value[tile.viz_id] ?? [];
      for (const f of fields) {
        if (!isSemantic(f.semanticType)) continue;
        const existing = acc.get(f.fid);
        if (existing) {
          if (!existing.tileIds.includes(tile.id)) existing.tileIds.push(tile.id);
          if (!existing.vizIds.includes(tile.viz_id)) existing.vizIds.push(tile.viz_id);
        } else {
          acc.set(f.fid, {
            fid: f.fid,
            name: f.name ?? f.fid,
            semanticType: f.semanticType,
            tileIds: [tile.id],
            vizIds: [tile.viz_id],
          });
        }
      }
    }
    return Array.from(acc.values()).sort((a, b) => a.fid.localeCompare(b.fid));
  });

  /** Returns true when this tile's source contains the given fid. */
  const tileHasField = (tileId: string, fid: string): boolean => {
    const tile = layout.value.tiles.find((t) => t.id === tileId);
    if (!tile || tile.viz_id == null) return false;
    const fields = fieldsByVizId.value[tile.viz_id] ?? [];
    return fields.some((f) => f.fid === fid);
  };

  return { availableFields, loading, refresh, tileHasField };
}

export const semanticToKind = (
  s: SemanticType,
): "categorical" | "numeric_range" | "date_range" => {
  if (s === "quantitative") return "numeric_range";
  if (s === "temporal") return "date_range";
  return "categorical";
};
