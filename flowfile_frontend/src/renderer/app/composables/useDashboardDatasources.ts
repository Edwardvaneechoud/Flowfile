import { computed, ref, watch, type ComputedRef, type Ref } from "vue";
import { CatalogApi } from "../api/catalog.api";
import type { ColumnSchema, ColumnStatsResponse, DashboardLayout } from "../types";

export interface DashboardDatasource {
  id: number;
  name: string;
  schema_columns: ColumnSchema[];
}

/** Resolves dashboard tiles → catalog tables.
 *
 * A tile carries ``viz_id`` only; we walk viz → ``catalog_table_id`` →
 * ``CatalogTable`` to discover (a) which catalog tables back the
 * dashboard and (b) the columns + dtypes available on each. SQL-source
 * visualizations have no ``catalog_table_id`` and therefore do not
 * surface as filterable datasources.
 *
 * Per-id fetches are cached for the lifetime of the composable.
 */
export function useDashboardDatasources(
  layout: Ref<DashboardLayout> | ComputedRef<DashboardLayout>,
) {
  // viz_id → catalog_table_id (null for SQL-source viz, or unresolved)
  const vizToTable = ref<Record<number, number | null>>({});
  // viz_id → display name
  const vizNameById = ref<Record<number, string>>({});
  // table_id → DashboardDatasource
  const tableCache = ref<Record<number, DashboardDatasource>>({});
  const loading = ref(false);

  const fetchViz = async (vizId: number) => {
    if (vizId in vizToTable.value) return;
    try {
      const viz = await CatalogApi.getVisualization(vizId);
      vizToTable.value[vizId] =
        viz.source_type === "table" ? (viz.catalog_table_id ?? null) : null;
      vizNameById.value[vizId] = viz.name;
    } catch (err) {
      console.warn(`[dashboard] could not fetch viz ${vizId}:`, err);
      vizToTable.value[vizId] = null;
    }
  };

  const fetchTable = async (tableId: number) => {
    if (tableId in tableCache.value) return;
    try {
      const table = await CatalogApi.getTable(tableId);
      tableCache.value[tableId] = {
        id: table.id,
        name: table.full_table_name ?? table.name,
        schema_columns: table.schema_columns ?? [],
      };
    } catch (err) {
      console.warn(`[dashboard] could not fetch table ${tableId}:`, err);
    }
  };

  const refresh = async () => {
    const vizIds = Array.from(
      new Set(
        layout.value.tiles.map((t) => t.viz_id).filter((id): id is number => id != null),
      ),
    );
    loading.value = true;
    try {
      await Promise.all(vizIds.map(fetchViz));
      const tableIds = Array.from(
        new Set(
          vizIds
            .map((v) => vizToTable.value[v])
            .filter((id): id is number => id != null),
        ),
      );
      await Promise.all(tableIds.map(fetchTable));
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

  const datasourcesInUse = computed<DashboardDatasource[]>(() => {
    const seen = new Set<number>();
    for (const tile of layout.value.tiles) {
      if (tile.viz_id == null) continue;
      const tid = vizToTable.value[tile.viz_id];
      if (tid != null) seen.add(tid);
    }
    return Array.from(seen)
      .map((id) => tableCache.value[id])
      .filter((d): d is DashboardDatasource => !!d)
      .sort((a, b) => a.name.localeCompare(b.name));
  });

  const tilesByDatasource = computed<Record<number, string[]>>(() => {
    const acc: Record<number, string[]> = {};
    for (const tile of layout.value.tiles) {
      if (tile.viz_id == null) continue;
      const tid = vizToTable.value[tile.viz_id];
      if (tid == null) continue;
      (acc[tid] ??= []).push(tile.id);
    }
    return acc;
  });

  const tileDatasource = (tileId: string): number | null => {
    const tile = layout.value.tiles.find((t) => t.id === tileId);
    if (!tile || tile.viz_id == null) return null;
    return vizToTable.value[tile.viz_id] ?? null;
  };

  const getSchema = (tableId: number): ColumnSchema[] =>
    tableCache.value[tableId]?.schema_columns ?? [];

  const getDatasource = (tableId: number): DashboardDatasource | null =>
    tableCache.value[tableId] ?? null;

  // (table_id, column_name) -> stats. Promise caching dedupes concurrent
  // requests; the resolved Promise stays in the cache so subsequent
  // lookups are synchronous.
  const statsCache = new Map<string, Promise<ColumnStatsResponse>>();
  const statsKey = (tableId: number, column: string) => `${tableId}:${column}`;

  const getColumnStats = (
    tableId: number,
    column: string,
    limit = 100,
  ): Promise<ColumnStatsResponse> => {
    const key = statsKey(tableId, column);
    let entry = statsCache.get(key);
    if (!entry) {
      entry = CatalogApi.getTableColumnStats(tableId, column, limit).catch((err) => {
        // Drop failures from the cache so the user can retry by re-opening
        // the filter; otherwise we'd lock the row to "no data".
        statsCache.delete(key);
        throw err;
      });
      statsCache.set(key, entry);
    }
    return entry;
  };

  const tileLabel = (tileId: string): string => {
    const tile = layout.value.tiles.find((t) => t.id === tileId);
    if (!tile) return tileId;
    if (tile.type === "text") return "Text tile";
    if (tile.viz_id == null) return `Tile ${tileId.slice(0, 6)}`;
    return vizNameById.value[tile.viz_id] ?? `Viz #${tile.viz_id}`;
  };

  return {
    datasourcesInUse,
    tilesByDatasource,
    tileDatasource,
    tileLabel,
    getSchema,
    getDatasource,
    getColumnStats,
    loading,
    refresh,
  };
}
