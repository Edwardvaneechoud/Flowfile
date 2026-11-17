import { computed, ref, watch, type ComputedRef, type Ref } from "vue";
import { CatalogApi } from "../api/catalog.api";
import type { ColumnSchema, ColumnStatsResponse, DashboardLayout, DashboardTile } from "../types";
import { tileSourceOf, type TileField } from "./useDashboardComputation";
import { createDashboardVizCache, type DashboardVizCache } from "./useDashboardVizCache";

export interface DashboardDatasource {
  id: number;
  name: string;
  schema_columns: ColumnSchema[];
  /** When the table's data last changed, as the catalog recorded it. */
  updated_at: string | null;
}

/** Resolves dashboard tiles → catalog tables.
 *
 * A KPI or table tile may name its table directly (``table_id``); every other
 * tile goes through its chart: viz → ``catalog_table_id`` → ``CatalogTable``.
 * Together they say (a) which catalog tables back the dashboard and (b) the
 * columns + dtypes available on each. SQL-source visualizations have no
 * ``catalog_table_id`` and therefore do not surface as filterable datasources.
 *
 * Per-id fetches are cached for the lifetime of the composable.
 */
export function useDashboardDatasources(
  layout: Ref<DashboardLayout> | ComputedRef<DashboardLayout>,
  vizCache: DashboardVizCache = createDashboardVizCache(),
) {
  // viz_id → catalog_table_id (null for SQL-source viz, or unresolved)
  const vizToTable = ref<Record<number, number | null>>({});
  // viz_id → display name
  const vizNameById = ref<Record<number, string>>({});
  // table_id → DashboardDatasource
  const tableCache = ref<Record<number, DashboardDatasource>>({});
  const loading = ref(false);

  // Force refetches assign on resolve (never clear-then-fetch): a transient
  // null datasource would change tiles' filterKey and double-remount them.
  const fetchViz = async (vizId: number, force = false) => {
    if (!force && vizId in vizToTable.value) return;
    try {
      const viz = await vizCache.getViz(vizId);
      vizToTable.value[vizId] = viz.source_type === "table" ? (viz.catalog_table_id ?? null) : null;
      vizNameById.value[vizId] = viz.name;
    } catch (err) {
      console.warn(`[dashboard] could not fetch viz ${vizId}:`, err);
      if (!(vizId in vizToTable.value)) vizToTable.value[vizId] = null;
    }
  };

  // Two refreshes can overlap; the second waits for the first one's request.
  const tableRequests = new Map<number, Promise<void>>();

  const fetchTable = (tableId: number, force = false): Promise<void> => {
    if (!force && tableId in tableCache.value) return Promise.resolve();
    const pending = tableRequests.get(tableId);
    if (pending) return pending;
    const request = (async () => {
      try {
        const table = await vizCache.getTable(tableId);
        tableCache.value[tableId] = {
          id: table.id,
          name: table.full_table_name ?? table.name,
          schema_columns: table.schema_columns ?? [],
          updated_at: table.updated_at ?? null,
        };
      } catch (err) {
        console.warn(`[dashboard] could not fetch table ${tableId}:`, err);
      } finally {
        tableRequests.delete(tableId);
      }
    })();
    tableRequests.set(tableId, request);
    return request;
  };

  // (table_id, column_name) -> stats. Promise caching dedupes concurrent
  // requests; the resolved Promise stays in the cache so subsequent
  // lookups are synchronous.
  const statsCache = new Map<string, Promise<ColumnStatsResponse>>();
  const statsKey = (tableId: number, column: string) => `${tableId}:${column}`;

  // viz_id → columns of the viz's data; filled on demand by loadTileFields
  const vizFields = ref<Record<number, TileField[]>>({});

  const fetchFields = async (vizId: number) => {
    if (vizId in vizFields.value) return;
    try {
      const resp = await vizCache.getFields(vizId);
      vizFields.value[vizId] = (resp.fields ?? []).map((f) => ({
        fid: String(f.fid),
        semanticType: f.semanticType,
      }));
    } catch (err) {
      console.warn(`[dashboard] could not fetch fields for viz ${vizId}:`, err);
    }
  };

  // table_id → columns of a table read directly; filled on demand by loadTileFields
  const directTableFields = ref<Record<number, TileField[]>>({});

  const fetchTableFields = async (tableId: number) => {
    if (tableId in directTableFields.value) return;
    try {
      const resp = await vizCache.getTableFields(tableId);
      directTableFields.value[tableId] = (resp.fields ?? []).map((f) => ({
        fid: String(f.fid),
        semanticType: f.semanticType,
      }));
    } catch (err) {
      console.warn(`[dashboard] could not fetch fields for table ${tableId}:`, err);
    }
  };

  const sourceIds = () => {
    const vizIds = new Set<number>();
    const tableIds = new Set<number>();
    for (const tile of layout.value.tiles) {
      const source = tileSourceOf(tile);
      if (source?.kind === "viz") vizIds.add(source.vizId);
      else if (source?.kind === "table") tableIds.add(source.tableId);
    }
    return { vizIds: [...vizIds], tableIds: [...tableIds] };
  };

  const refresh = async (force = false) => {
    if (force) {
      statsCache.clear();
      vizFields.value = {};
      directTableFields.value = {};
      vizCache.invalidate();
    }
    const { vizIds, tableIds: direct } = sourceIds();
    loading.value = true;
    try {
      await Promise.all(vizIds.map((id) => fetchViz(id, force)));
      const tableIds = Array.from(
        new Set([
          ...direct,
          ...vizIds.map((v) => vizToTable.value[v]).filter((id): id is number => id != null),
        ]),
      );
      await Promise.all(tableIds.map((id) => fetchTable(id, force)));
    } finally {
      loading.value = false;
    }
  };

  watch(
    () => JSON.stringify(sourceIds()),
    () => {
      refresh();
    },
    { immediate: true },
  );

  /** The catalog table a tile reads, directly or through its chart. */
  const tableOfTile = (tile: DashboardTile): number | null => {
    const source = tileSourceOf(tile);
    if (source == null) return null;
    return source.kind === "table" ? source.tableId : (vizToTable.value[source.vizId] ?? null);
  };

  const datasourcesInUse = computed<DashboardDatasource[]>(() => {
    const seen = new Set<number>();
    for (const tile of layout.value.tiles) {
      const tid = tableOfTile(tile);
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
      const tid = tableOfTile(tile);
      if (tid == null) continue;
      (acc[tid] ??= []).push(tile.id);
    }
    return acc;
  });

  /** Every tile that reads data (a chart, a KPI or a table), whatever its source. */
  const vizTileIds = computed<string[]>(() =>
    layout.value.tiles.filter((t) => tileSourceOf(t) != null).map((t) => t.id),
  );

  const tileDatasource = (tileId: string): number | null => {
    const tile = layout.value.tiles.find((t) => t.id === tileId);
    return tile ? tableOfTile(tile) : null;
  };

  const getSchema = (tableId: number): ColumnSchema[] =>
    tableCache.value[tableId]?.schema_columns ?? [];

  const getDatasource = (tableId: number): DashboardDatasource | null =>
    tableCache.value[tableId] ?? null;

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

  /** Columns of every tile that reads data, for the filter dialog's reach preview. */
  const loadTileFields = async () => {
    const { vizIds, tableIds } = sourceIds();
    await Promise.all([...vizIds.map(fetchFields), ...tableIds.map(fetchTableFields)]);
  };

  const tileFields = (tileId: string): TileField[] | null => {
    const tile = layout.value.tiles.find((t) => t.id === tileId);
    const source = tile ? tileSourceOf(tile) : null;
    if (source == null) return null;
    return source.kind === "table"
      ? (directTableFields.value[source.tableId] ?? null)
      : (vizFields.value[source.vizId] ?? null);
  };

  const KIND_LABEL: Partial<Record<DashboardTile["type"], string>> = { kpi: "KPI", table: "Table" };

  const tileLabel = (tileId: string): string => {
    const tile = layout.value.tiles.find((t) => t.id === tileId);
    if (!tile) return tileId;
    if (tile.type === "text") return "Text tile";
    if (tile.type === "separator") return "Separator";
    if (tile.type === "kpi" && tile.kpi?.label) return tile.kpi.label;
    if (tile.type === "table" && tile.title?.trim()) return tile.title.trim();
    const kind = KIND_LABEL[tile.type];
    const source = tileSourceOf(tile);
    if (source == null) return kind ?? `Tile ${tileId.slice(0, 6)}`;
    const sourceName =
      source.kind === "table"
        ? (tableCache.value[source.tableId]?.name ?? `Table #${source.tableId}`)
        : (vizNameById.value[source.vizId] ?? `Viz #${source.vizId}`);
    return kind ? `${kind} · ${sourceName}` : tile.title?.trim() || sourceName;
  };

  return {
    datasourcesInUse,
    tilesByDatasource,
    vizTileIds,
    tileDatasource,
    tileFields,
    loadTileFields,
    tileLabel,
    getSchema,
    getDatasource,
    getColumnStats,
    loading,
    refresh,
  };
}
