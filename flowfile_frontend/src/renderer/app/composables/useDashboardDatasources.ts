import { computed, ref, watch, type ComputedRef, type Ref } from "vue";
import { CatalogApi } from "../api/catalog.api";
import type {
  ColumnSchema,
  ColumnStatsResponse,
  DashboardFilter,
  DashboardLayout,
  DashboardTile,
} from "../types";
import {
  computeOnSource,
  filterSourceKey,
  filterSourceOf,
  tableSourceKey,
  tileSourceOf,
  vizSourceKey,
  type DatasourceKey,
  type TileField,
} from "./useDashboardComputation";
import { createDashboardVizCache, type DashboardVizCache } from "./useDashboardVizCache";
import {
  valueSearchPayload,
  valueSearchResult,
  VALUE_SEARCH_LIMIT,
  type ValueSearchResult,
} from "../views/DashboardsView/filterValues";

export interface DashboardDatasource {
  /** `table:<id>` or `viz:<id>`, the key tiles and filters are matched on. */
  key: DatasourceKey;
  /** A catalog table, or a SQL chart's query (a chart on a table is represented by its table). */
  kind: "table" | "viz";
  /** The table's id, or the chart's. */
  id: number;
  name: string;
  schema_columns: ColumnSchema[];
  /** When the table's data last changed, as the catalog recorded it; null for a SQL chart. */
  updated_at: string | null;
}

const SEMANTIC_DTYPE: Record<string, string> = {
  quantitative: "number",
  temporal: "date",
};

/** A chart's columns as the filter dialog lists them: no Polars dtype, only its kind. */
export const fieldsAsColumns = (fields: Record<string, unknown>[]): ColumnSchema[] =>
  fields.map((f) => ({
    name: String(f.fid),
    dtype: SEMANTIC_DTYPE[String(f.semanticType)] ?? "text",
  }));

/** Resolves dashboard tiles → catalog tables.
 *
 * A KPI or table tile may name its table directly (``table_id``); every other
 * tile goes through its chart: viz → ``catalog_table_id`` → ``CatalogTable``.
 * Together they say (a) which catalog tables back the dashboard and (b) the
 * columns + dtypes available on each. A SQL-source visualization has no
 * ``catalog_table_id``; it surfaces as a datasource of its own (``viz:<id>``),
 * its columns read from the chart's fields.
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
  // viz_id → its source type, set once the chart was read
  const vizSourceType = ref<Record<number, "table" | "sql">>({});
  // viz_id → the SQL chart's source as a datasource
  const vizSourceCache = ref<Record<number, DashboardDatasource>>({});
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
      vizSourceType.value[vizId] = viz.source_type === "sql" ? "sql" : "table";
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
          key: tableSourceKey(table.id),
          kind: "table",
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

  // A chart tile reads the same fields when it mounts, so the page cache answers both.
  const fetchVizSource = async (vizId: number, force = false) => {
    if (!force && vizId in vizSourceCache.value) return;
    try {
      const resp = await vizCache.getFields(vizId);
      vizSourceCache.value[vizId] = {
        key: vizSourceKey(vizId),
        kind: "viz",
        id: vizId,
        name: vizNameById.value[vizId] ?? `Chart #${vizId}`,
        schema_columns: fieldsAsColumns(resp.fields ?? []),
        updated_at: null,
      };
    } catch (err) {
      console.warn(`[dashboard] could not fetch the columns of chart ${vizId}:`, err);
    }
  };

  // (table_id, column_name) -> stats. Promise caching dedupes concurrent
  // requests; the resolved Promise stays in the cache so subsequent
  // lookups are synchronous.
  const statsCache = new Map<string, Promise<ColumnStatsResponse>>();
  const statsKey = (tableId: number, column: string) => `${tableId}:${column}`;
  // (source key, field, text) -> value search result; dropped on a force refresh.
  const searchCache = new Map<string, Promise<ValueSearchResult>>();

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
      searchCache.clear();
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
      const sqlVizIds = vizIds.filter((v) => vizSourceType.value[v] === "sql");
      await Promise.all([
        ...tableIds.map((id) => fetchTable(id, force)),
        ...sqlVizIds.map((id) => fetchVizSource(id, force)),
      ]);
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

  /** The source a tile reads as a key: its table (directly or through its chart), else its SQL
   * chart; null while the chart is not read yet. */
  const keyOfTile = (tile: DashboardTile): DatasourceKey | null => {
    const source = tileSourceOf(tile);
    if (source == null) return null;
    if (source.kind === "table") return tableSourceKey(source.tableId);
    const tableId = vizToTable.value[source.vizId];
    if (tableId != null) return tableSourceKey(tableId);
    return vizSourceType.value[source.vizId] === "sql" ? vizSourceKey(source.vizId) : null;
  };

  const datasourceOfKey = (key: DatasourceKey): DashboardDatasource | null => {
    const [kind, raw] = key.split(":");
    const id = Number(raw);
    return (kind === "table" ? tableCache.value[id] : vizSourceCache.value[id]) ?? null;
  };

  // A filter's table no tile reads still needs its name and columns, for the chip and the dialog.
  watch(
    () =>
      JSON.stringify(
        [...new Set(layout.value.filters.map((f) => f.datasource_id))].filter((id) => id != null),
      ),
    (raw) => {
      for (const id of JSON.parse(raw) as number[]) void fetchTable(id);
    },
    { immediate: true },
  );

  /** A source as far as this page has read it: a table any tile or filter names, or one the
   * filter dialog loaded, or a SQL chart a tile reads; null until it is loaded. */
  const datasourceOf = (key: DatasourceKey): DashboardDatasource | null => datasourceOfKey(key);

  /** Read a source's name and columns so the filter dialog can offer them; a table need not be
   * read by any tile. */
  const loadDatasource = async (key: DatasourceKey): Promise<void> => {
    const [kind, raw] = key.split(":");
    const id = Number(raw);
    if (!Number.isFinite(id)) return;
    if (kind === "table") return fetchTable(id);
    await fetchViz(id);
    if (vizSourceType.value[id] === "sql") await fetchVizSource(id);
  };

  const datasourcesInUse = computed<DashboardDatasource[]>(() => {
    const seen = new Set<DatasourceKey>();
    for (const tile of layout.value.tiles) {
      const key = keyOfTile(tile);
      if (key != null) seen.add(key);
    }
    return Array.from(seen)
      .map(datasourceOfKey)
      .filter((d): d is DashboardDatasource => !!d)
      .sort((a, b) => a.name.localeCompare(b.name));
  });

  const tilesByDatasource = computed<Record<DatasourceKey, string[]>>(() => {
    const acc: Record<DatasourceKey, string[]> = {};
    for (const tile of layout.value.tiles) {
      const key = keyOfTile(tile);
      if (key == null) continue;
      (acc[key] ??= []).push(tile.id);
    }
    return acc;
  });

  /** Every tile that reads data (a chart, a KPI or a table), whatever its source. */
  const vizTileIds = computed<string[]>(() =>
    layout.value.tiles.filter((t) => tileSourceOf(t) != null).map((t) => t.id),
  );

  const tileDatasource = (tileId: string): DatasourceKey | null => {
    const tile = layout.value.tiles.find((t) => t.id === tileId);
    return tile ? keyOfTile(tile) : null;
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

  /** Distinct values of a filter's field on its own source that contain `text`, ignoring case.
   * Other filters do not narrow them. */
  const searchFilterValues = (
    filter: DashboardFilter,
    text: string,
  ): Promise<ValueSearchResult> => {
    const source = filterSourceOf(filter);
    if (source == null || !filter.field_name) {
      return Promise.resolve({ values: [], capped: false });
    }
    const field = filter.field_name;
    const key = JSON.stringify([filterSourceKey(filter), field, text]);
    let entry = searchCache.get(key);
    if (!entry) {
      entry = computeOnSource(source, valueSearchPayload(field, text), VALUE_SEARCH_LIMIT + 1)
        .then((resp) => {
          if (resp.error) throw new Error(resp.error);
          return valueSearchResult(resp.rows ?? [], field);
        })
        .catch((err) => {
          searchCache.delete(key);
          throw err;
        });
      searchCache.set(key, entry);
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
    datasourceOf,
    loadDatasource,
    tileLabel,
    getSchema,
    getDatasource,
    getColumnStats,
    searchFilterValues,
    loading,
    refresh,
  };
}
