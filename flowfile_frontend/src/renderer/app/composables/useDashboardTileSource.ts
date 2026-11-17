import { computed, ref, watch } from "vue";
import type { CatalogTable, DashboardTile } from "../types";
import { tileSourceOf, type TileField, type TileSource } from "./useDashboardComputation";
import { useDashboardVizCache } from "./useDashboardVizCache";

/**
 * The source a KPI or table tile reads, with its columns and display name, through the page's
 * shared cache so a table read by several tiles is described once.
 *
 * ``fields`` is null while unknown; ``missing`` turns true when core answers 404 for the chart or
 * the table, which is the tile's "source missing" state.
 */
export function useDashboardTileSource(tile: () => DashboardTile, refreshNonce: () => number) {
  const vizCache = useDashboardVizCache();
  const source = computed(() => tileSourceOf(tile()));
  const fields = ref<TileField[] | null>(null);
  const sourceName = ref<string | null>(null);
  /** Column dtypes from the catalog table behind the source; null for SQL or when unknown. */
  const dtypes = ref<Record<string, string> | null>(null);
  const missing = ref(false);
  const fieldsError = ref<string | null>(null);
  let seq = 0;

  const load = async () => {
    const mine = ++seq;
    const current = source.value;
    fields.value = null;
    sourceName.value = null;
    dtypes.value = null;
    missing.value = false;
    fieldsError.value = null;
    if (current == null) return;
    try {
      const [resp, name] =
        current.kind === "table"
          ? await Promise.all([
              vizCache.getTableFields(current.tableId),
              vizCache.getTable(current.tableId).then((t) => t.full_table_name ?? t.name),
            ])
          : await Promise.all([
              vizCache.getFields(current.vizId),
              vizCache.getViz(current.vizId).then((v) => v.name),
            ]);
      const types = await dtypesFor(current);
      if (mine !== seq) return;
      sourceName.value = name;
      dtypes.value = types;
      fields.value = (resp.fields ?? []).map((f) => ({
        fid: String(f.fid),
        semanticType: f.semanticType,
      }));
    } catch (err: any) {
      if (mine !== seq) return;
      if (err?.response?.status === 404) {
        missing.value = true;
      } else {
        console.error("Dashboard tile source load failed:", err);
        fieldsError.value =
          current.kind === "table"
            ? "Could not load the table's columns"
            : "Could not load the visualization's columns";
      }
    }
  };

  const dtypesOf = (table: CatalogTable) =>
    Object.fromEntries((table.schema_columns ?? []).map((c) => [c.name, c.dtype]));

  // Best effort: a table the caller cannot read, or a SQL chart, just leaves the dtypes unknown.
  const dtypesFor = async (current: TileSource): Promise<Record<string, string> | null> => {
    try {
      let tableId: number | null = null;
      if (current.kind === "table") tableId = current.tableId;
      else {
        const viz = await vizCache.getViz(current.vizId);
        tableId = viz.source_type === "table" ? (viz.catalog_table_id ?? null) : null;
      }
      return tableId == null ? null : dtypesOf(await vizCache.getTable(tableId));
    } catch {
      return null;
    }
  };

  watch([() => JSON.stringify(source.value), refreshNonce], load, { immediate: true });

  const markMissing = () => {
    missing.value = true;
  };

  return { source, fields, sourceName, dtypes, missing, fieldsError, markMissing };
}
