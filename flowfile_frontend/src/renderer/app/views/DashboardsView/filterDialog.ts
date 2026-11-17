import type { ColumnSchema, DashboardFilter } from "../../types";
import type { DashboardDatasource } from "../../composables/useDashboardDatasources";
import type { DatasourceKey } from "../../composables/useDashboardComputation";

/** The columns the filter dialog offers for a source key. */
export const sourceFieldsOfKey = (
  datasources: DashboardDatasource[],
  key: DatasourceKey | null,
): ColumnSchema[] =>
  key == null ? [] : (datasources.find((d) => d.key === key)?.schema_columns ?? []);

/** The filter fields a source key stands for: a table's id or a SQL chart's, never both. */
export const sourceIdsOfKey = (
  key: DatasourceKey,
): Pick<DashboardFilter, "datasource_id" | "datasource_viz_id"> => {
  const [kind, raw] = key.split(":");
  const id = Number(raw);
  return kind === "viz"
    ? { datasource_id: null, datasource_viz_id: id }
    : { datasource_id: id, datasource_viz_id: null };
};

/**
 * The targets the dialog saves. No pick, or every candidate, is all tiles; a filter whose explicit
 * targets are all gone (`targetsNone`) keeps filtering no tile until the author chooses again.
 */
export const draftTargets = (
  candidates: string[],
  picked: string[],
  targetsNone: boolean,
): Pick<DashboardFilter, "target" | "target_tile_ids"> => {
  if (targetsNone) return { target: "tiles", target_tile_ids: [] };
  const explicit = picked.filter((id) => candidates.includes(id));
  const useAll = explicit.length === 0 || explicit.length === candidates.length;
  return useAll
    ? { target: "all", target_tile_ids: [] }
    : { target: "tiles", target_tile_ids: explicit };
};
