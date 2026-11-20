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

export interface FilterSourceOption {
  key: DatasourceKey;
  label: string;
}

export interface FilterSourceGroup {
  label: string;
  options: FilterSourceOption[];
}

const optionLabel = (d: DashboardDatasource): string =>
  d.kind === "viz" ? `${d.name} (SQL chart)` : d.name;

const byLabel = (a: FilterSourceOption, b: FilterSourceOption) => a.label.localeCompare(b.label);

/**
 * The dialog's source choices: the sources tiles on the dashboard read (tables and SQL charts)
 * first, then every other catalog table the user may read, each group by name. SQL charts are only
 * offered from the dashboard. `current` (the source of the filter being edited) is kept as a choice
 * when neither list holds it, so the select never shows a bare key.
 */
export const filterSourceGroups = (
  inUse: DashboardDatasource[],
  tables: { id: number; name: string }[],
  current: FilterSourceOption | null = null,
): FilterSourceGroup[] => {
  const dashboard = inUse.map((d) => ({ key: d.key, label: optionLabel(d) })).sort(byLabel);
  const used = new Set(dashboard.map((o) => o.key));
  const others = tables
    .map((t) => ({ key: `table:${t.id}`, label: t.name }))
    .filter((o) => !used.has(o.key))
    .sort(byLabel);
  if (current && !used.has(current.key) && !others.some((o) => o.key === current.key)) {
    dashboard.unshift(current);
  }
  return [
    { label: "On this dashboard", options: dashboard },
    { label: "Other catalog tables", options: others },
  ].filter((g) => g.options.length > 0);
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
