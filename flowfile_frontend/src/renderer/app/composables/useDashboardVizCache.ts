import { inject, provide, type InjectionKey } from "vue";
import { CatalogApi } from "../api/catalog.api";
import type { CatalogTable, CatalogVisualization, VisualizationFieldsResponse } from "../types";

export interface DashboardVizCache {
  getViz(vizId: number): Promise<CatalogVisualization>;
  getFields(vizId: number): Promise<VisualizationFieldsResponse>;
  /** A catalog table's row, for its name and the filter strip's columns. */
  getTable(tableId: number): Promise<CatalogTable>;
  /** Columns of a catalog table a KPI or table tile reads without a chart. */
  getTableFields(tableId: number): Promise<VisualizationFieldsResponse>;
  /** Forget one chart, or every chart and table, so the next read asks core again. */
  invalidate(vizId?: number): void;
}

/**
 * One request per chart or table for a whole dashboard page.
 *
 * Several tiles can show the same chart, a KPI reads its chart's columns, and the filter strip
 * reads every chart's source; each used to ask core on its own. They share the request here, the
 * ones still in flight included. A request that fails is not kept, so the next read tries again.
 */
export function createDashboardVizCache(): DashboardVizCache {
  const vizzes = new Map<number, Promise<CatalogVisualization>>();
  const fields = new Map<number, Promise<VisualizationFieldsResponse>>();
  const tables = new Map<number, Promise<CatalogTable>>();
  const tableFields = new Map<number, Promise<VisualizationFieldsResponse>>();

  const shared = <T>(held: Map<number, Promise<T>>, id: number, load: () => Promise<T>) => {
    const pending = held.get(id);
    if (pending) return pending;
    const request = load().catch((err) => {
      if (held.get(id) === request) held.delete(id);
      throw err;
    });
    held.set(id, request);
    return request;
  };

  return {
    getViz: (vizId) => shared(vizzes, vizId, () => CatalogApi.getVisualization(vizId)),
    getFields: (vizId) =>
      shared(fields, vizId, () => CatalogApi.getSavedVisualizationFields(vizId)),
    getTable: (tableId) => shared(tables, tableId, () => CatalogApi.getTable(tableId)),
    getTableFields: (tableId) =>
      shared(tableFields, tableId, () =>
        CatalogApi.getVisualizationFields({ source_type: "table", table_id: tableId }),
      ),
    invalidate: (vizId) => {
      if (vizId == null) {
        vizzes.clear();
        fields.clear();
        tables.clear();
        tableFields.clear();
        return;
      }
      vizzes.delete(vizId);
      fields.delete(vizId);
    },
  };
}

const KEY: InjectionKey<DashboardVizCache> = Symbol("dashboardVizCache");

/** Called by the dashboard page; everything it mounts then shares one cache. */
export function provideDashboardVizCache(): DashboardVizCache {
  const cache = createDashboardVizCache();
  provide(KEY, cache);
  return cache;
}

/** The page's cache, or one of its own for a tile mounted outside a dashboard page. */
export function useDashboardVizCache(): DashboardVizCache {
  return inject(KEY, null) ?? createDashboardVizCache();
}
