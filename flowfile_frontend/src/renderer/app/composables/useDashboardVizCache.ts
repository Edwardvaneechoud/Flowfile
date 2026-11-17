import { inject, provide, type InjectionKey } from "vue";
import { CatalogApi } from "../api/catalog.api";
import type { CatalogVisualization, VisualizationFieldsResponse } from "../types";

export interface DashboardVizCache {
  getViz(vizId: number): Promise<CatalogVisualization>;
  getFields(vizId: number): Promise<VisualizationFieldsResponse>;
  /** Forget one chart, or every chart, so the next read asks core again. */
  invalidate(vizId?: number): void;
}

/**
 * One request per chart for a whole dashboard page.
 *
 * Several tiles can show the same chart, a KPI reads its chart's columns, and the filter strip
 * reads every chart's source; each used to ask core on its own. They share the request here, the
 * ones still in flight included. A request that fails is not kept, so the next read tries again.
 */
export function createDashboardVizCache(): DashboardVizCache {
  const vizzes = new Map<number, Promise<CatalogVisualization>>();
  const fields = new Map<number, Promise<VisualizationFieldsResponse>>();

  const shared = <T>(held: Map<number, Promise<T>>, vizId: number, load: () => Promise<T>) => {
    const pending = held.get(vizId);
    if (pending) return pending;
    const request = load().catch((err) => {
      if (held.get(vizId) === request) held.delete(vizId);
      throw err;
    });
    held.set(vizId, request);
    return request;
  };

  return {
    getViz: (vizId) => shared(vizzes, vizId, () => CatalogApi.getVisualization(vizId)),
    getFields: (vizId) =>
      shared(fields, vizId, () => CatalogApi.getSavedVisualizationFields(vizId)),
    invalidate: (vizId) => {
      if (vizId == null) {
        vizzes.clear();
        fields.clear();
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
