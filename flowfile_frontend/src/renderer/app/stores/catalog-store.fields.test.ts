import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { VizSourceDescriptor } from "../types";

const { getVisualizationFieldsMock } = vi.hoisted(() => ({
  getVisualizationFieldsMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getVisualizationFields: getVisualizationFieldsMock,
  },
}));

import { useCatalogStore } from "./catalog-store";

const source: VizSourceDescriptor = { source_type: "table", table_id: 3 };
const fields = [{ fid: "a", semanticType: "quantitative" }];

describe("catalog-store visualization fields cache", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    getVisualizationFieldsMock.mockReset();
    getVisualizationFieldsMock.mockResolvedValue({ fields, cache_hit: false, error: null });
  });

  it("caches fields per source descriptor", async () => {
    const store = useCatalogStore();

    const first = await store.loadVisualizationFields(source);
    const second = await store.loadVisualizationFields({ ...source });

    expect(first).toEqual(fields);
    expect(second).toEqual(fields);
    expect(getVisualizationFieldsMock).toHaveBeenCalledTimes(1);
  });

  it("invalidateVisualizationFields empties the cache so the next load refetches", async () => {
    const store = useCatalogStore();

    await store.loadVisualizationFields(source);
    store.invalidateVisualizationFields();

    expect(store.visualizationFieldsBySource).toEqual({});
    await store.loadVisualizationFields(source);
    expect(getVisualizationFieldsMock).toHaveBeenCalledTimes(2);
  });

  it("does not cache errored responses", async () => {
    const store = useCatalogStore();
    getVisualizationFieldsMock.mockResolvedValue({ fields: [], cache_hit: false, error: "boom" });

    await store.loadVisualizationFields(source);
    await store.loadVisualizationFields(source);

    expect(store.visualizationFieldsBySource).toEqual({});
    expect(getVisualizationFieldsMock).toHaveBeenCalledTimes(2);
  });
});
