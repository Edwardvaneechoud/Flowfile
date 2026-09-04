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
const fieldsWithNewColumn = [...fields, { fid: "b", semanticType: "nominal" }];

describe("catalog-store visualization fields", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    getVisualizationFieldsMock.mockReset();
    getVisualizationFieldsMock.mockResolvedValue({ fields, cache_hit: false, error: null });
  });

  it("refetches on every load so a column added to the table shows up", async () => {
    const store = useCatalogStore();

    const first = await store.loadVisualizationFields(source);
    getVisualizationFieldsMock.mockResolvedValue({
      fields: fieldsWithNewColumn,
      cache_hit: false,
      error: null,
    });
    const second = await store.loadVisualizationFields({ ...source });

    expect(first).toEqual(fields);
    expect(second).toEqual(fieldsWithNewColumn);
    expect(getVisualizationFieldsMock).toHaveBeenCalledTimes(2);
  });

  it("returns the (empty) fields of an errored response", async () => {
    const store = useCatalogStore();
    getVisualizationFieldsMock.mockResolvedValue({ fields: [], cache_hit: false, error: "boom" });

    expect(await store.loadVisualizationFields(source)).toEqual([]);
  });
});
