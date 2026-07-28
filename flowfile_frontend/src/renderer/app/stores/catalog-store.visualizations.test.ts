import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogVisualization } from "../types";

const { updateVisualizationMock, listVisualizationLibraryMock } = vi.hoisted(() => ({
  updateVisualizationMock: vi.fn(),
  listVisualizationLibraryMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    updateVisualization: updateVisualizationMock,
    listVisualizationLibrary: listVisualizationLibraryMock,
  },
}));

import { useCatalogStore } from "./catalog-store";

const viz = (overrides: Partial<CatalogVisualization> = {}): CatalogVisualization =>
  ({
    id: 9,
    name: "Revenue",
    catalog_table_id: 3,
    spec: [],
    updated_at: "2026-01-01T00:00:00",
    access: { is_owner: true, access_level: "owner", shared_by: null },
    ...overrides,
  }) as unknown as CatalogVisualization;

describe("catalog-store updateVisualization refreshLibrary option", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    updateVisualizationMock.mockReset();
    listVisualizationLibraryMock.mockReset();
    listVisualizationLibraryMock.mockResolvedValue([]);
  });

  it("keeps the legacy library refresh by default", async () => {
    const store = useCatalogStore();
    updateVisualizationMock.mockResolvedValue(viz());

    await store.updateVisualization(9, { name: "Revenue" });

    expect(listVisualizationLibraryMock).toHaveBeenCalledTimes(1);
  });

  it("skips the refresh and patches entries in place when refreshLibrary is false", async () => {
    const store = useCatalogStore();
    store.visualizationLibrary = [viz({ name: "Old" })];
    store.visualizationsByTable = { 3: [viz({ name: "Old" })] };
    updateVisualizationMock.mockResolvedValue(viz({ name: "New", access: null }));

    await store.updateVisualization(9, { name: "New" }, { refreshLibrary: false });

    expect(listVisualizationLibraryMock).not.toHaveBeenCalled();
    expect(store.visualizationLibrary[0]?.name).toBe("New");
    expect(store.visualizationsByTable[3]?.[0]?.name).toBe("New");
    // Null access from the response must not wipe the known stamp.
    expect(store.visualizationLibrary[0]?.access?.access_level).toBe("owner");
    expect(store.visualizationsByTable[3]?.[0]?.access?.access_level).toBe("owner");
  });

  it("forwards the axios config to the API layer", async () => {
    const store = useCatalogStore();
    updateVisualizationMock.mockResolvedValue(viz());

    await store.updateVisualization(
      9,
      { name: "Revenue" },
      { refreshLibrary: false, config: { timeout: 15000 } },
    );

    expect(updateVisualizationMock).toHaveBeenCalledWith(
      9,
      { name: "Revenue" },
      { timeout: 15000 },
    );
  });
});
