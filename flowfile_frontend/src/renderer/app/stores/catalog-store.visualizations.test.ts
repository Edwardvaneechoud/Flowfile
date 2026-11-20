import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogVisualization } from "../types";

const {
  updateVisualizationMock,
  listVisualizationLibraryMock,
  detachVisualizationMock,
  pruneDashboardChartsMock,
} = vi.hoisted(() => ({
  updateVisualizationMock: vi.fn(),
  listVisualizationLibraryMock: vi.fn(),
  detachVisualizationMock: vi.fn(),
  pruneDashboardChartsMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    updateVisualization: updateVisualizationMock,
    listVisualizationLibrary: listVisualizationLibraryMock,
    detachVisualization: detachVisualizationMock,
    pruneDashboardCharts: pruneDashboardChartsMock,
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

describe("catalog-store charts that belong to a dashboard", () => {
  const owned = (id: number) => viz({ id, dashboard_id: 7, dashboard_name: "Sales" });

  beforeEach(() => {
    setActivePinia(createPinia());
    detachVisualizationMock.mockReset();
    pruneDashboardChartsMock.mockReset();
    listVisualizationLibraryMock.mockReset();
  });

  it("detach clears the owner in the library and the table's list without a refetch", async () => {
    const store = useCatalogStore();
    store.visualizationLibrary = [owned(9), owned(10)];
    store.visualizationsByTable = { 3: [owned(9)] };
    detachVisualizationMock.mockResolvedValue(
      viz({ id: 9, dashboard_id: null, dashboard_name: null }),
    );

    const kept = await store.detachVisualization(9);

    expect(kept.dashboard_id).toBeNull();
    expect(store.visualizationLibrary.map((v) => v.dashboard_id)).toEqual([null, 7]);
    expect(store.visualizationsByTable[3]?.[0]?.dashboard_name).toBeNull();
    expect(listVisualizationLibraryMock).not.toHaveBeenCalled();
  });

  it("prune drops the deleted charts and releases the ones another dashboard uses", async () => {
    const store = useCatalogStore();
    store.visualizationLibrary = [owned(9), owned(10), owned(11)];
    store.visualizationsByTable = { 3: [owned(9), owned(10), owned(11)] };
    pruneDashboardChartsMock.mockResolvedValue({ deleted: [9], released: [10] });

    await store.pruneDashboardCharts(7);

    expect(pruneDashboardChartsMock).toHaveBeenCalledWith(7);
    const library = store.visualizationLibrary.map((v) => [v.id, v.dashboard_id]);
    expect(library).toEqual([
      [10, null],
      [11, 7],
    ]);
    expect(store.visualizationsByTable[3]?.map((v) => v.id)).toEqual([10, 11]);
  });

  it("prune that finds nothing leaves the lists as they are", async () => {
    const store = useCatalogStore();
    const library = [owned(9)];
    store.visualizationLibrary = library;
    pruneDashboardChartsMock.mockResolvedValue({ deleted: [], released: [] });

    await store.pruneDashboardCharts(7);

    expect(store.visualizationLibrary).toEqual(library);
  });
});
