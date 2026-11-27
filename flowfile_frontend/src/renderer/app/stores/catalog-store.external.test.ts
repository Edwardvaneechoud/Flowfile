// @vitest-environment happy-dom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";

const { syncMock, tablesMock, tableMock, treeMock } = vi.hoisted(() => ({
  syncMock: vi.fn(),
  tablesMock: vi.fn(),
  tableMock: vi.fn(),
  treeMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    syncExternalCatalog: syncMock,
    getExternalTables: tablesMock,
    getExternalTable: tableMock,
    getNamespaceTree: treeMock,
    getActiveRuns: vi.fn().mockResolvedValue([]),
  },
}));

import { useCatalogStore } from "./catalog-store";
import type { NamespaceTree } from "../types";

const unchanged = {
  added: [],
  gone: [],
  back: [],
  not_shown: 0,
  synced_at: null,
  skipped: true,
  error: null,
};

describe("catalog-store with a catalog kept elsewhere", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    syncMock.mockReset();
    tablesMock.mockReset();
    tableMock.mockReset();
    treeMock.mockReset().mockResolvedValue([]);
  });

  it("reloads the tree after a sync only when a schema changed or it was forced", async () => {
    const store = useCatalogStore();
    syncMock.mockResolvedValueOnce(unchanged);
    await store.syncExternalCatalog(7);
    expect(treeMock).not.toHaveBeenCalled();

    syncMock.mockResolvedValueOnce({ ...unchanged, skipped: false, added: ["marketing"] });
    await store.syncExternalCatalog(7);
    syncMock.mockResolvedValueOnce(unchanged);
    await store.syncExternalCatalog(7, true);
    expect(treeMock).toHaveBeenCalledTimes(2);
    expect(syncMock).toHaveBeenLastCalledWith(7, true);
  });

  it("keeps each schema's tables apart and shows the catalog's sentence when it refuses", async () => {
    const store = useCatalogStore();
    tablesMock.mockResolvedValueOnce({
      tables: [{ name: "orders", reference: "polaris.sales.orders" }],
      listed_at: "2026-11-22T10:00:00",
      cached: false,
    });
    tablesMock.mockRejectedValueOnce({
      response: {
        data: { detail: "No longer in polaris's own catalog since 2026-11-22 10:00 UTC." },
      },
    });

    await store.loadExternalTables(7, 8);
    await store.loadExternalTables(7, 9);

    expect(store.externalTables["7:8"].tables.map((t) => t.name)).toEqual(["orders"]);
    expect(store.externalTables["7:9"]).toEqual({
      tables: [],
      listedAt: null,
      loading: false,
      error: "No longer in polaris's own catalog since 2026-11-22 10:00 UTC.",
    });
  });

  it("counts a schema's tables as stale until they arrive, and again a minute after", async () => {
    vi.useFakeTimers();
    try {
      const store = useCatalogStore();
      expect(store.externalTablesStale(70, 80)).toBe(true);
      tablesMock.mockResolvedValueOnce({ tables: [], listed_at: "t", cached: false });
      await store.loadExternalTables(70, 80);
      expect(store.externalTablesStale(70, 80)).toBe(false);
      vi.advanceTimersByTime(61_000);
      expect(store.externalTablesStale(70, 80)).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("uses a fresh list again, and asks core again once a run in this tab has finished", async () => {
    const store = useCatalogStore();
    const asked = () =>
      store.externalTablesStale(71, 81) ? store.loadExternalTables(71, 81) : Promise.resolve();
    tablesMock.mockResolvedValue({ tables: [], listed_at: "t", cached: false });
    await asked();
    await asked();
    expect(tablesMock).toHaveBeenCalledTimes(1);

    store.markExternalTablesStale();
    expect(store.externalTablesStale(71, 81)).toBe(true);
    await asked();
    expect(tablesMock).toHaveBeenCalledTimes(2);
    expect(tablesMock).toHaveBeenLastCalledWith(71, 81, false);
    expect(store.externalTablesStale(71, 81)).toBe(false);
  });

  it("leaves an ordinary catalog's tables alone when a run finishes", async () => {
    const store = useCatalogStore();
    const ordinary = { id: 3, name: "local", level: 1, children: [], tables: [{ id: 30 }] };
    store.tree = [ordinary] as unknown as NamespaceTree[];
    store.markExternalTablesStale();
    expect(store.tree).toEqual([ordinary]);
    expect(store.externalTables).toEqual({});
    expect(treeMock).not.toHaveBeenCalled();
    expect(tablesMock).not.toHaveBeenCalled();
  });

  it("keeps when each schema's list was taken, and moves it on a re-list of that schema only", async () => {
    const store = useCatalogStore();
    tablesMock.mockResolvedValueOnce({
      tables: [],
      listed_at: "2026-11-24T09:00:00",
      cached: false,
    });
    tablesMock.mockResolvedValueOnce({
      tables: [],
      listed_at: "2026-11-24T09:01:00",
      cached: false,
    });
    await store.loadExternalTables(7, 8);
    await store.loadExternalTables(7, 9);

    tablesMock.mockResolvedValueOnce({
      tables: [],
      listed_at: "2026-11-24T09:05:00",
      cached: false,
    });
    await store.loadExternalTables(7, 8, true);

    expect(tablesMock).toHaveBeenLastCalledWith(7, 8, true);
    expect(store.externalTablesListedAt(7, 8)).toBe("2026-11-24T09:05:00");
    expect(store.externalTablesListedAt(7, 9)).toBe("2026-11-24T09:01:00");
    expect(store.externalTablesListedAt(7, 10)).toBeNull();
  });

  it("drops an older listing of a schema that a newer one overtook", async () => {
    const store = useCatalogStore();
    const held: { resolve?: (value: unknown) => void } = {};
    tablesMock.mockReturnValueOnce(new Promise((resolve) => (held.resolve = resolve)));
    tablesMock.mockResolvedValueOnce({
      tables: [{ name: "new", reference: "c.s.new" }],
      listed_at: "t",
      cached: false,
    });

    const older = store.loadExternalTables(7, 8);
    await store.loadExternalTables(7, 8, true);
    held.resolve?.({
      tables: [{ name: "old", reference: "c.s.old" }],
      listed_at: "t",
      cached: true,
    });
    await older;

    expect(store.externalTables["7:8"].tables.map((t) => t.name)).toEqual(["new"]);
  });

  it("describes one table and keeps the error of a refused description", async () => {
    const store = useCatalogStore();
    tableMock.mockRejectedValueOnce({
      response: { data: { detail: "The catalog at x has no table s.t." } },
    });
    await store.loadExternalTable(7, 8, "t");
    expect(store.externalTable).toBeNull();
    expect(store.externalTableError).toBe("The catalog at x has no table s.t.");
  });
});
