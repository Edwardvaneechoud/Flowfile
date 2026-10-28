import { setImmediate } from "node:timers/promises";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogTable, DeltaTableHistory, NamespaceTree } from "../types";

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getTablePreview: vi.fn(),
    getTableHistory: vi.fn(),
    getTables: vi.fn(),
    optimizeTable: vi.fn(),
    vacuumTable: vi.fn(),
  },
}));

import { CatalogApi } from "../api/catalog.api";
import { useCatalogStore } from "./catalog-store";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const history = (version: number): DeltaTableHistory => ({ current_version: version, history: [] });
const table = (id: number): CatalogTable =>
  ({ id, table_type: "physical", is_remote_storage: true }) as CatalogTable;
const cacheKey = "flowfile.catalog.tableHistory";

function setup() {
  const store = useCatalogStore();
  store.tree = [{ tables: [table(1), table(2)], children: [] }] as unknown as NamespaceTree[];
  return store;
}

function cache(tableId: number, version: number, age = 0) {
  sessionStorage.setItem(
    cacheKey,
    JSON.stringify({ [tableId]: { ts: Date.now() - age, history: history(version) } }),
  );
}

function startRefreshes(cacheAge = 0) {
  const old = deferred<DeltaTableHistory>();
  const latest = deferred<DeltaTableHistory>();
  cache(1, 1, cacheAge);
  const store = setup();
  store.selectTable(1);
  vi.mocked(CatalogApi.getTableHistory)
    .mockReturnValueOnce(old.promise)
    .mockReturnValueOnce(latest.promise);
  const first = store.refreshTableHistory();
  const second = store.refreshTableHistory();
  return { store, old, latest, first, second };
}

describe("catalog table history ownership", () => {
  afterEach(() => vi.unstubAllGlobals());
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.resetAllMocks();
    const entries = new Map<string, string>();
    vi.stubGlobal("sessionStorage", {
      getItem: (key: string) => entries.get(key) ?? null,
      setItem: (key: string, value: string) => entries.set(key, value),
    });
    vi.mocked(CatalogApi.getTablePreview).mockResolvedValue({
      columns: [],
      dtypes: [],
      rows: [],
      total_rows: 0,
    });
    vi.mocked(CatalogApi.getTables).mockResolvedValue([]);
  });

  it.each(["success", "failure"])(
    "keeps the newest same-table refresh after old %s",
    async (outcome) => {
      const { store, old, latest, first, second } = startRefreshes();
      latest.resolve(history(3));
      await second;
      if (outcome === "success") old.resolve(history(2));
      else old.reject(new Error("old failure"));
      await first;
      expect(store.tableHistory).toEqual(history(3));
      expect(store.loadingTableHistory).toBe(false);
      store.selectTable(1);
      expect(store.tableHistory).toEqual(history(3));
      expect(CatalogApi.getTableHistory).toHaveBeenCalledTimes(2);
    },
  );

  it.each(["success", "failure"])("does not finish current loading on old %s", async (outcome) => {
    const { store, old, latest, first, second } = startRefreshes(300_001);
    if (outcome === "success") old.resolve(history(2));
    else old.reject(new Error("old failure"));
    await first;
    expect(store.loadingTableHistory).toBe(true);
    expect(store.tableHistory).toEqual(history(1));
    expect(store.tableHistoryStale).toBe(true);
    latest.reject(new Error("current failure"));
    await second;
    expect(store.tableHistory).toBeNull();
    expect(store.loadingTableHistory).toBe(false);
    store.selectTable(1);
    expect(store.tableHistory).toEqual(history(1));
    expect(store.tableHistoryStale).toBe(true);
  });

  it.each(["clear", "null", "virtual", "cache"])(
    "settles loading immediately on %s selection",
    async (transition) => {
      const old = deferred<DeltaTableHistory>();
      vi.mocked(CatalogApi.getTableHistory).mockReturnValueOnce(old.promise);
      const store = setup();
      store.selectTable(1);
      if (transition === "clear") store.clearTableSelection();
      else if (transition === "null") store.selectTable(null);
      else if (transition === "virtual") {
        store.tree[0].tables[1].table_type = "virtual";
        store.selectTable(2);
      } else {
        cache(2, 4, 300_001);
        store.selectTable(2);
      }
      expect(store.loadingTableHistory).toBe(false);
      old.resolve(history(0));
      await setImmediate();
      expect(store.tableHistory).toEqual(transition === "cache" ? history(4) : null);
      expect(store.tableHistoryStale).toBe(transition === "cache");
      expect(CatalogApi.getTableHistory).toHaveBeenCalledTimes(1);
    },
  );

  it.each(["optimize", "vacuum"])(
    "keeps B pending while late A %s refreshes A's cache",
    async (operation) => {
      cache(1, 1);
      const store = setup();
      store.selectTable(1);
      const mutation = deferred<{ size_bytes: number }>();
      vi.mocked(CatalogApi.optimizeTable).mockReturnValue(
        mutation.promise as ReturnType<typeof CatalogApi.optimizeTable>,
      );
      vi.mocked(CatalogApi.vacuumTable).mockReturnValue(
        mutation.promise as ReturnType<typeof CatalogApi.vacuumTable>,
      );
      const work =
        operation === "optimize" ? store.optimizeTable(1) : store.vacuumTable(1, 168, false);
      const b = deferred<DeltaTableHistory>();
      const a = deferred<DeltaTableHistory>();
      vi.mocked(CatalogApi.getTableHistory)
        .mockReturnValueOnce(b.promise)
        .mockReturnValueOnce(a.promise);
      store.selectTable(2);
      mutation.resolve({ size_bytes: 20 });
      await setImmediate();
      a.resolve(history(5));
      await work;
      expect(store.selectedTableId).toBe(2);
      expect(store.tableHistory).toBeNull();
      expect(store.loadingTableHistory).toBe(true);
      b.resolve(history(2));
      await setImmediate();
      expect(store.tableHistory).toEqual(history(2));
      expect(store.loadingTableHistory).toBe(false);
      store.selectTable(1);
      expect(store.tableHistory).toEqual(history(5));
      expect(CatalogApi.getTableHistory).toHaveBeenCalledTimes(2);
    },
  );

  it("ignores an old-table edit notification without replacing B's history request", async () => {
    const pending = deferred<DeltaTableHistory>();
    vi.mocked(CatalogApi.getTableHistory).mockReturnValue(pending.promise);
    const store = setup();
    store.selectTable(2);
    await store.handleTableEdited(table(1));
    expect(CatalogApi.getTableHistory).toHaveBeenCalledTimes(1);
    expect(store.loadingTableHistory).toBe(true);
    pending.resolve(history(2));
    await setImmediate();
    expect(store.tableHistory).toEqual(history(2));
  });

  it("requests B version zero, not latest, after A history arrives last", async () => {
    const a = deferred<DeltaTableHistory>();
    const b = deferred<DeltaTableHistory>();
    vi.mocked(CatalogApi.getTableHistory)
      .mockReturnValueOnce(a.promise)
      .mockReturnValueOnce(b.promise);
    const store = setup();
    store.selectTable(1);
    store.selectTable(2);
    b.resolve(history(2));
    await setImmediate();
    a.resolve(history(0));
    await setImmediate();
    store.selectVersion(0);
    await store.loadSelectedPreview();
    expect(CatalogApi.getTablePreview).toHaveBeenLastCalledWith(2, 100, 0);
    expect(store.tableHistory).toEqual(history(2));
    store.selectVersion(2);
    await store.loadSelectedPreview();
    expect(CatalogApi.getTablePreview).toHaveBeenLastCalledWith(2, 100, null);
  });
});
