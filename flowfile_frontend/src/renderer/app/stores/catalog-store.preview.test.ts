import { setImmediate } from "node:timers/promises";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogTable, CatalogTablePreview, NamespaceTree } from "../types";

vi.mock("../api/catalog.api", () => ({
  CatalogApi: { getTablePreview: vi.fn(), getTableHistory: vi.fn(), getTables: vi.fn() },
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

const preview = (total: number): CatalogTablePreview => ({
  columns: ["value"],
  dtypes: ["int64"],
  rows: [[total]],
  total_rows: total,
});

const table = (id: number, remote = false): CatalogTable =>
  ({ id, table_type: "physical", is_remote_storage: remote, row_count: 10 }) as CatalogTable;

function setup(remote = false) {
  const store = useCatalogStore();
  store.tree = [
    { tables: [table(1, remote), table(2, remote)], children: [] },
  ] as unknown as NamespaceTree[];
  return store;
}

function startRefreshes() {
  const old = deferred<CatalogTablePreview>();
  const latest = deferred<CatalogTablePreview>();
  vi.mocked(CatalogApi.getTablePreview)
    .mockReturnValueOnce(old.promise)
    .mockReturnValueOnce(latest.promise);
  const store = setup(true);
  store.selectTable(1);
  const first = store.loadSelectedPreview();
  const second = store.loadSelectedPreview();
  return { store, old, latest, first, second };
}

describe("catalog table preview ownership", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.resetAllMocks();
    vi.mocked(CatalogApi.getTableHistory).mockResolvedValue({ current_version: 1, history: [] });
    vi.mocked(CatalogApi.getTables).mockResolvedValue([]);
  });

  it("keeps selected B's preview when A completes last", async () => {
    const a = deferred<CatalogTablePreview>();
    const b = deferred<CatalogTablePreview>();
    vi.mocked(CatalogApi.getTablePreview)
      .mockReturnValueOnce(a.promise)
      .mockReturnValueOnce(b.promise);
    const store = setup();

    store.selectTable(1);
    store.selectTable(2);
    b.resolve(preview(300));
    await setImmediate();
    a.resolve(preview(111));
    await setImmediate();

    expect(store.selectedTableId).toBe(2);
    expect(store.tablePreview).toEqual(preview(300));
    expect(store.loadingTablePreview).toBe(false);
    expect(store.previewError).toBeNull();
    expect(store.selectedTable?.row_count).toBe(10);
  });

  it("keeps latest when version zero completes last", async () => {
    const old = deferred<CatalogTablePreview>();
    const latest = deferred<CatalogTablePreview>();
    const store = setup(true);
    store.selectTable(1);
    store.selectedTable!.is_remote_storage = false;
    store.tableHistory = { current_version: 1, history: [] };
    vi.mocked(CatalogApi.getTablePreview)
      .mockReturnValueOnce(old.promise)
      .mockReturnValueOnce(latest.promise);

    store.selectVersion(0);
    store.selectVersion(1);
    expect(CatalogApi.getTablePreview).toHaveBeenNthCalledWith(1, 1, 100, 0);
    expect(CatalogApi.getTablePreview).toHaveBeenNthCalledWith(2, 1, 100, null);
    latest.resolve(preview(301));
    await setImmediate();
    old.resolve(preview(23));
    await setImmediate();

    expect(store.selectedVersion).toBeNull();
    expect(store.tablePreview).toEqual(preview(301));
    expect(store.selectedTable?.row_count).toBe(10);
  });

  it("distinguishes repeated refreshes of the same table and version", async () => {
    const { store, old, latest, first, second } = startRefreshes();
    latest.resolve(preview(0));
    await second;
    old.resolve(preview(111));
    await first;

    expect(store.tablePreview).toEqual(preview(0));
    expect(store.loadingTablePreview).toBe(false);
  });

  it.each(["success", "failure"])(
    "ignores stale %s while the current request is loading",
    async (outcome) => {
      const { store, old, latest, first, second } = startRefreshes();
      if (outcome === "success") old.resolve(preview(111));
      else old.reject({ response: { data: { detail: "old failure" } } });
      await first;
      const stateWhilePending = {
        loading: store.loadingTablePreview,
        preview: store.tablePreview,
        error: store.previewError,
      };
      latest.resolve(preview(300));
      await second;
      expect(stateWhilePending).toEqual({ loading: true, preview: null, error: null });
      expect(store.tablePreview).toEqual(preview(300));
      expect(store.loadingTablePreview).toBe(false);
    },
  );

  it.each(["success", "failure"])("keeps the current error after stale %s", async (outcome) => {
    const { store, old, latest, first, second } = startRefreshes();
    latest.reject({ response: { data: { detail: "current failure" } } });
    await second;
    if (outcome === "success") old.resolve(preview(111));
    else old.reject(new Error("old failure"));
    await first;

    expect(store.tablePreview).toBeNull();
    expect(store.previewError).toBe("current failure");
    expect(store.loadingTablePreview).toBe(false);
  });

  it.each(["clear", "null", "remote", "virtual", "version", "edited"])(
    "invalidates pending previews on %s without starting another fetch",
    async (selection) => {
      const pending = deferred<CatalogTablePreview>();
      vi.mocked(CatalogApi.getTablePreview).mockReturnValueOnce(pending.promise);
      const store = setup(true);
      store.selectTable(1);
      store.selectVersion(0);
      store.tablePreview = preview(23);
      const request = store.loadSelectedPreview();
      if (selection === "clear") store.clearTableSelection();
      else if (selection === "null") store.selectTable(null);
      else if (selection === "remote") store.selectTable(2);
      else if (selection === "virtual") {
        store.tree[0].tables[1].table_type = "virtual";
        store.selectTable(2);
      } else if (selection === "version") store.selectVersion(null);
      else await store.handleTableEdited(table(1, true));
      const loadingAfterSelection = store.loadingTablePreview;
      pending.resolve(preview(111));
      await request;

      expect(loadingAfterSelection).toBe(false);
      expect(store.tablePreview).toBeNull();
      expect(store.previewError).toBeNull();
      expect(store.loadingTablePreview).toBe(false);
      expect(CatalogApi.getTablePreview).toHaveBeenCalledTimes(1);
    },
  );
});
