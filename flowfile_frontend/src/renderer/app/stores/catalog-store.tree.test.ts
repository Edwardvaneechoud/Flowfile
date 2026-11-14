import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogOverview, NamespaceTree } from "../types";

const getOverview = vi.fn();
const getRuns = vi.fn();
const getNamespaceTree = vi.fn();
const getFavorites = vi.fn();

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getOverview: (...args: unknown[]) => getOverview(...args),
    getRuns: (...args: unknown[]) => getRuns(...args),
    getNamespaceTree: (...args: unknown[]) => getNamespaceTree(...args),
    getFavorites: (...args: unknown[]) => getFavorites(...args),
  },
}));

import { useCatalogStore } from "./catalog-store";

const tree = (...ids: number[]) =>
  ids.map((id) => ({ id, name: `ns ${id}`, children: [] })) as unknown as NamespaceTree[];

const runsPage = { items: [], total: 0, total_success: 0, total_failed: 0, total_running: 0 };

const overview = (): CatalogOverview =>
  ({
    stats: { total_flows: 0 },
    tree: tree(1),
    flows: [],
    tables: [],
    favorites: [],
    schedules: [],
    active_runs: [],
    runs: runsPage,
    schedulers: [],
    default_namespace_id: null,
  }) as unknown as CatalogOverview;

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

describe("catalog-store first load versus refresh", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    getOverview.mockReset().mockResolvedValue(overview());
    getRuns.mockReset().mockResolvedValue(runsPage);
    getNamespaceTree.mockReset();
    getFavorites.mockReset().mockResolvedValue([]);
  });

  it("reports nothing loaded before the overview, and loaded once it lands", async () => {
    const store = useCatalogStore();
    expect(store.overviewLoaded).toBe(false);
    expect(store.overviewFailed).toBe(false);
    expect(store.treeLoaded).toBe(false);

    const pending = store.initialize();
    expect(store.loading).toBe(true);
    expect(store.overviewLoaded).toBe(false);
    await pending;

    expect(store.loading).toBe(false);
    expect(store.overviewLoaded).toBe(true);
    expect(store.treeLoaded).toBe(true);
  });

  it("is not loaded until a persisted runs page has been fetched too", async () => {
    const runs = deferred<typeof runsPage>();
    getRuns.mockReturnValueOnce(runs.promise);
    const store = useCatalogStore();
    store.runsPage = 2;

    const pending = store.initialize();
    await vi.waitFor(() => expect(getRuns).toHaveBeenCalled());
    expect(store.tree.map((n) => n.id)).toEqual([1]);
    expect(store.overviewLoaded).toBe(false);

    runs.resolve(runsPage);
    await pending;
    expect(store.overviewLoaded).toBe(true);
  });

  it("marks a failed first load as failed and clears it on retry", async () => {
    const retry = deferred<CatalogOverview>();
    getOverview.mockRejectedValueOnce(new Error("boom")).mockReturnValueOnce(retry.promise);
    const store = useCatalogStore();

    await store.initialize();
    expect(store.overviewFailed).toBe(true);
    expect(store.overviewError).toBe("boom");
    expect(store.overviewLoaded).toBe(false);

    const pending = store.initialize(true);
    expect(store.overviewFailed).toBe(false);
    retry.resolve(overview());
    await pending;
    expect(store.overviewLoaded).toBe(true);
    expect(store.overviewFailed).toBe(false);
  });

  it("does not call a later action's error an overview failure", async () => {
    const store = useCatalogStore();
    await store.initialize();
    getNamespaceTree.mockRejectedValueOnce(new Error("tree down"));

    await store.loadTree();
    expect(store.error).toBe("tree down");
    expect(store.overviewFailed).toBe(false);
  });

  it("refreshes the tree in place: the old tree stays and the loading flag never moves", async () => {
    const store = useCatalogStore();
    await store.initialize();
    const refreshed = deferred<NamespaceTree[]>();
    getNamespaceTree.mockReturnValueOnce(refreshed.promise);

    const pending = store.loadTree();
    expect(store.loading).toBe(false);
    expect(store.treeLoaded).toBe(true);
    expect(store.tree.map((n) => n.id)).toEqual([1]);

    refreshed.resolve(tree(1, 2));
    await pending;
    expect(store.loading).toBe(false);
    expect(store.tree.map((n) => n.id)).toEqual([1, 2]);
  });

  it("keeps the current tree when a refresh fails", async () => {
    const store = useCatalogStore();
    await store.initialize();
    getNamespaceTree.mockRejectedValueOnce(new Error("down"));

    await store.loadTree();
    expect(store.tree.map((n) => n.id)).toEqual([1]);
    expect(store.treeLoaded).toBe(true);
  });

  it("marks the tree loaded when a refresh lands before any overview", async () => {
    getNamespaceTree.mockResolvedValueOnce(tree(5));
    const store = useCatalogStore();

    await store.loadTree();
    expect(store.treeLoaded).toBe(true);
    expect(store.overviewLoaded).toBe(false);
  });

  it("ignores a tree refresh that resolves after a newer one", async () => {
    const store = useCatalogStore();
    const older = deferred<NamespaceTree[]>();
    const newer = deferred<NamespaceTree[]>();
    getNamespaceTree.mockReturnValueOnce(older.promise).mockReturnValueOnce(newer.promise);

    const first = store.loadTree();
    const second = store.loadTree();
    newer.resolve(tree(2));
    await second;
    older.resolve(tree(1));
    await first;

    expect(store.tree.map((n) => n.id)).toEqual([2]);
  });

  it("keeps a tree that was reloaded while a slower overview was out", async () => {
    const slow = deferred<CatalogOverview>();
    getOverview.mockReturnValueOnce(slow.promise);
    getNamespaceTree.mockResolvedValueOnce(tree(1, 2));
    const store = useCatalogStore();
    store.tree = tree(1);
    store.treeLoaded = true;

    const loading = store.initialize();
    await store.loadTree();
    slow.resolve(overview());
    await loading;

    expect(store.tree.map((n) => n.id)).toEqual([1, 2]);
    expect(store.overviewLoaded).toBe(true);
  });

  it("applies only the newest of two overviews, whichever lands last", async () => {
    const first = deferred<CatalogOverview>();
    const second = deferred<CatalogOverview>();
    getOverview.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const store = useCatalogStore();

    const older = store.initialize();
    const newer = store.initialize(true);
    second.resolve({ ...overview(), tree: tree(1, 2) });
    await newer;
    expect(store.loading).toBe(false);
    first.resolve(overview());
    await older;

    expect(store.tree.map((n) => n.id)).toEqual([1, 2]);
  });

  it("keeps a slice its own loader refreshed while the overview was out", async () => {
    const slow = deferred<CatalogOverview>();
    getOverview.mockReturnValueOnce(slow.promise);
    getFavorites.mockResolvedValueOnce([{ id: 9 }]);
    const store = useCatalogStore();

    const loading = store.initialize();
    await store.loadFavorites();
    slow.resolve(overview());
    await loading;

    expect(store.favorites.map((f) => f.id)).toEqual([9]);
  });

  it("reports a persisted runs page that failed to load instead of an empty list", async () => {
    getRuns.mockRejectedValueOnce(new Error("boom"));
    const store = useCatalogStore();
    store.runsPage = 3;

    await store.initialize();

    expect(store.overviewLoaded).toBe(true);
    expect(store.runsError).toBe("boom");
    expect(store.runs).toEqual([]);
  });

  it("reloads the tree, in place, on a switch to the catalog tab", async () => {
    getNamespaceTree.mockResolvedValueOnce(tree(1, 2));
    const store = useCatalogStore();
    await store.initialize();

    store.setActiveTab("catalog");
    expect(store.loading).toBe(false);
    await vi.waitFor(() => expect(store.tree.map((n) => n.id)).toEqual([1, 2]));
  });
});
