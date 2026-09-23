import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogOverview } from "../types";

const getOverview = vi.fn();
const getRuns = vi.fn();

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getOverview: (...args: unknown[]) => getOverview(...args),
    getRuns: (...args: unknown[]) => getRuns(...args),
  },
}));

import { OVERVIEW_TTL_MS, useCatalogStore } from "./catalog-store";

const runsPage = (ids: number[], total = ids.length) => ({
  items: ids.map((id) => ({ id, flow_name: `run ${id}` })),
  total,
  total_success: total,
  total_failed: 0,
  total_running: 0,
});

const overview = (): CatalogOverview =>
  ({
    stats: { total_flows: 1 },
    tree: [{ id: 1, name: "General", children: [] }],
    flows: [{ id: 10, name: "flow" }],
    tables: [{ id: 20, name: "table" }],
    favorites: [{ id: 10, name: "flow" }],
    schedules: [{ id: 30 }],
    active_runs: [{ id: 40 }],
    runs: runsPage([100, 101]),
    scheduler: { active: true },
    default_namespace_id: 2,
  }) as unknown as CatalogOverview;

describe("catalog-store initialize", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.useFakeTimers();
    getOverview.mockReset().mockResolvedValue(overview());
    getRuns.mockReset().mockResolvedValue(runsPage([200], 5));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("fills every slice from the single overview request", async () => {
    const store = useCatalogStore();
    await store.initialize();

    expect(getOverview).toHaveBeenCalledTimes(1);
    expect(getOverview).toHaveBeenCalledWith(store.runsPageSize);
    expect(getRuns).not.toHaveBeenCalled();
    expect(store.tree.map((n) => n.id)).toEqual([1]);
    expect(store.allFlows.map((f) => f.id)).toEqual([10]);
    expect(store.allTables.map((t) => t.id)).toEqual([20]);
    expect(store.favorites.map((f) => f.id)).toEqual([10]);
    expect(store.schedules.map((s) => s.id)).toEqual([30]);
    expect(store.activeRuns.map((r) => r.id)).toEqual([40]);
    expect(store.stats?.total_flows).toBe(1);
    expect(store.schedulerStatus?.active).toBe(true);
    expect(store.defaultNamespaceId).toBe(2);
    expect(store.runs.map((r) => r.id)).toEqual([100, 101]);
    expect(store.runsTotal).toBe(2);
    expect(store.loading).toBe(false);
    expect(store.error).toBeNull();
  });

  it("skips the request when re-entered within the TTL, refetches after it or when forced", async () => {
    const store = useCatalogStore();
    await store.initialize();
    await store.initialize();
    expect(getOverview).toHaveBeenCalledTimes(1);

    await store.initialize(true);
    expect(getOverview).toHaveBeenCalledTimes(2);

    vi.advanceTimersByTime(OVERVIEW_TTL_MS);
    await store.initialize();
    expect(getOverview).toHaveBeenCalledTimes(3);
  });

  it("re-queries runs when a page or filter other than the default is persisted", async () => {
    const store = useCatalogStore();
    store.runsPage = 3;
    await store.initialize();

    expect(getRuns).toHaveBeenCalledTimes(1);
    expect(store.runs.map((r) => r.id)).toEqual([200]);
    expect(store.runsTotal).toBe(5);
  });

  it("surfaces a failed overview and allows a retry", async () => {
    getOverview.mockRejectedValueOnce(new Error("boom"));
    const store = useCatalogStore();
    await store.initialize();

    expect(store.error).toBe("boom");
    expect(store.overviewLoadedAt).toBeNull();

    await store.initialize();
    expect(getOverview).toHaveBeenCalledTimes(2);
    expect(store.error).toBeNull();
  });
});
