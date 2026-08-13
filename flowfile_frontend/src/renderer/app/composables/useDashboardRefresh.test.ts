import { describe, expect, it, vi } from "vitest";
import { ref } from "vue";
import type { DashboardLayout, DashboardTile } from "../types";
import { bumpVizNonces, useDashboardRefresh } from "./useDashboardRefresh";

let tileSeq = 0;
const tile = (vizId: number | null): DashboardTile => ({
  id: `tile-${tileSeq++}`,
  type: vizId == null ? "text" : "viz",
  viz_id: vizId,
  chart_index: 0,
  x: 0,
  y: 0,
  w: 6,
  h: 6,
});

const layoutOf = (tiles: DashboardTile[]): DashboardLayout => ({
  tiles,
  grid: { cols: 12, row_height: 40, version: 1 },
  filters: [],
});

describe("bumpVizNonces", () => {
  it("bumps each distinct viz id once", () => {
    expect(bumpVizNonces({}, [tile(1), tile(2)])).toEqual({ 1: 1, 2: 1 });
  });

  it("skips text tiles (null viz_id)", () => {
    expect(bumpVizNonces({}, [tile(null), tile(3)])).toEqual({ 3: 1 });
  });

  it("dedupes tiles sharing a viz id to a single increment", () => {
    expect(bumpVizNonces({}, [tile(7), tile(7), tile(7)])).toEqual({ 7: 1 });
  });

  it("preserves untouched entries and increments existing ones", () => {
    expect(bumpVizNonces({ 1: 3, 9: 2 }, [tile(1)])).toEqual({ 1: 4, 9: 2 });
  });

  it("returns a new object", () => {
    const input = { 1: 1 };
    const result = bumpVizNonces(input, []);
    expect(result).not.toBe(input);
    expect(input).toEqual({ 1: 1 });
  });
});

describe("useDashboardRefresh", () => {
  const setup = (opts?: {
    refreshDatasources?: (force?: boolean) => Promise<void>;
    tiles?: DashboardTile[];
  }) => {
    const layout = ref(layoutOf(opts?.tiles ?? [tile(1), tile(2)]));
    const vizRefreshNonces = ref<Record<number, number>>({});
    const refreshDatasources = vi.fn(opts?.refreshDatasources ?? (() => Promise.resolve()));
    const invalidateFields = vi.fn();
    const api = useDashboardRefresh({
      layout,
      vizRefreshNonces,
      refreshDatasources,
      invalidateFields,
    });
    return { layout, vizRefreshNonces, refreshDatasources, invalidateFields, ...api };
  };

  it("invalidates fields before force-refreshing datasources", async () => {
    const calls: string[] = [];
    const { refreshAll, refreshDatasources, invalidateFields } = setup({
      refreshDatasources: () => {
        calls.push("refreshDatasources");
        return Promise.resolve();
      },
    });
    invalidateFields.mockImplementation(() => calls.push("invalidateFields"));

    await refreshAll();

    expect(calls).toEqual(["invalidateFields", "refreshDatasources"]);
    expect(refreshDatasources).toHaveBeenCalledWith(true);
  });

  it("is refreshing while the datasource fetch is in flight, then settles", async () => {
    let resolveFetch!: () => void;
    const { refreshAll, refreshing, vizRefreshNonces } = setup({
      refreshDatasources: () => new Promise<void>((resolve) => (resolveFetch = resolve)),
    });

    const pending = refreshAll();
    expect(refreshing.value).toBe(true);
    expect(vizRefreshNonces.value).toEqual({});

    resolveFetch();
    await pending;

    expect(refreshing.value).toBe(false);
    expect(vizRefreshNonces.value).toEqual({ 1: 1, 2: 1 });
  });

  it("bumps statsRefreshNonce once per call", async () => {
    const { refreshAll, statsRefreshNonce } = setup();

    expect(statsRefreshNonce.value).toBe(0);
    await refreshAll();
    expect(statsRefreshNonce.value).toBe(1);
    await refreshAll();
    expect(statsRefreshNonce.value).toBe(2);
  });

  it("still bumps nonces and clears refreshing when the fetch rejects, then rethrows", async () => {
    const { refreshAll, refreshing, statsRefreshNonce, vizRefreshNonces } = setup({
      refreshDatasources: () => Promise.reject(new Error("network down")),
    });

    await expect(refreshAll()).rejects.toThrow("network down");

    expect(refreshing.value).toBe(false);
    expect(statsRefreshNonce.value).toBe(1);
    expect(vizRefreshNonces.value).toEqual({ 1: 1, 2: 1 });
  });
});
