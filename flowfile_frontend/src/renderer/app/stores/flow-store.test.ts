// @vitest-environment happy-dom
// `../api/flow.api` and sibling stores are mocked: they reach axios.config at import time.
import { createPinia, setActivePinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { HistoryState } from "../types";

const mocks = vi.hoisted(() => ({
  bumpGraphVersion: vi.fn(),
  fetch: vi.fn(),
  placement: vi.fn(),
  editor: { graphVersion: 0, isRunning: false },
  results: {
    getRunResult: vi.fn(),
    applySettingsValidation: vi.fn(),
    applyPlacement: vi.fn(),
    placements: {} as Record<number, { cached_node_ids?: number[] }>,
  },
}));

vi.mock("../api/flow.api", () => ({
  FlowApi: { getSettingsValidation: mocks.fetch, getPlacement: mocks.placement },
}));
vi.mock("./editor-store", () => ({
  useEditorStore: () => ({ bumpGraphVersion: mocks.bumpGraphVersion, ...mocks.editor }),
}));
vi.mock("./results-store", () => ({ useResultsStore: () => mocks.results }));

import { useFlowStore } from "./flow-store";

const history = (flowId: number | null, undoCount: number): HistoryState => ({
  flow_id: flowId,
  can_undo: undoCount > 0,
  can_redo: false,
  undo_description: undoCount > 0 ? "Add filter node" : null,
  redo_description: null,
  undo_count: undoCount,
  redo_count: 0,
});

beforeEach(() => {
  vi.useFakeTimers();
  sessionStorage.clear();
  setActivePinia(createPinia());
  mocks.bumpGraphVersion.mockReset();
  mocks.fetch.mockReset();
  mocks.placement.mockReset();
  mocks.results.applySettingsValidation.mockReset();
  mocks.results.placements = {};
  mocks.results.applyPlacement.mockReset().mockImplementation((flowId, data) => {
    mocks.results.placements[flowId] = data;
  });
  mocks.results.getRunResult.mockReset().mockReturnValue(null);
  mocks.editor.graphVersion = 0;
  mocks.editor.isRunning = false;
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
});

async function refreshPlacement(store: ReturnType<typeof useFlowStore>) {
  store.fetchPlacement();
  await vi.advanceTimersByTimeAsync(400);
}

describe("flow-store updateHistoryState", () => {
  it("applies the active flow's history and bumps the dirty counter", () => {
    const store = useFlowStore();
    store.setFlowId(4);
    store.updateHistoryState(history(4, 2));
    expect(store.historyState.undo_count).toBe(2);
    expect(store.canUndo).toBe(true);
    expect(mocks.bumpGraphVersion).toHaveBeenCalledTimes(1);
  });

  it("ignores a history that belongs to another flow", () => {
    const store = useFlowStore();
    store.setFlowId(4);
    store.updateHistoryState(history(4, 1));
    store.updateHistoryState(history(9, 5));
    expect(store.historyState.flow_id).toBe(4);
    expect(store.historyState.undo_count).toBe(1);
    expect(mocks.bumpGraphVersion).toHaveBeenCalledTimes(1);
  });

  it("accepts a history without a flow id", () => {
    const store = useFlowStore();
    store.setFlowId(4);
    store.updateHistoryState(history(null, 3));
    expect(store.historyState.undo_count).toBe(3);
  });
});

describe("flow-store run state signal", () => {
  it("requestRunStateRefresh bumps a monotonic counter", () => {
    const store = useFlowStore();
    expect(store.pendingRunStateCounter).toBe(0);
    store.requestRunStateRefresh();
    store.requestRunStateRefresh();
    expect(store.pendingRunStateCounter).toBe(2);
  });
});

describe("known cached results", () => {
  it("keeps the last cache reply visible during edits until the replacement lands", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.placement.mockResolvedValue({ cached_node_ids: [1] });
    await refreshPlacement(store);
    expect(store.isNodeCached(1)).toBe(true);
    expect(store.isNodeCached(2)).toBe(false);
    expect(mocks.placement).toHaveBeenCalledTimes(1);
    expect(mocks.fetch).not.toHaveBeenCalled();
    mocks.editor.graphVersion++;
    expect(store.isNodeCached(1)).toBe(true);
    store.fetchPlacement();
    expect(store.isNodeCached(1)).toBe(true);
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [] });
    await vi.advanceTimersByTimeAsync(400);
    expect(store.isNodeCached(1)).toBe(false);
  });

  it("hides icons while running and keeps the last reply scoped to its flow", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.placement.mockResolvedValue({ cached_node_ids: [1] });
    await refreshPlacement(store);
    mocks.editor.isRunning = true;
    expect(store.isNodeCached(1)).toBe(false);
    mocks.editor.isRunning = false;
    mocks.results.getRunResult.mockReturnValue({ start_time: "new run" });
    expect(store.isNodeCached(1)).toBe(true);
    mocks.results.getRunResult.mockReturnValue(null);
    store.setFlowId(84);
    expect(store.isNodeCached(1)).toBe(false);
    store.setFlowId(83);
    expect(store.isNodeCached(1)).toBe(true);
  });

  it("keeps cache replies isolated when another flow with the same node ids runs", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [1] });
    await refreshPlacement(store);
    expect(store.isNodeCached(1)).toBe(true);

    store.setFlowId(84);
    expect(store.isNodeCached(1)).toBe(false);
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [2] });
    await refreshPlacement(store);
    mocks.editor.isRunning = true;
    expect(store.isNodeCached(1)).toBe(false);
    expect(store.isNodeCached(2)).toBe(false);
    mocks.editor.isRunning = false;
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [1, 2] });
    await refreshPlacement(store);
    expect(store.isNodeCached(1)).toBe(true);
    expect(store.isNodeCached(2)).toBe(true);

    store.setFlowId(83);
    expect(store.isNodeCached(1)).toBe(true);
    expect(store.isNodeCached(2)).toBe(false);
    expect(mocks.results.placements[83]?.cached_node_ids).toEqual([1]);
    expect(mocks.results.placements[84]?.cached_node_ids).toEqual([1, 2]);
  });

  it.each(["edit", "switch", "newer refresh"])(
    "ignores a positive reply after %s",
    async (change) => {
      const store = useFlowStore();
      store.setFlowId(83);
      let resolve!: (value: any) => void;
      mocks.placement.mockReturnValueOnce(
        new Promise((done) => {
          resolve = done;
        }),
      );
      store.fetchPlacement();
      await vi.advanceTimersByTimeAsync(400);
      if (change === "edit") mocks.editor.graphVersion++;
      if (change === "switch") {
        store.setFlowId(84);
        store.setFlowId(83);
      }
      if (change === "newer refresh") {
        mocks.placement.mockResolvedValueOnce({ cached_node_ids: [] });
        await refreshPlacement(store);
      }
      resolve({ cached_node_ids: [1] });
      await vi.advanceTimersByTimeAsync(0);
      expect(store.isNodeCached(1)).toBe(false);
      expect(mocks.results.placements[83]?.cached_node_ids ?? []).toEqual([]);
    },
  );

  it("keeps validation independent when placement fails and preserves the last cache reply", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [1] });
    await refreshPlacement(store);
    expect(store.isNodeCached(1)).toBe(true);

    mocks.placement.mockRejectedValueOnce(new Error("worker unavailable"));
    mocks.fetch.mockResolvedValueOnce({ enabled: true, nodes: [{ node_id: 2, issues: [] }] });
    store.fetchPlacement();
    expect(store.isNodeCached(1)).toBe(true);
    await store.fetchSettingsValidation();
    expect(mocks.results.applySettingsValidation).toHaveBeenCalledWith(83, {
      enabled: true,
      nodes: [{ node_id: 2, issues: [] }],
    });
    await vi.advanceTimersByTimeAsync(400);
    expect(store.isNodeCached(1)).toBe(true);
  });

  it("shows the last cache reply once a run is no longer active", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.editor.isRunning = true;
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [1] });
    await refreshPlacement(store);
    mocks.editor.isRunning = false;
    expect(store.isNodeCached(1)).toBe(true);
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [1] });
    await refreshPlacement(store);
    expect(store.isNodeCached(1)).toBe(true);
  });

  it.each([false, true])(
    "rechecks cached refusals without applying stale replies (edited=%s)",
    async (edited) => {
      const store = useFlowStore();
      store.setFlowId(83);
      const fast = {
        cached_node_ids: [],
        unchecked_backend_ids: ["remote"],
        refusals: [{ node_id: 1, backend_id: "remote", reason: "version mismatch" }],
      };
      let resolve!: (value: any) => void;
      mocks.placement.mockResolvedValueOnce(fast).mockReturnValueOnce(
        new Promise((done) => {
          resolve = done;
        }),
      );
      store.fetchPlacement();
      await vi.advanceTimersByTimeAsync(400);
      expect(mocks.results.applyPlacement).toHaveBeenCalledTimes(1);
      expect(mocks.results.applyPlacement).toHaveBeenLastCalledWith(83, fast);
      expect(mocks.placement).toHaveBeenNthCalledWith(2, 83, true);

      const recovered = { cached_node_ids: [], unchecked_backend_ids: [], refusals: [] };
      if (edited) mocks.editor.graphVersion++;
      resolve(recovered);
      await vi.advanceTimersByTimeAsync(0);
      expect(mocks.results.applyPlacement).toHaveBeenCalledTimes(edited ? 1 : 2);
      expect(mocks.results.applyPlacement).toHaveBeenLastCalledWith(83, edited ? fast : recovered);
    },
  );

  it.each([null, "previous run"])(
    "applies placement when run history loads (%s)",
    async (start) => {
      const store = useFlowStore();
      store.setFlowId(83);
      let resolve!: (value: any) => void;
      mocks.placement.mockReturnValueOnce(
        new Promise((done) => {
          resolve = done;
        }),
      );
      store.fetchPlacement();
      await vi.advanceTimersByTimeAsync(400);
      mocks.results.getRunResult.mockReturnValue({ start_time: start });
      resolve({ cached_node_ids: [1], refusals: [{ node_id: 2 }] });
      await vi.advanceTimersByTimeAsync(0);
      expect(mocks.results.placements[83]).toEqual({
        cached_node_ids: [1],
        refusals: [{ node_id: 2 }],
      });
      expect(store.isNodeCached(1)).toBe(true);
    },
  );

  it("does not cancel a pending placement when setting the already active flow", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [1] });
    store.fetchPlacement();
    store.setFlowId(83);
    await vi.advanceTimersByTimeAsync(400);
    expect(store.isNodeCached(1)).toBe(true);
    expect(mocks.placement).toHaveBeenCalledTimes(1);
  });

  it("uses one live request to refresh a previously refused backend", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.results.placements[83] = { cached_node_ids: [] };
    mocks.placement.mockImplementation((_flowId, probe) =>
      Promise.resolve({
        cached_node_ids: [],
        unchecked_backend_ids: probe ? [] : ["remote"],
      }),
    );
    await refreshPlacement(store);
    expect(mocks.placement).toHaveBeenCalledTimes(1);
    expect(mocks.placement).toHaveBeenCalledWith(83, true);
  });

  it("does not probe known backends again", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.placement.mockResolvedValueOnce({ cached_node_ids: [1], unchecked_backend_ids: [] });
    store.fetchPlacement();
    await vi.advanceTimersByTimeAsync(400);
    expect(mocks.placement).toHaveBeenCalledTimes(1);
  });
});
