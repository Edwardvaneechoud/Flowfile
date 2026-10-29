// @vitest-environment happy-dom
// `../api/flow.api` and sibling stores are mocked: they reach axios.config at import time.
import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HistoryState } from "../types";

const mocks = vi.hoisted(() => ({
  bumpGraphVersion: vi.fn(),
  fetch: vi.fn(),
  editor: { graphVersion: 0, isRunning: false },
  results: { getRunResult: vi.fn(), applySettingsValidation: vi.fn() },
}));

vi.mock("../api/flow.api", () => ({ FlowApi: { getSettingsValidation: mocks.fetch } }));
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
  sessionStorage.clear();
  setActivePinia(createPinia());
  mocks.bumpGraphVersion.mockReset();
  mocks.fetch.mockReset();
  mocks.results.getRunResult.mockReset().mockReturnValue(null);
  mocks.editor.graphVersion = 0;
  mocks.editor.isRunning = false;
});

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
  it("uses the existing refresh response and hides the marker after an edit", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.fetch.mockResolvedValue({ enabled: true, nodes: [], cached_node_ids: [1] });
    await store.fetchSettingsValidation();
    expect(store.isNodeCached(1)).toBe(true);
    expect(store.isNodeCached(2)).toBe(false);
    expect(mocks.fetch).toHaveBeenCalledTimes(1);
    mocks.editor.graphVersion++;
    expect(store.isNodeCached(1)).toBe(false);
  });

  it("hides stale knowledge while running, after a different run, or on another flow", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.fetch.mockResolvedValue({ enabled: false, nodes: [], cached_node_ids: [1] });
    await store.fetchSettingsValidation();
    mocks.editor.isRunning = true;
    expect(store.isNodeCached(1)).toBe(false);
    mocks.editor.isRunning = false;
    mocks.results.getRunResult.mockReturnValue({ start_time: "new run" });
    expect(store.isNodeCached(1)).toBe(false);
    mocks.results.getRunResult.mockReturnValue(null);
    store.setFlowId(84);
    store.setFlowId(83);
    expect(store.isNodeCached(1)).toBe(false);
  });

  it.each(["edit", "switch", "newer refresh", "new run"])(
    "ignores a positive reply after %s",
    async (change) => {
      const store = useFlowStore();
      store.setFlowId(83);
      let resolve!: (value: any) => void;
      mocks.fetch.mockReturnValueOnce(
        new Promise((done) => {
          resolve = done;
        }),
      );
      const pending = store.fetchSettingsValidation();
      if (change === "edit") mocks.editor.graphVersion++;
      if (change === "new run")
        mocks.results.getRunResult.mockReturnValue({ start_time: "new run" });
      if (change === "switch") {
        store.setFlowId(84);
        store.setFlowId(83);
      }
      if (change === "newer refresh") {
        mocks.fetch.mockResolvedValueOnce({ enabled: true, nodes: [], cached_node_ids: [] });
        await store.fetchSettingsValidation();
      }
      resolve({ enabled: true, nodes: [], cached_node_ids: [1] });
      await pending;
      expect(store.isNodeCached(1)).toBe(false);
    },
  );

  it("accepts older servers without cache metadata", async () => {
    const store = useFlowStore();
    store.setFlowId(83);
    mocks.fetch.mockResolvedValue({ enabled: true, nodes: [] });
    await store.fetchSettingsValidation();
    expect(store.isNodeCached(1)).toBe(false);
  });
});
