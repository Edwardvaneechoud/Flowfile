// @vitest-environment happy-dom
// The `../api` barrel and sibling stores are mocked: they reach axios.config at import time.
import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HistoryState } from "../types";

const mocks = vi.hoisted(() => ({ bumpGraphVersion: vi.fn() }));

vi.mock("../api", () => ({ FlowApi: {} }));
vi.mock("./editor-store", () => ({
  useEditorStore: () => ({ bumpGraphVersion: mocks.bumpGraphVersion }),
}));
vi.mock("./results-store", () => ({ useResultsStore: () => ({}) }));

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
