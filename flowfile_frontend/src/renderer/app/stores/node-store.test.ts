// Unit tests for the flow-aware nodeData cache in the node store. Node ids are
// per-flow integers that collide across flows, so the single-slot nodeData cache
// must be keyed by the owning flow or it hands back / patches the wrong flow's blob.
//
// The `../api` barrel and sibling stores are mocked because they reach axios.config
// (DOM globals) at import time and would crash under the node env.

import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { NodeData } from "../types";

const mocks = vi.hoisted(() => ({
  getNodeData: vi.fn(),
  flowState: { flowId: 1 },
}));

vi.mock("../api", () => ({
  NodeApi: { getNodeData: mocks.getNodeData },
  ExpressionsApi: {},
}));

vi.mock("./flow-store", () => ({
  useFlowStore: () => mocks.flowState,
}));

vi.mock("./results-store", () => ({
  useResultsStore: () => ({}),
}));

vi.mock("./editor-store", () => ({
  useEditorStore: () => ({}),
}));

import { useNodeStore } from "./node-store";

const nodeData = (nodeId: number, description = ""): NodeData => ({
  flow_id: 0,
  node_id: nodeId,
  flow_type: "test",
  has_run: false,
  is_cached: false,
  setting_input: { description, node_reference: "" },
});

beforeEach(() => {
  setActivePinia(createPinia());
  mocks.flowState.flowId = 1;
  mocks.getNodeData.mockReset();
});

describe("node-store getNodeData flow-aware cache", () => {
  it("serves the cache only within the same flow", async () => {
    const store = useNodeStore();
    mocks.getNodeData.mockImplementation((flowId: number, id: number) =>
      Promise.resolve(nodeData(id, `flow-${flowId}`)),
    );

    store.nodeId = 3;
    const first = await store.getNodeData(3, true);
    expect(first?.setting_input?.description).toBe("flow-1");
    expect(mocks.getNodeData).toHaveBeenCalledTimes(1);

    // Same node, same flow, cached -> no second fetch.
    await store.getNodeData(3, true);
    expect(mocks.getNodeData).toHaveBeenCalledTimes(1);
  });

  it("treats a same-numbered node in a different flow as a cache miss", async () => {
    const store = useNodeStore();
    mocks.getNodeData.mockImplementation((flowId: number, id: number) =>
      Promise.resolve(nodeData(id, `flow-${flowId}`)),
    );

    store.nodeId = 3;
    await store.getNodeData(3, true);
    expect(store.nodeDataFlowId).toBe(1);

    // Switch flows; node id 3 exists in flow 2 too.
    mocks.flowState.flowId = 2;
    const refetched = await store.getNodeData(3, true);

    expect(mocks.getNodeData).toHaveBeenCalledTimes(2);
    expect(mocks.getNodeData).toHaveBeenLastCalledWith(2, 3, false);
    expect(refetched?.setting_input?.description).toBe("flow-2");
  });

  it("resets the flow stamp when a fetch fails", async () => {
    const store = useNodeStore();
    mocks.getNodeData.mockRejectedValueOnce(new Error("boom"));

    store.nodeId = 3;
    const result = await store.getNodeData(3, true);

    expect(result).toBeNull();
    expect(store.nodeData).toBeNull();
    expect(store.nodeDataFlowId).toBe(-1);
  });
});

describe("node-store description write-back guard", () => {
  it("does not patch a nodeData blob owned by a different flow", () => {
    const store = useNodeStore();
    store.nodeData = nodeData(3, "original");
    store.nodeDataFlowId = 1;

    // Caching a description for the same node id but in flow 2 must not touch
    // flow 1's live blob.
    store.cacheNodeDescriptionDict(2, 3, "leaked");
    expect(store.nodeData?.setting_input?.description).toBe("original");

    // Same flow -> the live blob is updated.
    store.cacheNodeDescriptionDict(1, 3, "updated");
    expect(store.nodeData?.setting_input?.description).toBe("updated");
  });
});
