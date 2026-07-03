import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  fetchNextNodeSuggestions: vi.fn(),
  resolveSurface: vi.fn(() => ({ provider: "anthropic", model: "m" })),
  insertNode: vi.fn(),
  connectNode: vi.fn(),
  axiosPost: vi.fn(),
}));

vi.mock("../api/ai.api", () => ({
  fetchNextNodeSuggestions: mocks.fetchNextNodeSuggestions,
  AiDisabledError: class AiDisabledError extends Error {},
}));

vi.mock("../api/flow.api", () => ({
  FlowApi: { insertNode: mocks.insertNode, connectNode: mocks.connectNode },
}));

vi.mock("axios", () => ({ default: { post: mocks.axiosPost } }));

vi.mock("./ai-store", () => ({
  useAiStore: () => ({ resolveSurface: mocks.resolveSurface }),
}));

import { GhostNodeAnchor, useAiGhostNodeStore } from "./ai-ghost-node-store";
import type { NextNodeSuggestion } from "../api/ai.api";

const anchor = (): GhostNodeAnchor => ({
  upstreamNodeId: "3",
  screenX: 100,
  screenY: 200,
  nodeX: 10,
  nodeY: 20,
});

describe("useAiGhostNodeStore — intent flow", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    mocks.fetchNextNodeSuggestions.mockReset();
    mocks.insertNode.mockReset().mockResolvedValue({});
    mocks.connectNode.mockReset().mockResolvedValue({});
    mocks.axiosPost.mockReset().mockResolvedValue({});
    mocks.resolveSurface.mockReturnValue({ provider: "anthropic", model: "m" });
  });

  it("beginIntent opens the inline box at the anchor without firing a request", () => {
    const store = useAiGhostNodeStore();
    store.beginIntent(anchor(), 1);
    expect(store.awaitingIntent).toBe(true);
    expect(store.anchor).toEqual(anchor());
    expect(store.suggestions).toEqual([]);
    expect(store.degradedReason).toBeNull();
    expect(mocks.fetchNextNodeSuggestions).not.toHaveBeenCalled();
  });

  it("clear() resets the intent state and anchor", () => {
    const store = useAiGhostNodeStore();
    store.beginIntent(anchor(), 1);
    store.clear();
    expect(store.awaitingIntent).toBe(false);
    expect(store.anchor).toBeNull();
  });

  it("requestSuggestions leaves the intent state and fills the resolved provider + intent", async () => {
    mocks.fetchNextNodeSuggestions.mockResolvedValue({
      suggestions: [
        {
          nodeType: "filter",
          settings: {},
          label: "Filter",
          description: null,
          predictedOutputSchema: null,
          rationale: null,
        },
      ],
      degraded: false,
      reason: null,
    });
    const store = useAiGhostNodeStore();
    store.beginIntent(anchor(), 1);
    await store.requestSuggestions(
      { flowId: 1, upstreamNodeId: "3", intent: "filter to EU" },
      anchor(),
    );
    expect(store.awaitingIntent).toBe(false);
    expect(store.isLoading).toBe(false);
    expect(store.suggestions).toHaveLength(1);
    const sentBody = mocks.fetchNextNodeSuggestions.mock.calls[0][0];
    expect(sentBody.provider).toBe("anthropic");
    expect(sentBody.intent).toBe("filter to EU");
  });

  it("maps a degraded response's reason into degradedReason (frontend contract)", async () => {
    mocks.fetchNextNodeSuggestions.mockResolvedValue({
      suggestions: [],
      degraded: true,
      reason: "upstream_schema_unknown",
    });
    const store = useAiGhostNodeStore();
    await store.requestSuggestions({ flowId: 1, upstreamNodeId: "3" }, anchor());
    expect(store.degradedReason).toBe("upstream_schema_unknown");
    expect(store.suggestions).toEqual([]);
  });

  it("materialize sends pos_x/pos_y in the settings body so update_settings keeps the position", async () => {
    const suggestion = {
      nodeType: "filter",
      settings: { flow_id: 999, node_id: 999, filter_input: {} },
      label: "Filter",
      description: null,
      predictedOutputSchema: null,
      rationale: null,
    } as unknown as NextNodeSuggestion;

    const store = useAiGhostNodeStore();
    const newNodeId = await store.materialize(suggestion, 1, 4, 520, 150);

    expect(newNodeId).not.toBeNull();
    // add_node was placed at the computed position.
    expect(mocks.insertNode).toHaveBeenCalledWith(1, newNodeId, "filter", 520, 150);
    // update_settings replaces the whole setting_input, so the position must ride
    // along or the node snaps back to (0,0).
    const [url, body] = mocks.axiosPost.mock.calls[0];
    expect(url).toBe("update_settings/");
    expect(body).toMatchObject({ flow_id: 1, node_id: newNodeId, pos_x: 520, pos_y: 150 });
  });
});
