// W35 — unit tests for the diff approval store.
//
// The store talks to W41 through three pure-fetch wrappers; we mock the
// wrapper module so the tests don't reach the network. Mocking is done
// by replacing the entire `services/aiDiffClient` module with a small
// in-test factory; the real module reaches DOM globals (`window`,
// `localStorage`) at import time, which the Node test environment
// doesn't provide. The mock supplies its own `AiDiffHttpError` so the
// `instanceof` checks in the store still match.
//
// Pinia is set up per-test for isolation.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Hoisted into the module-init phase so it lands before the store's
// import chain runs. The exported `AiDiffHttpError` class is the *same*
// reference both the test and the store import — wired via the mock
// factory below.
const mockSymbols = vi.hoisted(() => {
  class AiDiffHttpError extends Error {
    constructor(
      public readonly status: number,
      public readonly detail: unknown,
    ) {
      super(typeof detail === "string" ? detail : `HTTP ${status}`);
      this.name = "AiDiffHttpError";
    }
  }
  return {
    AiDiffHttpError,
    stageDiff: vi.fn(),
    acceptDiff: vi.fn(),
    rejectDiff: vi.fn(),
  };
});

vi.mock("../services/aiDiffClient", () => ({
  AiDiffHttpError: mockSymbols.AiDiffHttpError,
  stageDiff: mockSymbols.stageDiff,
  acceptDiff: mockSymbols.acceptDiff,
  rejectDiff: mockSymbols.rejectDiff,
}));

import { useAiDiffStore } from "./ai-diff-store";

const {
  AiDiffHttpError,
  stageDiff: mockStage,
  acceptDiff: mockAccept,
  rejectDiff: mockReject,
} = mockSymbols;

interface StageDiffRequestShape {
  session_id: string;
  flow_id: number;
  rationale?: string | null;
  staged_results: Array<{
    tool_name: string;
    audit_id?: number | null;
    staged_node_payload?: Record<string, unknown>;
  }>;
}

const sampleStageRequest = (): StageDiffRequestShape => ({
  session_id: "sess-1",
  flow_id: 42,
  rationale: "Add a filter on amount",
  staged_results: [
    {
      tool_name: "flowfile.graph.add_filter",
      audit_id: 101,
      staged_node_payload: {
        node_type: "filter",
        settings: { node_id: 7, filter_input: { advanced_filter: "[amount] > 0" } },
        insertion_context: {
          upstream_node_ids: [3],
          right_input_node_id: null,
          pos_x: 200,
          pos_y: 100,
        },
        predicted_output_schema: [{ name: "amount", data_type: "Float64", nullable: true }],
      },
    },
    {
      tool_name: "flowfile.graph.connect",
      audit_id: 102,
      staged_node_payload: {
        connection: {
          output_connection_class: { node_id: 3, connection_class: "main" },
          input_connection_class: { node_id: 7, connection_class: "main" },
        },
      },
    },
    {
      tool_name: "flowfile.graph.delete_node",
      audit_id: 103,
      staged_node_payload: { delete_node_id: 99 },
    },
  ],
});

const stageOk = (diffId = "diff-abc") => ({ diff_id: diffId, op_count: 3 });

const acceptOk = (diffId = "diff-abc") => ({
  status: "accepted" as const,
  diff_id: diffId,
  applied_node_ids: [7],
  applied_connection_count: 1,
  removed_node_ids: [99],
  removed_connection_count: 0,
  audit_ids_updated: [101, 102, 103],
  history_action: "batch",
});

const rejectOk = (diffId = "diff-abc") => ({
  status: "rejected" as const,
  diff_id: diffId,
  audit_ids_updated: [101, 102, 103],
});

beforeEach(() => {
  setActivePinia(createPinia());
  mockStage.mockReset();
  mockAccept.mockReset();
  mockReject.mockReset();
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("ai-diff-store — stage", () => {
  it("populates currentDiff from the request shape on success", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    const store = useAiDiffStore();
    const request = sampleStageRequest();

    await store.stage(request);

    expect(store.currentDiff).not.toBeNull();
    expect(store.currentDiff!.diff_id).toBe("diff-1");
    expect(store.currentDiff!.flow_id).toBe(42);
    expect(store.currentDiff!.rationale).toBe("Add a filter on amount");
    expect(store.currentDiff!.additions).toHaveLength(1);
    expect(store.currentDiff!.additions[0].node_type).toBe("filter");
    expect(store.currentDiff!.additions[0].insertion_context.upstream_node_ids).toEqual([3]);
    expect(store.currentDiff!.additions[0].audit_id).toBe(101);
    expect(store.currentDiff!.connections_added).toHaveLength(1);
    expect(store.currentDiff!.deletions).toHaveLength(1);
    expect(store.currentDiff!.deletions[0].delete_node_id).toBe(99);
    expect(store.currentDiff!.connections_removed).toEqual([]);
    expect(store.loading).toBe(false);
    expect(store.error).toBeNull();
  });

  it("surfaces an http error and leaves currentDiff null", async () => {
    mockStage.mockRejectedValue(new AiDiffHttpError(422, "bad payload"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());

    expect(store.currentDiff).toBeNull();
    expect(store.error).toEqual({ kind: "http", status: 422, message: "bad payload" });
    expect(store.loading).toBe(false);
  });

  it("ignores AbortError silently when stage is cancelled mid-flight", async () => {
    mockStage.mockRejectedValue(new DOMException("aborted", "AbortError"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());

    expect(store.currentDiff).toBeNull();
    expect(store.error).toBeNull();
    expect(store.loading).toBe(false);
  });
});

describe("ai-diff-store — accept", () => {
  it("clears currentDiff and writes lastApplyResult on success", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockResolvedValue(acceptOk("diff-1"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockAccept).toHaveBeenCalledWith("diff-1", { flow_id: 42 }, expect.any(AbortSignal));
    expect(store.currentDiff).toBeNull();
    expect(store.lastApplyResult).not.toBeNull();
    expect(store.lastApplyResult!.applied_node_ids).toEqual([7]);
    expect(store.error).toBeNull();
  });

  it("keeps currentDiff staged when the backend reports drift (409)", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(409, {
        error: "diff_drift",
        missing_node_ids: [3, 99],
        diff_id: "diff-1",
      }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.currentDiff).not.toBeNull();
    expect(store.currentDiff!.diff_id).toBe("diff-1");
    expect(store.error).not.toBeNull();
    expect(store.error!.kind).toBe("drift");
    if (store.error!.kind === "drift") {
      expect(store.error!.missingNodeIds).toEqual([3, 99]);
      expect(store.error!.status).toBe(409);
    }
  });

  it("keeps currentDiff staged on a non-drift error so the user can retry", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(422, "apply failed"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.currentDiff).not.toBeNull();
    expect(store.error).toEqual({ kind: "http", status: 422, message: "apply failed" });
  });

  it("is a no-op when there is no staged diff", async () => {
    const store = useAiDiffStore();
    await store.accept();
    expect(mockAccept).not.toHaveBeenCalled();
    expect(store.error).toBeNull();
  });
});

describe("ai-diff-store — reject", () => {
  it("clears currentDiff on success", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockReject.mockResolvedValue(rejectOk("diff-1"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(mockReject).toHaveBeenCalledWith("diff-1", expect.any(AbortSignal));
    expect(store.currentDiff).toBeNull();
    expect(store.lastApplyResult).toBeNull();
  });

  it("preserves currentDiff on http error so the user can retry", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockReject.mockRejectedValue(new AiDiffHttpError(404, "not found"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(store.currentDiff).not.toBeNull();
    expect(store.error).toEqual({ kind: "http", status: 404, message: "not found" });
  });

  it("is a no-op when there is no staged diff", async () => {
    const store = useAiDiffStore();
    await store.reject();
    expect(mockReject).not.toHaveBeenCalled();
  });
});

describe("ai-diff-store — clear / setCurrentDiff", () => {
  it("clear() drops currentDiff and error without hitting the backend", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());

    store.clear();

    expect(store.currentDiff).toBeNull();
    expect(store.error).toBeNull();
    expect(store.lastApplyResult).toBeNull();
    expect(mockAccept).not.toHaveBeenCalled();
    expect(mockReject).not.toHaveBeenCalled();
  });

  it("setCurrentDiff replaces an in-progress staged diff", () => {
    const store = useAiDiffStore();
    store.setCurrentDiff({
      diff_id: "diff-direct",
      session_id: "sess",
      flow_id: 1,
      rationale: null,
      additions: [],
      connections_added: [],
      deletions: [],
      connections_removed: [],
    });

    expect(store.currentDiff?.diff_id).toBe("diff-direct");
    expect(store.error).toBeNull();
    expect(store.lastApplyResult).toBeNull();
  });
});

describe("ai-diff-store — drift detail parsing", () => {
  it("falls back to a generic http error if 409 detail isn't drift-shaped", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(409, { error: "something_else" }));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.error).not.toBeNull();
    expect(store.error!.kind).toBe("http");
    expect(store.error!.status).toBe(409);
  });

  it("falls back to a generic http error if missing_node_ids isn't an array", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(409, { error: "diff_drift", missing_node_ids: "broken" }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.error!.kind).toBe("http");
  });
});
