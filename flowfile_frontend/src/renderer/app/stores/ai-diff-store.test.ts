// Unit tests for the diff approval store.
//
// The store talks to the backend through three pure-fetch wrappers;
// we mock the wrapper module so the tests don't reach the network.
// Mocking is done by replacing the entire `services/aiDiffClient`
// module with a small in-test factory; the real module reaches DOM
// globals (`window`, `localStorage`) at import time, which the Node
// test environment doesn't provide. The mock supplies its own
// `AiDiffHttpError` so the `instanceof` checks in the store still
// match.
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

// `flow-store` transitively reaches `auth.service` which touches
// `window` at module load. Mock it out — the diff store only calls
// `requestReload()` after a successful accept, and the test asserts
// that call.
const mockRequestReload = vi.fn();
vi.mock("./flow-store", () => ({
  useFlowStore: () => ({ requestReload: mockRequestReload }),
}));

// `diff-store.reject()` coordinates with the agent store after a
// successful backend reject. Mock the agent store so tests can drive
// the followup hand-off without spinning up a real Pinia agent store.
const mockAgent = vi.hoisted(() => ({
  resumeAfterReject: vi.fn(),
  clearLastResultDiffPayload: vi.fn(),
  state: {
    currentSessionId: null as string | null,
    status: "idle" as
      | "idle"
      | "running"
      | "paused_drift"
      | "paused_user_action"
      | "awaiting_user_input"
      | "completed"
      | "aborted"
      | "failed",
  },
}));
vi.mock("./ai-agent-store", () => ({
  useAiAgentStore: () => ({
    get currentSessionId() {
      return mockAgent.state.currentSessionId;
    },
    get status() {
      return mockAgent.state.status;
    },
    resumeAfterReject: mockAgent.resumeAfterReject,
    clearLastResultDiffPayload: mockAgent.clearLastResultDiffPayload,
  }),
}));

// `diff-store.accept()` / `reject()` push synthetic decision messages
// into ``ai-store.messages`` so the chat trail records what the user
// clicked plus any rejection note. Mock the store's messages array
// so tests can assert push-shape without dragging the real ai-store
// import chain (which transitively reaches ``auth.service`` and
// touches ``window``).
const mockAiStoreMessages = vi.hoisted(() => ({ messages: [] as Array<Record<string, unknown>> }));
vi.mock("./ai-store", () => ({
  useAiStore: () => ({ messages: mockAiStoreMessages.messages }),
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
        // Mirrors `NodeConnection.model_dump()` from the executor —
        // see `flowfile_core/.../ai/tools/executor.py`. Field names
        // match the Pydantic shape exactly (no `_class` suffix);
        // `connection_class`
        // values are canonical `output-N` / `input-N`.
        connection: {
          output_connection: { node_id: 3, connection_class: "output-0" },
          input_connection: { node_id: 7, connection_class: "input-0" },
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
  mockRequestReload.mockReset();
  mockAgent.resumeAfterReject.mockReset();
  mockAgent.clearLastResultDiffPayload.mockReset();
  mockAgent.state.currentSessionId = null;
  mockAgent.state.status = "idle";
  mockAiStoreMessages.messages.length = 0;
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

  it("calls flowStore.requestReload() on accept-success so the canvas refreshes", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockResolvedValue(acceptOk("diff-1"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    // Bug fix: before this wiring the backend mutated the FlowGraph but
    // the VueFlow canvas didn't re-fetch — user accepted a diff and saw
    // no visible change.
    expect(mockRequestReload).toHaveBeenCalledTimes(1);
  });

  it("does NOT call requestReload() on accept-409 drift (canvas is unchanged)", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(409, {
        error: "diff_drift",
        missing_node_ids: [3],
        diff_id: "diff-1",
      }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockRequestReload).not.toHaveBeenCalled();
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
    // 422 (not 404) — 404 is now the "diff is gone" signal handled by
    // the dedicated stale-diff path, see the "stale diff" describe block.
    mockReject.mockRejectedValue(new AiDiffHttpError(422, "transient backend error"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(store.currentDiff).not.toBeNull();
    expect(store.error).toEqual({
      kind: "http",
      status: 422,
      message: "transient backend error",
    });
  });

  it("is a no-op when there is no staged diff", async () => {
    const store = useAiDiffStore();
    await store.reject();
    expect(mockReject).not.toHaveBeenCalled();
  });
});

describe("ai-diff-store — chat-trail decision messages", () => {
  it("pushes an [Accepted] message with apply counts on accept-success", async () => {
    mockStage.mockResolvedValue(stageOk("diff-acc"));
    mockAccept.mockResolvedValue(acceptOk("diff-acc"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockAiStoreMessages.messages).toHaveLength(1);
    const msg = mockAiStoreMessages.messages[0];
    expect(msg.role).toBe("user");
    expect(msg.content).toBe("[Accepted] applied 1 node(s), 1 connection(s).");
  });

  it("does NOT push an [Accepted] message when accept fails", async () => {
    mockStage.mockResolvedValue(stageOk("diff-fail"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(409, "drift"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockAiStoreMessages.messages).toHaveLength(0);
  });

  it("pushes a bare [Rejected] message when reject is called without a note", async () => {
    mockStage.mockResolvedValue(stageOk("diff-rej"));
    mockReject.mockResolvedValue(rejectOk("diff-rej"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(mockAiStoreMessages.messages).toHaveLength(1);
    expect(mockAiStoreMessages.messages[0].content).toBe("[Rejected]");
  });

  it("pushes a [Rejected] message with the user-supplied note when provided", async () => {
    mockStage.mockResolvedValue(stageOk("diff-rej-note"));
    mockReject.mockResolvedValue(rejectOk("diff-rej-note"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject("use the read node directly, not after the filter");

    expect(mockAiStoreMessages.messages).toHaveLength(1);
    expect(mockAiStoreMessages.messages[0].content).toBe(
      "[Rejected] use the read node directly, not after the filter",
    );
  });

  it("trims whitespace-only notes to the bare [Rejected] form", async () => {
    mockStage.mockResolvedValue(stageOk("diff-rej-blank"));
    mockReject.mockResolvedValue(rejectOk("diff-rej-blank"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject("   \n   ");

    expect(mockAiStoreMessages.messages).toHaveLength(1);
    expect(mockAiStoreMessages.messages[0].content).toBe("[Rejected]");
  });

  it("does NOT push a [Rejected] message when reject fails", async () => {
    mockStage.mockResolvedValue(stageOk("diff-rej-fail"));
    mockReject.mockRejectedValue(new AiDiffHttpError(422, "transient"));
    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject("doesn't matter");

    expect(mockAiStoreMessages.messages).toHaveLength(0);
  });
});

describe("ai-diff-store — reject (followup hand-off)", () => {
  it("hands off to agentStore.resumeAfterReject when session is completed", async () => {
    mockStage.mockResolvedValue(stageOk("diff-w49"));
    mockReject.mockResolvedValue(rejectOk("diff-w49"));
    mockAgent.state.currentSessionId = "sess-42";
    mockAgent.state.status = "completed";

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject("please use the read node directly");

    expect(mockReject).toHaveBeenCalledWith("diff-w49", expect.any(AbortSignal));
    expect(mockAgent.resumeAfterReject).toHaveBeenCalledWith(
      "sess-42",
      "please use the read node directly",
      "diff-w49",
    );
  });

  it("hands off with null note when reject() is called without a note", async () => {
    mockStage.mockResolvedValue(stageOk("diff-w49"));
    mockReject.mockResolvedValue(rejectOk("diff-w49"));
    mockAgent.state.currentSessionId = "sess-42";
    mockAgent.state.status = "completed";

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(mockAgent.resumeAfterReject).toHaveBeenCalledWith("sess-42", null, "diff-w49");
  });

  it("falls back to legacy clear-only when no agent session is active", async () => {
    mockStage.mockResolvedValue(stageOk("diff-w49"));
    mockReject.mockResolvedValue(rejectOk("diff-w49"));
    mockAgent.state.currentSessionId = null;
    mockAgent.state.status = "idle";

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject("anything");

    expect(mockReject).toHaveBeenCalled();
    expect(store.currentDiff).toBeNull();
    expect(mockAgent.resumeAfterReject).not.toHaveBeenCalled();
  });

  it("falls back to legacy clear-only when session is in non-resumable status", async () => {
    mockStage.mockResolvedValue(stageOk("diff-w49"));
    mockReject.mockResolvedValue(rejectOk("diff-w49"));
    // Session present but failed — not followup-resumable.
    mockAgent.state.currentSessionId = "sess-42";
    mockAgent.state.status = "failed";

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(store.currentDiff).toBeNull();
    expect(mockAgent.resumeAfterReject).not.toHaveBeenCalled();
  });

  it("hands off when session is awaiting_user_input as well", async () => {
    mockStage.mockResolvedValue(stageOk("diff-w49"));
    mockReject.mockResolvedValue(rejectOk("diff-w49"));
    mockAgent.state.currentSessionId = "sess-42";
    mockAgent.state.status = "awaiting_user_input";

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject("course correct please");

    expect(mockAgent.resumeAfterReject).toHaveBeenCalledWith(
      "sess-42",
      "course correct please",
      "diff-w49",
    );
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
      modifications: [],
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

// Diff inconsistency: the agent's own staged diff is broken (e.g. a
// `connect` op references a `to_node_id` that's neither live nor in
// the diff's additions). Mirrors the 409-drift code path: same
// posture (currentDiff stays staged so the user can Reject and
// retry), same fall-back to http error when the wire shape doesn't
// match the typed payload. The 422-`diff_inconsistent` shape is
// distinct from the existing 422-string path (cross-flow mismatch /
// generic mid-batch raise); the new parser must not capture those.
describe("ai-diff-store — inconsistency detail parsing", () => {
  it("keeps currentDiff staged when the backend reports a 422 diff_inconsistent", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(422, {
        error: "diff_inconsistent",
        missing_endpoints: [[77, "to"]],
        diff_id: "diff-1",
      }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.currentDiff).not.toBeNull();
    expect(store.currentDiff!.diff_id).toBe("diff-1");
    expect(store.error).not.toBeNull();
    expect(store.error!.kind).toBe("inconsistent");
    if (store.error!.kind === "inconsistent") {
      expect(store.error!.status).toBe(422);
      expect(store.error!.missingEndpoints).toEqual([{ nodeId: 77, role: "to" }]);
    }
  });

  it("does NOT call requestReload() on accept-422 inconsistent (canvas is unchanged)", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(422, {
        error: "diff_inconsistent",
        missing_endpoints: [[77, "to"]],
        diff_id: "diff-1",
      }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockRequestReload).not.toHaveBeenCalled();
  });

  it("captures multiple missing endpoints with mixed from/to roles", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(422, {
        error: "diff_inconsistent",
        missing_endpoints: [
          [77, "to"],
          [99, "from"],
        ],
        diff_id: "diff-1",
      }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.error!.kind).toBe("inconsistent");
    if (store.error!.kind === "inconsistent") {
      expect(store.error!.missingEndpoints).toEqual([
        { nodeId: 77, role: "to" },
        { nodeId: 99, role: "from" },
      ]);
    }
  });

  it("falls back to a generic http error if 422 detail is a string (legacy mid-batch raise)", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(422, "apply_diff failed: ..."));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.error).not.toBeNull();
    expect(store.error!.kind).toBe("http");
    expect(store.error!.status).toBe(422);
    if (store.error!.kind === "http") {
      expect(store.error!.message).toBe("apply_diff failed: ...");
    }
  });

  it("falls back to a generic http error if 422 detail is missing_endpoints-broken", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(422, {
        error: "diff_inconsistent",
        missing_endpoints: "broken",
      }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.error!.kind).toBe("http");
  });

  it("filters out malformed endpoint entries and keeps the well-formed ones", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockRejectedValue(
      new AiDiffHttpError(422, {
        error: "diff_inconsistent",
        missing_endpoints: [
          [77, "to"],
          [88, "sideways"], // bad role — dropped
          ["nope", "from"], // non-numeric id — dropped
          [99, "from"],
        ],
        diff_id: "diff-1",
      }),
    );

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.error!.kind).toBe("inconsistent");
    if (store.error!.kind === "inconsistent") {
      expect(store.error!.missingEndpoints).toEqual([
        { nodeId: 77, role: "to" },
        { nodeId: 99, role: "from" },
      ]);
    }
  });
});

// `connectionLabel` formats a staged `NodeConnection` for the diff
// preview. Pre-fix the renderer read `output_connection_class` /
// `input_connection_class` (with `_class` suffix) — neither field
// exists on the wire shape, so every connection rendered as `#?.main
// → #?.main`. The fix matches the shape `NodeConnection.model_dump()`
// actually produces (see `executor.py` and `diff.py`) —
// `output_connection` / `input_connection`, no suffix.
describe("aiDiffTypes — connectionLabel", () => {
  it("renders the default-handle case as bare `#<from> → #<to>`", async () => {
    const { connectionLabel } = await import("../features/ai/aiDiffTypes");

    const label = connectionLabel({
      connection: {
        output_connection: { node_id: 3, connection_class: "output-0" },
        input_connection: { node_id: 5, connection_class: "input-0" },
      },
      audit_id: null,
    });

    // Default handles (`output-0` / `input-0`) are internal wire
    // identifiers; hiding them on the common single-input /
    // single-output path drops the user-facing noise the original
    // fix introduced.
    expect(label).toBe("#3 → #5");
    expect(label).not.toContain("main");
    expect(label).not.toContain("output-0");
    expect(label).not.toContain("input-0");
    expect(label).not.toContain("?");
  });

  it("preserves non-default handles (e.g. `input-1` for the right-side join input)", async () => {
    const { connectionLabel } = await import("../features/ai/aiDiffTypes");

    const label = connectionLabel({
      connection: {
        output_connection: { node_id: 12, connection_class: "output-0" },
        input_connection: { node_id: 13, connection_class: "input-1" },
      },
      audit_id: null,
    });

    // Default output handle hidden, non-default input handle preserved — the
    // suffix is what disambiguates a join's right-side input from its left.
    expect(label).toBe("#12 → #13.input-1");
  });

  it("preserves a non-default output handle while hiding the default input", async () => {
    const { connectionLabel } = await import("../features/ai/aiDiffTypes");

    const label = connectionLabel({
      connection: {
        output_connection: { node_id: 7, connection_class: "output-1" },
        input_connection: { node_id: 8, connection_class: "input-0" },
      },
      audit_id: null,
    });

    expect(label).toBe("#7.output-1 → #8");
  });

  it("falls back to bare canonical form when handles are missing", async () => {
    const { connectionLabel } = await import("../features/ai/aiDiffTypes");

    // Defensive fallback path — in practice the wire payload always carries
    // both handles. Missing handles default to `output-0` / `input-0`, which
    // are then hidden as noise.
    const label = connectionLabel({
      connection: {
        output_connection: { node_id: 1 },
        input_connection: { node_id: 2 },
      },
      audit_id: null,
    });

    expect(label).toBe("#1 → #2");
    expect(label).not.toContain("main");
  });

  it("falls back to `?` for missing node ids without leaking `main` literal", async () => {
    const { connectionLabel } = await import("../features/ai/aiDiffTypes");

    const label = connectionLabel({ connection: {}, audit_id: null });

    expect(label).toBe("#? → #?");
    expect(label).not.toContain("main");
  });

  it("formats the synthesised diff payload from `synthesiseDiffFromStageRequest` end-to-end", async () => {
    // Round-trip check: a stage request shaped exactly like what the
    // executor emits → synthesised payload → `connectionLabel` →
    // bare form. Locks the wire shape ↔ renderer contract for this
    // code path.
    const { synthesiseDiffFromStageRequest, connectionLabel } =
      await import("../features/ai/aiDiffTypes");

    const diff = synthesiseDiffFromStageRequest(
      {
        session_id: "sess-w61",
        flow_id: 9,
        rationale: null,
        staged_results: [
          {
            tool_name: "flowfile.graph.connect",
            audit_id: 1,
            staged_node_payload: {
              connection: {
                output_connection: { node_id: 3, connection_class: "output-0" },
                input_connection: { node_id: 5, connection_class: "input-0" },
              },
            },
          },
        ],
      },
      "diff-w61",
    );

    expect(diff.connections_added).toHaveLength(1);
    expect(connectionLabel(diff.connections_added[0])).toBe("#3 → #5");
  });
});

// `richConnectionLabel` + `buildAdditionNodeTypes` — node-type-aware
// labels. Renderer wants `read_csv #3 → group_by #5` instead of
// `#3 → #5` when type info is available. Type comes from the diff's
// own additions for newly-staged nodes; the Vue component layers a
// flow-store lookup on top for existing nodes (untested at the unit
// level — we only test the pure helper here).
describe("aiDiffTypes — richConnectionLabel", () => {
  it("renders both sides with node type when both ids are in the lookup", async () => {
    const { richConnectionLabel } = await import("../features/ai/aiDiffTypes");
    const types = new Map<number, string>([
      [3, "read_csv"],
      [5, "group_by"],
    ]);
    const label = richConnectionLabel(
      {
        connection: {
          output_connection: { node_id: 3, connection_class: "output-0" },
          input_connection: { node_id: 5, connection_class: "input-0" },
        },
        audit_id: null,
      },
      types,
    );
    expect(label).toBe("read_csv #3 → group_by #5");
  });

  it("falls back to bare `#<id>` for sides whose type is missing", async () => {
    const { richConnectionLabel } = await import("../features/ai/aiDiffTypes");
    const types = new Map<number, string>([[5, "group_by"]]);
    const label = richConnectionLabel(
      {
        connection: {
          output_connection: { node_id: 3, connection_class: "output-0" },
          input_connection: { node_id: 5, connection_class: "input-0" },
        },
        audit_id: null,
      },
      types,
    );
    expect(label).toBe("#3 → group_by #5");
  });

  it("preserves non-default handles after the type label (e.g. join right input)", async () => {
    const { richConnectionLabel } = await import("../features/ai/aiDiffTypes");
    const types = new Map<number, string>([
      [12, "filter"],
      [13, "join"],
    ]);
    const label = richConnectionLabel(
      {
        connection: {
          output_connection: { node_id: 12, connection_class: "output-0" },
          input_connection: { node_id: 13, connection_class: "input-1" },
        },
        audit_id: null,
      },
      types,
    );
    expect(label).toBe("filter #12 → join #13.input-1");
  });

  it("falls back to `#?` for null node ids regardless of map state", async () => {
    const { richConnectionLabel } = await import("../features/ai/aiDiffTypes");
    const label = richConnectionLabel(
      { connection: {}, audit_id: null },
      new Map<number, string>(),
    );
    expect(label).toBe("#? → #?");
  });
});

describe("aiDiffTypes — buildAdditionNodeTypes", () => {
  it("maps each addition's settings.node_id to its declared node_type", async () => {
    const { buildAdditionNodeTypes } = await import("../features/ai/aiDiffTypes");
    const map = buildAdditionNodeTypes({
      diff_id: "d",
      session_id: "s",
      flow_id: 1,
      rationale: null,
      additions: [
        {
          node_type: "read_csv",
          settings: { node_id: 7 },
          insertion_context: {
            upstream_node_ids: [],
            right_input_node_id: null,
            pos_x: 0,
            pos_y: 0,
          },
          predicted_output_schema: null,
          audit_id: null,
        },
        {
          node_type: "group_by",
          settings: { node_id: 8, foo: "bar" },
          insertion_context: {
            upstream_node_ids: [7],
            right_input_node_id: null,
            pos_x: 0,
            pos_y: 0,
          },
          predicted_output_schema: null,
          audit_id: null,
        },
      ],
      modifications: [],
      connections_added: [],
      deletions: [],
      connections_removed: [],
    });
    expect(map.size).toBe(2);
    expect(map.get(7)).toBe("read_csv");
    expect(map.get(8)).toBe("group_by");
  });

  it("skips additions whose settings.node_id is missing or non-numeric", async () => {
    const { buildAdditionNodeTypes } = await import("../features/ai/aiDiffTypes");
    const map = buildAdditionNodeTypes({
      diff_id: "d",
      session_id: "s",
      flow_id: 1,
      rationale: null,
      additions: [
        {
          node_type: "filter",
          settings: {}, // no node_id
          insertion_context: {
            upstream_node_ids: [],
            right_input_node_id: null,
            pos_x: 0,
            pos_y: 0,
          },
          predicted_output_schema: null,
          audit_id: null,
        },
        {
          node_type: "sort",
          settings: { node_id: "9" }, // string, not number — defensive skip
          insertion_context: {
            upstream_node_ids: [],
            right_input_node_id: null,
            pos_x: 0,
            pos_y: 0,
          },
          predicted_output_schema: null,
          audit_id: null,
        },
      ],
      modifications: [],
      connections_added: [],
      deletions: [],
      connections_removed: [],
    });
    expect(map.size).toBe(0);
  });
});

// Modifications round-trip

const sampleModificationStageRequest = (): StageDiffRequestShape => ({
  session_id: "sess-mod",
  flow_id: 42,
  rationale: "Tighten the EU filter to amount > 100",
  staged_results: [
    {
      tool_name: "flowfile.graph.update_node_settings",
      audit_id: 201,
      staged_node_payload: {
        kind: "modification",
        node_id: 9,
        node_type: "filter",
        old_settings: {
          flow_id: 1,
          node_id: 9,
          depending_on_id: 1,
          filter_input: { filter_type: "advanced", advanced_filter: "[region]=='EU'" },
        },
        new_settings: {
          flow_id: 1,
          node_id: 9,
          depending_on_id: 1,
          filter_input: { filter_type: "advanced", advanced_filter: "[amount] > 100" },
        },
        predicted_output_schema: [{ name: "amount", data_type: "Float64", nullable: true }],
      },
    },
  ],
});

describe("ai-diff-store — modifications", () => {
  it("synthesises the modifications bucket from an update_node_settings staged result", async () => {
    mockStage.mockResolvedValue({ diff_id: "diff-mod-1", op_count: 1 });
    const store = useAiDiffStore();

    await store.stage(sampleModificationStageRequest());

    expect(store.currentDiff).not.toBeNull();
    expect(store.currentDiff!.modifications).toHaveLength(1);
    expect(store.currentDiff!.additions).toEqual([]);
    expect(store.currentDiff!.connections_added).toEqual([]);
    expect(store.currentDiff!.deletions).toEqual([]);

    const mod = store.currentDiff!.modifications[0];
    expect(mod.node_id).toBe(9);
    expect(mod.node_type).toBe("filter");
    expect(mod.audit_id).toBe(201);
    expect(mod.predicted_output_schema).toHaveLength(1);
    // Old + new are preserved verbatim from the wire so the diff preview
    // can render an old-vs-new view without re-fetching.
    expect((mod.old_settings.filter_input as Record<string, unknown>).advanced_filter).toBe(
      "[region]=='EU'",
    );
    expect((mod.new_settings.filter_input as Record<string, unknown>).advanced_filter).toBe(
      "[amount] > 100",
    );
    expect(store.error).toBeNull();
  });

  it("accept clears currentDiff and signals canvas reload after a modification-only diff", async () => {
    mockStage.mockResolvedValue({ diff_id: "diff-mod-1", op_count: 1 });
    mockAccept.mockResolvedValue({
      status: "accepted" as const,
      diff_id: "diff-mod-1",
      applied_node_ids: [],
      modified_node_ids: [9],
      applied_connection_count: 0,
      removed_node_ids: [],
      removed_connection_count: 0,
      audit_ids_updated: [201],
      history_action: "batch",
    });

    const store = useAiDiffStore();
    await store.stage(sampleModificationStageRequest());
    await store.accept();

    expect(store.currentDiff).toBeNull();
    expect(store.lastApplyResult).not.toBeNull();
    expect(store.lastApplyResult!.modified_node_ids).toEqual([9]);
    expect(mockRequestReload).toHaveBeenCalledTimes(1);
  });

  it("setCurrentDiff accepts a payload with only modifications", () => {
    const store = useAiDiffStore();
    store.setCurrentDiff({
      diff_id: "diff-mod-direct",
      session_id: "sess",
      flow_id: 1,
      rationale: null,
      additions: [],
      modifications: [
        {
          node_id: 5,
          node_type: "select",
          old_settings: { keep_missing: false },
          new_settings: { keep_missing: true },
          predicted_output_schema: null,
          audit_id: null,
        },
      ],
      connections_added: [],
      deletions: [],
      connections_removed: [],
    });
    expect(store.currentDiff?.modifications).toHaveLength(1);
    expect(store.currentDiff?.modifications[0].node_id).toBe(5);
  });
});

// Stale diff (404) — accept/reject after the backend has lost the diff,
// typically because ``flowfile_core`` was restarted between staging and the
// user clicking Accept/Reject. The store clears the staged diff, surfaces a
// transient toast via ``staleNotice``, and drops the agent store's persisted
// ``lastResult.diff_payload`` so a refresh doesn't re-hydrate the dead diff.

describe("ai-diff-store — stale diff (404 from accept)", () => {
  it("clears currentDiff and surfaces a toast when accept 404s", async () => {
    mockStage.mockResolvedValue(stageOk("diff-stale"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.currentDiff).toBeNull();
    expect(store.error).toBeNull();
    expect(store.staleNotice).toMatch(/no longer available/);
  });

  it("drops the agent store's lastResult.diff_payload when accept 404s", async () => {
    mockStage.mockResolvedValue(stageOk("diff-stale"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockAgent.clearLastResultDiffPayload).toHaveBeenCalled();
  });

  it("does NOT call requestReload() when accept 404s (canvas is unchanged)", async () => {
    mockStage.mockResolvedValue(stageOk("diff-stale"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockRequestReload).not.toHaveBeenCalled();
  });
});

describe("ai-diff-store — stale diff (404 from reject)", () => {
  it("clears currentDiff and surfaces a toast when reject 404s", async () => {
    mockStage.mockResolvedValue(stageOk("diff-stale"));
    mockReject.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(store.currentDiff).toBeNull();
    expect(store.error).toBeNull();
    expect(store.staleNotice).toMatch(/no longer available/);
  });

  it("drops the agent store's lastResult.diff_payload when reject 404s", async () => {
    mockStage.mockResolvedValue(stageOk("diff-stale"));
    mockReject.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(mockAgent.clearLastResultDiffPayload).toHaveBeenCalled();
  });

  it("does NOT call resumeAfterReject when reject 404s (rejection didn't happen)", async () => {
    mockStage.mockResolvedValue(stageOk("diff-stale"));
    mockReject.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));
    mockAgent.state.currentSessionId = "sess-99";
    mockAgent.state.status = "completed";

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject("with a note");

    expect(mockAgent.resumeAfterReject).not.toHaveBeenCalled();
  });
});

describe("ai-diff-store — staleNotice timer", () => {
  it("auto-dismisses the staleNotice after the timeout", async () => {
    vi.useFakeTimers();
    try {
      mockStage.mockResolvedValue(stageOk("diff-stale"));
      mockAccept.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

      const store = useAiDiffStore();
      await store.stage(sampleStageRequest());
      await store.accept();

      expect(store.staleNotice).not.toBeNull();
      vi.advanceTimersByTime(6000);
      expect(store.staleNotice).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it("dismissStaleNotice clears immediately and cancels the timer", async () => {
    vi.useFakeTimers();
    try {
      mockStage.mockResolvedValue(stageOk("diff-stale"));
      mockAccept.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

      const store = useAiDiffStore();
      await store.stage(sampleStageRequest());
      await store.accept();

      expect(store.staleNotice).not.toBeNull();
      store.dismissStaleNotice();
      expect(store.staleNotice).toBeNull();
      // Advancing past the timeout shouldn't re-introduce or change the
      // state — ``dismissStaleNotice`` cancelled the underlying setTimeout.
      vi.advanceTimersByTime(10000);
      expect(store.staleNotice).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it("clear() also wipes the staleNotice", async () => {
    mockStage.mockResolvedValue(stageOk("diff-stale"));
    mockAccept.mockRejectedValue(new AiDiffHttpError(404, "Unknown diff_id 'diff-stale'"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(store.staleNotice).not.toBeNull();
    store.clear();
    expect(store.staleNotice).toBeNull();
  });
});

describe("ai-diff-store — success-path diff_payload cleanup", () => {
  it("calls clearLastResultDiffPayload after a successful accept", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockAccept.mockResolvedValue(acceptOk("diff-1"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.accept();

    expect(mockAgent.clearLastResultDiffPayload).toHaveBeenCalled();
  });

  it("calls clearLastResultDiffPayload after a successful reject", async () => {
    mockStage.mockResolvedValue(stageOk("diff-1"));
    mockReject.mockResolvedValue(rejectOk("diff-1"));

    const store = useAiDiffStore();
    await store.stage(sampleStageRequest());
    await store.reject();

    expect(mockAgent.clearLastResultDiffPayload).toHaveBeenCalled();
  });
});
