// W40 — unit tests for the agent store.
//
// The store wraps three pure-fetch / SSE wrappers from
// `services/aiStreamClient` + `api/ai.api`. Both modules are mocked at
// import time so the tests don't reach the network or hit DOM globals.
// AiDiffHttpError, AiStreamHttpError, and AiDisabledError are re-declared
// in the mock so `instanceof` checks in the store still match.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Hoisted mock factories — must run before store imports.
const mockSymbols = vi.hoisted(() => {
  class AiStreamHttpError extends Error {
    constructor(
      public readonly status: number,
      public readonly detail: string,
    ) {
      super(detail || `HTTP ${status}`);
      this.name = "AiStreamHttpError";
    }
  }
  class AiDisabledError extends Error {
    constructor(message = "AI features are disabled.") {
      super(message);
      this.name = "AiDisabledError";
    }
  }
  return {
    AiStreamHttpError,
    AiDisabledError,
    streamAgentSession: vi.fn(),
    resumeAgentSessionStream: vi.fn(),
    abortAgentSession: vi.fn(),
    discardAgentSession: vi.fn(),
    getAgentSession: vi.fn(),
    diffStoreSetCurrentDiff: vi.fn(),
    editorStoreOpenAiDrawer: vi.fn(),
  };
});

vi.mock("../services/aiStreamClient", () => ({
  AiStreamHttpError: mockSymbols.AiStreamHttpError,
  streamAgentSession: mockSymbols.streamAgentSession,
  resumeAgentSessionStream: mockSymbols.resumeAgentSessionStream,
}));

vi.mock("../api/ai.api", () => ({
  AiDisabledError: mockSymbols.AiDisabledError,
  abortAgentSession: mockSymbols.abortAgentSession,
  discardAgentSession: mockSymbols.discardAgentSession,
  getAgentSession: mockSymbols.getAgentSession,
}));

vi.mock("./ai-diff-store", () => ({
  useAiDiffStore: () => ({
    setCurrentDiff: mockSymbols.diffStoreSetCurrentDiff,
  }),
}));

vi.mock("./editor-store", () => ({
  useEditorStore: () => ({
    openAiDrawer: mockSymbols.editorStoreOpenAiDrawer,
  }),
}));

import { useAiAgentStore } from "./ai-agent-store";

beforeEach(() => {
  setActivePinia(createPinia());
  mockSymbols.streamAgentSession.mockReset();
  mockSymbols.resumeAgentSessionStream.mockReset();
  mockSymbols.abortAgentSession.mockReset();
  mockSymbols.discardAgentSession.mockReset();
  mockSymbols.getAgentSession.mockReset();
  mockSymbols.diffStoreSetCurrentDiff.mockReset();
  mockSymbols.editorStoreOpenAiDrawer.mockReset();
});

afterEach(() => {
  vi.restoreAllMocks();
});

const startBody = () => ({
  flow_id: 1,
  prompt: "filter to EU",
  surface: "agent_complex" as const,
  provider: "anthropic",
});

describe("useAiAgentStore - start", () => {
  it("transitions through running -> completed and pushes diff to ai-diff-store", async () => {
    const fakeDiff = { diff_id: "d-1", flow_id: 1, additions: [] };
    const completePayload = {
      session_id: "s-1",
      diff_id: "d-1",
      op_count: 1,
      rationale: "did the thing",
      diff_payload: fakeDiff,
    };

    mockSymbols.streamAgentSession.mockImplementation(async (_body, handlers) => {
      handlers.onToolCallProposed?.({ id: "t1", name: "flowfile.graph.add_filter", arguments: {} });
      handlers.onToolCallStaged?.({
        id: "t1",
        name: "flowfile.graph.add_filter",
        node_id: 2,
        predicted_output_schema: null,
        warnings: [],
      });
      handlers.onComplete?.(completePayload);
    });

    const store = useAiAgentStore();
    await store.start(startBody());

    expect(store.status).toBe("completed");
    expect(store.lastResult).toEqual(completePayload);
    expect(store.events.length).toBeGreaterThanOrEqual(3); // proposed + staged + complete
    expect(mockSymbols.diffStoreSetCurrentDiff).toHaveBeenCalledWith(fakeDiff);
    expect(mockSymbols.editorStoreOpenAiDrawer).toHaveBeenCalled();
  });

  it("captures drift_detected and freezes status at paused_drift (W45 shape)", async () => {
    const drift = {
      missing_node_ids: [1],
      external_added_node_ids: [42],
      // Server-side dict[int, str] serialises with string keys.
      node_types: { "1": "filter", "42": "manual_input" },
    };
    mockSymbols.streamAgentSession.mockImplementation(async (_body, handlers) => {
      handlers.onDriftDetected?.(drift, "s-1");
      handlers.onPaused?.("graph_changed", "s-1");
    });

    const store = useAiAgentStore();
    await store.start(startBody());

    expect(store.status).toBe("paused_drift");
    expect(store.driftDetail).toEqual({
      missingNodeIds: [1],
      externalAddedNodeIds: [42],
      // String keys on the wire → numeric keys on the store.
      nodeTypes: { 1: "filter", 42: "manual_input" },
    });
  });

  it("surfaces AiStreamHttpError as failed status", async () => {
    mockSymbols.streamAgentSession.mockImplementation(async () => {
      throw new mockSymbols.AiStreamHttpError(409, "provider not configured");
    });

    const store = useAiAgentStore();
    await store.start(startBody());

    expect(store.status).toBe("failed");
    expect(store.error).toBe("provider not configured");
  });

  it("surfaces AiDisabledError and sets aiDisabled flag", async () => {
    mockSymbols.streamAgentSession.mockImplementation(async () => {
      throw new mockSymbols.AiDisabledError();
    });

    const store = useAiAgentStore();
    await store.start(startBody());

    expect(store.aiDisabled).toBe(true);
    expect(store.status).toBe("failed");
  });

  it("treats AbortError silently (no status flip)", async () => {
    const abortError = new Error("abort");
    abortError.name = "AbortError";
    mockSymbols.streamAgentSession.mockImplementation(async () => {
      throw abortError;
    });

    const store = useAiAgentStore();
    await store.start(startBody());

    // Status reset to "running" remains; the abort path doesn't flip to failed.
    // (The actual abort path uses store.abort() to flip to "aborted".)
    expect(store.status).toBe("running");
    expect(store.error).toBe(null);
  });
});

describe("useAiAgentStore - resume", () => {
  it("resumeContinue streams via the resume endpoint", async () => {
    mockSymbols.resumeAgentSessionStream.mockImplementation(async (_sid, handlers) => {
      handlers.onComplete?.({
        session_id: "s-9",
        diff_id: null,
        op_count: 0,
        rationale: null,
        diff_payload: null,
      });
    });

    const store = useAiAgentStore();
    await store.resumeContinue("s-9");

    expect(mockSymbols.resumeAgentSessionStream).toHaveBeenCalledWith(
      "s-9",
      expect.any(Object),
      expect.any(Object),
    );
    expect(store.status).toBe("completed");
  });

  it("resumeDiscard pops the session and flips to aborted", async () => {
    mockSymbols.discardAgentSession.mockResolvedValue({ status: "discarded", sessionId: "s-9" });
    const store = useAiAgentStore();
    store.currentSessionId = "s-9";
    await store.resumeDiscard("s-9");

    expect(mockSymbols.discardAgentSession).toHaveBeenCalledWith("s-9", expect.anything());
    expect(store.status).toBe("aborted");
    expect(store.currentSessionId).toBe(null);
  });
});

describe("useAiAgentStore - abort + clear", () => {
  it("abort calls abortAgentSession when sessionId is known", async () => {
    mockSymbols.abortAgentSession.mockResolvedValue({
      status: "aborted",
      sessionId: "s-9",
      partialDiffId: null,
    });
    const store = useAiAgentStore();
    store.currentSessionId = "s-9";
    await store.abort();
    expect(mockSymbols.abortAgentSession).toHaveBeenCalledWith("s-9");
    expect(store.status).toBe("aborted");
  });

  it("abort with no sessionId still flips local status without a network call", async () => {
    const store = useAiAgentStore();
    await store.abort();
    expect(mockSymbols.abortAgentSession).not.toHaveBeenCalled();
    expect(store.status).toBe("aborted");
  });

  it("clear resets all state", () => {
    const store = useAiAgentStore();
    store.currentSessionId = "s-9";
    store.status = "running";
    store.events = [{ kind: "thinking", payload: { text: "x" }, at: Date.now() }];
    store.error = "boom";
    store.driftDetail = { missingNodeIds: [], externalAddedNodeIds: [], nodeTypes: {} };

    store.clear();

    expect(store.currentSessionId).toBe(null);
    expect(store.status).toBe("idle");
    expect(store.events).toEqual([]);
    expect(store.error).toBe(null);
    expect(store.driftDetail).toBe(null);
  });
});

// --------------------------------------------------------------------------
// W45 — currentSessionId from-wire propagation + Q4 resume state machine
// --------------------------------------------------------------------------

describe("useAiAgentStore - W45 Q2 currentSessionId from wire", () => {
  it("onPaused populates currentSessionId from the SSE wire", async () => {
    mockSymbols.streamAgentSession.mockImplementation(async (_body, handlers) => {
      handlers.onPaused?.("graph_changed", "sess-pause");
    });
    const store = useAiAgentStore();
    await store.start(startBody());
    expect(store.currentSessionId).toBe("sess-pause");
  });

  it("onDriftDetected populates currentSessionId from the SSE wire", async () => {
    mockSymbols.streamAgentSession.mockImplementation(async (_body, handlers) => {
      handlers.onDriftDetected?.(
        { missing_node_ids: [], external_added_node_ids: [], node_types: {} },
        "sess-drift",
      );
    });
    const store = useAiAgentStore();
    await store.start(startBody());
    expect(store.currentSessionId).toBe("sess-drift");
  });

  it("onAbort populates currentSessionId from the SSE wire (Q2 propagation)", async () => {
    mockSymbols.streamAgentSession.mockImplementation(async (_body, handlers) => {
      handlers.onAbort?.("sess-abort");
    });
    const store = useAiAgentStore();
    await store.start(startBody());
    expect(store.currentSessionId).toBe("sess-abort");
    expect(store.status).toBe("aborted");
  });

  it("onComplete populates currentSessionId from the result payload (Q2 propagation)", async () => {
    mockSymbols.streamAgentSession.mockImplementation(async (_body, handlers) => {
      handlers.onComplete?.({
        session_id: "sess-complete",
        diff_id: null,
        op_count: 0,
        rationale: null,
        diff_payload: null,
      });
    });
    const store = useAiAgentStore();
    await store.start(startBody());
    expect(store.currentSessionId).toBe("sess-complete");
    expect(store.status).toBe("completed");
  });

  it("start() body without session_id leaves currentSessionId null until wire populates it", async () => {
    mockSymbols.streamAgentSession.mockImplementation(async () => {
      // No events at all — the wire never gets a chance to populate the id.
    });
    const store = useAiAgentStore();
    expect(store.currentSessionId).toBe(null);
    const body = startBody();
    expect((body as { session_id?: string }).session_id).toBeUndefined();
    await store.start(body);
    expect(store.currentSessionId).toBe(null);
  });
});

describe("useAiAgentStore - W45 Q4 resume state machine", () => {
  it("paused → resumeContinue transitions paused_drift → running, next event lands", async () => {
    // First stream: pause.
    mockSymbols.streamAgentSession.mockImplementation(async (_body, handlers) => {
      handlers.onPaused?.("graph_changed", "sess-q4");
    });
    const store = useAiAgentStore();
    await store.start(startBody());
    expect(store.status).toBe("paused_drift");
    expect(store.currentSessionId).toBe("sess-q4");

    // Resume stream: emit info + thinking; without a subsequent state-changing
    // event the store should remain in "running".
    mockSymbols.resumeAgentSessionStream.mockImplementation(async (_sid, handlers) => {
      handlers.onInfo?.({ message: "resumed; re-snapshotted graph" });
      handlers.onThinking?.("planning step 2");
    });
    await store.resumeContinue("sess-q4");
    expect(store.status).toBe("running");
    expect(store.driftDetail).toBeNull();
    expect(store.events.some((e) => e.kind === "thinking")).toBe(true);
    expect(store.events.some((e) => e.kind === "info")).toBe(true);
  });
});
