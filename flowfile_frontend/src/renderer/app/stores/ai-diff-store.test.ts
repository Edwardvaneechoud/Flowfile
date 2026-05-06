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

// `flow-store` transitively reaches `auth.service` which touches `window`
// at module load. Mock it out — the diff store only calls `requestReload()`
// after a successful accept (W46/Bug-1), and the test asserts that call.
const mockRequestReload = vi.fn();
vi.mock("./flow-store", () => ({
  useFlowStore: () => ({ requestReload: mockRequestReload }),
}));

// W49 — diff-store.reject() coordinates with the agent store after a
// successful backend reject. Mock the agent store so tests can drive the
// followup hand-off without spinning up a real Pinia agent store.
const mockAgent = vi.hoisted(() => ({
  resumeAfterReject: vi.fn(),
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
  }),
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
        // Mirrors `NodeConnection.model_dump()` from W31's executor — see
        // `flowfile_core/.../ai/tools/executor.py:649`. Field names match the
        // Pydantic shape exactly (no `_class` suffix); `connection_class`
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
  mockAgent.state.currentSessionId = null;
  mockAgent.state.status = "idle";
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

describe("ai-diff-store — reject (W49 followup hand-off)", () => {
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

// W61 — `connectionLabel` formats a staged `NodeConnection` for the diff preview.
// Pre-fix the renderer read `output_connection_class` / `input_connection_class`
// (with `_class` suffix) — neither field exists on the wire shape, so every
// connection rendered as `#?.main → #?.main`. The fix matches the shape
// `NodeConnection.model_dump()` actually produces (see `executor.py:649,797`
// and `diff.py:260,279`) — `output_connection` / `input_connection`, no suffix.
describe("aiDiffTypes — connectionLabel (W61, W68)", () => {
  it("renders the default-handle case as bare `#<from> → #<to>` (W68)", async () => {
    const { connectionLabel } = await import("../features/ai/aiDiffTypes");

    const label = connectionLabel({
      connection: {
        output_connection: { node_id: 3, connection_class: "output-0" },
        input_connection: { node_id: 5, connection_class: "input-0" },
      },
      audit_id: null,
    });

    // Default handles (`output-0` / `input-0`) are internal wire identifiers;
    // hiding them on the common single-input/single-output path drops the
    // user-facing noise the original W61 fix introduced.
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
    // Round-trip check: a stage request shaped exactly like what W31's
    // executor emits → synthesised payload → `connectionLabel` → bare form.
    // Locks the wire shape ↔ renderer contract for this code path.
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

// W68 (richConnectionLabel + buildAdditionNodeTypes) — node-type-aware labels.
// Renderer wants `read_csv #3 → group_by #5` instead of `#3 → #5` when type
// info is available. Type comes from the diff's own additions for newly-staged
// nodes; the Vue component layers a flow-store lookup on top for existing
// nodes (untested at the unit level — we only test the pure helper here).
describe("aiDiffTypes — richConnectionLabel (W68)", () => {
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

describe("aiDiffTypes — buildAdditionNodeTypes (W68)", () => {
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
      connections_added: [],
      deletions: [],
      connections_removed: [],
    });
    expect(map.size).toBe(0);
  });
});
