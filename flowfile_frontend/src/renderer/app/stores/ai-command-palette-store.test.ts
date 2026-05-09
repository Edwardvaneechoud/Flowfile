// Unit tests for `useAiCommandPaletteStore` close / clearResult /
// submit lifecycle.
//
// Three external surfaces are mocked at import time:
//   - `api/ai.api`         — `submitCommandPalette` + `AiDisabledError`.
//   - `./ai-diff-store`    — the success-path handoff via `setCurrentDiff`.
//   - `./editor-store`     — `openAiDrawer` so the success path doesn't
//                            try to mutate a real editor store.
//
// Pinia is set up per-test for isolation. The store under test is a
// composition-style Pinia store; `useAiCommandPaletteStore()` returns
// the live state refs alongside the actions.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mockSymbols = vi.hoisted(() => {
  class AiDisabledError extends Error {
    constructor(message = "AI features are disabled.") {
      super(message);
      this.name = "AiDisabledError";
    }
  }
  return {
    AiDisabledError,
    submitCommandPalette: vi.fn(),
    setCurrentDiff: vi.fn(),
    openAiDrawer: vi.fn(),
  };
});

vi.mock("../api/ai.api", () => ({
  AiDisabledError: mockSymbols.AiDisabledError,
  submitCommandPalette: mockSymbols.submitCommandPalette,
}));

vi.mock("./ai-diff-store", () => ({
  useAiDiffStore: () => ({ setCurrentDiff: mockSymbols.setCurrentDiff }),
}));

vi.mock("./editor-store", () => ({
  useEditorStore: () => ({ openAiDrawer: mockSymbols.openAiDrawer }),
}));

import { useAiCommandPaletteStore } from "./ai-command-palette-store";

const baseSubmitOptions = () => ({
  flowId: 1,
  prompt: "filter to last 30 days",
  provider: "anthropic",
  model: "claude-opus-4-7",
});

const seedFailureState = (store: ReturnType<typeof useAiCommandPaletteStore>): void => {
  store.error = "Every proposed action was refused — usually a missing column reference.";
  store.refused = [
    {
      toolName: "flowfile.graph.add_filter",
      refusalReason: "unknown_columns",
      refusalDetail: "missing 'order_date'",
      warnings: [],
    },
  ];
  store.rationale = "**Bold** rationale survived the close.";
  store.degradedReason = "all_refused";
};

beforeEach(() => {
  setActivePinia(createPinia());
  mockSymbols.submitCommandPalette.mockReset();
  mockSymbols.setCurrentDiff.mockReset();
  mockSymbols.openAiDrawer.mockReset();
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("close() preserves response (AC #3)", () => {
  it("keeps error / refused / rationale / degradedReason across reopen", () => {
    const store = useAiCommandPaletteStore();
    store.prompt = "a useful prompt";
    store.open();
    seedFailureState(store);

    store.close();

    // Closed but state intact.
    expect(store.isOpen).toBe(false);
    expect(store.error).toBe(
      "Every proposed action was refused — usually a missing column reference.",
    );
    expect(store.refused).toHaveLength(1);
    expect(store.rationale).toBe("**Bold** rationale survived the close.");
    expect(store.degradedReason).toBe("all_refused");
    // Prompt also preserved (existing palette behaviour).
    expect(store.prompt).toBe("a useful prompt");

    store.open();
    expect(store.isOpen).toBe(true);
    expect(store.error).toBe(
      "Every proposed action was refused — usually a missing column reference.",
    );
    expect(store.refused).toHaveLength(1);
    expect(store.rationale).toBe("**Bold** rationale survived the close.");
    expect(store.degradedReason).toBe("all_refused");
  });
});

describe("clearResult() wipes (AC #4)", () => {
  it("resets error / refused / rationale / degradedReason but keeps prompt", () => {
    const store = useAiCommandPaletteStore();
    store.prompt = "still here after clear";
    seedFailureState(store);

    store.clearResult();

    expect(store.error).toBeNull();
    expect(store.refused).toEqual([]);
    expect(store.rationale).toBeNull();
    expect(store.degradedReason).toBeNull();
    expect(store.aiDisabled).toBe(false);
    expect(store.prompt).toBe("still here after clear");
  });
});

describe("submit() auto-clears prior response at start (AC #5)", () => {
  it("wipes prior error / rationale before issuing the new request", async () => {
    const store = useAiCommandPaletteStore();
    seedFailureState(store);

    let observedRationaleAtCallTime: string | null = "not-observed";
    let observedErrorAtCallTime: string | null = "not-observed";
    mockSymbols.submitCommandPalette.mockImplementation(async () => {
      observedRationaleAtCallTime = store.rationale;
      observedErrorAtCallTime = store.error;
      return {
        diffId: "d1",
        opCount: 1,
        rationale: null,
        degraded: false,
        reason: null,
        diff: { rationale: null } as never,
        refused: [],
      };
    });

    await store.submit(baseSubmitOptions());

    expect(observedRationaleAtCallTime).toBeNull();
    expect(observedErrorAtCallTime).toBeNull();
  });

  it("preserves the prompt even when prior state is wiped", async () => {
    const store = useAiCommandPaletteStore();
    // The palette input is `v-model="palette.prompt"` so by the time
    // `submit()` runs, `store.prompt` already reflects what the user
    // typed. Mirror that here.
    store.prompt = "filter to last 30 days";
    seedFailureState(store);

    mockSymbols.submitCommandPalette.mockResolvedValue({
      diffId: null,
      opCount: 0,
      rationale: "Fresh rationale",
      degraded: true,
      reason: "no_tool_calls",
      diff: null,
      refused: [],
    });

    await store.submit(baseSubmitOptions());

    expect(store.prompt).toBe("filter to last 30 days");
    expect(store.rationale).toBe("Fresh rationale");
    expect(store.degradedReason).toBe("no_tool_calls");
  });

  it("validation refusal sets fresh error with prior state already wiped", async () => {
    const store = useAiCommandPaletteStore();
    seedFailureState(store);

    await store.submit({ ...baseSubmitOptions(), prompt: "   " });

    expect(mockSymbols.submitCommandPalette).not.toHaveBeenCalled();
    expect(store.error).toBe("Type a request first.");
    expect(store.refused).toEqual([]);
    expect(store.rationale).toBeNull();
    expect(store.degradedReason).toBeNull();
  });
});

describe("close while loading still aborts the in-flight request (AC #2 guard)", () => {
  it("aborts the controller and clears loading", async () => {
    const store = useAiCommandPaletteStore();

    let abortFired = false;
    mockSymbols.submitCommandPalette.mockImplementation(
      async (_body: unknown, signal?: AbortSignal) => {
        return new Promise((_resolve, reject) => {
          if (!signal) return;
          signal.addEventListener("abort", () => {
            abortFired = true;
            const err = new DOMException("aborted", "AbortError");
            reject(err);
          });
        });
      },
    );

    const inflight = store.submit(baseSubmitOptions());
    // Let the promise reach `await submitCommandPalette(...)`.
    await Promise.resolve();
    expect(store.loading).toBe(true);

    store.close();
    await inflight;

    expect(abortFired).toBe(true);
    expect(store.loading).toBe(false);
    expect(store.isOpen).toBe(false);
  });
});

describe("backdrop dismissal removed (AC #1)", () => {
  it("the store exposes no `dismiss` / backdrop-only path that wipes state", () => {
    // The Cmd+K palette closes via Esc, the explicit ✕ button, or the
    // success-path handoff. We pin the surface here so a future refactor
    // doesn't quietly reintroduce a wipe-on-close codepath.
    const store = useAiCommandPaletteStore();
    seedFailureState(store);
    store.close();

    // Failure state survives — the only way to clear it is `clearResult()`
    // or a fresh `submit()`.
    expect(store.error).not.toBeNull();
    expect(store.rationale).not.toBeNull();
    expect(store.refused.length).toBeGreaterThan(0);
    expect(store.degradedReason).not.toBeNull();
  });
});
