import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

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
    streamChat: vi.fn(),
    streamGenerateDocumentation: vi.fn(),
    streamInlineAction: vi.fn(),
    streamLineageQuestion: vi.fn(),
    streamRunFailureExplanation: vi.fn(),
    routeMessage: vi.fn(),
    fetchAiProviders: vi.fn(async () => []),
    agentStoreStart: vi.fn(),
    agentStoreAbort: vi.fn(),
    flowStoreState: { flowId: 1 as number | null, vueFlowInstance: null as unknown },
    editorStoreState: {
      isAiOpen: false,
      openAiDrawer: vi.fn(),
      closeAiDrawer: vi.fn(),
    },
    loadPersistedAiState: vi.fn(() => ({
      messages: [],
      selectedProvider: null as string | null,
      selectedModel: null as string | null,
      autoPromote: null as boolean | null,
    })),
    persistAiState: vi.fn(),
    clearPersistedAiState: vi.fn(),
  };
});

vi.mock("../services/axios.config", () => {
  const axiosStub = {
    defaults: {},
    interceptors: {
      request: { use: vi.fn() },
      response: { use: vi.fn() },
    },
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  };
  return { default: axiosStub };
});

vi.mock("../services/auth.service", () => {
  const stub = {
    getToken: vi.fn(async () => null),
    logout: vi.fn(),
    isInDesktopMode: () => false,
    setModeFromBackend: vi.fn(),
    initialize: vi.fn(async () => false),
    hasValidToken: () => false,
    requiresLogin: () => false,
    isAuthenticated: () => false,
    getCurrentUser: vi.fn(async () => null),
  };
  return { default: stub, authService: stub };
});

vi.mock("../services/aiStreamClient", () => ({
  AiStreamHttpError: mockSymbols.AiStreamHttpError,
  streamChat: mockSymbols.streamChat,
  streamGenerateDocumentation: mockSymbols.streamGenerateDocumentation,
  streamInlineAction: mockSymbols.streamInlineAction,
  streamLineageQuestion: mockSymbols.streamLineageQuestion,
  streamRunFailureExplanation: mockSymbols.streamRunFailureExplanation,
  routeMessage: mockSymbols.routeMessage,
}));

vi.mock("../api/ai.api", () => ({
  AiStreamHttpError: mockSymbols.AiStreamHttpError,
  AiDisabledError: mockSymbols.AiDisabledError,
  fetchAiProviders: mockSymbols.fetchAiProviders,
  routeMessage: mockSymbols.routeMessage,
  streamChat: mockSymbols.streamChat,
  streamGenerateDocumentation: mockSymbols.streamGenerateDocumentation,
  streamInlineAction: mockSymbols.streamInlineAction,
  streamLineageQuestion: mockSymbols.streamLineageQuestion,
  streamRunFailureExplanation: mockSymbols.streamRunFailureExplanation,
}));

vi.mock("./ai-agent-store", () => ({
  useAiAgentStore: () => ({
    start: mockSymbols.agentStoreStart,
    abort: mockSymbols.agentStoreAbort,
  }),
}));

vi.mock("./editor-store", () => ({
  useEditorStore: () => mockSymbols.editorStoreState,
}));

vi.mock("./flow-store", () => ({
  useFlowStore: () => mockSymbols.flowStoreState,
}));

vi.mock("./ai-store-persistence", () => ({
  highestPersistedMessageId: () => 0,
  loadPersistedAiState: mockSymbols.loadPersistedAiState,
  persistAiState: mockSymbols.persistAiState,
  clearPersistedAiState: mockSymbols.clearPersistedAiState,
}));

// localModelApi imports config/constants, which reads ``window`` at module
// load — mock it (like every other sibling) so the store imports cleanly in
// the jsdom-free unit env. Default ``fetchLocalModelStatus`` → null so no
// synthetic "local" provider is injected, preserving the BYOK-only
// expectations in the loadProviders specs below.
vi.mock("../views/AiProvidersView/localModelApi", () => ({
  LOCAL_PROVIDER_ID: "local",
  fetchLocalModelStatus: vi.fn(async () => null),
  generateFlow: vi.fn(),
  selectLocalModel: vi.fn(),
  streamLocalModelInstall: vi.fn(),
}));

// Simple-build applies via the diff client (acceptDiff); mock it so the store
// imports cleanly without the fetch layer.
vi.mock("../services/aiDiffClient", () => ({
  acceptDiff: vi.fn(),
}));

import { useAiStore } from "./ai-store";

beforeEach(() => {
  setActivePinia(createPinia());
  mockSymbols.streamChat.mockReset();
  mockSymbols.routeMessage.mockReset();
  mockSymbols.agentStoreStart.mockReset();
  mockSymbols.agentStoreAbort.mockReset();
  mockSymbols.fetchAiProviders.mockReset();
  mockSymbols.fetchAiProviders.mockImplementation(async () => []);
  mockSymbols.flowStoreState.flowId = 1;
  mockSymbols.loadPersistedAiState.mockReset();
  mockSymbols.loadPersistedAiState.mockImplementation(() => ({
    messages: [],
    selectedProvider: null,
    selectedModel: null,
    autoPromote: null,
  }));
});

afterEach(() => {
  vi.restoreAllMocks();
});

const _withProvider = (): ReturnType<typeof useAiStore> => {
  const store = useAiStore();
  store.selectedProvider = "anthropic";
  store.selectedModel = "claude-sonnet-4-6";
  return store;
};

describe("useAiStore - sendMessage with mode", () => {
  it("promotes to agent when verdict='agent'", async () => {
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "agent",
      kind: "build",
      confidence: 0.85,
      reason: "user wants to add a group_by",
      latencyMs: 320,
    });
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);

    const store = _withProvider();
    expect(store.mode).toBe("auto");
    await store.sendMessage("add a group_by node grouping by status");

    expect(mockSymbols.routeMessage).toHaveBeenCalledTimes(1);
    expect(mockSymbols.routeMessage).toHaveBeenCalledWith({
      message: "add a group_by node grouping by status",
      provider: "anthropic",
      model: "claude-sonnet-4-6",
      // First send of the session — no prior turns, so `history` is omitted.
      history: undefined,
    });
    expect(mockSymbols.agentStoreStart).toHaveBeenCalledTimes(1);
    expect(mockSymbols.agentStoreStart).toHaveBeenCalledWith(
      expect.objectContaining({
        flow_id: 1,
        prompt: "add a group_by node grouping by status",
        // Default agent surface is now ``agent_live`` (REPL-style), so
        // auto-promote dispatches there. Users can override via the
        // settings popover.
        surface: "agent_live",
        provider: "anthropic",
      }),
    );
    expect(mockSymbols.streamChat).not.toHaveBeenCalled();
    // Banner is set so AiAssistant.vue renders the "switch back to chat" affordance.
    expect(store.promotionBanner).toEqual({
      reason: "user wants to add a group_by",
      message: "add a group_by node grouping by status",
    });
    // The user message lands in the chat trail without an `[Agent]` prefix —
    // the banner conveys the mode change.
    expect(store.messages).toHaveLength(1);
    expect(store.messages[0]).toMatchObject({
      role: "user",
      content: "add a group_by node grouping by status",
    });
  });

  it("forwards recent chat history to /ai/route", async () => {
    // Short follow-ups like "can you implement?" only classify
    // correctly when the LLM sees the prior assistant suggestion.
    // The store passes the last few non-pending messages to /ai/route.
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "agent",
      kind: "build",
      confidence: 0.82,
      reason: "follow-up to a suggestion",
      latencyMs: 410,
    });
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);

    const store = _withProvider();
    // Seed the chat trail with a prior question + response.
    store.messages = [
      {
        id: 1,
        createdAt: 1,
        role: "user",
        content: "how do I count customers per city?",
      },
      {
        id: 2,
        createdAt: 2,
        role: "assistant",
        content: "Add a group_by node grouping on city ...",
      },
    ];

    await store.sendMessage("can you implement?");

    expect(mockSymbols.routeMessage).toHaveBeenCalledTimes(1);
    const callBody = mockSymbols.routeMessage.mock.calls[0][0];
    expect(callBody.message).toBe("can you implement?");
    expect(callBody.history).toEqual([
      { role: "user", content: "how do I count customers per city?" },
      { role: "assistant", content: "Add a group_by node grouping on city ..." },
    ]);
    expect(mockSymbols.agentStoreStart).toHaveBeenCalledTimes(1);

    // The agent's prompt is enriched with the chat transcript so the
    // planner knows what to build. Without this, the agent receives
    // only "can you implement?" and asks the user to clarify what
    // they want — defeating the auto-promotion.
    const agentArgs = mockSymbols.agentStoreStart.mock.calls[0][0];
    expect(agentArgs.prompt).toContain("how do I count customers per city?");
    expect(agentArgs.prompt).toContain("Add a group_by node grouping on city");
    expect(agentArgs.prompt).toContain("can you implement?");
    expect(agentArgs.prompt).toContain("User:");
    expect(agentArgs.prompt).toContain("Assistant:");
  });

  it("dispatches the bare message when there's no prior chat history", async () => {
    // First message of a session has no transcript to include; the
    // enriched-prompt logic falls back to the verbatim text.
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "agent",
      kind: "build",
      confidence: 0.9,
      reason: "clear build phrase",
      latencyMs: 200,
    });
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);

    const store = _withProvider();
    expect(store.messages).toHaveLength(0);
    await store.sendMessage("add a sort node");

    const agentArgs = mockSymbols.agentStoreStart.mock.calls[0][0];
    expect(agentArgs.prompt).toBe("add a sort node");
  });

  it("pushes the user message before awaiting /ai/route (optimistic UX)", async () => {
    // Without optimistic push the user's input clears, then they see
    // *nothing* in the chat trail for the classifier latency (~800 ms
    // p50 / ~1500 ms p95). Mirror the agent-mode dispatch's existing
    // posture: push the message first, then route, then dispatch.
    let captureSnapshot: { messageCount: number; lastContent: string | undefined } | null = null;
    mockSymbols.routeMessage.mockImplementation(async () => {
      // Inspect the store at the moment the classifier round-trip is in
      // flight — the user's message must already be visible.
      const snapshot = useAiStore();
      captureSnapshot = {
        messageCount: snapshot.messages.length,
        lastContent: snapshot.messages[snapshot.messages.length - 1]?.content,
      };
      return {
        verdict: "agent",
        kind: "build",
        confidence: 0.85,
        reason: "...",
        latencyMs: 700,
      };
    });
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);

    const store = _withProvider();
    expect(store.messages).toHaveLength(0);
    await store.sendMessage("add a sort node");

    expect(captureSnapshot).not.toBeNull();
    expect(captureSnapshot!.messageCount).toBe(1);
    expect(captureSnapshot!.lastContent).toBe("add a sort node");
    // Promoted-agent dispatch must NOT push a duplicate user message.
    const userMessages = store.messages.filter((m) => m.role === "user");
    expect(userMessages).toHaveLength(1);
    expect(userMessages[0].content).toBe("add a sort node");
  });

  it("falls through to chat when verdict='chat'", async () => {
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "chat",
      kind: "chat",
      confidence: 0.9,
      reason: "message opens with a question word",
      latencyMs: 14,
    });
    mockSymbols.streamChat.mockImplementation(async (_body, handlers) => {
      handlers.onChunk?.("answer ");
      handlers.onDone?.("stop");
    });

    const store = _withProvider();
    await store.sendMessage("what columns does node 2 have");

    expect(mockSymbols.routeMessage).toHaveBeenCalledTimes(1);
    expect(mockSymbols.streamChat).toHaveBeenCalledTimes(1);
    expect(mockSymbols.agentStoreStart).not.toHaveBeenCalled();
    expect(store.promotionBanner).toBeNull();
    // user + assistant placeholder pushed.
    expect(store.messages).toHaveLength(2);
  });

  it("skips /ai/route entirely when mode is chat", async () => {
    mockSymbols.streamChat.mockImplementation(async (_body, handlers) => {
      handlers.onDone?.("stop");
    });
    const store = _withProvider();
    store.setMode("chat");
    await store.sendMessage("add a sort node");

    expect(mockSymbols.routeMessage).not.toHaveBeenCalled();
    expect(mockSymbols.streamChat).toHaveBeenCalledTimes(1);
    expect(mockSymbols.agentStoreStart).not.toHaveBeenCalled();
  });

  it("skips /ai/route when no flow is loaded", async () => {
    mockSymbols.flowStoreState.flowId = null;
    mockSymbols.streamChat.mockImplementation(async (_body, handlers) => {
      handlers.onDone?.("stop");
    });
    const store = _withProvider();
    await store.sendMessage("add a sort node");

    expect(mockSymbols.routeMessage).not.toHaveBeenCalled();
    expect(mockSymbols.streamChat).toHaveBeenCalledTimes(1);
  });

  it("falls back to chat when /ai/route throws", async () => {
    mockSymbols.routeMessage.mockRejectedValue(
      new mockSymbols.AiStreamHttpError(503, "AI features are disabled."),
    );
    mockSymbols.streamChat.mockImplementation(async (_body, handlers) => {
      handlers.onDone?.("stop");
    });
    const store = _withProvider();
    await store.sendMessage("add a sort node");

    expect(mockSymbols.routeMessage).toHaveBeenCalledTimes(1);
    expect(mockSymbols.streamChat).toHaveBeenCalledTimes(1);
    expect(mockSymbols.agentStoreStart).not.toHaveBeenCalled();
    expect(store.promotionBanner).toBeNull();
  });

  it("does not call /ai/route or dispatch when no provider is selected", async () => {
    const store = useAiStore();
    // selectedProvider stays null
    await store.sendMessage("add a sort node");

    expect(mockSymbols.routeMessage).not.toHaveBeenCalled();
    expect(mockSymbols.streamChat).not.toHaveBeenCalled();
    expect(mockSymbols.agentStoreStart).not.toHaveBeenCalled();
    expect(store.streamingState).toBe("error");
  });

  it("low-confidence build gets verdict='chat' from the backend and stays as chat", async () => {
    // The verdict-mapping is server-side; the store doesn't apply the
    // threshold. This test exercises that contract.
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "chat",
      kind: "build",
      confidence: 0.3,
      reason: "weak signal",
      latencyMs: 200,
    });
    mockSymbols.streamChat.mockImplementation(async (_body, handlers) => {
      handlers.onDone?.("stop");
    });
    const store = _withProvider();
    await store.sendMessage("maybe drop trailing whitespace");

    expect(mockSymbols.streamChat).toHaveBeenCalledTimes(1);
    expect(mockSymbols.agentStoreStart).not.toHaveBeenCalled();
  });
});

describe("useAiStore - undoPromotion", () => {
  it("aborts the agent, flips mode to chat, and re-dispatches as chat", async () => {
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "agent",
      kind: "build",
      confidence: 0.85,
      reason: "build signal",
      latencyMs: 300,
    });
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);
    mockSymbols.agentStoreAbort.mockResolvedValue(undefined);
    mockSymbols.streamChat.mockImplementation(async (_body, handlers) => {
      handlers.onDone?.("stop");
    });

    const store = _withProvider();
    await store.sendMessage("add a sort node");
    expect(store.promotionBanner).not.toBeNull();
    expect(store.mode).toBe("auto");
    expect(mockSymbols.agentStoreStart).toHaveBeenCalledTimes(1);
    // Pre-undo: chat stream should not have been opened.
    expect(mockSymbols.streamChat).not.toHaveBeenCalled();

    await store.undoPromotion();

    expect(mockSymbols.agentStoreAbort).toHaveBeenCalledTimes(1);
    expect(store.mode).toBe("chat");
    expect(store.promotionBanner).toBeNull();
    // After undo: a chat stream is opened; the saved user message stays
    // exactly once in `messages` (no second push).
    expect(mockSymbols.streamChat).toHaveBeenCalledTimes(1);
    const userMessages = store.messages.filter((m) => m.role === "user");
    expect(userMessages).toHaveLength(1);
    expect(userMessages[0].content).toBe("add a sort node");
  });

  it("is a no-op when no banner is showing", async () => {
    const store = _withProvider();
    await store.undoPromotion();
    expect(mockSymbols.agentStoreAbort).not.toHaveBeenCalled();
    expect(mockSymbols.streamChat).not.toHaveBeenCalled();
  });

  it("also clears agentModeAccepted (mutually exclusive with continue-as-agent)", async () => {
    // Undo means "back to chat". If a prior accept set
    // agentModeAccepted=true, leaving it on would force the next send
    // back to agent and silently erase the undo.
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "agent",
      kind: "build",
      confidence: 0.85,
      reason: "build signal",
      latencyMs: 200,
    });
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);
    mockSymbols.agentStoreAbort.mockResolvedValue(undefined);
    mockSymbols.streamChat.mockImplementation(async (_body, handlers) => {
      handlers.onDone?.("stop");
    });

    const store = _withProvider();
    await store.sendMessage("add a sort node");
    store.acceptPromotion();
    expect(store.agentModeAccepted).toBe(true);

    // Re-arm the banner (acceptPromotion clears it) so undo has something
    // to act on.
    store.promotionBanner = { reason: "...", message: "add a sort node" };
    await store.undoPromotion();

    expect(store.agentModeAccepted).toBe(false);
    expect(store.mode).toBe("chat");
  });
});

describe("useAiStore - acceptPromotion (round 7)", () => {
  it("flips agentModeAccepted=true and clears the banner", async () => {
    mockSymbols.routeMessage.mockResolvedValue({
      verdict: "agent",
      kind: "build",
      confidence: 0.85,
      reason: "build signal",
      latencyMs: 200,
    });
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);

    const store = _withProvider();
    await store.sendMessage("add a sort node");
    expect(store.promotionBanner).not.toBeNull();
    expect(store.agentModeAccepted).toBe(false);

    store.acceptPromotion();

    expect(store.agentModeAccepted).toBe(true);
    expect(store.promotionBanner).toBeNull();
    // Accept does NOT abort the in-flight run — "continue", not "restart".
    expect(mockSymbols.agentStoreAbort).not.toHaveBeenCalled();
  });

  it("subsequent sendMessage skips /ai/route and dispatches agent directly", async () => {
    mockSymbols.agentStoreStart.mockResolvedValue(undefined);

    const store = _withProvider();
    // Simulate post-accept state: banner cleared, flag flipped.
    store.agentModeAccepted = true;
    expect(store.messages).toHaveLength(0);

    await store.sendMessage("now add a sort downstream");

    expect(mockSymbols.routeMessage).not.toHaveBeenCalled();
    expect(mockSymbols.agentStoreStart).toHaveBeenCalledTimes(1);
    expect(mockSymbols.streamChat).not.toHaveBeenCalled();
    const agentArgs = mockSymbols.agentStoreStart.mock.calls[0][0];
    expect(agentArgs.flow_id).toBe(1);
    // Default agent surface is ``agent_live``, so auto-promote dispatches there.
    expect(agentArgs.surface).toBe("agent_live");
    // The user message lands in the chat trail exactly once (round 6
    // optimistic-push contract preserved).
    expect(store.messages.filter((m) => m.role === "user")).toHaveLength(1);
  });

  it("acceptPromotion is a no-op when no banner is showing", () => {
    const store = _withProvider();
    expect(store.promotionBanner).toBeNull();
    store.acceptPromotion();
    expect(store.agentModeAccepted).toBe(false);
  });

  it("clearMessages resets agentModeAccepted", async () => {
    const store = _withProvider();
    store.agentModeAccepted = true;
    store.messages = [{ id: 1, createdAt: 1, role: "user", content: "hi" }];

    store.clearMessages();
    expect(store.agentModeAccepted).toBe(false);
    expect(store.messages).toHaveLength(0);
  });
});

describe("useAiStore - mode default + autoPromote migration shim", () => {
  // `mode` lives in sessionStorage now; the test environment has no
  // `window`, so `_readPersistedMode` returns null and the migration
  // shim ALWAYS drives the seed in these tests. Asserts that:
  //   - legacy `autoPromote: false` from localStorage seeds mode="chat"
  //   - legacy `autoPromote: true` / null / missing seeds mode="auto"
  it("seeds mode='chat' when legacy autoPromote=false is in localStorage", () => {
    mockSymbols.loadPersistedAiState.mockImplementation(() => ({
      messages: [],
      selectedProvider: null,
      selectedModel: null,
      autoPromote: false,
    }));
    const store = useAiStore();
    expect(store.mode).toBe("chat");
  });

  it("seeds mode='auto' when no legacy autoPromote field is present", () => {
    mockSymbols.loadPersistedAiState.mockImplementation(() => ({
      messages: [],
      selectedProvider: null,
      selectedModel: null,
      autoPromote: null,
    }));
    const store = useAiStore();
    expect(store.mode).toBe("auto");
  });

  it("seeds mode='auto' when legacy autoPromote=true is in localStorage", () => {
    mockSymbols.loadPersistedAiState.mockImplementation(() => ({
      messages: [],
      selectedProvider: null,
      selectedModel: null,
      autoPromote: true,
    }));
    const store = useAiStore();
    expect(store.mode).toBe("auto");
  });
});

describe("useAiStore - split model tiers (resolveSurface)", () => {
  const _seed = (): ReturnType<typeof useAiStore> => {
    const store = useAiStore();
    store.providers = [
      {
        provider: "openrouter",
        supportsTools: true,
        supportsStreaming: true,
        defaultModel: "qwen/qwen3-coder",
        surfaces: { cron: "anthropic/claude-haiku-4.5" },
        status: "configured",
        credential: null,
      },
      {
        provider: "local",
        supportsTools: false,
        supportsStreaming: true,
        defaultModel: "qwen2.5-coder-3b",
        surfaces: {},
        status: "configured",
        credential: null,
      },
    ];
    store.setSelectedProvider("openrouter");
    return store;
  };

  it("routes every surface to the main selection when split is off", () => {
    const store = _seed();
    store.setSelectedModel("anthropic/claude-opus-4.5");
    expect(store.splitModels).toBe(false);
    expect(store.resolveSurface("ghost_node")).toEqual({
      provider: "openrouter",
      model: "anthropic/claude-opus-4.5",
    });
    // cron is a simple surface but split is off → still the main selection.
    expect(store.resolveSurface("cron")).toEqual({
      provider: "openrouter",
      model: "anthropic/claude-opus-4.5",
    });
  });

  it("routes simple surfaces to the simple tier (own provider) when split is on", () => {
    const store = _seed();
    store.setSelectedModel("anthropic/claude-opus-4.5");
    store.setSplitModels(true);
    // Simple tier can be a different provider entirely (e.g. local).
    store.setSimpleProvider("local");
    expect(store.resolveSurface("cron")).toEqual({
      provider: "local",
      model: "qwen2.5-coder-3b",
    });
    expect(store.resolveSurface("settings_autocomplete").provider).toBe("local");
    // Complex surfaces still use the main selection.
    expect(store.resolveSurface("ghost_node")).toEqual({
      provider: "openrouter",
      model: "anthropic/claude-opus-4.5",
    });
  });

  it("setSplitModels seeds the simple tier from the main selection", () => {
    const store = _seed();
    store.setSelectedModel("anthropic/claude-opus-4.5");
    store.setSplitModels(true);
    expect(store.simpleProvider).toBe("openrouter");
    expect(store.simpleModel).toBe("anthropic/claude-opus-4.5");
  });

  it("setSimpleProvider defaults the simple model to that provider's default", () => {
    const store = _seed();
    store.setSplitModels(true);
    store.setSimpleProvider("local");
    expect(store.simpleModel).toBe("qwen2.5-coder-3b");
  });

  it("modelForSurface falls back to the surface preset, then provider default", () => {
    const store = _seed();
    store.setSelectedModel(null);
    expect(store.modelForSurface("cron")).toBe("anthropic/claude-haiku-4.5");
    expect(store.modelForSurface("mystery")).toBe("qwen/qwen3-coder");
  });

  it("modelForSurface returns null when no provider is selected", () => {
    const store = useAiStore();
    store.selectedProvider = null;
    expect(store.modelForSurface("ghost_node")).toBeNull();
  });

  it("changing the complex provider leaves the independent simple tier intact", () => {
    const store = _seed();
    store.setSplitModels(true);
    store.setSimpleProvider("local");
    store.setSelectedProvider("openrouter");
    // Simple tier is independent — not reset by a complex-provider change.
    expect(store.simpleProvider).toBe("local");
    expect(store.resolveSurface("cron").provider).toBe("local");
  });
});
