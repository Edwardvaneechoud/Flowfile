// AI Store — chat-drawer state for the read-only chat surface (W20).
//
// Only owns *what's currently being talked about* — the open/close flag
// for the drawer is mirrored from `editor-store.ts` so other panels can
// participate in `hideAllPanels()` without coupling to this module. The
// chat payload + streaming state live here.
//
// Future workstreams will extend this:
//   - W22: pin-context (selected node, schemas) into the request body.
//   - W42/W43: persist messages + sessions per flow.
//   - W31+: handle `tool_call` events and wire diff acceptance.

import { defineStore } from "pinia";
import { computed, ref, watch } from "vue";

import {
  AiStreamHttpError,
  fetchAiProviders,
  streamChat,
  streamGenerateDocumentation,
  streamRunFailureExplanation,
  type ChatMessageBody,
} from "../api/ai.api";
import type { AiProvider } from "../views/AiProvidersView/aiProviderTypes";
import {
  highestPersistedMessageId,
  loadPersistedAiState,
  persistAiState,
} from "./ai-store-persistence";
import { useEditorStore } from "./editor-store";

export type ChatRole = "user" | "assistant";

export interface ChatMessage {
  id: number;
  role: ChatRole;
  content: string;
  /** Set on the assistant placeholder while the stream is open. */
  pending?: boolean;
  /** Reason why a message is in an error state, if any. */
  error?: string | null;
}

export type StreamingState = "idle" | "streaming" | "error";

let _messageCounter = 0;
const nextMessageId = (): number => {
  _messageCounter += 1;
  return _messageCounter;
};

// W27 — interim browser-side persistence. Throttled writes coalesce streaming
// chunk deltas into ~4 saves/sec instead of one per token. Server-side
// per-flow persistence (W42 / W43, sidecar at `{user_dir}/ai_sessions/{flow_id}/`
// per D007) will obsolete this; keep the persistence path thin so it's easy
// to delete or migrate when that lands.
const PERSIST_THROTTLE_MS = 250;

export const useAiStore = defineStore("ai", () => {
  const editorStore = useEditorStore();

  // ----- providers (from W12 / W16) -----
  const providers = ref<AiProvider[]>([]);
  const providersLoading = ref(false);
  const providersError = ref<string | null>(null);

  const selectedProvider = ref<string | null>(null);
  const selectedModel = ref<string | null>(null);

  // ----- messages -----
  const messages = ref<ChatMessage[]>([]);

  // ----- stream lifecycle -----
  const streamingState = ref<StreamingState>("idle");
  const streamError = ref<string | null>(null);
  let activeAbort: AbortController | null = null;

  // ----- W27 hydrate from sessionStorage -----
  // Order matters: hydrate refs BEFORE wiring the watch so the initial
  // assignment doesn't trigger a redundant write of what we just read.
  const _hydrated = loadPersistedAiState();
  if (_hydrated.messages.length > 0) {
    messages.value = _hydrated.messages;
    // Bump the module-scoped counter past any persisted ids so the next
    // `nextMessageId()` call doesn't collide with a hydrated message.
    const persistedMax = highestPersistedMessageId(_hydrated.messages);
    if (persistedMax > _messageCounter) {
      _messageCounter = persistedMax;
    }
  }
  if (_hydrated.selectedProvider !== null) {
    selectedProvider.value = _hydrated.selectedProvider;
  }
  if (_hydrated.selectedModel !== null) {
    selectedModel.value = _hydrated.selectedModel;
  }

  // Throttled save. SessionStorage writes are sync + main-thread; coalescing
  // streaming chunk deltas into ~4 writes/sec keeps the cost negligible
  // during a long response. User-driven changes (Send, Clear, provider pick)
  // get a 0 ms delay so the next tick captures them — durable for any
  // refresh past one event-loop turn (~milliseconds).
  let saveTimer: ReturnType<typeof setTimeout> | null = null;
  const queuePersist = (): void => {
    const isStreaming = streamingState.value === "streaming";
    if (saveTimer !== null) {
      if (isStreaming) return;
      // Stream just settled — cancel the long throttle and flush asap so
      // the final state is durable on the next tick rather than ≤ 250 ms
      // later.
      clearTimeout(saveTimer);
      saveTimer = null;
    }
    saveTimer = setTimeout(
      () => {
        saveTimer = null;
        persistAiState({
          messages: messages.value,
          selectedProvider: selectedProvider.value,
          selectedModel: selectedModel.value,
        });
      },
      isStreaming ? PERSIST_THROTTLE_MS : 0,
    );
  };

  // `flush: "sync"` so each mutation is evaluated against the current
  // `streamingState` value, not the post-flush value. This matters because
  // sendMessage pushes the user message *and* flips streamingState in the
  // same sync block: a deferred-flush watcher would see the post-flush
  // "streaming" state and route the user-message push through the slow
  // throttle path.
  watch(messages, queuePersist, { deep: true, flush: "sync" });
  watch(selectedProvider, queuePersist, { flush: "sync" });
  watch(selectedModel, queuePersist, { flush: "sync" });
  watch(streamingState, queuePersist, { flush: "sync" });

  // ----- derived -----
  const isAiOpen = computed<boolean>({
    get: () => editorStore.isAiOpen,
    set: (value: boolean) => {
      editorStore.isAiOpen = value;
    },
  });

  const isStreaming = computed(() => streamingState.value === "streaming");

  const configuredProviders = computed(() =>
    providers.value.filter((p) => p.status === "configured" || p.status === "env_fallback"),
  );

  const hasConfiguredProvider = computed(() => configuredProviders.value.length > 0);

  // ----- actions -----

  const openAiDrawer = (): void => {
    editorStore.isAiOpen = true;
  };

  const closeAiDrawer = (): void => {
    editorStore.isAiOpen = false;
  };

  const toggleAiDrawer = (): void => {
    editorStore.isAiOpen = !editorStore.isAiOpen;
  };

  const clearMessages = (): void => {
    abortStream();
    messages.value = [];
  };

  const loadProviders = async (): Promise<void> => {
    providersLoading.value = true;
    providersError.value = null;
    try {
      const list = await fetchAiProviders();
      providers.value = list;
      if (selectedProvider.value === null) {
        const first = list.find((p) => p.status === "configured" || p.status === "env_fallback");
        if (first) {
          selectedProvider.value = first.provider;
          selectedModel.value = first.credential?.defaultModel ?? first.defaultModel ?? null;
        }
      }
    } catch (err) {
      providersError.value = err instanceof Error ? err.message : String(err);
      providers.value = [];
    } finally {
      providersLoading.value = false;
    }
  };

  const setSelectedProvider = (name: string): void => {
    selectedProvider.value = name;
    const meta = providers.value.find((p) => p.provider === name);
    if (meta) {
      selectedModel.value = meta.credential?.defaultModel ?? meta.defaultModel ?? null;
    }
  };

  const setSelectedModel = (model: string | null): void => {
    selectedModel.value = model;
  };

  const abortStream = (): void => {
    if (activeAbort !== null) {
      activeAbort.abort();
      activeAbort = null;
    }
    if (streamingState.value === "streaming") {
      streamingState.value = "idle";
      // Mark any pending placeholder as cancelled so the user sees something.
      const pending = messages.value.find((m) => m.pending);
      if (pending && !pending.content) {
        pending.content = "[cancelled]";
      }
      if (pending) {
        pending.pending = false;
      }
    }
  };

  const sendMessage = async (rawText: string): Promise<void> => {
    const text = rawText.trim();
    if (!text) return;
    if (streamingState.value === "streaming") return;
    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return;
    }

    const userMessage: ChatMessage = {
      id: nextMessageId(),
      role: "user",
      content: text,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(userMessage, assistantPlaceholder);
    // The local `assistantPlaceholder` is a raw object; mutating it bypasses
    // Vue's reactive Proxy and the chat would only repaint when something
    // else (e.g. `streamingState`) re-triggered. Re-read from the array so
    // we mutate through the Proxy and each token causes a re-render.
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    const wireMessages: ChatMessageBody[] = messages.value
      .filter((m) => !m.pending)
      .map((m) => ({ role: m.role, content: m.content }));

    activeAbort = new AbortController();
    try {
      await streamChat(
        {
          provider: selectedProvider.value,
          model: selectedModel.value,
          messages: wireMessages,
        },
        {
          onChunk: (delta) => {
            reactivePlaceholder.content += delta;
          },
          onDone: () => {
            reactivePlaceholder.pending = false;
            streamingState.value = "idle";
          },
          onError: (message) => {
            reactivePlaceholder.error = message;
            reactivePlaceholder.pending = false;
            streamingState.value = "error";
            streamError.value = message;
          },
        },
        activeAbort.signal,
      );
      // streamChat resolves on stream close. If we never saw a `done` event
      // (e.g. server cut the connection mid-flight), still flip the state
      // back so the UI is interactable.
      reactivePlaceholder.pending = false;
      if (streamingState.value === "streaming") {
        streamingState.value = "idle";
      }
    } catch (err) {
      reactivePlaceholder.pending = false;
      const message =
        err instanceof AiStreamHttpError
          ? `HTTP ${err.status}: ${err.detail}`
          : err instanceof Error
            ? err.message
            : String(err);
      // A user-triggered abort surfaces as DOMException("AbortError"); that's
      // not a streaming error, just a cancellation.
      const isAbort = err instanceof DOMException && err.name === "AbortError";
      if (isAbort) {
        streamingState.value = "idle";
      } else {
        reactivePlaceholder.error = message;
        streamingState.value = "error";
        streamError.value = message;
      }
    } finally {
      activeAbort = null;
    }
  };

  const explainRunFailure = async (
    flowId: number,
    nodeId: number,
    errorMessage: string,
    nodeName?: string,
  ): Promise<void> => {
    // W23 — "Fix with AI" entry point. Opens the drawer, drops a synthetic
    // user/assistant pair into the chat, then streams the server-built
    // failure explanation into the assistant placeholder. The wire-level
    // user message is composed by the backend via W22's render_prompt_context;
    // the chat-visible synthetic turn is purely cosmetic so the user has a
    // visual anchor for what they just asked.
    openAiDrawer();

    if (streamingState.value === "streaming") {
      // Don't trample an in-flight chat. The button click counts as
      // user intent to open the drawer; the streaming guard mirrors
      // ``sendMessage``.
      return;
    }
    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return;
    }

    const trimmedError = errorMessage.trim();
    const headline = nodeName ? `node "${nodeName}"` : `node ${nodeId}`;
    const userVisibleText = trimmedError
      ? `Help me fix this error in ${headline}:\n\n${trimmedError}`
      : `Help me fix this error in ${headline}.`;

    const userMessage: ChatMessage = {
      id: nextMessageId(),
      role: "user",
      content: userVisibleText,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(userMessage, assistantPlaceholder);
    // See note in `sendMessage` — mutate through the reactive Proxy so each
    // chunk re-renders the message rather than landing in one paint.
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    activeAbort = new AbortController();
    try {
      await streamRunFailureExplanation(
        {
          flow_id: flowId,
          node_id: nodeId,
          provider: selectedProvider.value,
          model: selectedModel.value,
          error_message: trimmedError || null,
        },
        {
          onChunk: (delta) => {
            reactivePlaceholder.content += delta;
          },
          onDone: () => {
            reactivePlaceholder.pending = false;
            streamingState.value = "idle";
          },
          onError: (message) => {
            reactivePlaceholder.error = message;
            reactivePlaceholder.pending = false;
            streamingState.value = "error";
            streamError.value = message;
          },
        },
        activeAbort.signal,
      );
      reactivePlaceholder.pending = false;
      if (streamingState.value === "streaming") {
        streamingState.value = "idle";
      }
    } catch (err) {
      reactivePlaceholder.pending = false;
      const message =
        err instanceof AiStreamHttpError
          ? `HTTP ${err.status}: ${err.detail}`
          : err instanceof Error
            ? err.message
            : String(err);
      const isAbort = err instanceof DOMException && err.name === "AbortError";
      if (isAbort) {
        streamingState.value = "idle";
      } else {
        reactivePlaceholder.error = message;
        streamingState.value = "error";
        streamError.value = message;
      }
    } finally {
      activeAbort = null;
    }
  };

  const generateDocumentation = async (flowId: number, flowName?: string): Promise<void> => {
    // W50 — "Generate documentation" entry point. Same shape as
    // ``explainRunFailure``: opens the drawer, drops a synthetic
    // user/assistant pair into the chat, then streams the server-built
    // markdown doc into the assistant placeholder. The wire-level user
    // message is composed by the backend via W22's render_prompt_context
    // (surface="docgen") + the W50 ``## Documentation request`` block;
    // the chat-visible synthetic turn is purely cosmetic.
    openAiDrawer();

    if (streamingState.value === "streaming") {
      return;
    }
    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return;
    }

    const trimmedName = flowName?.trim();
    const headline = trimmedName ? `\`${trimmedName}\`` : `flow ${flowId}`;
    const userVisibleText = `Generate documentation for ${headline}.`;

    const userMessage: ChatMessage = {
      id: nextMessageId(),
      role: "user",
      content: userVisibleText,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(userMessage, assistantPlaceholder);
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    activeAbort = new AbortController();
    try {
      await streamGenerateDocumentation(
        {
          flow_id: flowId,
          provider: selectedProvider.value,
          model: selectedModel.value,
        },
        {
          onChunk: (delta) => {
            reactivePlaceholder.content += delta;
          },
          onDone: () => {
            reactivePlaceholder.pending = false;
            streamingState.value = "idle";
          },
          onError: (message) => {
            reactivePlaceholder.error = message;
            reactivePlaceholder.pending = false;
            streamingState.value = "error";
            streamError.value = message;
          },
        },
        activeAbort.signal,
      );
      reactivePlaceholder.pending = false;
      if (streamingState.value === "streaming") {
        streamingState.value = "idle";
      }
    } catch (err) {
      reactivePlaceholder.pending = false;
      const message =
        err instanceof AiStreamHttpError
          ? `HTTP ${err.status}: ${err.detail}`
          : err instanceof Error
            ? err.message
            : String(err);
      const isAbort = err instanceof DOMException && err.name === "AbortError";
      if (isAbort) {
        streamingState.value = "idle";
      } else {
        reactivePlaceholder.error = message;
        streamingState.value = "error";
        streamError.value = message;
      }
    } finally {
      activeAbort = null;
    }
  };

  return {
    // state
    providers,
    providersLoading,
    providersError,
    selectedProvider,
    selectedModel,
    messages,
    streamingState,
    streamError,
    // computed
    isAiOpen,
    isStreaming,
    configuredProviders,
    hasConfiguredProvider,
    // actions
    openAiDrawer,
    closeAiDrawer,
    toggleAiDrawer,
    clearMessages,
    loadProviders,
    setSelectedProvider,
    setSelectedModel,
    abortStream,
    sendMessage,
    explainRunFailure,
    generateDocumentation,
  };
});
