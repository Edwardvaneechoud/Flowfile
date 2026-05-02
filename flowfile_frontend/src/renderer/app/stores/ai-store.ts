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
import { computed, ref } from "vue";

import {
  AiStreamHttpError,
  fetchAiProviders,
  streamChat,
  streamRunFailureExplanation,
  type ChatMessageBody,
} from "../api/ai.api";
import type { AiProvider } from "../views/AiProvidersView/aiProviderTypes";
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
  };
});
