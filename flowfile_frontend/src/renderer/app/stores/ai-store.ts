// AI Store — chat-drawer state for the read-only chat surface (W20).
//
// Only owns *what's currently being talked about* — the open/close flag
// for the drawer is mirrored from `editor-store.ts` so other panels can
// participate in `hideAllPanels()` without coupling to this module. The
// chat payload + streaming state live here.
//
// Persistence is per-flow + browser-local (W27 → W43). Each flow's chat
// trail lives under `flowfile.ai.chat.v1.{flow_id}` in `localStorage` so
// switching flows shows the right conversation, and chat survives
// Electron app restart. The `flowStore.flowId` watcher below performs an
// atomic save-then-load swap on flow switch so round-tripping A → B → A
// preserves A's history.

import { defineStore } from "pinia";
import { computed, ref, watch } from "vue";

import {
  AiStreamHttpError,
  fetchAiProviders,
  routeMessage,
  streamChat,
  streamGenerateDocumentation,
  streamInlineAction,
  streamLineageQuestion,
  streamRunFailureExplanation,
  type ChatMessageBody,
  type InlineActionType,
  type RouteHistoryEntry,
} from "../api/ai.api";
import type { AiProvider } from "../views/AiProvidersView/aiProviderTypes";
import { useAiAgentStore } from "./ai-agent-store";
import {
  highestPersistedMessageId,
  loadPersistedAiState,
  persistAiState,
} from "./ai-store-persistence";
import { useEditorStore } from "./editor-store";
import { useFlowStore } from "./flow-store";

export type ChatRole = "user" | "assistant";

export interface ChatMessage {
  id: number;
  role: ChatRole;
  content: string;
  /** ``Date.now()`` at push time. The chat timeline (AiAssistant.vue
   * ``timelineItems``) sorts by this — ``id`` is a monotonic counter, not
   * a wall-clock timestamp, so it can't be used to interleave messages
   * with agent events (which carry ``Date.now()`` in their ``at`` field). */
  createdAt: number;
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

// W27 → W43 — browser-side persistence. Throttled writes coalesce streaming
// chunk deltas into ~4 saves/sec instead of one per token. W43 made
// persistence per-flow (`localStorage` keyed by flow_id) so switching flows
// shows the right conversation and chat survives Electron restart.
const PERSIST_THROTTLE_MS = 250;

// `flowStore.flowId` is initialised to `-1` for "no flow loaded" (see
// `flow-store.ts:28`). Translate the sentinel to `null` so the persistence
// helpers route through the `unscoped` bucket cleanly.
const _scopedFlowId = (id: number): number | null => (id === -1 ? null : id);

export const useAiStore = defineStore("ai", () => {
  const editorStore = useEditorStore();
  // The chat store coordinates with the flow store both at hydration time
  // (load the right per-flow bucket) and on every flow switch (save
  // outgoing → load incoming). Resolve the store reference once at setup
  // so the watcher / queuePersist closures don't pay the lookup cost on
  // every tick.
  const flowStore = useFlowStore();

  // ----- providers (from W12 / W16) -----
  const providers = ref<AiProvider[]>([]);
  const providersLoading = ref(false);
  const providersError = ref<string | null>(null);

  const selectedProvider = ref<string | null>(null);
  const selectedModel = ref<string | null>(null);

  // W71 v1.9 — user-selectable agent surface. Defaults to ``agent_staged``
  // (v1's locked decision) but exposed in the UI as a third picker
  // alongside provider / model so users can opt into the legacy
  // two-stage ``agent`` or single-shot ``agent_complex`` surfaces
  // without editing code. The store-side getter / setter mirrors the
  // model picker pattern; the UI control lives in
  // ``AiAssistant.vue:header``. Persisted alongside the other AI
  // selections via ``ai-store-persistence``.
  const selectedAgentSurface = ref<"agent_complex" | "agent_staged" | "agent_live">("agent_staged");

  // ----- messages -----
  const messages = ref<ChatMessage[]>([]);

  // ----- stream lifecycle -----
  const streamingState = ref<StreamingState>("idle");
  const streamError = ref<string | null>(null);
  let activeAbort: AbortController | null = null;

  // ----- W58 chat → agent auto-promotion -----
  // ``autoPromote`` is a sticky session preference: defaults to true, persisted
  // alongside the chat history. Users flip it off via the AI Settings tab or
  // implicitly via the promotion-banner "undo" affordance. ``promotionBanner``
  // is purely transient — present only while a just-promoted agent run is in
  // flight (or until the user dismisses it via Stop / Clear).
  //
  // Round 7: ``agentModeAccepted`` is the post-promotion accept flag. When
  // the user clicks the banner's "Continue as agent" button, this flips to
  // ``true`` and short-circuits classification on subsequent sends — every
  // future message goes straight to the agent. Persisted via sessionStorage
  // so a tab refresh keeps the user's choice. Cleared by ``undoPromotion``
  // (which means "back to chat") and ``clearMessages``.
  const autoPromote = ref<boolean>(true);
  const agentModeAccepted = ref<boolean>(false);
  const promotionBanner = ref<{ reason: string; message: string } | null>(null);

  // ----- W27 + W43 hydrate from localStorage under the active flow's key -----
  // Order matters: hydrate refs BEFORE wiring the watch so the initial
  // assignment doesn't trigger a redundant write of what we just read.
  // Flow id is read from the flow store at hydration time so the right
  // per-flow bucket loads — this is the W43 update to the W27 path. When
  // no flow is loaded yet (flow store sentinel `-1`) we fall through to
  // the `unscoped` bucket which round-trips correctly once the user later
  // opens a flow (the watcher saves the unscoped state under that key
  // before swapping to the new flow's state).
  const _hydrated = loadPersistedAiState(undefined, _scopedFlowId(flowStore.flowId));
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
  if (_hydrated.autoPromote !== null && _hydrated.autoPromote !== undefined) {
    autoPromote.value = _hydrated.autoPromote;
  }
  if (_hydrated.agentModeAccepted !== null && _hydrated.agentModeAccepted !== undefined) {
    agentModeAccepted.value = _hydrated.agentModeAccepted;
  }
  if (
    _hydrated.selectedAgentSurface !== null &&
    _hydrated.selectedAgentSurface !== undefined
  ) {
    selectedAgentSurface.value = _hydrated.selectedAgentSurface;
  }

  // Throttled save. localStorage writes are sync + main-thread; coalescing
  // streaming chunk deltas into ~4 writes/sec keeps the cost negligible
  // during a long response. User-driven changes (Send, Clear, provider pick)
  // get a 0 ms delay so the next tick captures them — durable for any
  // refresh past one event-loop turn (~milliseconds). The flow_id is
  // resolved at write time (not at schedule time) so a swap that lands
  // between the schedule and the timer still writes under the right key —
  // and the swap watcher itself does its own atomic save so this path
  // doesn't need to coordinate with it.
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
        persistAiState(
          {
            messages: messages.value,
            selectedProvider: selectedProvider.value,
            selectedModel: selectedModel.value,
            autoPromote: autoPromote.value,
            agentModeAccepted: agentModeAccepted.value,
            selectedAgentSurface: selectedAgentSurface.value,
          },
          undefined,
          _scopedFlowId(flowStore.flowId),
        );
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
  watch(selectedAgentSurface, queuePersist, { flush: "sync" });
  watch(streamingState, queuePersist, { flush: "sync" });
  watch(autoPromote, queuePersist, { flush: "sync" });
  watch(agentModeAccepted, queuePersist, { flush: "sync" });

  // W43 — flow switch handler. Persist the *outgoing* flow's chat under its
  // own key BEFORE clearing `messages.value`, then load the *incoming*
  // flow's persisted state (or fresh defaults if none). `flush: "sync"`
  // mirrors the queuePersist watchers so the save lands before any
  // reactive consumer sees the swapped message array — round-tripping
  // A → B → A preserves A's history.
  //
  // Streaming consideration: an in-flight stream's `reactivePlaceholder`
  // is captured from the *outgoing* `messages.value` array. Reassigning
  // `messages.value` would orphan that proxy, and `streamingState` would
  // later flip to "idle" globally — clobbering any stream the user kicks
  // off on the incoming flow. `abortStream()` cuts the stream cleanly
  // before the swap so the outgoing snapshot includes the partial
  // assistant response (frozen at swap time, no spinner on rehydrate
  // because `sanitizeMessage` strips `pending`).
  watch(
    () => flowStore.flowId,
    (newId, oldId) => {
      const outgoing = _scopedFlowId(oldId);
      const incoming = _scopedFlowId(newId);
      if (outgoing === incoming) return;

      abortStream();

      persistAiState(
        {
          messages: messages.value,
          selectedProvider: selectedProvider.value,
          selectedModel: selectedModel.value,
          autoPromote: autoPromote.value,
          agentModeAccepted: agentModeAccepted.value,
          selectedAgentSurface: selectedAgentSurface.value,
        },
        undefined,
        outgoing,
      );

      const loaded = loadPersistedAiState(undefined, incoming);
      messages.value = loaded.messages;
      // Loaded provider/model take precedence ONLY when present; absent
      // values keep the user's current pick so a fresh-flow switch
      // doesn't reset the picker mid-session.
      if (loaded.selectedProvider !== null) {
        selectedProvider.value = loaded.selectedProvider;
      }
      if (loaded.selectedModel !== null) {
        selectedModel.value = loaded.selectedModel;
      }
      // autoPromote / agentModeAccepted DO reset to defaults on a fresh
      // flow — these are session preferences for the conversation, not
      // for the user's account. A flow with no prior chat shouldn't
      // inherit the previous flow's "continue as agent" lock.
      autoPromote.value = loaded.autoPromote ?? true;
      agentModeAccepted.value = loaded.agentModeAccepted ?? false;
      // W71 v1.9 — selectedAgentSurface is a per-flow session
      // preference (matches autoPromote semantics). Fall back to the
      // store's default when the flow has no persisted value.
      selectedAgentSurface.value = loaded.selectedAgentSurface ?? "agent_staged";
      promotionBanner.value = null;
      streamError.value = null;

      // Bump the module-scoped counter past any persisted ids on the
      // incoming flow's bucket so the next `nextMessageId()` call doesn't
      // collide with a hydrated message. Counter is monotonic across the
      // whole session — never reset — so messages from the outgoing flow
      // and the incoming flow can't share an id even briefly.
      const persistedMax = highestPersistedMessageId(loaded.messages);
      if (persistedMax > _messageCounter) {
        _messageCounter = persistedMax;
      }
    },
    { flush: "sync" },
  );

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
    promotionBanner.value = null;
    // W58 round 7 — clearing the chat resets the session-scoped accept;
    // a fresh chat shouldn't inherit the prior session's mode commitment.
    agentModeAccepted.value = false;
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

  const setSelectedAgentSurface = (surface: "agent_complex" | "agent_staged" | "agent_live"): void => {
    selectedAgentSurface.value = surface;
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

  // W58 — open a chat stream for the *current* tail of `messages` (no extra
  // user message push). Used by both ``sendMessage`` (after pushing the
  // user message itself) and ``undoPromotion`` (where the user message
  // is already in history from the original promoted send).
  const _openChatStream = async (): Promise<void> => {
    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return;
    }
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(assistantPlaceholder);
    // Re-read through the reactive Proxy so per-chunk mutations re-render.
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    // Filter out:
    //   - pending placeholders (stream still open)
    //   - empty-content messages (an assistant placeholder that failed
    //     mid-stream / aborted before any chunks arrived stays in history
    //     with content="" and pending=false; sending it triggers the
    //     backend's `min_length=1` validator and 422s the whole request).
    // Empty-content user messages can't happen (the composer trims + early
    // returns) but the same filter catches them defensively.
    const wireMessages: ChatMessageBody[] = messages.value
      .filter((m) => !m.pending && m.content.trim().length > 0)
      .map((m) => ({ role: m.role, content: m.content }));

    // W28 — pass the active flow_id so the backend can build a rich
    // PromptContext via W22 (subgraph + schemas). Omitted when no flow is
    // loaded yet → backend falls back to the W26 identity-only prompt.
    const flowStore = useFlowStore();
    const activeFlowId = flowStore.flowId ?? null;

    activeAbort = new AbortController();
    let sawErrorEvent = false;
    try {
      await streamChat(
        {
          provider: selectedProvider.value,
          model: selectedModel.value,
          messages: wireMessages,
          flow_id: activeFlowId,
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
            sawErrorEvent = true;
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
      } else if (!sawErrorEvent) {
        // If the server already delivered a structured `event: error` payload,
        // keep that message — a follow-up fetch reader failure (e.g. an
        // abrupt connection close after the error frame) would otherwise
        // clobber it with a generic "network error".
        reactivePlaceholder.error = message;
        streamingState.value = "error";
        streamError.value = message;
      }
    } finally {
      activeAbort = null;
    }
  };

  // W58 — dispatch a freshly-promoted agent run. The caller (``sendMessage``)
  // is responsible for pushing the user message into ``messages`` *before*
  // invoking this helper — that's how round-6's optimistic-push UX works,
  // and skipping the push here also means the message can't accidentally
  // duplicate when the caller already did it.
  //
  // Round-5: the agent's ``prompt`` is *enriched* with the recent chat
  // transcript (so the planner knows what to implement, e.g. when the user
  // says *"Can you implement?"* after a chat turn that proposed concrete
  // nodes). Per-turn payload is capped at 2 K chars to bound the first-call
  // token cost.
  const _dispatchPromotedAgent = async (text: string, reason: string): Promise<void> => {
    const flowStore = useFlowStore();
    const flowId = flowStore.flowId;
    if (flowId === null) {
      // Defensive — sendMessage gates on flowId before classifying, but we
      // double-check so undoPromotion's fall-through logic is sound.
      streamError.value = "Open a flow before starting the agent.";
      streamingState.value = "error";
      return;
    }

    const enrichedPrompt = _buildPromotedAgentPrompt(text);
    promotionBanner.value = { reason, message: text };

    const agentStore = useAiAgentStore();
    await agentStore.start({
      flow_id: flowId,
      prompt: enrichedPrompt,
      // W71 v1.9 — auto-promote-from-chat (W58) honors the user's
      // selected agent surface (defaults to ``agent_staged``). The
      // direct agent-mode toggle in ``AiAssistant.vue`` reads the same
      // ref so both dispatch paths stay consistent.
      surface: selectedAgentSurface.value,
      provider: selectedProvider.value ?? "anthropic",
      model: selectedModel.value ?? null,
    });
  };

  // W58 round 5 — assemble the enriched agent prompt. Returns ``text``
  // verbatim when there's no prior chat history (first message of a
  // session — the agent doesn't need a transcript header for a single
  // turn). When there is prior chat, frames it as an explicit transcript
  // followed by the current confirmation so the planner reads it as
  // *"context, then instruction"* rather than as a continuous user message.
  //
  // Round-6 nuance: the latest user message has been pushed optimistically
  // by ``sendMessage`` before classification, so the trailing entry in
  // ``messages`` is *the same* as ``text``. Strip it from the transcript
  // so the prompt doesn't render *"User: Can you implement?"* in the body
  // *and* repeat it in the *"User's latest message"* tail.
  const _PROMOTED_AGENT_TURN_CAP = 2_000;
  const _PROMOTED_AGENT_HISTORY_TURNS = 4;
  const _buildPromotedAgentPrompt = (text: string): string => {
    const visible = messages.value.filter((m) => !m.pending && m.content.trim().length > 0);
    const trailing = visible[visible.length - 1];
    const transcriptSource =
      trailing !== undefined && trailing.role === "user" && trailing.content === text
        ? visible.slice(0, -1)
        : visible;
    const recent = transcriptSource.slice(-_PROMOTED_AGENT_HISTORY_TURNS);
    if (recent.length === 0) return text;
    const transcript = recent
      .map((m) => {
        const trimmed =
          m.content.length > _PROMOTED_AGENT_TURN_CAP
            ? m.content.slice(0, _PROMOTED_AGENT_TURN_CAP - 3).trimEnd() + "..."
            : m.content;
        const label = m.role === "user" ? "User" : "Assistant";
        return `${label}: ${trimmed}`;
      })
      .join("\n\n");
    return [
      "The user previously had this chat conversation in the AI drawer (the chat",
      "assistant operates read-only and only suggests; you are the agent that",
      "actually applies changes):",
      "",
      transcript,
      "",
      "---",
      "",
      `User's latest message: ${text}`,
      "",
      "Use the conversation above as the context for what to build.",
    ].join("\n");
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
    // Each new send wipes any leftover promotion banner from a prior turn
    // — the banner is contextual to the in-flight run, not the chat.
    promotionBanner.value = null;

    // W58 round 6 — capture the chat history for the classifier *before*
    // we push the new user message so the classifier sees only the prior
    // turns. Then push the message *immediately* so it appears in the
    // chat trail while the classifier round-trip is in flight (~800 ms
    // p50 / ~1500 ms p95). Without optimistic push the user sees their
    // input vanish during the routing await — feels like a UI glitch.
    //
    // Round 7: the ``agentModeAccepted`` short-circuit takes precedence
    // over ``autoPromote``. The user clicked "Continue as agent" on a
    // prior promotion banner, so every subsequent send in this session
    // skips classification and dispatches as agent directly.
    const flowStore = useFlowStore();
    const wantsClassification =
      !agentModeAccepted.value && autoPromote.value && flowStore.flowId !== null;
    // ``ChatRole`` is already ``"user" | "assistant"`` (no "system" — those
    // come from the server, never the chat trail), so the cast to
    // ``RouteHistoryEntry`` is shape-compatible without a runtime guard.
    const classifierHistory: RouteHistoryEntry[] = wantsClassification
      ? messages.value
          .filter((m) => !m.pending && m.content.trim().length > 0)
          .slice(-4)
          .map((m) => ({ role: m.role, content: m.content }))
      : [];

    const userMessage: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "user",
      content: text,
    };
    messages.value.push(userMessage);

    // W58 round 7 — once accepted, every send goes straight to the agent
    // without re-classifying. We still gate on a flow being loaded; otherwise
    // dispatching the agent would surface a confusing "Open a flow first"
    // error from ``_dispatchPromotedAgent`` for what looks like a normal chat.
    if (agentModeAccepted.value && flowStore.flowId !== null) {
      await _dispatchPromotedAgent(text, "session set to continue as agent");
      return;
    }

    if (wantsClassification) {
      // The classifier is fail-quiet (any timeout / parse error / provider
      // hiccup collapses to verdict "chat" server-side) so a transient
      // backend failure can't block the chat dispatch — the catch below
      // handles outright network errors the same way.
      try {
        const result = await routeMessage({
          message: text,
          provider: selectedProvider.value,
          model: selectedModel.value,
          history: classifierHistory.length > 0 ? classifierHistory : undefined,
        });
        if (result.verdict === "agent") {
          await _dispatchPromotedAgent(text, result.reason);
          return;
        }
      } catch (err) {
        // Network error / 401 / 503: silently fall through to chat dispatch.
        // The user gets their message answered as chat — better than a
        // cryptic toast about a routing decision they didn't ask for.
        console.warn("ai-store: /ai/route failed, falling back to chat", err);
      }
    }

    await _openChatStream();
  };

  const setAutoPromote = (value: boolean): void => {
    autoPromote.value = value;
  };

  const dismissPromotionBanner = (): void => {
    promotionBanner.value = null;
  };

  const undoPromotion = async (): Promise<void> => {
    // W58 — banner "Click here to keep this as chat instead" affordance.
    // Aborts the in-flight agent run, flips the session-scoped autoPromote
    // off (so a follow-up Send doesn't re-classify the same way), then
    // re-dispatches the saved message as a regular chat. The user message
    // is already in ``messages`` from the original promoted send — we
    // skip pushing it again to keep the chat trail readable.
    //
    // Round 7: also reset ``agentModeAccepted`` since "back to chat" and
    // "continue as agent" are mutually exclusive — leaving the accept on
    // would force the next send back to agent and erase the undo.
    const banner = promotionBanner.value;
    if (banner === null) return;
    promotionBanner.value = null;
    autoPromote.value = false;
    agentModeAccepted.value = false;

    try {
      const agentStore = useAiAgentStore();
      await agentStore.abort();
    } catch (err) {
      console.warn("ai-store: failed to abort agent during undoPromotion", err);
    }

    await _openChatStream();
  };

  const acceptPromotion = (): void => {
    // W58 round 7 — banner "Continue as agent" affordance. The user has
    // confirmed they want subsequent sends to go directly to the agent.
    // Flip the session-scoped flag so ``sendMessage`` skips
    // ``routeMessage`` going forward; clear the banner since its job is
    // done. We do NOT abort the in-flight agent run — accepting means
    // *"keep going"*, not *"start over"*.
    if (promotionBanner.value === null) return;
    agentModeAccepted.value = true;
    promotionBanner.value = null;
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
      createdAt: Date.now(),
      role: "user",
      content: userVisibleText,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
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
    let sawErrorEvent = false;
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
            sawErrorEvent = true;
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
      } else if (!sawErrorEvent) {
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
      createdAt: Date.now(),
      role: "user",
      content: userVisibleText,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(userMessage, assistantPlaceholder);
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    activeAbort = new AbortController();
    let sawErrorEvent = false;
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
            sawErrorEvent = true;
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
      } else if (!sawErrorEvent) {
        reactivePlaceholder.error = message;
        streamingState.value = "error";
        streamError.value = message;
      }
    } finally {
      activeAbort = null;
    }
  };

  const runInlineAction = async (
    flowId: number,
    nodeId: number,
    action: InlineActionType,
    nodeName?: string,
  ): Promise<void> => {
    // W21 — Inline ✨ menu entry point. Same shape as ``explainRunFailure``
    // / ``generateDocumentation``: opens the drawer, drops a synthetic
    // user/assistant pair into the chat, then streams the server-built
    // response into the assistant placeholder. The wire-level user
    // message is composed by the backend via W22's render_prompt_context
    // (surface="explain") + the W21 ``## Action`` block; the chat-visible
    // synthetic turn is purely cosmetic so the user has a visual anchor
    // for what they just asked.
    openAiDrawer();

    if (streamingState.value === "streaming") {
      // Don't trample an in-flight request.
      return;
    }
    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return;
    }

    const headline = nodeName ? `\`${nodeName}\`` : `node ${nodeId}`;
    const userVisibleText = inlineActionUserPrompt(action, headline);

    const userMessage: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "user",
      content: userVisibleText,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(userMessage, assistantPlaceholder);
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    activeAbort = new AbortController();
    let sawErrorEvent = false;
    try {
      await streamInlineAction(
        {
          flow_id: flowId,
          node_id: nodeId,
          action,
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
            sawErrorEvent = true;
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
      } else if (!sawErrorEvent) {
        reactivePlaceholder.error = message;
        streamingState.value = "error";
        streamError.value = message;
      }
    } finally {
      activeAbort = null;
    }
  };

  const askLineageQuestion = async (
    flowId: number,
    question: string,
    focusNodeId?: number,
  ): Promise<void> => {
    // W51 — "Ask about lineage" entry point. Same shape as
    // ``generateDocumentation`` / ``runInlineAction``: opens the drawer,
    // drops a synthetic user/assistant pair into the chat, then streams
    // the server-built lineage answer into the assistant placeholder.
    // The wire-level user message is composed by the backend via W22's
    // render_prompt_context (surface="lineage") + the W51 ``## Run
    // history`` + ``## Question`` blocks; the chat-visible synthetic
    // turn is purely cosmetic.
    openAiDrawer();

    if (streamingState.value === "streaming") {
      return;
    }
    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return;
    }

    const trimmedQuestion = question.trim();
    if (!trimmedQuestion) {
      streamError.value = "Type a lineage question first.";
      streamingState.value = "error";
      return;
    }

    const userVisibleText = `[Lineage] ${trimmedQuestion}`;
    const userMessage: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "user",
      content: userVisibleText,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(userMessage, assistantPlaceholder);
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    activeAbort = new AbortController();
    let sawErrorEvent = false;
    try {
      await streamLineageQuestion(
        {
          flow_id: flowId,
          question: trimmedQuestion,
          provider: selectedProvider.value,
          model: selectedModel.value,
          focus_node_id: focusNodeId ?? null,
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
            sawErrorEvent = true;
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
      } else if (!sawErrorEvent) {
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
    autoPromote,
    agentModeAccepted,
    promotionBanner,
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
    selectedAgentSurface,
    setSelectedAgentSurface,
    abortStream,
    sendMessage,
    explainRunFailure,
    generateDocumentation,
    runInlineAction,
    askLineageQuestion,
    setAutoPromote,
    dismissPromotionBanner,
    undoPromotion,
    acceptPromotion,
    // 2026-05-07 — public re-export of ``_buildPromotedAgentPrompt`` so
    // ``AiAssistant.vue``'s manual-Agent-toggle path can fold in chat
    // history the same way auto-promotion does. Without this, flipping
    // the explicit Agent toggle and typing *"Implement the plan"* sent
    // a context-less prompt to the planner, which then asked *"I'm not
    // sure which specific plan you'd like me to implement?"* — the
    // prior chat assistant had laid out the plan but the agent never
    // saw it.
    buildAgentPromptWithHistory: _buildPromotedAgentPrompt,
  };
});

const inlineActionUserPrompt = (action: InlineActionType, headline: string): string => {
  // Cosmetic-only — the wire-level prompt is built server-side by W22.
  // Keep these short and human-readable so the chat shows a clean trace
  // of what the user clicked.
  switch (action) {
    case "explain":
      return `Explain ${headline}.`;
    case "optimise":
      return `Suggest optimisations for ${headline}.`;
    case "document":
      return `Write a description for ${headline}.`;
    case "regenerate_code":
      return `Regenerate the code in ${headline}.`;
    case "suggest_filters":
      return `Suggest filters for ${headline}.`;
  }
};
