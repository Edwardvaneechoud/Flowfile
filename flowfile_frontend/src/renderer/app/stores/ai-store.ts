// AI Store — chat-drawer state for the read-only chat surface.
//
// Only owns *what's currently being talked about* — the open/close flag
// for the drawer is mirrored from `editor-store.ts` so other panels can
// participate in `hideAllPanels()` without coupling to this module. The
// chat payload + streaming state live here.
//
// Persistence is per-flow + browser-local. Each flow's chat trail lives
// under `flowfile.ai.chat.v1.{flow_id}` in `localStorage` so switching
// flows shows the right conversation, and chat survives Electron app
// restart. The `flowStore.flowId` watcher below performs an atomic
// save-then-load swap on flow switch so round-tripping A → B → A
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
import { parseMentions } from "../features/ai/mentionVocabulary";
import type { AiProvider } from "../views/AiProvidersView/aiProviderTypes";
import { useAiAgentStore } from "./ai-agent-store";
import {
  highestPersistedMessageId,
  loadPersistedAiState,
  persistAiState,
} from "./ai-store-persistence";
import { useEditorStore } from "./editor-store";
import { useFlowStore } from "./flow-store";
import { useNodeStore } from "./node-store";

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

/** Unified send-mode enum that drives the drawer's Send dropdown.
 *   - ``"chat"`` — read-only chat. Send dispatches the chat path; the
 *     classifier never auto-promotes regardless of intent.
 *   - ``"auto"`` (default) — Send dispatches chat by default; the
 *     classifier auto-promotes to agent when an editing intent is
 *     detected.
 *   - ``"agent"`` — Send always dispatches the agent path with the
 *     configured surface. Sticky across sends instead of per-send. */
export type AiMode = "chat" | "auto" | "agent";

const _AI_MODES: ReadonlyArray<AiMode> = ["chat", "auto", "agent"];

/** Per-tab sessionStorage key for the user's send-mode preference.
 * Survives page refresh; resets fresh on tab close. Mirrors
 * ``flow-store.ts``'s sessionStorage idiom. */
const MODE_STORAGE_KEY = "flowfile.ai.mode";

const _readPersistedMode = (): AiMode | null => {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.sessionStorage.getItem(MODE_STORAGE_KEY);
    if (raw !== null && (_AI_MODES as ReadonlyArray<string>).includes(raw)) {
      return raw as AiMode;
    }
  } catch {
    // sessionStorage disabled / private mode — fall through to default.
  }
  return null;
};

const _writePersistedMode = (value: AiMode): void => {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(MODE_STORAGE_KEY, value);
  } catch {
    // Ignore — quota exceeded / storage disabled. The mode still works
    // in-memory; the user just loses refresh-survival.
  }
};

/** Resolve the initial mode at store-init time. Priority: sessionStorage
 * (fresh per tab), then legacy ``autoPromote`` migration shim from
 * localStorage (true / null → "auto", false → "chat"), then default. */
const _seedMode = (legacyAutoPromote: boolean | null | undefined): AiMode => {
  const persisted = _readPersistedMode();
  if (persisted !== null) return persisted;
  if (legacyAutoPromote === false) return "chat";
  return "auto";
};

let _messageCounter = 0;
const nextMessageId = (): number => {
  _messageCounter += 1;
  return _messageCounter;
};

// Browser-side persistence. Throttled writes coalesce streaming chunk
// deltas into ~4 saves/sec instead of one per token. Persistence is
// per-flow (`localStorage` keyed by flow_id) so switching flows shows
// the right conversation and chat survives Electron restart.
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

  // ----- providers -----
  const providers = ref<AiProvider[]>([]);
  const providersLoading = ref(false);
  const providersError = ref<string | null>(null);

  const selectedProvider = ref<string | null>(null);
  const selectedModel = ref<string | null>(null);

  // User-selectable agent surface. Defaults to ``agent_live`` so the
  // REPL-style canvas-mutating surface is what new sessions get; users
  // can switch to ``agent_staged`` (multi-stage planner) or
  // ``agent_complex`` (single-shot full catalog) via the settings
  // popover. Persisted alongside the other AI selections via
  // ``ai-store-persistence``.
  const selectedAgentSurface = ref<"agent_complex" | "agent_staged" | "agent_live">("agent_live");

  // Opt-in verify-completion gate. When true, after classify picks
  // ``op_kind="other"`` the agent runs one extra LLM round at
  // ``stage="verify_completion"`` to walk the plan as a checklist
  // before the loop terminates. Default off (extra round per run); the
  // user opts in via the agent settings panel checkbox. Persisted
  // per-flow alongside ``selectedAgentSurface``.
  const verifyPlanCompletion = ref<boolean>(false);

  // ----- messages -----
  const messages = ref<ChatMessage[]>([]);

  // ----- stream lifecycle -----
  const streamingState = ref<StreamingState>("idle");
  const streamError = ref<string | null>(null);
  let activeAbort: AbortController | null = null;

  // Track the IMMEDIATELY previous interaction kind so
  // ``_dispatchPromotedAgent`` can decide whether to skip the plan
  // stage. The naive "any assistant message in history" check is too
  // coarse — old chat replies remain in history after subsequent agent
  // runs, so the skip-plan path would fire even when the agent had
  // been the most recent activity. The required behaviour: previous
  // action was a chat reply → skip plan (chat already produced
  // reasoning); previous action was an agent run → run plan (no fresh
  // chat reasoning since). This ref captures that ordering directly.
  // Set to ``"chat"`` when the streaming chat reply finishes; set to
  // ``"agent"`` when the agent run completes (via the status-watcher
  // in ``ai-agent-store.ts``); ``null`` only at session start before
  // either has fired.
  const lastInteractionKind = ref<"chat" | "agent" | null>(null);

  // ----- Unified send-mode -----
  // ``mode`` is the single source of truth for "what does Send do?".
  // See ``AiMode`` for semantics. Persisted to sessionStorage (per-tab,
  // survives refresh, resets on tab close). Migrated from any legacy
  // ``autoPromote`` value found in the localStorage chat blob on first
  // hydrate.
  //
  // ``agentModeAccepted`` is the post-promotion accept flag: when the
  // (currently hidden) promotion banner's "Continue as agent" button
  // is clicked, this flips to ``true`` and short-circuits
  // classification on subsequent sends so every future message goes
  // straight to the agent. Cleared by ``undoPromotion`` and
  // ``clearMessages``.
  // Initialised to the default; the actual seed runs in the hydration
  // block below once ``_hydrated`` is available. Declaring it here
  // (rather than after `_hydrated`) so it stays grouped with the other
  // top-level state refs.
  const mode = ref<AiMode>("auto");
  const agentModeAccepted = ref<boolean>(false);
  const promotionBanner = ref<{ reason: string; message: string } | null>(null);

  // ----- Hydrate from localStorage under the active flow's key -----
  // Order matters: hydrate refs BEFORE wiring the watch so the initial
  // assignment doesn't trigger a redundant write of what we just read.
  // Flow id is read from the flow store at hydration time so the right
  // per-flow bucket loads. When no flow is loaded yet (flow store
  // sentinel `-1`) we fall through to the `unscoped` bucket which
  // round-trips correctly once the user later opens a flow (the
  // watcher saves the unscoped state under that key before swapping to
  // the new flow's state).
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
  // Seed ``mode`` from sessionStorage (per-tab, fresh on tab close);
  // fall back to the legacy ``autoPromote`` from the localStorage blob
  // when no sessionStorage value is present (migration shim).
  mode.value = _seedMode(_hydrated.autoPromote);
  if (_hydrated.agentModeAccepted !== null && _hydrated.agentModeAccepted !== undefined) {
    agentModeAccepted.value = _hydrated.agentModeAccepted;
  }
  if (_hydrated.selectedAgentSurface !== null && _hydrated.selectedAgentSurface !== undefined) {
    selectedAgentSurface.value = _hydrated.selectedAgentSurface;
  }
  if (_hydrated.verifyPlanCompletion !== null && _hydrated.verifyPlanCompletion !== undefined) {
    verifyPlanCompletion.value = _hydrated.verifyPlanCompletion;
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
            agentModeAccepted: agentModeAccepted.value,
            selectedAgentSurface: selectedAgentSurface.value,
            verifyPlanCompletion: verifyPlanCompletion.value,
          },
          undefined,
          _scopedFlowId(flowStore.flowId),
        );
      },
      isStreaming ? PERSIST_THROTTLE_MS : 0,
    );
  };

  // `mode` lives in sessionStorage, not the localStorage chat blob,
  // so it gets its own writer. ``immediate: true`` writes the seeded
  // value once at store init so subsequent reads (other tabs / dev
  // tools) see the live value.
  watch(mode, (value) => _writePersistedMode(value), { immediate: true });

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
  watch(verifyPlanCompletion, queuePersist, { flush: "sync" });
  watch(streamingState, queuePersist, { flush: "sync" });
  watch(agentModeAccepted, queuePersist, { flush: "sync" });

  // Flow switch handler. Persist the *outgoing* flow's chat under its
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
          agentModeAccepted: agentModeAccepted.value,
          selectedAgentSurface: selectedAgentSurface.value,
          verifyPlanCompletion: verifyPlanCompletion.value,
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
      // ``mode`` is session-global (sessionStorage), NOT per-flow, so
      // a flow switch doesn't touch it. Only ``agentModeAccepted``
      // resets per-flow ("Continue as agent" is a
      // conversation-scoped commitment, not a session-scoped one).
      agentModeAccepted.value = loaded.agentModeAccepted ?? false;
      // selectedAgentSurface is a per-flow session preference. Fall
      // back to the store's default when the flow has no persisted
      // value.
      selectedAgentSurface.value = loaded.selectedAgentSurface ?? "agent_live";
      // verifyPlanCompletion is per-flow alongside the surface picker.
      // Falls back to off (default) when the incoming flow has no
      // persisted value.
      verifyPlanCompletion.value = loaded.verifyPlanCompletion ?? false;
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
    // Clearing the chat resets the session-scoped accept; a fresh chat
    // shouldn't inherit the prior session's mode commitment.
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

  const setSelectedAgentSurface = (
    surface: "agent_complex" | "agent_staged" | "agent_live",
  ): void => {
    selectedAgentSurface.value = surface;
  };

  // Opt-in verify-completion mode setter. Mirror of the surface setter
  // so the AiAssistant.vue checkbox flips the flag through the same
  // store-action pattern as the other agent settings.
  const setVerifyPlanCompletion = (value: boolean): void => {
    verifyPlanCompletion.value = value;
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

  // Shared streaming pipeline for the read-only chat surfaces
  // (run-failure, docgen, inline action, lineage). Each surface follows
  // the same pattern: open drawer, push synthetic user + assistant
  // placeholder, set streaming/abort state, dispatch the surface's SSE
  // call, drain into the placeholder, settle on done/error/abort.
  // Surface-specific bits flow in through ``streamFn`` (which body to
  // POST + which endpoint) and ``onSuccess`` (post-stream side effect,
  // e.g. write back to nodeStore for ``add_description``).
  //
  // Why a helper instead of separate composables: each surface needs
  // access to closure-private state (``activeAbort``, ``nextMessageId``)
  // that is intentionally not part of the store's public surface.
  // Pulling the pattern up here keeps a single source of truth for the
  // streaming/abort lifecycle.
  interface StreamedSurfaceOptions {
    userVisibleText: string;
    streamFn: (handlers: {
      onChunk: (delta: string) => void;
      onDone: () => void;
      onError: (message: string) => void;
    }, signal: AbortSignal) => Promise<void>;
    /** Optional post-success side effect; receives the final assistant text. */
    onSuccess?: (finalContent: string) => void;
  }

  const _runStreamedChatSurface = async (opts: StreamedSurfaceOptions): Promise<void> => {
    openAiDrawer();

    if (streamingState.value === "streaming") {
      // Don't trample an in-flight stream. The button click counts as
      // user intent to open the drawer; the streaming guard mirrors
      // ``sendMessage``.
      return;
    }
    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return;
    }

    const userMessage: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "user",
      content: opts.userVisibleText,
    };
    const assistantPlaceholder: ChatMessage = {
      id: nextMessageId(),
      createdAt: Date.now(),
      role: "assistant",
      content: "",
      pending: true,
    };
    messages.value.push(userMessage, assistantPlaceholder);
    // Re-read through the reactive Proxy so per-chunk mutations re-render.
    const reactivePlaceholder = messages.value[messages.value.length - 1];

    streamingState.value = "streaming";
    streamError.value = null;

    activeAbort = new AbortController();
    let sawErrorEvent = false;
    try {
      await opts.streamFn(
        {
          onChunk: (delta) => {
            reactivePlaceholder.content += delta;
          },
          onDone: () => {
            reactivePlaceholder.pending = false;
            streamingState.value = "idle";
            if (opts.onSuccess) {
              opts.onSuccess(reactivePlaceholder.content);
            }
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

  // Open a chat stream for the *current* tail of `messages` (no extra
  // user message push). Used by both ``sendMessage`` (after pushing
  // the user message itself) and ``undoPromotion`` (where the user
  // message is already in history from the original promoted send).
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

    // Pass the active flow_id so the backend can build a rich
    // PromptContext (subgraph + schemas). Omitted when no flow is
    // loaded yet → backend falls back to the identity-only prompt.
    const flowStore = useFlowStore();
    const activeFlowId = flowStore.flowId ?? null;

    // Forward the user's `@`-mentions and the canvas selection so the
    // backend's `render_prompt_context` pins the right subgraph
    // instead of silently defaulting to `@flow`. Mirrors the agent
    // path's `_readCanvasSelection` in ai-agent-store.ts.
    let lastUserText = "";
    for (let i = messages.value.length - 1; i >= 0; i -= 1) {
      if (messages.value[i].role === "user") {
        lastUserText = messages.value[i].content;
        break;
      }
    }
    const parsedMentions = parseMentions(lastUserText);

    const vfInstance = flowStore.vueFlowInstance;
    const selectedRefs = vfInstance?.getSelectedNodes?.value ?? [];
    type SelNode = { id: string; data?: { id?: number | string } };
    const selectedNodeIds = (selectedRefs as SelNode[])
      .map((n) => Number(n.data?.id ?? n.id))
      .filter((id) => Number.isFinite(id));

    activeAbort = new AbortController();
    let sawErrorEvent = false;
    try {
      await streamChat(
        {
          provider: selectedProvider.value,
          model: selectedModel.value,
          messages: wireMessages,
          flow_id: activeFlowId,
          selected_node_ids: selectedNodeIds.length > 0 ? selectedNodeIds : null,
          mentions: parsedMentions.length > 0 ? parsedMentions : null,
          chat_mode: mode.value === "chat" ? "chat" : "auto_agent",
        },
        {
          onChunk: (delta) => {
            reactivePlaceholder.content += delta;
          },
          onDone: () => {
            reactivePlaceholder.pending = false;
            streamingState.value = "idle";
            // Chat reply finished. Mark this as the most-recent
            // interaction so the next ``_dispatchPromotedAgent`` call
            // can skip the plan stage (the chat reply IS the
            // plan-equivalent). The status-watcher in ai-agent-store
            // flips this back to ``"agent"`` whenever an agent run
            // completes — which is what makes the per-turn ordering
            // work correctly across mixed chat/agent sessions.
            lastInteractionKind.value = "chat";
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

  // Dispatch a freshly-promoted agent run. The caller
  // (``sendMessage``) is responsible for pushing the user message into
  // ``messages`` *before* invoking this helper — that's how the
  // optimistic-push UX works, and skipping the push here also means
  // the message can't accidentally duplicate when the caller already
  // did it.
  //
  // The agent's ``prompt`` is *enriched* with the recent chat
  // transcript (so the planner knows what to implement, e.g. when the
  // user says *"Can you implement?"* after a chat turn that proposed
  // concrete nodes). Per-turn payload is capped at 2 K chars to bound
  // the first-call token cost.
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

    // Skip the plan stage ONLY when the IMMEDIATELY-PREVIOUS
    // interaction was a chat reply (i.e. there's fresh chat-mode
    // reasoning the agent can lean on). A coarse "any assistant
    // message in history" check would misfire in the common pattern:
    // chat → "yes implement" → agent runs → user types follow-up.
    // The old chat reply is still in messages, so the coarse check
    // would return true and skip_plan would fire even though the most
    // recent activity was the AGENT run (which leaves no chat
    // reasoning in its wake) — making the agent jump straight to
    // modify ops on a follow-up without re-planning.
    //
    // ``lastInteractionKind`` is the per-turn signal:
    //   - ``"chat"``  → chat assistant reply just landed (skip plan)
    //   - ``"agent"`` → agent run just completed (run plan)
    //   - ``null``    → first message of session (run plan)
    const skipPlan = lastInteractionKind.value === "chat";

    const agentStore = useAiAgentStore();
    await agentStore.start({
      flow_id: flowId,
      prompt: enrichedPrompt,
      // Auto-promote-from-chat honors the user's selected agent
      // surface (defaults to ``agent_live``). The direct agent-mode
      // toggle in ``AiAssistant.vue`` reads the same ref so both
      // dispatch paths stay consistent.
      surface: selectedAgentSurface.value,
      provider: selectedProvider.value ?? "anthropic",
      model: selectedModel.value ?? null,
      skip_plan: skipPlan,
      verify_plan_completion: verifyPlanCompletion.value,
    });
  };

  // Assemble the enriched agent prompt. Returns ``text`` verbatim when
  // there's no prior chat history (first message of a session — the
  // agent doesn't need a transcript header for a single turn). When
  // there is prior chat, frames it as an explicit transcript followed
  // by the current confirmation so the planner reads it as
  // *"context, then instruction"* rather than as a continuous user
  // message.
  //
  // The latest user message has been pushed optimistically by
  // ``sendMessage`` before classification, so the trailing entry in
  // ``messages`` is *the same* as ``text``. Strip it from the
  // transcript so the prompt doesn't render *"User: Can you implement?"*
  // in the body *and* repeat it in the *"User's latest message"* tail.
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

    // Capture the chat history for the classifier *before* we push
    // the new user message so the classifier sees only the prior
    // turns. Then push the message *immediately* so it appears in the
    // chat trail while the classifier round-trip is in flight (~800 ms
    // p50 / ~1500 ms p95). Without optimistic push the user sees their
    // input vanish during the routing await — feels like a UI glitch.
    //
    // The ``agentModeAccepted`` short-circuit takes precedence over
    // the user's ``mode`` choice. If the user clicked "Continue as
    // agent" on a prior promotion banner, every subsequent send in
    // this session skips classification and dispatches as agent
    // directly. Otherwise: classification only happens in ``"auto"``
    // mode — ``"chat"`` skips it (force-chat) and ``"agent"`` bypasses
    // it (force-agent, handled by AiAssistant.vue's handleSend before
    // this store action runs).
    const flowStore = useFlowStore();
    const wantsClassification =
      !agentModeAccepted.value && mode.value === "auto" && flowStore.flowId !== null;
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

    // Once accepted, every send goes straight to the agent without
    // re-classifying. We still gate on a flow being loaded; otherwise
    // dispatching the agent would surface a confusing "Open a flow
    // first" error from ``_dispatchPromotedAgent`` for what looks
    // like a normal chat.
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

  const setMode = (value: AiMode): void => {
    if (!(_AI_MODES as ReadonlyArray<string>).includes(value)) return;
    mode.value = value;
  };

  const dismissPromotionBanner = (): void => {
    promotionBanner.value = null;
  };

  const undoPromotion = async (): Promise<void> => {
    // Banner "Click here to keep this as chat instead" affordance.
    // Aborts the in-flight agent run, flips the session mode to
    // ``"chat"`` (so a follow-up Send doesn't re-classify the same
    // way), then re-dispatches the saved message as a regular chat.
    // The user message is already in ``messages`` from the original
    // promoted send — we skip pushing it again to keep the chat trail
    // readable.
    //
    // Also reset ``agentModeAccepted`` since "back to chat" and
    // "continue as agent" are mutually exclusive — leaving the accept
    // on would force the next send back to agent and erase the undo.
    const banner = promotionBanner.value;
    if (banner === null) return;
    promotionBanner.value = null;
    mode.value = "chat";
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
    // Banner "Continue as agent" affordance. The user has confirmed
    // they want subsequent sends to go directly to the agent. Flip
    // the session-scoped flag so ``sendMessage`` skips ``routeMessage``
    // going forward; clear the banner since its job is done. We do NOT
    // abort the in-flight agent run — accepting means *"keep going"*,
    // not *"start over"*.
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
    // "Fix with AI" entry point. Drops a synthetic user/assistant pair
    // into the chat then streams the server-built failure explanation
    // into the assistant placeholder. The wire-level user message is
    // composed by the backend via ``render_prompt_context``; the
    // chat-visible synthetic turn is purely cosmetic so the user has a
    // visual anchor for what they just asked.
    const trimmedError = errorMessage.trim();
    const headline = nodeName ? `node "${nodeName}"` : `node ${nodeId}`;
    const userVisibleText = trimmedError
      ? `Help me fix this error in ${headline}:\n\n${trimmedError}`
      : `Help me fix this error in ${headline}.`;

    await _runStreamedChatSurface({
      userVisibleText,
      streamFn: (handlers, signal) =>
        streamRunFailureExplanation(
          {
            flow_id: flowId,
            node_id: nodeId,
            provider: selectedProvider.value!,
            model: selectedModel.value,
            error_message: trimmedError || null,
          },
          handlers,
          signal,
        ),
    });
  };

  const generateDocumentation = async (flowId: number, flowName?: string): Promise<void> => {
    // "Generate documentation" entry point. The wire-level user
    // message is composed by the backend via ``render_prompt_context``
    // (surface="docgen") + the ``## Documentation request`` block; the
    // chat-visible synthetic turn is purely cosmetic.
    const trimmedName = flowName?.trim();
    const headline = trimmedName ? `\`${trimmedName}\`` : `flow ${flowId}`;
    const userVisibleText = `Generate documentation for ${headline}.`;

    await _runStreamedChatSurface({
      userVisibleText,
      streamFn: (handlers, signal) =>
        streamGenerateDocumentation(
          {
            flow_id: flowId,
            provider: selectedProvider.value!,
            model: selectedModel.value,
          },
          handlers,
          signal,
        ),
    });
  };

  const runInlineAction = async (
    flowId: number,
    nodeId: number,
    action: InlineActionType,
    nodeName?: string,
  ): Promise<void> => {
    // Inline ✨ menu entry point. The wire-level user message is composed
    // by the backend via ``render_prompt_context`` (surface="explain") +
    // the ``## Action`` block; the chat-visible synthetic turn is purely
    // cosmetic. ``add_description`` action additionally writes the
    // generated text back to the node's description field via nodeStore
    // — handled in the ``onSuccess`` post-stream hook.
    const headline = nodeName ? `\`${nodeName}\`` : `node ${nodeId}`;
    const userVisibleText = inlineActionUserPrompt(action, headline);

    await _runStreamedChatSurface({
      userVisibleText,
      streamFn: (handlers, signal) =>
        streamInlineAction(
          {
            flow_id: flowId,
            node_id: nodeId,
            action,
            provider: selectedProvider.value!,
            model: selectedModel.value,
          },
          handlers,
          signal,
        ),
      onSuccess: (finalContent) => {
        if (action === "add_description") {
          const generated = finalContent.trim();
          if (generated) {
            useNodeStore()
              .setNodeDescription(nodeId, generated)
              .catch(() => {
                // node-store already logs; the streamed text remains
                // visible in the chat so the user can copy-paste as a fallback.
              });
          }
        }
      },
    });
  };

  const runBulkAddDescriptions = async (
    flowId: number,
    nodeIds: readonly number[],
  ): Promise<{ succeeded: number; failed: number; aborted: boolean }> => {
    // Canvas right-click "Add description to all nodes" entry point.
    // Quiet bulk variant of runInlineAction: streams the add_description
    // action sequentially per node WITHOUT pushing synthetic user/assistant
    // pairs into the chat drawer (would flood it with N message pairs for
    // N nodes). Each completed stream is written to the node's description
    // field via nodeStore.setNodeDescription, the same path the per-node
    // ✨ menu uses.
    let succeeded = 0;
    let failed = 0;
    let aborted = false;

    if (selectedProvider.value === null) {
      streamError.value = "Pick a provider first.";
      streamingState.value = "error";
      return { succeeded, failed: nodeIds.length, aborted: false };
    }

    const nodeStoreRef = useNodeStore();

    for (const nodeId of nodeIds) {
      if (streamingState.value === "streaming") {
        // Defensive: another stream slipped in. Bail rather than trample.
        break;
      }

      let buffer = "";
      let stopped = false;
      let iterationAborted = false;

      activeAbort = new AbortController();
      streamingState.value = "streaming";
      streamError.value = null;

      try {
        await streamInlineAction(
          {
            flow_id: flowId,
            node_id: nodeId,
            action: "add_description",
            provider: selectedProvider.value,
            model: selectedModel.value,
          },
          {
            onChunk: (delta) => {
              buffer += delta;
            },
            onError: () => {
              stopped = true;
            },
          },
          activeAbort.signal,
        );
      } catch (err) {
        stopped = true;
        if (err instanceof DOMException && err.name === "AbortError") {
          iterationAborted = true;
        }
      } finally {
        activeAbort = null;
        streamingState.value = "idle";
      }

      if (stopped) {
        failed += 1;
        if (iterationAborted) {
          // User explicitly aborted via abortStream(); honour it across
          // the remaining queued nodes rather than silently grinding on.
          aborted = true;
          break;
        }
        continue;
      }

      const generated = buffer.trim();
      if (!generated) {
        failed += 1;
        continue;
      }

      try {
        await nodeStoreRef.setNodeDescription(nodeId, generated);
        succeeded += 1;
      } catch {
        failed += 1;
      }
    }

    return { succeeded, failed, aborted };
  };

  const askLineageQuestion = async (
    flowId: number,
    question: string,
    focusNodeId?: number,
  ): Promise<void> => {
    // "Ask about lineage" entry point. The wire-level user message is
    // composed by the backend via ``render_prompt_context``
    // (surface="lineage") + the ``## Run history`` + ``## Question``
    // blocks; the chat-visible synthetic turn is purely cosmetic. The
    // empty-question guard runs BEFORE ``_runStreamedChatSurface`` so
    // we don't open the drawer + push a placeholder for an obviously
    // bad call.
    const trimmedQuestion = question.trim();
    if (!trimmedQuestion) {
      streamError.value = "Type a lineage question first.";
      streamingState.value = "error";
      return;
    }

    const userVisibleText = `[Lineage] ${trimmedQuestion}`;

    await _runStreamedChatSurface({
      userVisibleText,
      streamFn: (handlers, signal) =>
        streamLineageQuestion(
          {
            flow_id: flowId,
            question: trimmedQuestion,
            provider: selectedProvider.value!,
            model: selectedModel.value,
            focus_node_id: focusNodeId ?? null,
          },
          handlers,
          signal,
        ),
    });
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
    mode,
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
    verifyPlanCompletion,
    setVerifyPlanCompletion,
    // Exposed so ai-agent-store's status watcher can flip it to
    // "agent" when a run terminates.
    lastInteractionKind,
    abortStream,
    sendMessage,
    explainRunFailure,
    generateDocumentation,
    runInlineAction,
    runBulkAddDescriptions,
    askLineageQuestion,
    setMode,
    dismissPromotionBanner,
    undoPromotion,
    acceptPromotion,
    // Public re-export of ``_buildPromotedAgentPrompt`` so
    // ``AiAssistant.vue``'s manual-Agent-toggle path can fold in chat
    // history the same way auto-promotion does. Without this, flipping
    // the explicit Agent toggle and typing *"Implement the plan"*
    // sends a context-less prompt to the planner, which then asks
    // *"I'm not sure which specific plan you'd like me to implement?"*
    // — the prior chat assistant has laid out the plan but the agent
    // never saw it.
    buildAgentPromptWithHistory: _buildPromotedAgentPrompt,
  };
});

const inlineActionUserPrompt = (action: InlineActionType, headline: string): string => {
  // Cosmetic-only — the wire-level prompt is built server-side.
  // Keep these short and human-readable so the chat shows a clean trace
  // of what the user clicked.
  switch (action) {
    case "explain":
      return `Explain ${headline}.`;
    case "add_description":
      return `Add description to ${headline}.`;
    case "regenerate_code":
      return `Regenerate the code in ${headline}.`;
  }
};
