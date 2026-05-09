// Pinia store for the multi-turn planner agent.
//
// State machine:
//   idle → running → (paused_drift | completed | aborted | failed)
//   paused_drift → running (continue) | aborted (discard) | aborted (abort)
//
// On `complete`, the bundled GraphDiffPayload is pushed into
// useAiDiffStore.setCurrentDiff(...) so AiDiffPanel renders the staged
// diff for accept/reject. The drawer is opened via
// useEditorStore.openAiDrawer() so the user sees the diff immediately.
//
// Cancellation: each lifecycle action (start / resume / abort / discard) gets
// a fresh AbortController that supersedes any in-flight one. Abort during a
// paused_drift is a no-op on the wire (session is already paused server-side)
// but flips local status so the UI clears.

import { defineStore } from "pinia";
import { ref, watch } from "vue";

import {
  abortAgentSession,
  AiDisabledError,
  discardAgentSession,
  getAgentSession,
  type AgentDriftDetail,
  type AgentSessionState,
} from "../api/ai.api";
import {
  AiStreamHttpError,
  resumeAgentSessionStream,
  streamAgentFollowup,
  streamAgentSession,
  type AgentCompleteResult,
  type AgentFollowupRequest,
  type AgentStartRequest,
} from "../services/aiStreamClient";
import { buildAgentHandlers } from "./ai-agent-handlers";
import {
  clearPersistedAgentState,
  loadPersistedAgentState,
  persistAgentState,
} from "./ai-agent-store-persistence";
import { useAiDiffStore } from "./ai-diff-store";
import { useEditorStore } from "./editor-store";
import { useFlowStore } from "./flow-store";

// Read the user's canvas selection from the live VueFlow instance and
// project to the int ``node_id`` set the backend expects. Mirrors the
// extraction in ``AiCommandPalette.vue``. Returns ``undefined`` when no
// instance is mounted (drawer opened without a flow loaded — should
// not happen in practice since the agent button is gated on flowId,
// but the helper stays defensive).
const _readCanvasSelection = (): number[] | undefined => {
  const flowStore = useFlowStore();
  const instance = flowStore.vueFlowInstance;
  if (!instance) return undefined;
  const selectedRefs = instance.getSelectedNodes?.value ?? [];
  type SelNode = { id: string; data?: { id?: number | string } };
  const ids = (selectedRefs as SelNode[])
    .map((n) => Number(n.data?.id ?? n.id))
    .filter((id) => Number.isFinite(id));
  return ids.length > 0 ? ids : undefined;
};

// Mirror of the throttle in `ai-store.ts`. Streaming events arrive
// ~10/s during an active agent run; coalescing them into ~4 writes/sec
// keeps the sessionStorage cost negligible. User-driven actions
// (start, abort, clear) flush on the next tick so the next refresh
// sees them.
const AGENT_PERSIST_THROTTLE_MS = 250;

export type AgentStoreStatus =
  | "idle"
  | "running"
  | "paused_drift"
  | "paused_user_action"
  | "awaiting_user_input"
  | "completed"
  | "aborted"
  | "failed";

export interface AgentEvent {
  kind:
    | "thinking"
    | "tool_call_proposed"
    | "tool_call_staged"
    | "tool_call_warned"
    | "tool_call_rejected"
    | "tool_call_applied"
    | "drift_detected"
    | "paused"
    | "retry"
    | "abort"
    | "complete"
    | "awaiting_user_input"
    | "stage_advanced"
    | "info";
  payload: Record<string, unknown>;
  at: number;
}

/** Stage in the ``agent_staged`` state machine. Mirrors the server's
 * ``PlannerStage`` literal in ``flowfile_core.ai.sessions``. */
export type AgentStage =
  | "classify"
  | "pick_type"
  | "pick_upstream"
  | "fill_settings"
  | "single_stage_op";

/** Op kind chosen by stage 0 (``classify_intent``). Mirrors
 * ``PlannerOpKind`` server-side. */
export type AgentOpKind =
  | "add"
  | "modify"
  | "delete"
  | "connect"
  | "disconnect"
  | "other";

const isAbortError = (err: unknown): boolean => {
  if (err instanceof DOMException && err.name === "AbortError") return true;
  if (err instanceof Error && err.name === "AbortError") return true;
  return false;
};

export const useAiAgentStore = defineStore("ai-agent", () => {
  const currentSessionId = ref<string | null>(null);
  const status = ref<AgentStoreStatus>("idle");
  const events = ref<AgentEvent[]>([]);
  const error = ref<string | null>(null);
  const driftDetail = ref<AgentDriftDetail | null>(null);
  const lastResult = ref<AgentCompleteResult | null>(null);
  const aiDisabled = ref(false);

  // Agent_staged state-machine fields. Updated by the
  // ``stage_advanced`` SSE handler so the agent panel can render a
  // per-stage badge ("Step 1/4: classifying intent" etc.). Stays at
  // ``"classify"`` / null for legacy ``agent`` / ``agent_complex``
  // surfaces that don't drive transitions; the UI conditional checks
  // ``currentSurface === "agent_staged"`` before showing the badge.
  const stage = ref<AgentStage>("classify");
  const pickedOpKind = ref<AgentOpKind | null>(null);
  const pickedNodeType = ref<string | null>(null);
  const currentSurface = ref<"agent" | "agent_complex" | "agent_staged" | "agent_live" | null>(null);

  // agent_live post-run layout-reorganize prompt. Counts the nodes the
  // in-flight (or just-finished) agent_live session committed live to
  // the canvas. On the ``complete`` event, if this is non-zero AND the
  // surface was agent_live, the banner shows up at the top of the chat
  // trail with a [Reorganize] / [Dismiss] choice. Reset on each new
  // session start so a re-run doesn't carry stale state.
  const liveAppliedCount = ref<number>(0);
  const liveLayoutPromptVisible = ref<boolean>(false);

  let activeAbort: AbortController | null = null;

  // ----- Per-flow persistence helpers -----
  // The chat store keys per-flow via ``chatPersistenceKey(flowId)``.
  // Mirror the chat shape: every load/save passes
  // ``_scopedFlowId(flowStore.flowId)``, and the ``flowStore.flowId``
  // watcher below swaps in-memory state on flow change. The local
  // ``_pinnedFlowStore`` is used inside the SSE handlers / promise
  // callbacks where ``useFlowStore()`` would re-resolve a fresh ref
  // each call.
  const _pinnedFlowStore = useFlowStore();
  const _scopedFlowId = (id: number | null | undefined): number | null =>
    id === null || id === undefined || id < 0 ? null : id;

  // ----- Hydrate from sessionStorage -----
  // Order matters: hydrate refs BEFORE wiring the watchers so the
  // initial assignment doesn't trigger a redundant write. The
  // persistence helper normalises `running` → `idle` on load (the SSE
  // stream is dead post-refresh; there's no re-attach route).
  // `paused_drift` survives so the user can still hit the resume
  // buttons via `currentSessionId`.
  const _hydrated = loadPersistedAgentState(undefined, _scopedFlowId(_pinnedFlowStore.flowId));
  if (_hydrated.events.length > 0) events.value = _hydrated.events;
  if (_hydrated.currentSessionId) currentSessionId.value = _hydrated.currentSessionId;
  status.value = _hydrated.status;
  driftDetail.value = _hydrated.driftDetail;
  lastResult.value = _hydrated.lastResult;
  if (_hydrated.error) error.value = _hydrated.error;

  // Also rehydrate the diff store from `lastResult.diff_payload`.
  // The agent-store persistence restores `lastResult`, but without this
  // hand-off the diff store stays empty and AiDiffPanel's v-if hides
  // the Accept / Reject buttons after a refresh. Skip the push if the
  // diff store already has a staged diff so we don't clobber an
  // actively-staged one (e.g. a different code path beat us here).
  if (_hydrated.lastResult?.diff_payload) {
    try {
      const diffStore = useAiDiffStore();
      if (diffStore.currentDiff === null) {
        diffStore.setCurrentDiff(_hydrated.lastResult.diff_payload as unknown as never);
      }
    } catch (err) {
      console.error("ai-agent-store: failed to rehydrate diff store", err);
    }
  }

  let saveTimer: ReturnType<typeof setTimeout> | null = null;
  const queuePersist = (): void => {
    const isStreaming = status.value === "running";
    if (saveTimer !== null) {
      if (isStreaming) return;
      clearTimeout(saveTimer);
      saveTimer = null;
    }
    saveTimer = setTimeout(
      () => {
        saveTimer = null;
        persistAgentState(
          {
            events: events.value,
            currentSessionId: currentSessionId.value,
            status: status.value,
            driftDetail: driftDetail.value,
            lastResult: lastResult.value,
            error: error.value,
          },
          undefined,
          _scopedFlowId(_pinnedFlowStore.flowId),
        );
      },
      isStreaming ? AGENT_PERSIST_THROTTLE_MS : 0,
    );
  };

  // `flush: "sync"` so each mutation is evaluated against the current
  // `status` value, not the post-flush value — same posture as the
  // `ai-store.ts` watchers.
  watch(events, queuePersist, { deep: true, flush: "sync" });
  watch(currentSessionId, queuePersist, { flush: "sync" });
  watch(status, queuePersist, { flush: "sync" });
  watch(driftDetail, queuePersist, { deep: true, flush: "sync" });
  watch(lastResult, queuePersist, { deep: true, flush: "sync" });
  watch(error, queuePersist, { flush: "sync" });

  // Bulletproof end-of-run canvas refresh for agent_live. The
  // per-handler refreshes (onComplete / onAwaitingUserInput /
  // onToolCallRejected / onError / onAbort) cover the explicit
  // terminal events. But the SSE stream can also close silently
  // (network blip, server cut, or any path where
  // ``streamAgentSession`` resolves / rejects without firing a
  // terminal event handler) and the start() catch / finally block
  // would flip status without ever touching ``requestReload``.
  // Watching ``status`` directly catches every transition from
  // ``running`` to ANY terminal state — covers the silent path
  // uniformly. Every per-handler refresh becomes belt-and-suspenders.
  //
  // The same status-transition is the canonical "agent run just
  // finished" signal across the entire codebase. Use it to flip
  // ``aiStore.lastInteractionKind`` to ``"agent"`` so the next
  // ``_dispatchPromotedAgent`` call correctly triggers the plan stage
  // (no fresh chat reasoning since the agent ran). Avoids a circular
  // ai-store ↔ ai-agent-store dependency by deferring the import until
  // the watcher fires.
  watch(status, (newStatus, oldStatus) => {
    if (oldStatus !== "running") return;
    if (newStatus === "running") return;
    if (currentSurface.value === "agent_live") {
      useFlowStore().requestReload();
    }
    // Use a deferred import so the ai-store doesn't get pulled into
    // ai-agent-store's module graph (Pinia stores can resolve each
    // other lazily but TS module-cycle detection chokes on the static
    // form). ``lastInteractionKind`` is exposed as a writable ref on
    // the ai-store for exactly this cross-store flip.
    import("./ai-store").then(({ useAiStore }) => {
      try {
        useAiStore().lastInteractionKind = "agent";
      } catch {
        /* store unavailable in test contexts */
      }
    }).catch(() => {
      /* dynamic-import resolution failed; non-fatal */
    });
  });

  // Per-flow swap. When the user opens a different flow, freeze the
  // outgoing flow's agent state under its own key, then load the
  // incoming flow's state (or fresh-empty defaults if no prior run on
  // that flow). Mirrors the chat-store pattern in ai-store.ts: abort
  // any in-flight stream, persist outgoing, load incoming, reset
  // transient session-only refs (currentSurface / stage / picked* /
  // live*) since those describe a run on a specific flow and don't
  // carry across.
  watch(
    () => _pinnedFlowStore.flowId,
    (newId, oldId) => {
      const outgoing = _scopedFlowId(oldId);
      const incoming = _scopedFlowId(newId);
      if (outgoing === incoming) return;

      // Cut any live SSE on the outgoing flow so the in-memory
      // state we're about to freeze isn't mutating mid-write.
      if (activeAbort) {
        try {
          activeAbort.abort();
        } catch {
          /* prior controller already done */
        }
        activeAbort = null;
      }

      // Freeze outgoing.
      persistAgentState(
        {
          events: events.value,
          currentSessionId: currentSessionId.value,
          status: status.value,
          driftDetail: driftDetail.value,
          lastResult: lastResult.value,
          error: error.value,
        },
        undefined,
        outgoing,
      );

      // Load incoming.
      const loaded = loadPersistedAgentState(undefined, incoming);
      events.value = loaded.events;
      currentSessionId.value = loaded.currentSessionId;
      status.value = loaded.status;
      driftDetail.value = loaded.driftDetail;
      lastResult.value = loaded.lastResult;
      error.value = loaded.error;

      // Per-session state always resets on a flow swap — these
      // describe an in-flight run on the previous flow and don't
      // carry over.
      stage.value = "classify";
      pickedOpKind.value = null;
      pickedNodeType.value = null;
      currentSurface.value = null;
      aiDisabled.value = false;
      liveAppliedCount.value = 0;
      liveLayoutPromptVisible.value = false;
    },
  );

  const _newController = (): AbortController => {
    if (activeAbort) {
      try {
        activeAbort.abort();
      } catch {
        /* prior controller already done */
      }
    }
    activeAbort = new AbortController();
    return activeAbort;
  };

  const _appendEvent = (kind: AgentEvent["kind"], payload: Record<string, unknown>): void => {
    events.value = [...events.value, { kind, payload, at: Date.now() }];
  };

  // Event-handler factory lives in ai-agent-handlers.ts so the SSE→state
  // translation stays in one inspectable seam. We pass the deps inline
  // here so the factory has no knowledge of pinia / cross-store imports —
  // it just sees plain refs + callbacks.
  const _buildHandlers = () =>
    buildAgentHandlers({
      currentSessionId,
      currentSurface,
      status,
      driftDetail,
      lastResult,
      stage,
      pickedOpKind,
      pickedNodeType,
      liveAppliedCount,
      liveLayoutPromptVisible,
      error,
      events,
      appendEvent: _appendEvent,
      refreshFlow: () => useFlowStore().requestReload(),
      openAiDrawer: () => {
        try {
          useEditorStore().openAiDrawer();
        } catch {
          /* editor store not registered in this context (e.g. tests) */
        }
      },
      setCurrentDiff: (payload) =>
        useAiDiffStore().setCurrentDiff(payload as unknown as never),
    });

  const start = async (body: AgentStartRequest): Promise<void> => {
    // Auto-attach the user's canvas selection so the planner's
    // ``_resolve_insertion_context`` has a contextual signal when the
    // LLM doesn't emit explicit ``upstream_node_ids``. Caller can
    // override by setting ``selected_node_ids`` on the body explicitly
    // (including ``[]`` / ``null`` to opt out).
    const resolvedBody: AgentStartRequest =
      body.selected_node_ids === undefined
        ? { ...body, selected_node_ids: _readCanvasSelection() ?? null }
        : body;
    const controller = _newController();
    currentSessionId.value = resolvedBody.session_id ?? null;
    // Keep prior runs' events visible — the chat trail accumulates across
    // agent invocations so the user can scroll up to the previous run.
    // ``currentSessionId`` / ``status`` / ``driftDetail`` / ``lastResult``
    // are per-run state and DO reset, but ``events`` is the visible
    // history. Insert a small "info" boundary so the user can see where
    // a new run started.
    if (events.value.length > 0) {
      events.value = [
        ...events.value,
        {
          kind: "info",
          payload: { message: "── new agent run ──" },
          at: Date.now(),
        },
      ];
    }
    status.value = "running";
    error.value = null;
    driftDetail.value = null;
    lastResult.value = null;
    aiDisabled.value = false;
    // Fresh run, fresh state machine. The first stage_advanced event
    // from the server will overwrite these as the agent classifies
    // intent / picks node type / etc.
    stage.value = "classify";
    pickedOpKind.value = null;
    pickedNodeType.value = null;
    // Fresh run wipes the layout-prompt state so a stale banner from
    // a prior session can't linger.
    liveAppliedCount.value = 0;
    liveLayoutPromptVisible.value = false;
    // Capture the surface the run was started on so the badge UI can
    // gate on agent_staged-only without a separate prop. ``surface``
    // is required on AgentStartRequest; defensive ``?? null`` keeps
    // TypeScript happy.
    currentSurface.value = (resolvedBody.surface as typeof currentSurface.value) ?? null;

    const handlers = _buildHandlers();
    try {
      await streamAgentSession(resolvedBody, handlers, controller.signal);
    } catch (err) {
      if (isAbortError(err)) return;
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        error.value = "AI features are disabled.";
        status.value = "failed";
        return;
      }
      if (err instanceof AiStreamHttpError) {
        error.value = err.detail || `HTTP ${err.status}`;
        status.value = "failed";
        return;
      }
      console.error("ai-agent-store: streamAgentSession failed", err);
      error.value = err instanceof Error ? err.message : String(err);
      status.value = "failed";
    } finally {
      if (activeAbort === controller) activeAbort = null;
    }
  };

  const resumeContinue = async (sessionId: string): Promise<void> => {
    const controller = _newController();
    currentSessionId.value = sessionId;
    status.value = "running";
    error.value = null;
    driftDetail.value = null;
    aiDisabled.value = false;

    const handlers = _buildHandlers();
    try {
      await resumeAgentSessionStream(sessionId, handlers, controller.signal);
    } catch (err) {
      if (isAbortError(err)) return;
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        error.value = "AI features are disabled.";
        status.value = "failed";
        return;
      }
      if (err instanceof AiStreamHttpError) {
        error.value = err.detail || `HTTP ${err.status}`;
        status.value = "failed";
        return;
      }
      console.error("ai-agent-store: resumeContinue failed", err);
      error.value = err instanceof Error ? err.message : String(err);
      status.value = "failed";
    } finally {
      if (activeAbort === controller) activeAbort = null;
    }
  };

  const _resumeViaFollowup = async (
    sessionId: string,
    body: AgentFollowupRequest,
  ): Promise<void> => {
    // Shared scaffolding for ``resumeAfterReject`` +
    // ``resumeAfterMessage``. Reuses ``_buildHandlers`` so the resumed
    // run renders into the same ``events`` array — no
    // ``── new agent run ──`` boundary, the chat trail continues
    // unbroken.
    const controller = _newController();
    currentSessionId.value = sessionId;
    status.value = "running";
    error.value = null;
    driftDetail.value = null;
    aiDisabled.value = false;
    // ``lastResult`` carries the previous run's diff_payload — clear so
    // the diff store hand-off in onComplete doesn't see a stale entry.
    lastResult.value = null;

    const handlers = _buildHandlers();
    try {
      await streamAgentFollowup(sessionId, body, handlers, controller.signal);
    } catch (err) {
      if (isAbortError(err)) return;
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        error.value = "AI features are disabled.";
        status.value = "failed";
        return;
      }
      if (err instanceof AiStreamHttpError) {
        error.value = err.detail || `HTTP ${err.status}`;
        status.value = "failed";
        return;
      }
      console.error("ai-agent-store: streamAgentFollowup failed", err);
      error.value = err instanceof Error ? err.message : String(err);
      status.value = "failed";
    } finally {
      if (activeAbort === controller) activeAbort = null;
    }
  };

  const resumeAfterReject = async (
    sessionId: string,
    note: string | null,
    rejectedDiffId: string | null = null,
  ): Promise<void> => {
    // Called from ``ai-diff-store.reject(note?)`` after the backend
    // reject succeeded. Re-enters the same session with a synthetic
    // rejection feedback turn so the planner can course-correct.
    await _resumeViaFollowup(sessionId, {
      action: "rejected_diff",
      message: note,
      rejected_diff_id: rejectedDiffId,
    });
  };

  const resumeAfterMessage = async (sessionId: string, message: string): Promise<void> => {
    // Called from ``AiAssistant.handleSend`` when the active session
    // is ``completed`` or ``awaiting_user_input``. Routes the user's
    // typed message through the followup endpoint instead of
    // allocating a fresh session via ``start()``.
    await _resumeViaFollowup(sessionId, {
      action: "user_message",
      message,
    });
  };

  const resumeDiscard = async (sessionId: string): Promise<void> => {
    const controller = _newController();
    try {
      await discardAgentSession(sessionId, controller.signal);
      status.value = "aborted";
      currentSessionId.value = null;
      driftDetail.value = null;
    } catch (err) {
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        error.value = "AI features are disabled.";
        return;
      }
      console.error("ai-agent-store: resumeDiscard failed", err);
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      if (activeAbort === controller) activeAbort = null;
    }
  };

  const abort = async (): Promise<void> => {
    const sid = currentSessionId.value;
    // Always cancel any in-flight stream first.
    if (activeAbort) {
      try {
        activeAbort.abort();
      } catch {
        /* ignore */
      }
    }
    if (!sid) {
      status.value = "aborted";
      return;
    }
    try {
      await abortAgentSession(sid);
    } catch (err) {
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        error.value = "AI features are disabled.";
        return;
      }
      console.error("ai-agent-store: abort failed", err);
      // Don't surface the abort failure to the UI — local state is already aborted.
    }
    status.value = "aborted";
  };

  const refreshState = async (sessionId: string): Promise<AgentSessionState | null> => {
    try {
      const state = await getAgentSession(sessionId);
      currentSessionId.value = state.sessionId;
      // Mirror the server's status back to the local enum (one-to-one mapping
      // except the server's "awaiting_user" which the store collapses to running).
      // ``paused_user_action`` rides through unchanged so the UI can
      // surface a cold-start re-attach prompt.
      const mapped: AgentStoreStatus =
        state.status === "running" || state.status === "awaiting_user"
          ? "running"
          : (state.status as AgentStoreStatus);
      status.value = mapped;
      driftDetail.value = state.driftDetail;
      return state;
    } catch (err) {
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        return null;
      }
      console.error("ai-agent-store: refreshState failed", err);
      return null;
    }
  };

  const reattach = async (): Promise<void> => {
    // Page-reload re-attach. Called once on store init (after
    // hydration has populated currentSessionId from sessionStorage).
    // Three branches:
    //
    // 1. ``paused_drift`` / ``paused_user_action`` — surface the existing
    //    pause UI; user clicks Continue / Discard. No SSE stream opened
    //    here (a click will call resumeContinue or resumeDiscard).
    // 2. ``running`` — the planner is logically still running on the server
    //    (or about to be flipped to paused_user_action by the GET itself).
    //    Open a new SSE stream via the resume route with ``Last-Event-ID``
    //    derived from server's ``step_count`` so the replay buffer can flush
    //    buffered frames newer than the cursor before the live planner
    //    resumes.
    // 3. terminal (``completed`` / ``aborted`` / ``failed``) — keep the
    //    persisted ``lastResult`` / ``events`` rendered; no streaming.
    const sid = currentSessionId.value;
    if (!sid) return;

    const state = await refreshState(sid);
    if (state === null) return;

    if (state.status === "paused_drift" || state.status === "paused_user_action") {
      // UI shows resume buttons; no SSE.
      return;
    }
    if (state.status !== "running" && state.status !== "awaiting_user") {
      // Terminal — nothing to reattach to. ``refreshState`` already mapped
      // the status into the local enum.
      return;
    }

    // Build the cursor. The server's ``step_count`` is the planner's NEXT
    // step counter; the highest emitted-step id we COULD have received is
    // ``step_count - 1``. We use that as the cursor so any frame at
    // ``step_count`` or later replays before live events resume.
    let lastEventId: string | undefined;
    if (state.stepCount > 0) {
      lastEventId = `${sid}.${state.stepCount - 1}`;
    }

    const controller = _newController();
    error.value = null;
    aiDisabled.value = false;

    const handlers = _buildHandlers();
    try {
      await resumeAgentSessionStream(sid, handlers, controller.signal, lastEventId);
    } catch (err) {
      if (isAbortError(err)) return;
      if (err instanceof AiDisabledError) {
        aiDisabled.value = true;
        error.value = "AI features are disabled.";
        status.value = "failed";
        return;
      }
      if (err instanceof AiStreamHttpError) {
        error.value = err.detail || `HTTP ${err.status}`;
        status.value = "failed";
        return;
      }
      console.error("ai-agent-store: reattach failed", err);
      error.value = err instanceof Error ? err.message : String(err);
      status.value = "failed";
    } finally {
      if (activeAbort === controller) activeAbort = null;
    }
  };

  const clear = (): void => {
    if (activeAbort) {
      try {
        activeAbort.abort();
      } catch {
        /* ignore */
      }
      activeAbort = null;
    }
    currentSessionId.value = null;
    status.value = "idle";
    events.value = [];
    error.value = null;
    driftDetail.value = null;
    lastResult.value = null;
    // Reset the staged state machine on clear so the next run starts
    // at stage 0 even if the previous run was on agent_staged.
    stage.value = "classify";
    pickedOpKind.value = null;
    pickedNodeType.value = null;
    currentSurface.value = null;
    aiDisabled.value = false;
    // Also wipe the persisted copy so a fresh refresh doesn't
    // resurrect the cleared state from sessionStorage. Flow-scoped, so
    // ``clear()`` only drops the active flow's bucket; other flows
    // keep their agent history.
    clearPersistedAgentState(undefined, _scopedFlowId(_pinnedFlowStore.flowId));
  };

  // Drop only the persisted ``diff_payload`` from the last completed run,
  // leaving the rest of ``lastResult`` (session_id, diff_id, op_count,
  // rationale) intact so any "session complete" surfaces still render.
  // Called by the diff store on (a) successful accept / reject, and (b) a
  // 404 from the apply route — in both cases the diff is no longer live,
  // and without this clear ``ai-agent-store.ts:140-149`` would re-push the
  // stale payload into the diff store on the next refresh.
  const clearLastResultDiffPayload = (): void => {
    const current = lastResult.value;
    if (current === null) return;
    if (current.diff_payload === null || current.diff_payload === undefined) return;
    // Spread so the deep watcher fires and the persistence layer sees the
    // updated value — mutating the existing object in place is not enough.
    lastResult.value = { ...current, diff_payload: null };
  };

  // Post-run layout-reorganize prompt actions.
  //
  // ``acceptLayoutReorganize`` calls the existing canvas
  // "Reset layout graph" routine via ``flowStore.requestLayoutReset()``
  // (which Canvas.vue watches and dispatches to
  // ``handleResetLayoutGraph``). Both helpers also dismiss the banner
  // so it doesn't linger.
  const acceptLayoutReorganize = (): void => {
    useFlowStore().requestLayoutReset();
    liveLayoutPromptVisible.value = false;
  };
  const dismissLayoutReorganize = (): void => {
    liveLayoutPromptVisible.value = false;
  };

  return {
    // state
    currentSessionId,
    status,
    events,
    error,
    driftDetail,
    lastResult,
    aiDisabled,
    // staged state-machine refs
    stage,
    pickedOpKind,
    pickedNodeType,
    currentSurface,
    // agent_live layout-reorganize prompt
    liveAppliedCount,
    liveLayoutPromptVisible,
    // actions
    start,
    resumeContinue,
    resumeDiscard,
    resumeAfterReject,
    resumeAfterMessage,
    abort,
    refreshState,
    reattach,
    clear,
    clearLastResultDiffPayload,
    acceptLayoutReorganize,
    dismissLayoutReorganize,
  };
});
