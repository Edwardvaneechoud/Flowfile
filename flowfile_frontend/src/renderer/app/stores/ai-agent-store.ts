// W40 — Pinia store for the multi-turn planner agent.
//
// State machine:
//   idle → running → (paused_drift | completed | aborted | failed)
//   paused_drift → running (continue) | aborted (discard) | aborted (abort)
//
// On `complete`, the bundled GraphDiffPayload is pushed into
// useAiDiffStore.setCurrentDiff(...) so the existing W35 AiDiffPanel renders
// the staged diff for accept/reject. The drawer is opened via
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
  type AgentAwaitingUserInputResult,
  type AgentCompleteResult,
  type AgentFollowupRequest,
  type AgentSessionHandlers,
  type AgentStartRequest,
  type AgentToolCallProposed,
  type AgentToolCallRejected,
  type AgentToolCallStaged,
} from "../services/aiStreamClient";
import {
  clearPersistedAgentState,
  loadPersistedAgentState,
  persistAgentState,
} from "./ai-agent-store-persistence";
import { useAiDiffStore } from "./ai-diff-store";
import { useEditorStore } from "./editor-store";
import { useFlowStore } from "./flow-store";

// W57 — read the user's canvas selection from the live VueFlow instance and
// project to the int ``node_id`` set the backend expects. Mirrors the
// extraction in ``AiCommandPalette.vue`` (W33). Returns ``undefined`` when no
// instance is mounted (drawer opened without a flow loaded — should not
// happen in practice since the agent button is gated on flowId, but the
// helper stays defensive).
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

// Mirror of W27's throttle in `ai-store.ts`. Streaming events arrive ~10/s
// during an active agent run; coalescing them into ~4 writes/sec keeps the
// sessionStorage cost negligible. User-driven actions (start, abort, clear)
// flush on the next tick so the next refresh sees them.
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
    | "drift_detected"
    | "paused"
    | "retry"
    | "abort"
    | "complete"
    | "awaiting_user_input"
    | "info";
  payload: Record<string, unknown>;
  at: number;
}

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

  let activeAbort: AbortController | null = null;

  // ----- Hydrate from sessionStorage -----
  // Order matters: hydrate refs BEFORE wiring the watchers so the initial
  // assignment doesn't trigger a redundant write. The persistence helper
  // normalises `running` → `idle` on load (the SSE stream is dead post-
  // refresh; W40 has no re-attach route). `paused_drift` survives so the
  // user can still hit the resume buttons via `currentSessionId` (W45 Q2).
  const _hydrated = loadPersistedAgentState();
  if (_hydrated.events.length > 0) events.value = _hydrated.events;
  if (_hydrated.currentSessionId) currentSessionId.value = _hydrated.currentSessionId;
  status.value = _hydrated.status;
  driftDetail.value = _hydrated.driftDetail;
  lastResult.value = _hydrated.lastResult;
  if (_hydrated.error) error.value = _hydrated.error;

  // W55 — also rehydrate the W35 diff store from `lastResult.diff_payload`.
  // The agent-store persistence (W45/W55 follow-up) restores `lastResult`,
  // but without this hand-off the diff store stays empty and AiDiffPanel's
  // v-if hides the Accept / Reject buttons after a refresh. Skip the push
  // if the diff store already has a staged diff so we don't clobber an
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
        persistAgentState({
          events: events.value,
          currentSessionId: currentSessionId.value,
          status: status.value,
          driftDetail: driftDetail.value,
          lastResult: lastResult.value,
          error: error.value,
        });
      },
      isStreaming ? AGENT_PERSIST_THROTTLE_MS : 0,
    );
  };

  // `flush: "sync"` so each mutation is evaluated against the current
  // `status` value, not the post-flush value — same posture as W27's
  // `ai-store.ts` watchers.
  watch(events, queuePersist, { deep: true, flush: "sync" });
  watch(currentSessionId, queuePersist, { flush: "sync" });
  watch(status, queuePersist, { flush: "sync" });
  watch(driftDetail, queuePersist, { deep: true, flush: "sync" });
  watch(lastResult, queuePersist, { deep: true, flush: "sync" });
  watch(error, queuePersist, { flush: "sync" });

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

  const _buildHandlers = (): AgentSessionHandlers => {
    const diffStore = useAiDiffStore();
    const editorStore = useEditorStore();

    const open = (): void => {
      try {
        editorStore.openAiDrawer();
      } catch {
        /* editor store not registered in this context (e.g. tests) */
      }
    };

    return {
      onThinking: (text) => _appendEvent("thinking", { text }),
      onToolCallProposed: (tc: AgentToolCallProposed) =>
        _appendEvent("tool_call_proposed", tc as unknown as Record<string, unknown>),
      onToolCallStaged: (entry: AgentToolCallStaged) =>
        _appendEvent("tool_call_staged", entry as unknown as Record<string, unknown>),
      onToolCallWarned: (entry: AgentToolCallStaged) =>
        _appendEvent("tool_call_warned", entry as unknown as Record<string, unknown>),
      onToolCallRejected: (refusal: AgentToolCallRejected) =>
        _appendEvent("tool_call_rejected", refusal as unknown as Record<string, unknown>),
      onDriftDetected: (drift, sessionId) => {
        // Populate currentSessionId from the wire — start() can't because
        // the server allocates the id and streams it back with each event.
        // Without this the resume buttons silently no-op (handler at
        // AiAssistant.vue:184-187 / 189-192 early-returns on null sid).
        if (sessionId) currentSessionId.value = sessionId;
        // SSE wire shape is snake_case; the store exposes camelCase to match
        // ai.api.ts AgentDriftDetail.
        const nodeTypes: Record<number, string> = {};
        if (drift.node_types) {
          for (const [k, v] of Object.entries(drift.node_types)) {
            const id = Number(k);
            if (Number.isFinite(id) && typeof v === "string") nodeTypes[id] = v;
          }
        }
        driftDetail.value = {
          missingNodeIds: drift.missing_node_ids ?? [],
          externalAddedNodeIds: drift.external_added_node_ids ?? [],
          nodeTypes,
        };
        _appendEvent("drift_detected", {
          drift: drift as unknown as Record<string, unknown>,
          session_id: sessionId,
        });
      },
      onPaused: (reason, sessionId) => {
        if (sessionId) currentSessionId.value = sessionId;
        status.value = "paused_drift";
        _appendEvent("paused", { reason, session_id: sessionId });
      },
      onRetry: (attempt, max) => _appendEvent("retry", { attempt, max }),
      onAbort: (sessionId) => {
        // Q2 W45 — propagate session_id from the wire (defensive consistency
        // with onPaused / onDriftDetected; abort flow doesn't gate on it but
        // refreshState callers may still want the id available).
        if (sessionId) currentSessionId.value = sessionId;
        status.value = "aborted";
        _appendEvent("abort", { session_id: sessionId });
      },
      onAwaitingUserInput: (result: AgentAwaitingUserInputResult) => {
        // W49 — model ended on a clarifying question with no staged ops.
        // Distinct from "complete + nothing to stage": the frontend renders
        // *"Agent waiting for your reply…"* and the next user message is
        // routed through ``resumeAfterMessage`` so the planner re-enters
        // the same session rather than spawning a fresh one.
        if (result.session_id) currentSessionId.value = result.session_id;
        status.value = "awaiting_user_input";
        _appendEvent("awaiting_user_input", result as unknown as Record<string, unknown>);
      },
      onComplete: (result) => {
        // Q2 W45 — propagate session_id from the wire on completion too.
        // ``result.session_id`` is part of AgentCompleteResult.
        if (result.session_id) currentSessionId.value = result.session_id;
        status.value = "completed";
        lastResult.value = result;
        _appendEvent("complete", result as unknown as Record<string, unknown>);
        // Push the bundled GraphDiffPayload to the W35 diff store so the
        // existing AiDiffPanel renders accept/reject for the user.
        if (result.diff_payload) {
          try {
            diffStore.setCurrentDiff(result.diff_payload as unknown as never);
            open();
          } catch (err) {
            console.error("ai-agent-store: failed to set current diff", err);
          }
        } else {
          // W55 — flag the SSE-serialisation theory: agent claims staged
          // ops on the wire but no diff_payload arrived. The persistence-
          // rehydration fix in the hydration block above won't help if the
          // wire is the broken path, so we log here to make that visible.
          const stagedCount = events.value.reduce(
            (n, e) => (e.kind === "tool_call_staged" ? n + 1 : n),
            0,
          );
          if (stagedCount > 0) {
            console.warn(
              "ai-agent-store: onComplete with no diff_payload despite",
              stagedCount,
              "tool_call_staged event(s)",
              { session_id: result.session_id, op_count: result.op_count },
            );
          }
        }
      },
      onInfo: (payload) => _appendEvent("info", payload),
      onError: (message) => {
        error.value = message;
        if (status.value === "running") status.value = "failed";
      },
    };
  };

  const start = async (body: AgentStartRequest): Promise<void> => {
    // W57 — auto-attach the user's canvas selection so the planner's
    // ``_resolve_insertion_context`` has a contextual signal when the LLM
    // doesn't emit explicit ``upstream_node_ids``. Caller can override by
    // setting ``selected_node_ids`` on the body explicitly (including ``[]``
    // / ``null`` to opt out).
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
    // W49 — shared scaffolding for ``resumeAfterReject`` + ``resumeAfterMessage``.
    // Reuses ``_buildHandlers`` so the resumed run renders into the same
    // ``events`` array — no ``── new agent run ──`` boundary, the chat
    // trail continues unbroken.
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
    // W49 — called from ``ai-diff-store.reject(note?)`` after the backend
    // reject succeeded. Re-enters the same session with a synthetic
    // rejection feedback turn so the planner can course-correct.
    await _resumeViaFollowup(sessionId, {
      action: "rejected_diff",
      message: note,
      rejected_diff_id: rejectedDiffId,
    });
  };

  const resumeAfterMessage = async (sessionId: string, message: string): Promise<void> => {
    // W49 — called from ``AiAssistant.handleSend`` when the active session
    // is ``completed`` or ``awaiting_user_input``. Routes the user's typed
    // message through the followup endpoint instead of allocating a fresh
    // session via ``start()``.
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
      // W42 — ``paused_user_action`` rides through unchanged so the UI can
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
    // W42 — page-reload re-attach. Called once on store init (after hydration
    // has populated currentSessionId from sessionStorage). Three branches:
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
    aiDisabled.value = false;
    // Also wipe the persisted copy so a fresh refresh doesn't resurrect
    // the cleared state from sessionStorage.
    clearPersistedAgentState();
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
  };
});
