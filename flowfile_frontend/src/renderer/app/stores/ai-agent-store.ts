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
  streamAgentSession,
  type AgentCompleteResult,
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

// Mirror of W27's throttle in `ai-store.ts`. Streaming events arrive ~10/s
// during an active agent run; coalescing them into ~4 writes/sec keeps the
// sessionStorage cost negligible. User-driven actions (start, abort, clear)
// flush on the next tick so the next refresh sees them.
const AGENT_PERSIST_THROTTLE_MS = 250;

export type AgentStoreStatus =
  | "idle"
  | "running"
  | "paused_drift"
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
    const controller = _newController();
    currentSessionId.value = body.session_id ?? null;
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
      await streamAgentSession(body, handlers, controller.signal);
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
    abort,
    refreshState,
    clear,
  };
});
