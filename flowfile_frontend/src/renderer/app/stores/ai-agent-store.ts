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
import { ref } from "vue";

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
import { useAiDiffStore } from "./ai-diff-store";
import { useEditorStore } from "./editor-store";

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
        // SSE wire shape is snake_case; the store exposes camelCase to match
        // ai.api.ts AgentDriftDetail.
        driftDetail.value = {
          missingNodeIds: drift.missing_node_ids ?? [],
          mutatedNodeIds: drift.mutated_node_ids ?? [],
          schemaChangedNodeIds: drift.schema_changed_node_ids ?? [],
        };
        _appendEvent("drift_detected", {
          drift: drift as unknown as Record<string, unknown>,
          session_id: sessionId,
        });
      },
      onPaused: (reason, sessionId) => {
        status.value = "paused_drift";
        _appendEvent("paused", { reason, session_id: sessionId });
      },
      onRetry: (attempt, max) => _appendEvent("retry", { attempt, max }),
      onAbort: (sessionId) => {
        status.value = "aborted";
        _appendEvent("abort", { session_id: sessionId });
      },
      onComplete: (result) => {
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
    status.value = "running";
    events.value = [];
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
