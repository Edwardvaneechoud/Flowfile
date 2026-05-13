// SSE event-handler factory for the planner agent's lifecycle.
//
// Pure factory — takes a small `AgentHandlerDependencies` interface and
// returns the `AgentSessionHandlers` dictionary the streaming client
// drives during an active agent run. Extracted from `ai-agent-store.ts`
// so the store stays focused on lifecycle actions (`start`, `resume`,
// `abort`, etc.) and the SSE→state translation lives in one
// inspectable seam.
//
// Why a deps interface and not a direct store reference: the handlers
// mutate ~11 reactive refs and need 4 cross-store callbacks
// (canvas refresh, drawer open, diff push). Passing them through an
// interface keeps the factory testable in isolation (mock the deps,
// assert on the resulting refs) and surfaces the coupling explicitly
// at the call site.

import type { Ref } from "vue";

import type {
  AgentAwaitingUserInputResult,
  AgentSessionHandlers,
  AgentToolCallApplied,
  AgentToolCallProposed,
  AgentToolCallRejected,
  AgentToolCallStaged,
  AgentCompleteResult,
} from "../services/aiStreamClient";
import type { AgentDriftDetail } from "../api/ai.api";
import type {
  AgentEvent,
  AgentOpKind,
  AgentStage,
  AgentStoreStatus,
} from "./ai-agent-store";

/** Surface label the store tracks. Repeated here (not imported) to keep
 * the handler module decoupled from the store's reactive declarations. */
type AgentSurfaceTag = "agent" | "agent_complex" | "agent_staged" | "agent_live" | null;

/** Dependencies the handler factory needs.
 *
 * - **Refs** are the store's reactive state, written by the handlers
 *   to advance the lifecycle.
 * - **`appendEvent`** is the store's `_appendEvent`, hoisted so the
 *   factory doesn't need to know `events.value` exists.
 * - **`refreshFlow`, `openAiDrawer`, `setCurrentDiff`** are
 *   cross-store callbacks; passed as functions so this module never
 *   imports `useFlowStore` / `useEditorStore` / `useAiDiffStore`
 *   (which would risk circular module loads at boot). */
export interface AgentHandlerDependencies {
  // Reactive store state
  currentSessionId: Ref<string | null>;
  currentSurface: Ref<AgentSurfaceTag>;
  status: Ref<AgentStoreStatus>;
  driftDetail: Ref<AgentDriftDetail | null>;
  lastResult: Ref<AgentCompleteResult | null>;
  stage: Ref<AgentStage>;
  pickedOpKind: Ref<AgentOpKind | null>;
  pickedNodeType: Ref<string | null>;
  liveAppliedCount: Ref<number>;
  liveLayoutPromptVisible: Ref<boolean>;
  error: Ref<string | null>;
  events: Ref<AgentEvent[]>;
  // Event log push (typically `_appendEvent` from the store)
  appendEvent: (kind: AgentEvent["kind"], payload: Record<string, unknown>) => void;
  // Cross-store side effects, injected so this module stays import-light
  refreshFlow: () => void;
  openAiDrawer: () => void;
  setCurrentDiff: (payload: never) => void;
}

export const buildAgentHandlers = (deps: AgentHandlerDependencies): AgentSessionHandlers => {
  const {
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
    appendEvent,
    refreshFlow,
    openAiDrawer,
    setCurrentDiff,
  } = deps;

  const refreshIfLive = (): void => {
    if (currentSurface.value === "agent_live") {
      refreshFlow();
    }
  };

  return {
    onThinking: (text) => appendEvent("thinking", { text }),
    onToolCallProposed: (tc: AgentToolCallProposed) =>
      appendEvent("tool_call_proposed", tc as unknown as Record<string, unknown>),
    onToolCallStaged: (entry: AgentToolCallStaged) => {
      appendEvent("tool_call_staged", entry as unknown as Record<string, unknown>);
      // In ``agent_live`` ALL ops dispatch with mode=apply, but only
      // ``add_*`` ops go through the observation path that emits
      // ``tool_call_applied``. The other live mutations
      // (``connect`` / ``delete_connection`` /
      // ``update_node_settings`` / ``delete_node``) fall through to
      // the staged-results path that emits ``tool_call_staged`` —
      // the event name is misleading for agent_live since the server
      // already mutated the live graph. Refresh the canvas here too
      // so wire changes / settings updates / deletes become visible
      // immediately. Other surfaces (agent_staged / agent_complex)
      // genuinely STAGE — for them the canvas changes only after the
      // user accepts the diff, so this condition gates correctly.
      refreshIfLive();
    },
    onToolCallWarned: (entry: AgentToolCallStaged) => {
      appendEvent("tool_call_warned", entry as unknown as Record<string, unknown>);
      // Same rationale as ``onToolCallStaged`` above — ``warned``
      // means the apply succeeded with non-fatal warnings, so
      // the live graph IS mutated and the canvas needs refresh.
      refreshIfLive();
    },
    onToolCallRejected: (refusal: AgentToolCallRejected) => {
      appendEvent("tool_call_rejected", refusal as unknown as Record<string, unknown>);
      // agent_live's auto-undo path emits ``tool_call_rejected``
      // (not ``tool_call_applied``) when the post-apply observation
      // fails: the node was added then deleted server-side. Re-sync
      // the canvas so the user's view matches the
      // now-deleted-back-out state.
      refreshIfLive();
    },
    onToolCallApplied: (entry: AgentToolCallApplied) => {
      // agent_live committed a node to the live graph. Record the
      // event for the chat trail AND ask the canvas store to
      // re-fetch the flow so the new node materialises on the user's
      // canvas. ``flow-store.requestReload()`` bumps a counter that
      // ``Canvas.vue``'s watcher debounces into a single re-render
      // even if multiple ``tool_call_applied`` events fire rapidly
      // during a multi-step agent_live run.
      appendEvent("tool_call_applied", entry as unknown as Record<string, unknown>);
      refreshFlow();
      // Track the applied count so the post-run banner only appears
      // when at least one node landed on the canvas.
      liveAppliedCount.value += 1;
    },
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
      appendEvent("drift_detected", {
        drift: drift as unknown as Record<string, unknown>,
        session_id: sessionId,
      });
    },
    onPaused: (reason, sessionId) => {
      if (sessionId) currentSessionId.value = sessionId;
      status.value = "paused_drift";
      appendEvent("paused", { reason, session_id: sessionId });
    },
    onRetry: (attempt, max) => appendEvent("retry", { attempt, max }),
    onAbort: (sessionId) => {
      // Propagate session_id from the wire (defensive consistency
      // with onPaused / onDriftDetected; abort flow doesn't gate on
      // it but refreshState callers may still want the id available).
      if (sessionId) currentSessionId.value = sessionId;
      status.value = "aborted";
      appendEvent("abort", { session_id: sessionId });
      // agent_live ends on abort too. Refresh canvas so it reflects
      // whatever applied before the abort.
      refreshIfLive();
    },
    onAwaitingUserInput: (result: AgentAwaitingUserInputResult) => {
      // Model ended on a clarifying question with no staged ops.
      // Distinct from "complete + nothing to stage": the frontend
      // renders *"Agent waiting for your reply…"* and the next user
      // message is routed through ``resumeAfterMessage`` so the
      // planner re-enters the same session rather than spawning a
      // fresh one.
      if (result.session_id) currentSessionId.value = result.session_id;
      status.value = "awaiting_user_input";
      appendEvent("awaiting_user_input", result as unknown as Record<string, unknown>);
      // agent_live runs frequently end with the model asking *"want
      // me to do anything else?"*, which routes here instead of
      // through ``onComplete``. Mirror the same layout-prompt
      // trigger so the banner still appears when at least one node
      // was committed live to the canvas.
      if (currentSurface.value === "agent_live" && liveAppliedCount.value > 0) {
        liveLayoutPromptVisible.value = true;
      }
      // Defensive end-of-run canvas refresh for agent_live. Per-step
      // ``tool_call_applied`` events already trigger a reload, but
      // if any of them was missed (auto-undo path emitting
      // tool_call_rejected, transient frontend race, etc.) the
      // canvas can drift from the server's authoritative state. One
      // reload at run-end re-syncs.
      refreshIfLive();
    },
    onComplete: (result) => {
      // Propagate session_id from the wire on completion too.
      // ``result.session_id`` is part of AgentCompleteResult.
      if (result.session_id) currentSessionId.value = result.session_id;
      status.value = "completed";
      lastResult.value = result;
      appendEvent("complete", result as unknown as Record<string, unknown>);
      // Post-run layout-reorganize prompt for agent_live ONLY.
      // agent_staged and agent_complex bundle into a diff the user
      // reviews before accept; agent_live commits each step to the
      // canvas live, which in a multi-step run can leave the
      // newly-added nodes scattered. Surface the banner so the user
      // can opt into the existing "Reset layout graph" routine.
      if (currentSurface.value === "agent_live" && liveAppliedCount.value > 0) {
        liveLayoutPromptVisible.value = true;
      }
      // Defensive end-of-run canvas refresh for agent_live. Same
      // rationale as in ``onAwaitingUserInput``: if any per-step
      // ``tool_call_applied`` was missed (auto-undo path, transient
      // race, etc.) the canvas would drift from the server's
      // authoritative state. One reload at run-end re-syncs.
      refreshIfLive();
      // Push the bundled GraphDiffPayload to the diff store so the
      // existing AiDiffPanel renders accept/reject for the user.
      if (result.diff_payload) {
        try {
          setCurrentDiff(result.diff_payload as unknown as never);
          openAiDrawer();
        } catch (err) {
          console.error("ai-agent-store: failed to set current diff", err);
        }
      } else {
        // SSE-serialisation guard: agent claims staged ops on the
        // wire but no diff_payload arrived. The persistence-
        // rehydration fix in the hydration block above won't help if
        // the wire is the broken path, so we log here to make that
        // visible.
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
    onStageAdvanced: (payload) => {
      // agent_staged state-machine transition. Update the local refs
      // so the agent panel can render the current stage; record the
      // event so the chat trail keeps a debug record.
      if (payload.session_id) currentSessionId.value = payload.session_id;
      if (
        payload.to === "classify" ||
        payload.to === "pick_type" ||
        payload.to === "pick_upstream" ||
        payload.to === "fill_settings" ||
        payload.to === "single_stage_op"
      ) {
        stage.value = payload.to;
      }
      if (payload.op_kind) {
        const ok = payload.op_kind;
        if (
          ok === "add" ||
          ok === "modify" ||
          ok === "delete" ||
          ok === "connect" ||
          ok === "disconnect" ||
          ok === "other"
        ) {
          pickedOpKind.value = ok;
        }
      }
      if (payload.picked_node_type !== undefined) {
        pickedNodeType.value = payload.picked_node_type ?? null;
      }
      // After a successful add or single-stage non-add op, the planner
      // resets the state machine (transitions ``to=classify``). Clear
      // the picked refs so the badge reflects the next round's clean
      // start.
      if (payload.completed_op || payload.to === "classify") {
        pickedOpKind.value = null;
        pickedNodeType.value = null;
      }
      appendEvent("stage_advanced", payload as unknown as Record<string, unknown>);
    },
    onInfo: (payload) => appendEvent("info", payload),
    onError: (message) => {
      error.value = message;
      if (status.value === "running") status.value = "failed";
      // agent_live error path. The planner emits ``error`` on
      // max_retries exhaustion and other terminal failures; per-step
      // ``tool_call_applied`` events may have already mutated the
      // live graph before the error fired. Refresh so the canvas
      // reflects the server's actual state at termination.
      refreshIfLive();
    },
  };
};
