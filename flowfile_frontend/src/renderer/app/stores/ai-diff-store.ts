// W35 — Pinia store owning the *currently staged* GraphDiff.
//
// Backed by W41's three routes via `aiDiffClient.ts`:
//   - stage()    → POST /ai/diff/stage           returns { diff_id, op_count }
//   - accept()   → POST /ai/diff/{id}/accept     returns AcceptDiffResponse
//   - reject()   → POST /ai/diff/{id}/reject     returns RejectDiffResponse
//
// Lifecycle invariants:
//   - At most one diff is staged at a time. `stage()` and `setCurrentDiff()`
//     replace whatever was there; the inflight request (if any) is aborted.
//   - On accept-success the store clears `currentDiff` and writes
//     `lastApplyResult` so consumers can render a brief confirmation.
//   - On accept-409 (D006 drift) the diff stays staged so the user can
//     reconcile the underlying graph and retry. `error.kind === "drift"`
//     carries the missing node ids the backend reported.
//   - On reject-success the store clears `currentDiff`.
//   - `clear()` is a pure local reset; it does not call the backend.

import { defineStore } from "pinia";
import { ref } from "vue";

import {
  AiDiffHttpError,
  acceptDiff,
  rejectDiff,
  stageDiff,
  type AcceptDiffResponse,
} from "../services/aiDiffClient";
import {
  type DiffStoreError,
  type GraphDiffPayload,
  type StageDiffRequest,
  synthesiseDiffFromStageRequest,
} from "../features/ai/aiDiffTypes";
import { useAiAgentStore } from "./ai-agent-store";
import { useFlowStore } from "./flow-store";

const isAbortError = (err: unknown): boolean =>
  err instanceof DOMException && err.name === "AbortError";

interface DiffDriftPayload {
  error?: string;
  missing_node_ids?: number[];
  diff_id?: string;
}

const parseDriftDetail = (status: number, detail: unknown): DiffStoreError => {
  if (status === 409 && detail && typeof detail === "object") {
    const candidate = detail as DiffDriftPayload;
    if (candidate.error === "diff_drift" && Array.isArray(candidate.missing_node_ids)) {
      return {
        kind: "drift",
        status: 409,
        message: `Graph changed since this diff was staged — ${candidate.missing_node_ids.length} node id(s) no longer exist.`,
        missingNodeIds: candidate.missing_node_ids,
      };
    }
  }
  const message =
    typeof detail === "string"
      ? detail
      : detail !== null && typeof detail === "object"
        ? JSON.stringify(detail)
        : `HTTP ${status}`;
  return { kind: "http", status, message };
};

export const useAiDiffStore = defineStore("aiDiff", () => {
  const currentDiff = ref<GraphDiffPayload | null>(null);
  const loading = ref(false);
  const error = ref<DiffStoreError | null>(null);
  const lastApplyResult = ref<AcceptDiffResponse | null>(null);
  let inflight: AbortController | null = null;

  const cancel = (): void => {
    if (inflight !== null) {
      inflight.abort();
      inflight = null;
    }
  };

  const clear = (): void => {
    cancel();
    currentDiff.value = null;
    error.value = null;
    lastApplyResult.value = null;
    loading.value = false;
  };

  const setCurrentDiff = (diff: GraphDiffPayload): void => {
    cancel();
    currentDiff.value = diff;
    error.value = null;
    lastApplyResult.value = null;
  };

  const _newController = (): AbortController => {
    cancel();
    const controller = new AbortController();
    inflight = controller;
    return controller;
  };

  const _handleError = (err: unknown): void => {
    if (isAbortError(err)) return;
    if (err instanceof AiDiffHttpError) {
      error.value = parseDriftDetail(err.status, err.detail);
    } else {
      error.value = {
        kind: "http",
        status: 0,
        message: err instanceof Error ? err.message : String(err),
      };
    }
  };

  const stage = async (request: StageDiffRequest): Promise<void> => {
    const controller = _newController();
    loading.value = true;
    error.value = null;
    lastApplyResult.value = null;
    try {
      const response = await stageDiff(request, controller.signal);
      currentDiff.value = synthesiseDiffFromStageRequest(request, response.diff_id);
    } catch (err) {
      _handleError(err);
    } finally {
      if (inflight === controller) inflight = null;
      loading.value = false;
    }
  };

  const accept = async (): Promise<void> => {
    const diff = currentDiff.value;
    if (diff === null) return;
    const controller = _newController();
    loading.value = true;
    error.value = null;
    try {
      const response = await acceptDiff(diff.diff_id, { flow_id: diff.flow_id }, controller.signal);
      lastApplyResult.value = response;
      currentDiff.value = null;
      // The backend mutated the live FlowGraph (W41 `apply_diff`). Signal
      // the canvas to re-fetch so the new nodes/connections render —
      // without this the user accepts a diff and sees no visible change
      // until they manually reload the page.
      try {
        useFlowStore().requestReload();
      } catch (reloadErr) {
        // Pinia not registered (e.g. tests with no flow store mock) —
        // swallowing keeps the accept path successful even when the
        // canvas-reload signal can't fire.
        console.warn("ai-diff-store: requestReload failed", reloadErr);
      }
    } catch (err) {
      _handleError(err);
      // On 409 drift the diff stays staged. On any other error we also
      // keep the diff so the user can retry without losing context — the
      // backend already rolled back the graph (per W41's apply contract).
    } finally {
      if (inflight === controller) inflight = null;
      loading.value = false;
    }
  };

  const reject = async (note?: string | null): Promise<void> => {
    // W49 — ``reject(note?)`` accepts an optional user-supplied rejection
    // note. When the rejected diff belongs to a still-alive completed agent
    // session, this method:
    //
    // 1. Calls the backend reject (W41) to mark the diff itself rejected.
    // 2. Hands off to ``aiAgentStore.resumeAfterReject(sid, note)`` so the
    //    agent re-enters the same session with a synthetic rejection turn,
    //    rather than the user having to start a new session and lose the
    //    conversation context.
    //
    // Falls back to the legacy "just clear locally" path when no agent
    // session is active or the session is not in a followup-resumable
    // status (e.g. diff came from a W33 cmd-K palette flow).
    const diff = currentDiff.value;
    if (diff === null) return;
    const controller = _newController();
    loading.value = true;
    error.value = null;
    try {
      await rejectDiff(diff.diff_id, controller.signal);
      const rejectedDiffId = diff.diff_id;
      currentDiff.value = null;
      lastApplyResult.value = null;

      // Hand off to the agent store if the rejected diff belongs to a
      // followup-resumable session. We resolve the agent store lazily so
      // tests / non-Pinia contexts that import the diff store in isolation
      // don't blow up on the cross-store reference.
      let agentStore: ReturnType<typeof useAiAgentStore> | null = null;
      try {
        agentStore = useAiAgentStore();
      } catch (storeErr) {
        console.warn("ai-diff-store: useAiAgentStore unavailable", storeErr);
      }
      if (agentStore !== null) {
        const sid = agentStore.currentSessionId;
        const isResumable =
          agentStore.status === "completed" || agentStore.status === "awaiting_user_input";
        if (sid !== null && isResumable) {
          // Fire-and-forget — the SSE stream re-renders into agentStore.events.
          // ``await`` would serialise the next user click behind the resume
          // stream, but ``reject()`` is called from the AiDiffPanel's
          // synchronous click handler and we don't need the caller to block.
          void agentStore.resumeAfterReject(sid, note ?? null, rejectedDiffId);
        }
      }
    } catch (err) {
      _handleError(err);
    } finally {
      if (inflight === controller) inflight = null;
      loading.value = false;
    }
  };

  return {
    // state
    currentDiff,
    loading,
    error,
    lastApplyResult,
    // actions
    stage,
    setCurrentDiff,
    accept,
    reject,
    clear,
    cancel,
  };
});
