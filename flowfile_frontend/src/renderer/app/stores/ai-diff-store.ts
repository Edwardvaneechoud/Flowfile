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
//   - On accept-422 with `error: "diff_inconsistent"` (W70) the diff also
//     stays staged so the user can Reject and ask the agent to retry.
//     `error.kind === "inconsistent"` carries the offending endpoint(s).
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
import { useAiStore } from "./ai-store";
import { useFlowStore } from "./flow-store";

const isAbortError = (err: unknown): boolean =>
  err instanceof DOMException && err.name === "AbortError";

interface DiffDriftPayload {
  error?: string;
  missing_node_ids?: number[];
  diff_id?: string;
}

interface DiffInconsistentPayload {
  error?: string;
  missing_endpoints?: Array<[number, string]>;
  diff_id?: string;
}

const parseDiffErrorDetail = (status: number, detail: unknown): DiffStoreError => {
  // 404 from accept/reject means the staged diff is gone from the backend
  // (typically: ``flowfile_core`` was restarted and the on-disk sidecar
  // didn't survive). The store will clear ``currentDiff`` in response and
  // surface a transient toast — the inline error message here is just a
  // fallback in case the toast path is somehow bypassed. Status-only check
  // (no detail-string match) so this also covers the sibling 404 case
  // where the backend says ``Flow {id} not found`` (``diff_routes.py:130``).
  if (status === 404) {
    const message = typeof detail === "string" && detail.length > 0 ? detail : `HTTP 404`;
    return { kind: "not_found", status: 404, message, diffId: "" };
  }
  if (detail && typeof detail === "object") {
    if (status === 409) {
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
    if (status === 422) {
      const candidate = detail as DiffInconsistentPayload;
      if (candidate.error === "diff_inconsistent" && Array.isArray(candidate.missing_endpoints)) {
        const missingEndpoints = candidate.missing_endpoints
          .filter(
            (entry): entry is [number, "from" | "to"] =>
              Array.isArray(entry) &&
              entry.length === 2 &&
              typeof entry[0] === "number" &&
              (entry[1] === "from" || entry[1] === "to"),
          )
          .map(([nodeId, role]) => ({ nodeId, role }));
        return {
          kind: "inconsistent",
          status: 422,
          message:
            "This proposed change is inconsistent — a connection references a node id that doesn't exist. Reject and ask the agent to retry.",
          missingEndpoints,
        };
      }
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

const STALE_NOTICE_TIMEOUT_MS = 6000;
const STALE_NOTICE_MESSAGE =
  "This proposed change is no longer available — the backend lost track of it. " +
  "Ask the agent again if you still want the change.";

export const useAiDiffStore = defineStore("aiDiff", () => {
  const currentDiff = ref<GraphDiffPayload | null>(null);
  const loading = ref(false);
  const error = ref<DiffStoreError | null>(null);
  const lastApplyResult = ref<AcceptDiffResponse | null>(null);
  // Transient banner shown after the backend reports the diff is gone (404)
  // — auto-dismisses after ``STALE_NOTICE_TIMEOUT_MS`` so it doesn't
  // accumulate across sessions. The user can also dismiss it manually via
  // ``dismissStaleNotice``.
  const staleNotice = ref<string | null>(null);
  let inflight: AbortController | null = null;
  let staleTimer: ReturnType<typeof setTimeout> | null = null;

  const cancel = (): void => {
    if (inflight !== null) {
      inflight.abort();
      inflight = null;
    }
  };

  // 2026-05-07 — push a synthetic ``role="user"`` message into the chat trail
  // when the user clicks Accept / Reject on a staged diff. Without this, the
  // chat history goes silent on the user's decision — the diff disappears
  // and the next message just shows the agent's followup without any record
  // of what the user did. The reject note in particular is load-bearing
  // context the user wants preserved (W49 already feeds it back to the
  // planner; the chat trail should mirror what the planner sees). Lazy
  // store-resolution so non-Pinia contexts (tests, isolated imports) don't
  // blow up — same defensive pattern reject() uses for ``useAiAgentStore``.
  const _pushDecisionMessage = (content: string): void => {
    try {
      const aiStore = useAiStore();
      aiStore.messages.push({
        id: Date.now(),
        createdAt: Date.now(),
        role: "user",
        content,
      });
    } catch (err) {
      console.warn("ai-diff-store: useAiStore unavailable for decision message", err);
    }
  };

  const _cancelStaleTimer = (): void => {
    if (staleTimer !== null) {
      clearTimeout(staleTimer);
      staleTimer = null;
    }
  };

  const dismissStaleNotice = (): void => {
    _cancelStaleTimer();
    staleNotice.value = null;
  };

  const _setStaleNotice = (message: string): void => {
    _cancelStaleTimer();
    staleNotice.value = message;
    staleTimer = setTimeout(() => {
      staleNotice.value = null;
      staleTimer = null;
    }, STALE_NOTICE_TIMEOUT_MS);
  };

  const clear = (): void => {
    cancel();
    _cancelStaleTimer();
    currentDiff.value = null;
    error.value = null;
    lastApplyResult.value = null;
    staleNotice.value = null;
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

  // Returns the parsed error (or null on an AbortError) so callers can
  // branch on the kind without re-reading ``error.value`` — the indirection
  // through the ref defeats TS's flow narrowing inside catch blocks.
  const _handleError = (err: unknown): DiffStoreError | null => {
    if (isAbortError(err)) return null;
    const parsed: DiffStoreError =
      err instanceof AiDiffHttpError
        ? parseDiffErrorDetail(err.status, err.detail)
        : {
            kind: "http",
            status: 0,
            message: err instanceof Error ? err.message : String(err),
          };
    error.value = parsed;
    return parsed;
  };

  // Drop the agent store's persisted ``lastResult.diff_payload`` so the
  // next page refresh doesn't re-hydrate a diff that's already done with
  // (accepted, rejected, or lost on the backend). Lazy-resolved + try/catch
  // mirrors the existing posture in ``reject()`` for the agent store call.
  const _clearAgentDiffPayload = (): void => {
    try {
      useAiAgentStore().clearLastResultDiffPayload();
    } catch (storeErr) {
      console.warn("ai-diff-store: clearLastResultDiffPayload unavailable", storeErr);
    }
  };

  // Backend says the diff is gone — clear local state and surface the
  // transient toast. Called from accept/reject catch blocks when
  // ``error.value?.kind === "not_found"``.
  const _handleStaleDiff = (): void => {
    const lostDiffId = currentDiff.value?.diff_id ?? null;
    currentDiff.value = null;
    error.value = null;
    _setStaleNotice(STALE_NOTICE_MESSAGE);
    _clearAgentDiffPayload();
    if (lostDiffId !== null) {
      console.info("ai-diff-store: cleared stale diff", lostDiffId);
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
      // Mirror the accept decision in the chat trail so the user can see the
      // history of what they decided alongside the agent's outputs. Counts
      // come straight from the apply response so the trail records actual
      // mutation scope, not just *"accepted"*.
      const nodeCount = response.applied_node_ids.length;
      const connCount = response.applied_connection_count;
      _pushDecisionMessage(`[Accepted] applied ${nodeCount} node(s), ${connCount} connection(s).`);
      // Drop the persisted ``lastResult.diff_payload`` so a refresh after
      // a successful accept doesn't re-hydrate the (already-applied) diff.
      _clearAgentDiffPayload();
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
      const handled = _handleError(err);
      // On 409 drift the diff stays staged. On any other error we also
      // keep the diff so the user can retry without losing context — the
      // backend already rolled back the graph (per W41's apply contract).
      // Exception: a 404 means the backend has lost the diff entirely
      // (typically: ``flowfile_core`` was restarted) — keeping it staged
      // would just guarantee another 404 on the next click. Clear it and
      // surface a toast.
      if (handled?.kind === "not_found") {
        _handleStaleDiff();
      }
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
      // Mirror the reject decision in the chat trail. The note is load-
      // bearing: it's the same string W49 forwards to the planner as the
      // synthetic followup user-turn, so showing it in the trail keeps
      // the chat and the agent's input view in sync. Empty note → bare
      // ``[Rejected]`` so the user still sees the decision happened.
      const trimmedNote = (note ?? "").trim();
      _pushDecisionMessage(trimmedNote.length > 0 ? `[Rejected] ${trimmedNote}` : "[Rejected]");

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
        // Drop the persisted ``lastResult.diff_payload`` so a refresh after
        // a successful reject doesn't re-hydrate the (already-rejected) diff.
        try {
          agentStore.clearLastResultDiffPayload();
        } catch (storeErr) {
          console.warn("ai-diff-store: clearLastResultDiffPayload failed", storeErr);
        }
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
      const handled = _handleError(err);
      // 404 = the diff is gone server-side (e.g. backend restarted). Clear
      // local state so the user isn't stuck with a phantom panel that
      // 404s again on the next click.
      if (handled?.kind === "not_found") {
        _handleStaleDiff();
      }
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
    staleNotice,
    // actions
    stage,
    setCurrentDiff,
    accept,
    reject,
    clear,
    cancel,
    dismissStaleNotice,
  };
});
