// Browser-side session persistence for the W40 planner agent store.
//
// Mirrors the W27 pattern (`ai-store-persistence.ts`) — the regular chat
// drawer already persists across refresh; the agent's events / drift detail
// / session id were getting wiped on every page reload, which is a real UX
// loss when the user runs an agent then F5s. Same shape: pure helpers, no
// Vue/Pinia deps, `sessionStorage` for per-tab semantics, MAX cap to keep
// the JSON payload bounded.
//
// Hydration normalises an in-flight `status === "running"` to `"idle"`
// because the SSE stream is gone post-refresh and there is no re-attach
// route in W40 (the resume route is for D006 drift-pause only). Same rule
// for `paused_drift` — the user can still see the drift detail and pick up
// from there using the existing resume buttons (W45 Q2 wired
// `currentSessionId` from the wire so the buttons fire correctly).

import type { AgentDriftDetail } from "../api/ai.api";
import type { AgentCompleteResult } from "../services/aiStreamClient";
import type { AgentEvent, AgentStoreStatus } from "./ai-agent-store";

export const AGENT_PERSISTENCE_KEY = "flowfile.ai.agent.v1";
export const MAX_PERSISTED_AGENT_EVENTS = 200;

export interface PersistedAgentState {
  events: AgentEvent[];
  currentSessionId: string | null;
  status: AgentStoreStatus;
  driftDetail: AgentDriftDetail | null;
  lastResult: AgentCompleteResult | null;
  error: string | null;
}

const EMPTY_STATE: PersistedAgentState = {
  events: [],
  currentSessionId: null,
  status: "idle",
  driftDetail: null,
  lastResult: null,
  error: null,
};

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const resolveStorage = (storage?: StorageLike | null): StorageLike | null => {
  if (storage) return storage;
  if (typeof window === "undefined") return null;
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
};

const VALID_STATUSES: ReadonlySet<AgentStoreStatus> = new Set([
  "idle",
  "running",
  "paused_drift",
  "paused_user_action",
  "completed",
  "aborted",
  "failed",
]);

const VALID_EVENT_KINDS: ReadonlySet<AgentEvent["kind"]> = new Set([
  "thinking",
  "tool_call_proposed",
  "tool_call_staged",
  "tool_call_warned",
  "tool_call_rejected",
  "drift_detected",
  "paused",
  "retry",
  "abort",
  "complete",
  "info",
]);

const isAgentEventKind = (value: unknown): value is AgentEvent["kind"] =>
  typeof value === "string" && VALID_EVENT_KINDS.has(value as AgentEvent["kind"]);

const sanitizeEvent = (raw: unknown): AgentEvent | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const obj = raw as Record<string, unknown>;
  if (!isAgentEventKind(obj.kind)) return null;
  if (typeof obj.payload !== "object" || obj.payload === null) return null;
  if (typeof obj.at !== "number") return null;
  return {
    kind: obj.kind,
    payload: obj.payload as Record<string, unknown>,
    at: obj.at,
  };
};

const sanitizeDriftDetail = (raw: unknown): AgentDriftDetail | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const obj = raw as Record<string, unknown>;
  const missing = Array.isArray(obj.missingNodeIds)
    ? obj.missingNodeIds.filter((n) => typeof n === "number")
    : [];
  const externalAdded = Array.isArray(obj.externalAddedNodeIds)
    ? obj.externalAddedNodeIds.filter((n) => typeof n === "number")
    : [];
  const nodeTypes: Record<number, string> = {};
  if (obj.nodeTypes && typeof obj.nodeTypes === "object") {
    for (const [k, v] of Object.entries(obj.nodeTypes as Record<string, unknown>)) {
      const id = Number(k);
      if (Number.isFinite(id) && typeof v === "string") nodeTypes[id] = v;
    }
  }
  return { missingNodeIds: missing, externalAddedNodeIds: externalAdded, nodeTypes };
};

const sanitizeLastResult = (raw: unknown): AgentCompleteResult | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const obj = raw as Record<string, unknown>;
  if (typeof obj.session_id !== "string") return null;
  return {
    session_id: obj.session_id,
    diff_id: typeof obj.diff_id === "string" ? obj.diff_id : null,
    op_count: typeof obj.op_count === "number" ? obj.op_count : 0,
    rationale: typeof obj.rationale === "string" ? obj.rationale : null,
    diff_payload:
      obj.diff_payload && typeof obj.diff_payload === "object"
        ? (obj.diff_payload as Record<string, unknown>)
        : null,
  };
};

const normaliseStatus = (raw: unknown): AgentStoreStatus => {
  if (typeof raw !== "string") return "idle";
  if (!VALID_STATUSES.has(raw as AgentStoreStatus)) return "idle";
  // The SSE stream is dead post-refresh; W40 has no re-attach route. A
  // session that was "running" has effectively been orphaned — collapse
  // to "idle" so the UI shows a fresh slate. `paused_drift` survives
  // because the resume route lets the user pick back up.
  if (raw === "running") return "idle";
  return raw as AgentStoreStatus;
};

export const loadPersistedAgentState = (storage?: StorageLike | null): PersistedAgentState => {
  const store = resolveStorage(storage);
  if (!store) return { ...EMPTY_STATE, events: [] };

  let raw: string | null;
  try {
    raw = store.getItem(AGENT_PERSISTENCE_KEY);
  } catch {
    return { ...EMPTY_STATE, events: [] };
  }
  if (raw === null) return { ...EMPTY_STATE, events: [] };

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    try {
      store.removeItem(AGENT_PERSISTENCE_KEY);
    } catch {
      // private mode / quota — best effort.
    }
    return { ...EMPTY_STATE, events: [] };
  }

  if (typeof parsed !== "object" || parsed === null) {
    return { ...EMPTY_STATE, events: [] };
  }
  const payload = parsed as Record<string, unknown>;

  const rawEvents = Array.isArray(payload.events) ? payload.events : [];
  const events = rawEvents.map(sanitizeEvent).filter((e): e is AgentEvent => e !== null);

  return {
    events,
    currentSessionId:
      typeof payload.currentSessionId === "string" ? payload.currentSessionId : null,
    status: normaliseStatus(payload.status),
    driftDetail: sanitizeDriftDetail(payload.driftDetail),
    lastResult: sanitizeLastResult(payload.lastResult),
    error: typeof payload.error === "string" ? payload.error : null,
  };
};

export const persistAgentState = (
  state: PersistedAgentState,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;

  const trimmed: PersistedAgentState = {
    events: state.events.slice(-MAX_PERSISTED_AGENT_EVENTS),
    currentSessionId: state.currentSessionId,
    status: state.status,
    driftDetail: state.driftDetail,
    lastResult: state.lastResult,
    error: state.error,
  };

  let payload: string;
  try {
    payload = JSON.stringify(trimmed);
  } catch {
    return;
  }

  try {
    store.setItem(AGENT_PERSISTENCE_KEY, payload);
  } catch {
    // QuotaExceededError or storage disabled — fail silent.
  }
};

export const clearPersistedAgentState = (storage?: StorageLike | null): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  try {
    store.removeItem(AGENT_PERSISTENCE_KEY);
  } catch {
    // ignore
  }
};
