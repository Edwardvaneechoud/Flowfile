// Browser-side chat persistence for the AI drawer (W27 → W43).
//
// Pure helpers — no Vue/Pinia deps — so they're easy to unit-test without
// jsdom. W43 changes the surface in two ways relative to W27's original cut:
//
//   1. Storage default flips from `sessionStorage` to `localStorage` so chat
//      survives Electron app restart / browser close. Quota is still ~5 MB,
//      plenty for chat history bounded by `MAX_PERSISTED_MESSAGES`.
//   2. Persistence keys are flow-scoped: `flowfile.ai.chat.v1.{flow_id}`
//      for a real flow id, `flowfile.ai.chat.v1.unscoped` for entry paths
//      that open the chat without a flow context. The store's flow_id
//      watcher uses these helpers to swap the persisted bucket when the
//      user switches flows so the trail stays bound to the conversation
//      it belongs to. Bare `PERSISTENCE_KEY` is still exported as the
//      versioned prefix so a future schema bump can orphan a whole
//      generation of entries by writing under a new prefix.
//
// Storage-injection seam (`StorageLike`) preserved verbatim from W27 so the
// vitest suite stays node-only and the same call sites can target an
// in-memory mock for per-test isolation.

import type { ChatMessage } from "./ai-store";

export const PERSISTENCE_KEY = "flowfile.ai.chat.v1";
export const MAX_PERSISTED_MESSAGES = 200;

/** Flow-scoped storage key. `null` flow_id (no flow open) maps to the
 * `unscoped` bucket so chat opened from entry paths without a flow
 * context still has somewhere to land — and round-trips correctly when
 * the user later opens a flow and then comes back. */
export const chatPersistenceKey = (flowId: number | null): string =>
  `${PERSISTENCE_KEY}.${flowId === null ? "unscoped" : flowId}`;

export type PersistedAgentSurface = "agent_complex" | "agent_staged";

export interface PersistedAiState {
  messages: ChatMessage[];
  selectedProvider: string | null;
  selectedModel: string | null;
  /** W58 — chat → agent auto-promotion preference. ``null`` (or omitted)
   * means "no persisted value, use the store's default" so an existing
   * v1 entry predating W58 doesn't get its absent field treated as
   * ``false``. Optional so callers from the W27 era don't need to pass
   * an explicit ``null``. */
  autoPromote?: boolean | null;
  /** W58 round 7 — session-scoped "Continue as agent" acceptance flag,
   * set when the user clicks the promotion banner's primary button to
   * lock subsequent sends into agent mode without re-classification.
   * Optional / nullable for the same backward-compat reason as
   * ``autoPromote`` — pre-W58-round-7 entries don't carry the field. */
  agentModeAccepted?: boolean | null;
  /** W71 v1.9 — user-selected agent surface ("agent" / "agent_complex" /
   * "agent_staged"). Optional / nullable so pre-v1.9 entries fall
   * through to the store's default (``"agent_staged"``). */
  selectedAgentSurface?: PersistedAgentSurface | null;
}

const EMPTY_STATE: PersistedAiState = {
  messages: [],
  selectedProvider: null,
  selectedModel: null,
  autoPromote: null,
  agentModeAccepted: null,
  selectedAgentSurface: null,
};

const _AGENT_SURFACE_VALUES: ReadonlyArray<PersistedAgentSurface> = [
  "agent_complex",
  "agent_staged",
];

const isAgentSurface = (value: unknown): value is PersistedAgentSurface =>
  typeof value === "string" &&
  (_AGENT_SURFACE_VALUES as ReadonlyArray<string>).includes(value);

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const resolveStorage = (storage?: StorageLike | null): StorageLike | null => {
  if (storage) return storage;
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
};

const isChatRole = (value: unknown): value is "user" | "assistant" =>
  value === "user" || value === "assistant";

const sanitizeMessage = (raw: unknown): ChatMessage | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const obj = raw as Record<string, unknown>;
  if (typeof obj.id !== "number") return null;
  if (!isChatRole(obj.role)) return null;
  if (typeof obj.content !== "string") return null;
  // ``createdAt`` was added after the persistence format shipped — older
  // entries in storage may not have it. Fall back to ``id`` (small
  // counter, but at least monotonic) so the timeline still has *some*
  // ordering signal. New messages always carry ``createdAt`` (Date.now).
  const createdAt = typeof obj.createdAt === "number" ? obj.createdAt : obj.id;
  // A pending message means the stream was open at persist time. On
  // hydration the stream is gone, so resurrect it as a non-pending message
  // (with the partial content) rather than a stuck spinner.
  return {
    id: obj.id,
    createdAt,
    role: obj.role,
    content: obj.content,
    pending: false,
    error: typeof obj.error === "string" ? obj.error : null,
  };
};

export const loadPersistedAiState = (
  storage?: StorageLike | null,
  flowId: number | null = null,
): PersistedAiState => {
  const store = resolveStorage(storage);
  if (!store) return { ...EMPTY_STATE, messages: [] };

  const key = chatPersistenceKey(flowId);
  let raw: string | null;
  try {
    raw = store.getItem(key);
  } catch {
    return { ...EMPTY_STATE, messages: [] };
  }
  if (raw === null) return { ...EMPTY_STATE, messages: [] };

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    // Corrupt JSON — drop just *this* flow's entry so the user starts fresh
    // next time without crashing. Other flows' keys are untouched (the
    // removal is scoped to the per-flow key, never the v1 prefix).
    try {
      store.removeItem(key);
    } catch {
      // Storage rejected the removal (private mode quirks) — best
      // effort, swallow.
    }
    return { ...EMPTY_STATE, messages: [] };
  }

  if (typeof parsed !== "object" || parsed === null) {
    return { ...EMPTY_STATE, messages: [] };
  }
  const payload = parsed as Record<string, unknown>;

  const rawMessages = Array.isArray(payload.messages) ? payload.messages : [];
  const messages = rawMessages.map(sanitizeMessage).filter((m): m is ChatMessage => m !== null);

  return {
    messages,
    selectedProvider:
      typeof payload.selectedProvider === "string" ? payload.selectedProvider : null,
    selectedModel: typeof payload.selectedModel === "string" ? payload.selectedModel : null,
    autoPromote: typeof payload.autoPromote === "boolean" ? payload.autoPromote : null,
    agentModeAccepted:
      typeof payload.agentModeAccepted === "boolean" ? payload.agentModeAccepted : null,
    selectedAgentSurface: isAgentSurface(payload.selectedAgentSurface)
      ? payload.selectedAgentSurface
      : null,
  };
};

export const persistAiState = (
  state: PersistedAiState,
  storage?: StorageLike | null,
  flowId: number | null = null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;

  // Cap before serialization so the JSON payload itself is bounded. Keeps
  // the most recent N messages — chat history is most useful at the tail.
  const trimmed: PersistedAiState = {
    messages: state.messages.slice(-MAX_PERSISTED_MESSAGES),
    selectedProvider: state.selectedProvider,
    selectedModel: state.selectedModel,
    autoPromote: state.autoPromote ?? null,
    agentModeAccepted: state.agentModeAccepted ?? null,
    selectedAgentSurface: state.selectedAgentSurface ?? null,
  };

  let payload: string;
  try {
    payload = JSON.stringify(trimmed);
  } catch {
    return;
  }

  try {
    store.setItem(chatPersistenceKey(flowId), payload);
  } catch {
    // QuotaExceededError or storage disabled — fail silent. The chat
    // continues to work in-memory; the user just loses refresh-survival.
  }
};

export const clearPersistedAiState = (
  storage?: StorageLike | null,
  flowId: number | null = null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  try {
    store.removeItem(chatPersistenceKey(flowId));
  } catch {
    // ignore
  }
};

export const highestPersistedMessageId = (messages: ChatMessage[]): number => {
  let max = 0;
  for (const m of messages) {
    if (m.id > max) max = m.id;
  }
  return max;
};
