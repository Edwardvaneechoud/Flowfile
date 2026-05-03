// Browser-side session persistence for the AI chat drawer (W27).
//
// Pure helpers — no Vue/Pinia deps — so they're easy to unit-test and trivial
// for W42/W43 to delete or migrate when server-side per-flow chat history
// (D007 sidecar) lands. Per-tab semantics via `sessionStorage` (not
// `localStorage`) match the "ephemeral while iterating" framing and avoid
// cross-tab bleed. Capped at MAX_PERSISTED_MESSAGES so an unbounded
// conversation can't blow past sessionStorage's ~5 MB ceiling.
//
// Storage key is versioned (`flowfile.ai.chat.v1`) so a future schema bump
// — or W42/W43's invalidation pass — can ignore stale shapes by writing a
// new key and leaving the old one orphaned for the user to drop.

import type { ChatMessage } from "./ai-store";

export const PERSISTENCE_KEY = "flowfile.ai.chat.v1";
export const MAX_PERSISTED_MESSAGES = 200;

export interface PersistedAiState {
  messages: ChatMessage[];
  selectedProvider: string | null;
  selectedModel: string | null;
}

const EMPTY_STATE: PersistedAiState = {
  messages: [],
  selectedProvider: null,
  selectedModel: null,
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

const isChatRole = (value: unknown): value is "user" | "assistant" =>
  value === "user" || value === "assistant";

const sanitizeMessage = (raw: unknown): ChatMessage | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const obj = raw as Record<string, unknown>;
  if (typeof obj.id !== "number") return null;
  if (!isChatRole(obj.role)) return null;
  if (typeof obj.content !== "string") return null;
  // A pending message means the stream was open at persist time. On
  // hydration the stream is gone, so resurrect it as a non-pending message
  // (with the partial content) rather than a stuck spinner.
  return {
    id: obj.id,
    role: obj.role,
    content: obj.content,
    pending: false,
    error: typeof obj.error === "string" ? obj.error : null,
  };
};

export const loadPersistedAiState = (storage?: StorageLike | null): PersistedAiState => {
  const store = resolveStorage(storage);
  if (!store) return { ...EMPTY_STATE, messages: [] };

  let raw: string | null;
  try {
    raw = store.getItem(PERSISTENCE_KEY);
  } catch {
    return { ...EMPTY_STATE, messages: [] };
  }
  if (raw === null) return { ...EMPTY_STATE, messages: [] };

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    // Corrupt JSON — drop it so the user starts fresh next time and we don't
    // crash on every drawer open.
    try {
      store.removeItem(PERSISTENCE_KEY);
    } catch {
      // sessionStorage rejected the removal (private mode quirks) — best
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
  };
};

export const persistAiState = (state: PersistedAiState, storage?: StorageLike | null): void => {
  const store = resolveStorage(storage);
  if (!store) return;

  // Cap before serialization so the JSON payload itself is bounded. Keeps
  // the most recent N messages — chat history is most useful at the tail.
  const trimmed: PersistedAiState = {
    messages: state.messages.slice(-MAX_PERSISTED_MESSAGES),
    selectedProvider: state.selectedProvider,
    selectedModel: state.selectedModel,
  };

  let payload: string;
  try {
    payload = JSON.stringify(trimmed);
  } catch {
    return;
  }

  try {
    store.setItem(PERSISTENCE_KEY, payload);
  } catch {
    // QuotaExceededError or storage disabled — fail silent. The chat
    // continues to work in-memory; the user just loses refresh-survival.
  }
};

export const clearPersistedAiState = (storage?: StorageLike | null): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  try {
    store.removeItem(PERSISTENCE_KEY);
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
