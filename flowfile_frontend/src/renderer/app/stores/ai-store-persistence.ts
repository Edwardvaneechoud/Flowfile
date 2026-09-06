// Browser-side persistence for the AI drawer.
//
// Pure helpers — no Vue/Pinia deps — so they're easy to unit-test
// without jsdom. Two buckets, both in `localStorage` so state survives
// Electron app restart / browser close:
//
//   1. Per-flow chat trail: `flowfile.ai.chat.v1.{flow_id}` for a real
//      flow id, `flowfile.ai.chat.v1.unscoped` for entry paths that open
//      the chat without a flow context. The store's flow_id watcher swaps
//      buckets on flow switch so the trail stays bound to its conversation.
//      Bare `PERSISTENCE_KEY` is exported as the versioned prefix so a
//      future schema bump can orphan a whole generation of entries.
//   2. Device-wide AI settings: `flowfile.ai.settings.v1` — provider/model
//      choice, the simple-tier split and the agent behaviour toggles. These
//      are preferences, not conversation state, so they live in one bucket
//      regardless of which flow is open. Older builds stored them inside
//      the per-flow blob; `loadPersistedAiState` still reads those legacy
//      fields so the store can migrate them once, but `persistAiState` no
//      longer writes them.
//
// `StorageLike` is the injection seam so the vitest suite stays
// node-only and the same call sites can target an in-memory mock for
// per-test isolation.

import type { ChatMessage } from "./ai-store";

export const PERSISTENCE_KEY = "flowfile.ai.chat.v1";
export const SETTINGS_PERSISTENCE_KEY = "flowfile.ai.settings.v1";
export const MAX_PERSISTED_MESSAGES = 200;

/** Flow-scoped storage key. `null` flow_id (no flow open) maps to the
 * `unscoped` bucket so chat opened from entry paths without a flow
 * context still has somewhere to land — and round-trips correctly when
 * the user later opens a flow and then comes back. */
export const chatPersistenceKey = (flowId: number | null): string =>
  `${PERSISTENCE_KEY}.${flowId === null ? "unscoped" : flowId}`;

export type PersistedAgentSurface = "agent_complex" | "agent_staged" | "agent_live";

/** Device-wide AI preferences. Every field is nullable: `null` means
 * "not set, use the store default". */
export interface PersistedAiSettings {
  selectedProvider: string | null;
  selectedModel: string | null;
  /** When true, simple-tier surfaces (cron, settings autocomplete) run on
   * ``simpleProvider`` / ``simpleModel`` instead of the main selection. */
  splitModels: boolean | null;
  /** Simple-tier provider. `null` → fall back to the main provider. */
  simpleProvider: string | null;
  /** Simple-tier model. `null` → use the provider's per-surface preset. */
  simpleModel: string | null;
  /** User-selected agent surface. `null` → the store default (``agent_live``). */
  selectedAgentSurface: PersistedAgentSurface | null;
  /** Opt-in verify-completion gate. `null` → off. */
  verifyPlanCompletion: boolean | null;
}

export interface PersistedAiState {
  messages: ChatMessage[];
  /** Conversation-scoped "Continue as agent" acceptance flag, set when the
   * user clicks the promotion banner's primary button to lock subsequent
   * sends into agent mode without re-classification. */
  agentModeAccepted?: boolean | null;
  /** **DEPRECATED** — replaced by the `mode` enum in the runtime store.
   * Read on hydration ONLY for the one-shot migration shim in
   * `ai-store.ts`; never written. */
  autoPromote?: boolean | null;
  /** **LEGACY** — the settings below used to live in the per-flow blob.
   * They are read (so the store can seed the device-wide settings bucket
   * once) but no longer written; after the next save cycle they disappear
   * from the per-flow entry. */
  selectedProvider?: string | null;
  selectedModel?: string | null;
  splitModels?: boolean | null;
  simpleProvider?: string | null;
  simpleModel?: string | null;
  selectedAgentSurface?: PersistedAgentSurface | null;
  verifyPlanCompletion?: boolean | null;
}

const EMPTY_STATE: PersistedAiState = {
  messages: [],
  agentModeAccepted: null,
};

const _AGENT_SURFACE_VALUES: ReadonlyArray<PersistedAgentSurface> = [
  "agent_complex",
  "agent_staged",
  "agent_live",
];

const isAgentSurface = (value: unknown): value is PersistedAgentSurface =>
  typeof value === "string" && (_AGENT_SURFACE_VALUES as ReadonlyArray<string>).includes(value);

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

/** Pick the settings fields out of a parsed payload, validating each one.
 * Shared by the settings bucket and the legacy per-flow fields. */
const readSettingsFields = (payload: Record<string, unknown>): PersistedAiSettings => ({
  selectedProvider: typeof payload.selectedProvider === "string" ? payload.selectedProvider : null,
  selectedModel: typeof payload.selectedModel === "string" ? payload.selectedModel : null,
  splitModels: typeof payload.splitModels === "boolean" ? payload.splitModels : null,
  simpleProvider: typeof payload.simpleProvider === "string" ? payload.simpleProvider : null,
  simpleModel: typeof payload.simpleModel === "string" ? payload.simpleModel : null,
  selectedAgentSurface: isAgentSurface(payload.selectedAgentSurface)
    ? payload.selectedAgentSurface
    : null,
  verifyPlanCompletion:
    typeof payload.verifyPlanCompletion === "boolean" ? payload.verifyPlanCompletion : null,
});

/** Parse one JSON entry into a plain object, scrubbing a corrupt entry so
 * later reads aren't repeatedly corrupt. `null` when absent or unusable. */
const readJsonObject = (store: StorageLike, key: string): Record<string, unknown> | null => {
  let raw: string | null;
  try {
    raw = store.getItem(key);
  } catch {
    return null;
  }
  if (raw === null) return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    try {
      store.removeItem(key);
    } catch {
      // Storage rejected the removal (private mode quirks) — best
      // effort, swallow.
    }
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  return parsed as Record<string, unknown>;
};

export const loadPersistedAiState = (
  storage?: StorageLike | null,
  flowId: number | null = null,
): PersistedAiState => {
  const store = resolveStorage(storage);
  if (!store) return { ...EMPTY_STATE, messages: [] };

  const payload = readJsonObject(store, chatPersistenceKey(flowId));
  if (payload === null) return { ...EMPTY_STATE, messages: [] };

  const rawMessages = Array.isArray(payload.messages) ? payload.messages : [];
  const messages = rawMessages.map(sanitizeMessage).filter((m): m is ChatMessage => m !== null);

  return {
    messages,
    agentModeAccepted:
      typeof payload.agentModeAccepted === "boolean" ? payload.agentModeAccepted : null,
    autoPromote: typeof payload.autoPromote === "boolean" ? payload.autoPromote : null,
    ...readSettingsFields(payload),
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
  // Only conversation state is written; the legacy settings fields and the
  // deprecated `autoPromote` drop off the entry on the next save.
  const trimmed: PersistedAiState = {
    messages: state.messages.slice(-MAX_PERSISTED_MESSAGES),
    agentModeAccepted: state.agentModeAccepted ?? null,
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

/** Device-wide settings bucket. Returns `null` when nothing has ever been
 * written (so the caller can tell "fresh install" apart from "all fields
 * unset" and run the legacy migration). */
export const loadPersistedAiSettings = (
  storage?: StorageLike | null,
): PersistedAiSettings | null => {
  const store = resolveStorage(storage);
  if (!store) return null;
  const payload = readJsonObject(store, SETTINGS_PERSISTENCE_KEY);
  if (payload === null) return null;
  return readSettingsFields(payload);
};

export const persistAiSettings = (
  settings: PersistedAiSettings,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  let payload: string;
  try {
    payload = JSON.stringify(settings);
  } catch {
    return;
  }
  try {
    store.setItem(SETTINGS_PERSISTENCE_KEY, payload);
  } catch {
    // Quota / disabled storage — the settings still apply in-memory.
  }
};

export const highestPersistedMessageId = (messages: ChatMessage[]): number => {
  let max = 0;
  for (const m of messages) {
    if (m.id > max) max = m.id;
  }
  return max;
};
