// Pure persistence helpers for the catalog tree's expand/collapse state.
// No Vue imports — the `StorageLike` seam keeps these unit-testable in the
// node-env Vitest runner (mirrors stores/ai-store-persistence.ts).

export const TREE_EXPANSION_KEY = "flowfile.catalog.treeExpansion.v1";

export type ExpansionRecord = Record<string, boolean>;

export interface StorageLike {
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

export const loadTreeExpansion = (storage?: StorageLike | null): ExpansionRecord => {
  const store = resolveStorage(storage);
  if (!store) return {};

  let raw: string | null;
  try {
    raw = store.getItem(TREE_EXPANSION_KEY);
  } catch {
    return {};
  }
  if (raw === null) return {};

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    try {
      store.removeItem(TREE_EXPANSION_KEY);
    } catch {
      // Best effort — storage rejected the removal.
    }
    return {};
  }

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return {};

  const record: ExpansionRecord = {};
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof value === "boolean") record[key] = value;
  }
  return record;
};

export const persistTreeExpansion = (
  record: ExpansionRecord,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;

  let payload: string;
  try {
    payload = JSON.stringify(record);
  } catch {
    return;
  }

  try {
    store.setItem(TREE_EXPANSION_KEY, payload);
  } catch {
    // QuotaExceededError or storage disabled — the tree still works
    // in-memory, it just loses refresh-survival.
  }
};
