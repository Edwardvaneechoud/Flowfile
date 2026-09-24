// localStorage persistence for the catalog SQL editor (open query tabs, layout, run history); results are intentionally not persisted.
import type { StorageLike } from "./notebook-store-persistence";

export const PERSISTENCE_KEY = "flowfile.sql-editor.v1";
// Pre-existing key, kept so run history survives the move into the store.
export const HISTORY_KEY = "flowfile_sql_history";
export const MAX_PERSISTED_TABS = 50;
export const MAX_HISTORY = 50;
export const DEFAULT_EDITOR_HEIGHT = 220;
export const DEFAULT_MAX_ROWS = 10_000;
export const MAX_ROWS_LIMIT = 100_000;

// The catalog virtual table a tab was saved as; `savedQuery` is what the table holds.
export interface SqlTabLink {
  tableId: number;
  tableName: string;
  savedQuery: string;
}

export interface PersistedSqlTab {
  id: string;
  name: string;
  query: string;
  link: SqlTabLink | null;
}

export interface PersistedSqlEditorState {
  tabs: PersistedSqlTab[];
  activeTabId: string | null;
  editorHeight: number;
  maxRows: number;
}

export interface SqlHistoryItem {
  query: string;
  timestamp: number;
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

const readJson = (store: StorageLike, key: string): unknown => {
  let raw: string | null;
  try {
    raw = store.getItem(key);
  } catch {
    return null;
  }
  if (raw === null) return null;
  try {
    return JSON.parse(raw);
  } catch {
    try {
      store.removeItem(key);
    } catch {
      // private mode / disabled — best effort
    }
    return null;
  }
};

const writeJson = (store: StorageLike, key: string, value: unknown): void => {
  try {
    store.setItem(key, JSON.stringify(value));
  } catch {
    // QuotaExceededError or storage disabled — best effort.
  }
};

const emptyState = (): PersistedSqlEditorState => ({
  tabs: [],
  activeTabId: null,
  editorHeight: DEFAULT_EDITOR_HEIGHT,
  maxRows: DEFAULT_MAX_ROWS,
});

const sanitizeLink = (raw: unknown): SqlTabLink | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  if (typeof o.tableId !== "number" || typeof o.savedQuery !== "string") return null;
  return {
    tableId: o.tableId,
    tableName: typeof o.tableName === "string" ? o.tableName : `table ${o.tableId}`,
    savedQuery: o.savedQuery,
  };
};

const sanitizeTab = (raw: unknown): PersistedSqlTab | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  if (typeof o.id !== "string" || !o.id) return null;
  return {
    id: o.id,
    name: typeof o.name === "string" && o.name.trim() ? o.name : "Query",
    query: typeof o.query === "string" ? o.query : "",
    link: sanitizeLink(o.link),
  };
};

const isPositiveNumber = (v: unknown): v is number =>
  typeof v === "number" && Number.isFinite(v) && v > 0;

export const loadPersistedSqlEditor = (storage?: StorageLike | null): PersistedSqlEditorState => {
  const store = resolveStorage(storage);
  if (!store) return emptyState();
  const parsed = readJson(store, PERSISTENCE_KEY);
  if (typeof parsed !== "object" || parsed === null) return emptyState();
  const p = parsed as Record<string, unknown>;
  const tabs: PersistedSqlTab[] = [];
  for (const raw of Array.isArray(p.tabs) ? p.tabs : []) {
    const tab = sanitizeTab(raw);
    if (tab && !tabs.some((t) => t.id === tab.id)) tabs.push(tab);
  }
  return {
    tabs: tabs.slice(0, MAX_PERSISTED_TABS),
    activeTabId: typeof p.activeTabId === "string" ? p.activeTabId : null,
    editorHeight: isPositiveNumber(p.editorHeight) ? p.editorHeight : DEFAULT_EDITOR_HEIGHT,
    maxRows: isPositiveNumber(p.maxRows)
      ? Math.min(Math.round(p.maxRows), MAX_ROWS_LIMIT)
      : DEFAULT_MAX_ROWS,
  };
};

export const persistSqlEditor = (
  state: PersistedSqlEditorState,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  writeJson(store, PERSISTENCE_KEY, {
    ...state,
    tabs: state.tabs.slice(0, MAX_PERSISTED_TABS),
  });
};

export const loadSqlHistory = (storage?: StorageLike | null): SqlHistoryItem[] => {
  const store = resolveStorage(storage);
  if (!store) return [];
  const parsed = readJson(store, HISTORY_KEY);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .filter(
      (h): h is SqlHistoryItem =>
        typeof h === "object" &&
        h !== null &&
        typeof h.query === "string" &&
        typeof h.timestamp === "number",
    )
    .slice(0, MAX_HISTORY);
};

export const persistSqlHistory = (
  history: SqlHistoryItem[],
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  writeJson(store, HISTORY_KEY, history.slice(0, MAX_HISTORY));
};
