// localStorage persistence for open notebook tabs (cells in wire format); outputs, exec-state and sessionFlowId are intentionally not persisted.
import type { NotebookCellType, NotebookCellWire } from "../api/notebook.api";

export const PERSISTENCE_KEY = "flowfile.notebook.v1";
export const MAX_PERSISTED_NOTEBOOKS = 10;

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

export interface PersistedNotebook {
  tabId: string;
  persistedId: number | null;
  name: string;
  description: string | null;
  namespaceId: number | null;
  cells: NotebookCellWire[];
  kernelId: string | null;
  dirty: boolean;
}

export interface PersistedNotebookState {
  openNotebooks: PersistedNotebook[];
  activeTabId: string | null;
}

const EMPTY: PersistedNotebookState = { openNotebooks: [], activeTabId: null };

/** SQL cells were dropped — coerce any legacy type to a supported one. */
const coerceType = (t: unknown): NotebookCellType => (t === "markdown" ? "markdown" : "python");

const sanitizeCell = (raw: unknown): NotebookCellWire | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  if (typeof o.id !== "string") return null;
  return {
    id: o.id,
    type: coerceType(o.type),
    source: typeof o.source === "string" ? o.source : "",
    metadata:
      typeof o.metadata === "object" && o.metadata !== null
        ? (o.metadata as Record<string, any>)
        : {},
  };
};

const sanitizeNotebook = (raw: unknown): PersistedNotebook | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  if (typeof o.tabId !== "string") return null;
  const cells = (Array.isArray(o.cells) ? o.cells : [])
    .map(sanitizeCell)
    .filter((c): c is NotebookCellWire => c !== null);
  return {
    tabId: o.tabId,
    persistedId: typeof o.persistedId === "number" ? o.persistedId : null,
    name: typeof o.name === "string" ? o.name : "Untitled notebook",
    description: typeof o.description === "string" ? o.description : null,
    namespaceId: typeof o.namespaceId === "number" ? o.namespaceId : null,
    cells,
    kernelId: typeof o.kernelId === "string" ? o.kernelId : null,
    dirty: typeof o.dirty === "boolean" ? o.dirty : false,
  };
};

export const loadPersistedNotebooks = (storage?: StorageLike | null): PersistedNotebookState => {
  const store = resolveStorage(storage);
  if (!store) return { ...EMPTY };
  let raw: string | null;
  try {
    raw = store.getItem(PERSISTENCE_KEY);
  } catch {
    return { ...EMPTY };
  }
  if (raw === null) return { ...EMPTY };
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    try {
      store.removeItem(PERSISTENCE_KEY);
    } catch {
      // private mode / disabled — best effort
    }
    return { ...EMPTY };
  }
  if (typeof parsed !== "object" || parsed === null) return { ...EMPTY };
  const p = parsed as Record<string, unknown>;
  const openNotebooks = (Array.isArray(p.openNotebooks) ? p.openNotebooks : [])
    .map(sanitizeNotebook)
    .filter((n): n is PersistedNotebook => n !== null)
    .slice(0, MAX_PERSISTED_NOTEBOOKS);
  const activeTabId = typeof p.activeTabId === "string" ? p.activeTabId : null;
  return { openNotebooks, activeTabId };
};

export const persistNotebooks = (
  state: PersistedNotebookState,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  const trimmed: PersistedNotebookState = {
    openNotebooks: state.openNotebooks.slice(0, MAX_PERSISTED_NOTEBOOKS),
    activeTabId: state.activeTabId,
  };
  try {
    store.setItem(PERSISTENCE_KEY, JSON.stringify(trimmed));
  } catch {
    // QuotaExceededError or storage disabled — best effort.
  }
};

export const clearPersistedNotebooks = (storage?: StorageLike | null): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  try {
    store.removeItem(PERSISTENCE_KEY);
  } catch {
    // best effort
  }
};
