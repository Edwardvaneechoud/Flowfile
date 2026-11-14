// Remembers how many versions each catalog table listed, so the table page can size the
// Version History placeholder to what will load. A view preference, hence localStorage.

export const HISTORY_ROWS_KEY = "flowfile.catalog.tableHistoryRows.v1";

// Past this many tables the map starts over; a forgotten table only costs a taller placeholder.
const MAX_TABLES = 500;

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

const resolveStorage = (storage?: StorageLike | null): StorageLike | null => {
  if (storage) return storage;
  try {
    return (globalThis as { localStorage?: StorageLike }).localStorage ?? null;
  } catch {
    return null;
  }
};

const readRows = (store: StorageLike): Record<string, number> => {
  try {
    const parsed: unknown = JSON.parse(store.getItem(HISTORY_ROWS_KEY) ?? "{}");
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return {};
    return parsed as Record<string, number>;
  } catch {
    return {};
  }
};

/** The version count last seen for a table, or null when it was never loaded in this browser. */
export const recallHistoryRows = (tableId: number, storage?: StorageLike | null): number | null => {
  const store = resolveStorage(storage);
  if (!store) return null;
  const rows = readRows(store)[String(tableId)];
  return Number.isInteger(rows) && rows >= 0 ? rows : null;
};

export const rememberHistoryRows = (
  tableId: number,
  rows: number,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;
  const known = readRows(store);
  if (known[String(tableId)] === rows) return;
  const next = Object.keys(known).length >= MAX_TABLES ? {} : known;
  next[String(tableId)] = rows;
  try {
    store.setItem(HISTORY_ROWS_KEY, JSON.stringify(next));
  } catch {
    // Quota or storage disabled: the placeholder falls back to its full height.
  }
};
