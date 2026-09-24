// Unit tests for the SQL editor's browser-persistence helpers (injected StorageLike, no jsdom).
import { describe, expect, it } from "vitest";
import {
  DEFAULT_EDITOR_HEIGHT,
  DEFAULT_MAX_ROWS,
  HISTORY_KEY,
  MAX_HISTORY,
  MAX_ROWS_LIMIT,
  PERSISTENCE_KEY,
  loadPersistedSqlEditor,
  loadSqlHistory,
  persistSqlEditor,
  persistSqlHistory,
  type PersistedSqlEditorState,
} from "./sql-editor-store-persistence";
import type { StorageLike } from "./notebook-store-persistence";

function makeStorage(
  initial: Record<string, string> = {},
): StorageLike & { _data: Record<string, string> } {
  const data: Record<string, string> = { ...initial };
  return {
    _data: data,
    getItem: (k) => (k in data ? data[k] : null),
    setItem: (k, v) => {
      data[k] = v;
    },
    removeItem: (k) => {
      delete data[k];
    },
  };
}

const sample = (): PersistedSqlEditorState => ({
  tabs: [
    { id: "a", name: "Query 1", query: "SELECT 1", link: null },
    {
      id: "b",
      name: "orders",
      query: "SELECT * FROM orders WHERE x > 1",
      link: { tableId: 7, tableName: "main.sales.orders", savedQuery: "SELECT * FROM orders" },
    },
  ],
  activeTabId: "b",
  editorHeight: 340,
  maxRows: 500,
});

describe("loadPersistedSqlEditor / persistSqlEditor", () => {
  it("round-trips tabs, links, the active tab and layout", () => {
    const storage = makeStorage();
    persistSqlEditor(sample(), storage);
    expect(loadPersistedSqlEditor(storage)).toEqual(sample());
  });

  it("returns defaults when nothing is stored", () => {
    expect(loadPersistedSqlEditor(makeStorage())).toEqual({
      tabs: [],
      activeTabId: null,
      editorHeight: DEFAULT_EDITOR_HEIGHT,
      maxRows: DEFAULT_MAX_ROWS,
    });
  });

  it("drops corrupt JSON and clears the key", () => {
    const storage = makeStorage({ [PERSISTENCE_KEY]: "{not json" });
    expect(loadPersistedSqlEditor(storage).tabs).toEqual([]);
    expect(storage._data[PERSISTENCE_KEY]).toBeUndefined();
  });

  it("sanitizes malformed tabs, duplicate ids, bad links and out-of-range numbers", () => {
    const storage = makeStorage({
      [PERSISTENCE_KEY]: JSON.stringify({
        tabs: [
          { id: "a", name: "A", query: "SELECT 1", link: { tableId: "7", savedQuery: "x" } },
          { id: "a", name: "dup", query: "SELECT 2" },
          { name: "no id" },
          { id: "c", name: "  ", query: 5, link: { tableId: 3, savedQuery: "SELECT 3" } },
        ],
        activeTabId: 12,
        editorHeight: -20,
        maxRows: 10_000_000,
      }),
    });
    const loaded = loadPersistedSqlEditor(storage);
    expect(loaded.tabs).toEqual([
      { id: "a", name: "A", query: "SELECT 1", link: null },
      {
        id: "c",
        name: "Query",
        query: "",
        link: { tableId: 3, tableName: "table 3", savedQuery: "SELECT 3" },
      },
    ]);
    expect(loaded.activeTabId).toBeNull();
    expect(loaded.editorHeight).toBe(DEFAULT_EDITOR_HEIGHT);
    expect(loaded.maxRows).toBe(MAX_ROWS_LIMIT);
  });
});

describe("loadSqlHistory / persistSqlHistory", () => {
  it("keeps the pre-existing history key and filters malformed entries", () => {
    const storage = makeStorage({
      [HISTORY_KEY]: JSON.stringify([
        { query: "SELECT 1", timestamp: 1 },
        { query: 2, timestamp: 2 },
        null,
        { query: "SELECT 3", timestamp: 3 },
      ]),
    });
    expect(loadSqlHistory(storage)).toEqual([
      { query: "SELECT 1", timestamp: 1 },
      { query: "SELECT 3", timestamp: 3 },
    ]);
  });

  it("caps what it writes at MAX_HISTORY", () => {
    const storage = makeStorage();
    const many = Array.from({ length: MAX_HISTORY + 5 }, (_, i) => ({
      query: `SELECT ${i}`,
      timestamp: i,
    }));
    persistSqlHistory(many, storage);
    expect(JSON.parse(storage._data[HISTORY_KEY])).toHaveLength(MAX_HISTORY);
  });
});
