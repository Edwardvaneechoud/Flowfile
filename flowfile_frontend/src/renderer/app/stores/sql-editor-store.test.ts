// Unit tests for the catalog SQL editor store (CatalogApi mocked, Pinia and browser storage per test).
import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ executeSqlQuery: vi.fn() }));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: { executeSqlQuery: mocks.executeSqlQuery },
}));

import {
  DEFAULT_QUERY,
  MIN_EDITOR_HEIGHT,
  MIN_RESULTS_HEIGHT,
  clampEditorHeight,
  hasUnsavedWork,
  isBlankQuery,
  isEditedSinceSaved,
  nextQueryName,
  sqlFileName,
  tabNameFromFileName,
  uniqueTabName,
  useSqlEditorStore,
} from "./sql-editor-store";
import { HISTORY_KEY, PERSISTENCE_KEY } from "./sql-editor-store-persistence";
import type { CatalogTable, SqlQueryResult } from "../types";

function makeStorage(initial: Record<string, string> = {}) {
  const data: Record<string, string> = { ...initial };
  return {
    _data: data,
    getItem: (k: string) => (k in data ? data[k] : null),
    setItem: (k: string, v: string) => {
      data[k] = v;
    },
    removeItem: (k: string) => {
      delete data[k];
    },
  };
}

function okResult(rows: any[][] = [[1]]): SqlQueryResult {
  return {
    columns: ["a"],
    dtypes: ["Int64"],
    rows,
    total_rows: rows.length,
    truncated: false,
    execution_time_ms: 3,
    used_tables: [],
    error: null,
  };
}

function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

function vtable(overrides: Partial<CatalogTable> = {}): CatalogTable {
  return {
    id: 7,
    name: "orders_summary",
    qualified_name: "main.sales.orders_summary",
    full_table_name: "sales.orders_summary",
    sql_query: "SELECT * FROM orders",
    ...overrides,
  } as CatalogTable;
}

let storage: ReturnType<typeof makeStorage>;

beforeEach(() => {
  setActivePinia(createPinia());
  vi.clearAllMocks();
  vi.useFakeTimers();
  storage = makeStorage();
  vi.stubGlobal("window", { localStorage: storage });
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("pure helpers", () => {
  it("treats empty and the default prefill as blank", () => {
    expect(isBlankQuery("")).toBe(true);
    expect(isBlankQuery("  select * from  ")).toBe(true);
    expect(isBlankQuery(DEFAULT_QUERY)).toBe(true);
    expect(isBlankQuery("SELECT * FROM t")).toBe(false);
  });

  it("numbers new tabs after the highest auto name and de-duplicates names", () => {
    expect(nextQueryName([])).toBe("Query 1");
    expect(nextQueryName(["Query 1", "orders", "Query 4"])).toBe("Query 5");
    expect(uniqueTabName("orders", ["orders", "orders (2)"])).toBe("orders (3)");
    expect(uniqueTabName("orders", ["other"])).toBe("orders");
  });

  it("clamps the editor height between its minimum and the room left for results", () => {
    expect(clampEditorHeight(10, 800)).toBe(MIN_EDITOR_HEIGHT);
    expect(clampEditorHeight(5000, 800)).toBe(800 - MIN_RESULTS_HEIGHT);
    expect(clampEditorHeight(300.4, 800)).toBe(300);
    expect(clampEditorHeight(300, 100)).toBe(MIN_EDITOR_HEIGHT);
  });

  it("maps tab names to .sql file names and back", () => {
    expect(sqlFileName("orders by region")).toBe("orders by region.sql");
    expect(sqlFileName('a/b:c*"d')).toBe("a_b_c_d.sql");
    expect(sqlFileName("report.SQL")).toBe("report.SQL");
    expect(sqlFileName("   ")).toBe("query.sql");
    expect(tabNameFromFileName("orders.sql")).toBe("orders");
    expect(tabNameFromFileName(".sql")).toBe("Query");
  });

  it("knows when closing a tab would lose work", () => {
    const link = { tableId: 1, tableName: "t", savedQuery: "SELECT 1" };
    expect(hasUnsavedWork({ query: DEFAULT_QUERY, link: null })).toBe(false);
    expect(hasUnsavedWork({ query: "SELECT 2", link: null })).toBe(true);
    expect(hasUnsavedWork({ query: " SELECT 1 ", link })).toBe(false);
    expect(isEditedSinceSaved({ query: "SELECT 2", link })).toBe(true);
    expect(hasUnsavedWork({ query: "SELECT 2", link })).toBe(true);
  });
});

describe("hydration and persistence", () => {
  it("starts one blank tab when nothing is stored", () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    expect(store.tabs).toHaveLength(1);
    expect(store.active?.name).toBe("Query 1");
    expect(store.active?.query).toBe(DEFAULT_QUERY);
  });

  it("restores tabs, links, the active tab and layout, plus the existing run history", () => {
    storage.setItem(
      PERSISTENCE_KEY,
      JSON.stringify({
        tabs: [
          { id: "a", name: "Query 1", query: "SELECT 1", link: null },
          {
            id: "b",
            name: "orders",
            query: "SELECT 2",
            link: { tableId: 7, tableName: "x", savedQuery: "SELECT 2" },
          },
        ],
        activeTabId: "b",
        editorHeight: 333,
        maxRows: 42,
      }),
    );
    storage.setItem(HISTORY_KEY, JSON.stringify([{ query: "SELECT 9", timestamp: 1 }]));
    const store = useSqlEditorStore();
    store.ensureHydrated();
    expect(store.tabs.map((t) => t.id)).toEqual(["a", "b"]);
    expect(store.active?.link?.tableId).toBe(7);
    expect(store.editorHeight).toBe(333);
    expect(store.maxRows).toBe(42);
    expect(store.history).toEqual([{ query: "SELECT 9", timestamp: 1 }]);
  });

  it("persists edits after the debounce, and immediately on flush", () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    store.setQuery(store.active!.id, "SELECT 42");
    expect(storage._data[PERSISTENCE_KEY]).toBeUndefined();
    vi.advanceTimersByTime(300);
    expect(JSON.parse(storage._data[PERSISTENCE_KEY]).tabs[0].query).toBe("SELECT 42");

    store.renameTab(store.active!.id, "renamed");
    store.flushPersist();
    expect(JSON.parse(storage._data[PERSISTENCE_KEY]).tabs[0].name).toBe("renamed");
  });

  it("never persists results or run state", () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    store.active!.result = okResult();
    store.active!.error = "boom";
    store.setQuery(store.active!.id, "SELECT 1");
    store.flushPersist();
    expect(Object.keys(JSON.parse(storage._data[PERSISTENCE_KEY]).tabs[0]).sort()).toEqual([
      "id",
      "link",
      "name",
      "query",
    ]);
  });
});

describe("tab lifecycle", () => {
  it("closing the active tab activates its neighbour; closing the last opens a fresh one", () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    const a = store.active!.id;
    const b = store.newTab().id;
    const c = store.newTab().id;
    store.setActive(b);
    store.closeTab(b);
    expect(store.activeTabId).toBe(c);
    store.closeTab(c);
    expect(store.activeTabId).toBe(a);
    store.closeTab(a);
    expect(store.tabs).toHaveLength(1);
    expect(store.active?.query).toBe(DEFAULT_QUERY);
  });

  it("duplicates a linked tab as an unlinked draft", () => {
    const store = useSqlEditorStore();
    const linked = store.openVirtualTable(vtable());
    const copy = store.duplicateTab(linked.id)!;
    expect(copy.link).toBeNull();
    expect(copy.query).toBe(linked.query);
    expect(copy.name).toBe("orders_summary copy");
  });
});

describe("openQuery", () => {
  it("fills a blank active tab and names it", () => {
    const store = useSqlEditorStore();
    const tab = store.openQuery("SELECT * FROM orders", "orders");
    expect(store.tabs).toHaveLength(1);
    expect(tab.name).toBe("orders");
    expect(store.active?.query).toBe("SELECT * FROM orders");
  });

  it("never overwrites work: opens a new tab when the active one has SQL", () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    store.setQuery(store.active!.id, "SELECT 1");
    store.openQuery("SELECT * FROM orders", "orders");
    expect(store.tabs).toHaveLength(2);
    expect(store.tabs[0].query).toBe("SELECT 1");
    expect(store.active?.query).toBe("SELECT * FROM orders");
  });

  it("focuses a tab that already holds the same query", () => {
    const store = useSqlEditorStore();
    const first = store.openQuery("SELECT * FROM orders", "orders");
    store.newTab();
    store.openQuery("  SELECT * FROM orders ", "orders");
    expect(store.tabs).toHaveLength(2);
    expect(store.activeTabId).toBe(first.id);
  });
});

describe("virtual table links", () => {
  it("opens a virtual table's SQL linked to it", () => {
    const store = useSqlEditorStore();
    const tab = store.openVirtualTable(vtable());
    expect(tab.query).toBe("SELECT * FROM orders");
    expect(tab.link).toEqual({
      tableId: 7,
      tableName: "main.sales.orders_summary",
      savedQuery: "SELECT * FROM orders",
    });
    expect(isEditedSinceSaved(tab)).toBe(false);
  });

  it("re-opening a linked table focuses its tab and keeps unsaved edits", () => {
    const store = useSqlEditorStore();
    const tab = store.openVirtualTable(vtable());
    store.setQuery(tab.id, "SELECT * FROM orders WHERE id > 1");
    store.newTab();
    store.openVirtualTable(vtable());
    expect(store.activeTabId).toBe(tab.id);
    expect(store.active?.query).toBe("SELECT * FROM orders WHERE id > 1");
    expect(isEditedSinceSaved(store.active!)).toBe(true);
  });

  it("opens as an unlinked draft without manage access", () => {
    const store = useSqlEditorStore();
    const tab = store.openVirtualTable(vtable(), { link: false });
    expect(tab.link).toBeNull();
    expect(tab.query).toBe("SELECT * FROM orders");
  });

  it("linkTab records the saved query and names only auto-named tabs", () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    const auto = store.active!;
    store.linkTab(auto.id, vtable(), "SELECT 1");
    expect(auto.name).toBe("orders_summary");
    expect(auto.link?.savedQuery).toBe("SELECT 1");

    const named = store.newTab("SELECT 2", "my exploration");
    store.linkTab(named.id, vtable({ id: 8, name: "other" }), "SELECT 2");
    expect(named.name).toBe("my exploration");

    store.unlinkTab(named.id);
    expect(named.link).toBeNull();
  });
});

describe("runQuery", () => {
  it("delivers the result to the tab that ran it, even after switching tabs", async () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    const a = store.active!;
    store.setQuery(a.id, "SELECT 1");
    const pending = deferred<SqlQueryResult>();
    mocks.executeSqlQuery.mockReturnValueOnce(pending.promise);

    const run = store.runQuery();
    expect(a.executing).toBe(true);
    const b = store.newTab();
    pending.resolve(okResult([[1], [2]]));
    await run;

    expect(a.executing).toBe(false);
    expect(a.result?.total_rows).toBe(2);
    expect(a.lastExecutedQuery).toBe("SELECT 1");
    expect(b.result).toBeNull();
    expect(store.history[0].query).toBe("SELECT 1");
    expect(mocks.executeSqlQuery).toHaveBeenCalledWith("SELECT 1", store.maxRows);
  });

  it("ignores a superseded response", async () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    const tab = store.active!;
    store.setQuery(tab.id, "SELECT 1");
    const slow = deferred<SqlQueryResult>();
    mocks.executeSqlQuery.mockReturnValueOnce(slow.promise);
    mocks.executeSqlQuery.mockResolvedValueOnce(okResult([[2]]));

    const first = store.runQuery(tab.id);
    await store.runQuery(tab.id);
    slow.resolve(okResult([[1], [1], [1]]));
    await first;

    expect(tab.result?.total_rows).toBe(1);
    expect(tab.executing).toBe(false);
  });

  it("shows backend and transport errors on the tab", async () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    const tab = store.active!;
    store.setQuery(tab.id, "SELECT nope");
    mocks.executeSqlQuery.mockResolvedValueOnce({ ...okResult(), error: "no such column" });
    await store.runQuery();
    expect(tab.error).toBe("no such column");
    expect(tab.result).toBeNull();

    mocks.executeSqlQuery.mockRejectedValueOnce({ response: { data: { detail: "offline" } } });
    await store.runQuery();
    expect(tab.error).toBe("offline");
    expect(tab.executing).toBe(false);
  });

  it("does nothing for an empty query", async () => {
    const store = useSqlEditorStore();
    store.ensureHydrated();
    store.setQuery(store.active!.id, "   ");
    await store.runQuery();
    expect(mocks.executeSqlQuery).not.toHaveBeenCalled();
  });
});
