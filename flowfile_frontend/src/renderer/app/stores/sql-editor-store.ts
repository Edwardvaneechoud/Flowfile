// Catalog SQL editor: open query tabs (browser-only drafts, optionally linked to the query virtual table they were saved as) plus per-tab run state that outlives the panel.
import { defineStore } from "pinia";
import { markRaw } from "vue";
import { CatalogApi } from "../api/catalog.api";
import type { CatalogTable, SqlQueryResult } from "../types";
import {
  DEFAULT_EDITOR_HEIGHT,
  DEFAULT_MAX_ROWS,
  MAX_HISTORY,
  MAX_ROWS_LIMIT,
  loadPersistedSqlEditor,
  loadSqlHistory,
  persistSqlEditor,
  persistSqlHistory,
  type PersistedSqlEditorState,
  type SqlHistoryItem,
  type SqlTabLink,
} from "./sql-editor-store-persistence";

export type { SqlTabLink } from "./sql-editor-store-persistence";

export const DEFAULT_QUERY = "SELECT * FROM ";
export const MIN_EDITOR_HEIGHT = 80;
export const MIN_RESULTS_HEIGHT = 120;

export interface SqlTab {
  id: string;
  name: string;
  query: string;
  link: SqlTabLink | null;
  executing: boolean;
  result: SqlQueryResult | null;
  error: string | null;
  lastExecutedQuery: string;
  // Bumped per run: stale-response guard and the results panel's remount key.
  runSeq: number;
}

/** True for a tab nobody has typed into yet, so it can be reused or closed without asking. */
export function isBlankQuery(query: string): boolean {
  const q = query.trim().toUpperCase();
  return q === "" || q === DEFAULT_QUERY.trim();
}

export function isAutoTabName(name: string): boolean {
  return /^Query \d+$/.test(name);
}

/** A linked tab whose SQL differs from what its virtual table holds. */
export function isEditedSinceSaved(tab: Pick<SqlTab, "query" | "link">): boolean {
  return !!tab.link && tab.query.trim() !== tab.link.savedQuery.trim();
}

/** Closing loses work: a draft with SQL in it, or edits its virtual table doesn't have yet. */
export function hasUnsavedWork(tab: Pick<SqlTab, "query" | "link">): boolean {
  return tab.link ? isEditedSinceSaved(tab) : !isBlankQuery(tab.query);
}

export function tableDisplayName(
  table: Pick<CatalogTable, "name" | "qualified_name" | "full_table_name">,
): string {
  return table.qualified_name ?? table.full_table_name ?? table.name;
}

export function nextQueryName(names: string[]): string {
  let max = 0;
  for (const name of names) {
    const m = /^Query (\d+)$/.exec(name);
    if (m) max = Math.max(max, Number(m[1]));
  }
  return `Query ${max + 1}`;
}

export function uniqueTabName(base: string, names: string[]): string {
  if (!names.includes(base)) return base;
  let i = 2;
  while (names.includes(`${base} (${i})`)) i++;
  return `${base} (${i})`;
}

export function sqlFileName(tabName: string): string {
  const stem = tabName.replace(/[\\/:*?"<>|]+/g, "_").trim() || "query";
  return /\.sql$/i.test(stem) ? stem : `${stem}.sql`;
}

export function tabNameFromFileName(fileName: string): string {
  return fileName.replace(/\.sql$/i, "").trim() || "Query";
}

/** Editor height kept inside `available` px, always leaving room for the results pane. */
export function clampEditorHeight(height: number, available: number): number {
  const max = Math.max(MIN_EDITOR_HEIGHT, available - MIN_RESULTS_HEIGHT);
  return Math.round(Math.min(Math.max(height, MIN_EDITOR_HEIGHT), max));
}

let _seq = 0;
function newTabId(): string {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
  _seq += 1;
  return `sql-${Date.now()}-${_seq}`;
}

function makeTab(id: string, name: string, query: string, link: SqlTabLink | null): SqlTab {
  return {
    id,
    name,
    query,
    link,
    executing: false,
    result: null,
    error: null,
    lastExecutedQuery: "",
    runSeq: 0,
  };
}

interface SqlEditorState {
  tabs: SqlTab[];
  activeTabId: string | null;
  editorHeight: number;
  maxRows: number;
  history: SqlHistoryItem[];
  hydrated: boolean;
}

let _persistTimer: ReturnType<typeof setTimeout> | null = null;

export const useSqlEditorStore = defineStore("sqlEditor", {
  state: (): SqlEditorState => ({
    tabs: [],
    activeTabId: null,
    editorHeight: DEFAULT_EDITOR_HEIGHT,
    maxRows: DEFAULT_MAX_ROWS,
    history: [],
    hydrated: false,
  }),

  getters: {
    active(state): SqlTab | null {
      return state.tabs.find((t) => t.id === state.activeTabId) ?? null;
    },
  },

  actions: {
    _snapshot(): PersistedSqlEditorState {
      return {
        tabs: this.tabs.map((t) => ({
          id: t.id,
          name: t.name,
          query: t.query,
          link: t.link ? { ...t.link } : null,
        })),
        activeTabId: this.activeTabId,
        editorHeight: this.editorHeight,
        maxRows: this.maxRows,
      };
    },

    _schedulePersist() {
      if (_persistTimer) clearTimeout(_persistTimer);
      _persistTimer = setTimeout(() => {
        _persistTimer = null;
        persistSqlEditor(this._snapshot());
      }, 300);
    },

    /** Write any pending change now (panel unmount, page unload). */
    flushPersist() {
      if (!_persistTimer) return;
      clearTimeout(_persistTimer);
      _persistTimer = null;
      persistSqlEditor(this._snapshot());
    },

    /** Restore tabs from browser storage on first use; start one blank tab if there are none. */
    ensureHydrated() {
      if (this.hydrated) return;
      this.hydrated = true;
      const persisted = loadPersistedSqlEditor();
      this.editorHeight = persisted.editorHeight;
      this.maxRows = persisted.maxRows;
      this.history = loadSqlHistory();
      this.tabs = persisted.tabs.map((t) => makeTab(t.id, t.name, t.query, t.link));
      if (!this.tabs.length) {
        this.newTab();
        return;
      }
      this.activeTabId = this.tabs.some((t) => t.id === persisted.activeTabId)
        ? persisted.activeTabId
        : this.tabs[0].id;
    },

    newTab(query: string = DEFAULT_QUERY, name?: string): SqlTab {
      const names = this.tabs.map((t) => t.name);
      this.tabs.push(makeTab(newTabId(), name ?? nextQueryName(names), query, null));
      // Re-read through the reactive array so callers mutate the tracked proxy.
      const tab = this.tabs[this.tabs.length - 1];
      this.activeTabId = tab.id;
      this._schedulePersist();
      return tab;
    },

    setActive(id: string) {
      if (this.activeTabId === id || !this.tabs.some((t) => t.id === id)) return;
      this.activeTabId = id;
      this._schedulePersist();
    },

    closeTab(id: string) {
      const idx = this.tabs.findIndex((t) => t.id === id);
      if (idx < 0) return;
      this.tabs.splice(idx, 1);
      if (this.activeTabId === id) {
        this.activeTabId = (this.tabs[idx] ?? this.tabs[idx - 1])?.id ?? null;
      }
      if (!this.tabs.length) this.newTab();
      this._schedulePersist();
    },

    renameTab(id: string, name: string) {
      const tab = this.tabs.find((t) => t.id === id);
      const trimmed = name.trim();
      if (!tab || !trimmed || tab.name === trimmed) return;
      tab.name = trimmed;
      this._schedulePersist();
    },

    setQuery(id: string, query: string) {
      const tab = this.tabs.find((t) => t.id === id);
      if (!tab || tab.query === query) return;
      tab.query = query;
      this._schedulePersist();
    },

    /** The copy is a draft: two tabs must never both update the same virtual table. */
    duplicateTab(id: string): SqlTab | null {
      const src = this.tabs.find((t) => t.id === id);
      if (!src) return null;
      const names = this.tabs.map((t) => t.name);
      return this.newTab(src.query, uniqueTabName(`${src.name} copy`, names));
    },

    /** Mark a tab as stored in `table`, holding `savedQuery`; an auto-named tab takes the table's name. */
    linkTab(id: string, table: CatalogTable, savedQuery: string) {
      const tab = this.tabs.find((t) => t.id === id);
      if (!tab) return;
      tab.link = { tableId: table.id, tableName: tableDisplayName(table), savedQuery };
      if (isAutoTabName(tab.name)) {
        const others = this.tabs.filter((t) => t.id !== id).map((t) => t.name);
        tab.name = uniqueTabName(table.name, others);
      }
      this._schedulePersist();
    },

    unlinkTab(id: string) {
      const tab = this.tabs.find((t) => t.id === id);
      if (!tab?.link) return;
      tab.link = null;
      this._schedulePersist();
    },

    /** Fill the active tab when it is an untouched draft, else open a new tab. */
    _openInTab(query: string, name: string | undefined, link: SqlTabLink | null): SqlTab {
      const active = this.active;
      if (
        active &&
        !active.link &&
        isBlankQuery(active.query) &&
        !active.executing &&
        !active.result &&
        !active.error
      ) {
        active.query = query;
        active.link = link;
        if (name && isAutoTabName(active.name)) {
          const others = this.tabs.filter((t) => t.id !== active.id).map((t) => t.name);
          active.name = uniqueTabName(name, others);
        }
        this._schedulePersist();
        return active;
      }
      const names = this.tabs.map((t) => t.name);
      const tab = this.newTab(query, name ? uniqueTabName(name, names) : undefined);
      tab.link = link;
      return tab;
    },

    /** Open `query` as a draft, focusing a tab that already holds it. */
    openQuery(query: string, name?: string): SqlTab {
      this.ensureHydrated();
      const existing = this.tabs.find((t) => t.query.trim() === query.trim());
      if (existing) {
        this.setActive(existing.id);
        return existing;
      }
      return this._openInTab(query, name, null);
    },

    /** Open a query virtual table's SQL. Linked (the default) means saving updates that
     * table; an already-linked tab is focused as-is so its unsaved edits survive. */
    openVirtualTable(table: CatalogTable, { link = true }: { link?: boolean } = {}): SqlTab {
      this.ensureHydrated();
      const sqlText = table.sql_query ?? "";
      if (link) {
        const existing = this.tabs.find((t) => t.link?.tableId === table.id);
        if (existing) {
          this.setActive(existing.id);
          return existing;
        }
      }
      const tabLink = link
        ? { tableId: table.id, tableName: tableDisplayName(table), savedQuery: sqlText }
        : null;
      return this._openInTab(sqlText, table.name, tabLink);
    },

    setEditorHeight(height: number) {
      const h = Math.max(MIN_EDITOR_HEIGHT, Math.round(height));
      if (h === this.editorHeight) return;
      this.editorHeight = h;
      this._schedulePersist();
    },

    setMaxRows(value: number | null | undefined) {
      if (value == null || !Number.isFinite(value)) return;
      this.maxRows = Math.min(Math.max(1, Math.round(value)), MAX_ROWS_LIMIT);
      this._schedulePersist();
    },

    async runQuery(id?: string) {
      const tab = this.tabs.find((t) => t.id === (id ?? this.activeTabId));
      if (!tab) return;
      const q = tab.query.trim();
      if (!q) return;

      const seq = ++tab.runSeq;
      // A newer run of this tab, or closing it, makes this response irrelevant.
      const isCurrent = () => tab.runSeq === seq && this.tabs.some((t) => t.id === tab.id);
      tab.executing = true;
      tab.error = null;
      tab.result = null;

      try {
        const res = await CatalogApi.executeSqlQuery(q, this.maxRows);
        this._recordHistory(q);
        if (!isCurrent()) return;
        if (res.error) {
          tab.error = res.error;
        } else {
          // Rows are never edited; skip deep reactivity over potentially 100k of them.
          tab.result = markRaw(res);
          tab.lastExecutedQuery = q;
        }
      } catch (e: any) {
        if (!isCurrent()) return;
        tab.error = e?.response?.data?.detail ?? e?.message ?? "Unknown error";
      } finally {
        if (isCurrent()) tab.executing = false;
      }
    },

    _recordHistory(query: string) {
      this.history = [
        { query, timestamp: Date.now() },
        ...this.history.filter((h) => h.query !== query),
      ].slice(0, MAX_HISTORY);
      persistSqlHistory(this.history);
    },

    clearHistory() {
      this.history = [];
      persistSqlHistory([]);
    },
  },
});
