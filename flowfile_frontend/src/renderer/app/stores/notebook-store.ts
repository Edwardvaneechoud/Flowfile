// Notebook store: multi-notebook state + execution routing (python -> KernelApi, markdown -> client-side) for the Catalog notebook tab.
import { defineStore } from "pinia";
import { KernelApi } from "../api/kernel.api";
import { NotebookApi } from "../api/notebook.api";
import type { NotebookCellWire, NotebookSummary } from "../api/notebook.api";
import type { CellType, NotebookCellModel } from "../components/notebook/types";
import { sanitiseMarkdown } from "../features/ai/markdown";
import {
  loadPersistedNotebooks,
  persistNotebooks,
  type PersistedNotebook,
} from "./notebook-store-persistence";

let _seq = 0;
function uid(prefix: string): string {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
  _seq += 1;
  return `${prefix}-${Date.now()}-${_seq}`;
}

/** Stable int node_id for the kernel display-output store, keyed off the cell
 * uuid. Array index is NOT stable across reorders, so hash the id instead. */
export function cellNodeId(cellId: string): number {
  let hash = 0;
  for (let i = 0; i < cellId.length; i++) {
    hash = (hash * 31 + cellId.charCodeAt(i)) | 0;
  }
  return Math.abs(hash);
}

/** A fresh, collision-free negative session key for an unsaved notebook. Real
 * notebooks use -id (small negatives); ephemeral sessions sit far below that. */
function newEphemeralSessionId(): number {
  return -(1_500_000_000 + Math.floor(Math.random() * 100_000_000));
}

function newCell(cellType: CellType): NotebookCellModel {
  return {
    id: uid("cell"),
    cellType,
    code: "",
    metadata: {},
    output: null,
    renderedHtml: null,
    execState: "idle",
    editing: cellType === "markdown",
  };
}

function fromWire(cells: NotebookCellWire[]): NotebookCellModel[] {
  return cells.map((c) => {
    // SQL cells were dropped — coerce legacy "sql" to "python" (it's just code).
    const cellType: CellType = c.type === "markdown" ? "markdown" : "python";
    return {
      id: c.id,
      cellType,
      code: c.source,
      metadata: c.metadata ?? {},
      output: null,
      renderedHtml: cellType === "markdown" ? sanitiseMarkdown(c.source) : null,
      execState: "idle",
      editing: false,
    };
  });
}

function toWire(cells: NotebookCellModel[]): NotebookCellWire[] {
  return cells.map((c) => ({
    id: c.id,
    type: c.cellType,
    source: c.code,
    metadata: c.metadata ?? {},
  }));
}

function ensureCells(cells: NotebookCellModel[]): NotebookCellModel[] {
  return cells.length ? cells : [newCell("python")];
}

export interface OpenNotebook {
  tabId: string;
  persistedId: number | null;
  sessionFlowId: number;
  name: string;
  description: string | null;
  namespaceId: number | null;
  cells: NotebookCellModel[];
  kernelId: string | null;
  dirty: boolean;
  saving: boolean;
  executionCount: number;
}

function hydrateTab(p: PersistedNotebook): OpenNotebook {
  return {
    tabId: p.tabId,
    persistedId: p.persistedId,
    sessionFlowId: p.persistedId != null ? -p.persistedId : newEphemeralSessionId(),
    name: p.name,
    description: p.description,
    namespaceId: p.namespaceId,
    cells: ensureCells(fromWire(p.cells)),
    kernelId: p.kernelId,
    dirty: p.dirty,
    saving: false,
    executionCount: 0,
  };
}

interface NotebookState {
  notebooks: NotebookSummary[];
  openNotebooks: OpenNotebook[];
  activeTabId: string | null;
  loading: boolean;
  hydrated: boolean;
}

let _persistTimer: ReturnType<typeof setTimeout> | null = null;

export const useNotebookStore = defineStore("notebook", {
  state: (): NotebookState => ({
    notebooks: [],
    openNotebooks: [],
    activeTabId: null,
    loading: false,
    hydrated: false,
  }),

  getters: {
    active(state): OpenNotebook | null {
      return state.openNotebooks.find((n) => n.tabId === state.activeTabId) ?? null;
    },
    hasPythonCells(): boolean {
      return this.active?.cells.some((c) => c.cellType === "python") ?? false;
    },
  },

  actions: {
    _snapshot() {
      return {
        openNotebooks: this.openNotebooks.map((n) => ({
          tabId: n.tabId,
          persistedId: n.persistedId,
          name: n.name,
          description: n.description,
          namespaceId: n.namespaceId,
          cells: toWire(n.cells),
          kernelId: n.kernelId,
          dirty: n.dirty,
        })),
        activeTabId: this.activeTabId,
      };
    },

    _schedulePersist() {
      const snapshot = this._snapshot();
      if (_persistTimer) clearTimeout(_persistTimer);
      _persistTimer = setTimeout(() => persistNotebooks(snapshot), 400);
    },

    /** Restore open tabs from browser storage on first use; start one blank
     * notebook if there's nothing persisted. Idempotent. */
    ensureHydrated() {
      if (this.hydrated) return;
      this.hydrated = true;
      const persisted = loadPersistedNotebooks();
      if (persisted.openNotebooks.length) {
        this.openNotebooks = persisted.openNotebooks.map(hydrateTab);
        this.activeTabId =
          persisted.activeTabId && this.openNotebooks.some((n) => n.tabId === persisted.activeTabId)
            ? persisted.activeTabId
            : this.openNotebooks[0].tabId;
      } else {
        this.newTab();
      }
    },

    async loadList() {
      this.loading = true;
      try {
        this.notebooks = await NotebookApi.list();
      } finally {
        this.loading = false;
      }
    },

    newTab(): OpenNotebook {
      const tab: OpenNotebook = {
        tabId: uid("tab"),
        persistedId: null,
        sessionFlowId: newEphemeralSessionId(),
        name: "Untitled notebook",
        description: null,
        namespaceId: null,
        cells: [newCell("python")],
        kernelId: this.active?.kernelId ?? null, // inherit the current kernel
        dirty: false,
        saving: false,
        executionCount: 0,
      };
      this.openNotebooks.push(tab);
      this.activeTabId = tab.tabId;
      this._schedulePersist();
      return tab;
    },

    async openNotebook(id: number) {
      // Hydrate first so a later panel mount doesn't clobber the tab we add here.
      this.ensureHydrated();
      const existing = this.openNotebooks.find((n) => n.persistedId === id);
      if (existing) {
        this.activeTabId = existing.tabId;
        this._schedulePersist();
        return;
      }
      this.loading = true;
      try {
        const nb = await NotebookApi.get(id);
        const tab: OpenNotebook = {
          tabId: uid("tab"),
          persistedId: nb.id,
          sessionFlowId: -nb.id,
          name: nb.name,
          description: nb.description,
          namespaceId: nb.namespace_id,
          cells: ensureCells(fromWire(nb.cells)),
          kernelId: nb.default_kernel_id ?? this.active?.kernelId ?? null,
          dirty: false,
          saving: false,
          executionCount: 0,
        };
        this.openNotebooks.push(tab);
        this.activeTabId = tab.tabId;
        this._schedulePersist();
      } finally {
        this.loading = false;
      }
    },

    setActiveTab(tabId: string) {
      if (this.openNotebooks.some((n) => n.tabId === tabId)) {
        this.activeTabId = tabId;
        this._schedulePersist();
      }
    },

    closeTab(tabId: string) {
      const idx = this.openNotebooks.findIndex((n) => n.tabId === tabId);
      if (idx < 0) return;
      const tab = this.openNotebooks[idx];
      if (tab.kernelId) {
        KernelApi.clearNamespace(tab.kernelId, tab.sessionFlowId).catch(() => undefined);
      }
      this.openNotebooks.splice(idx, 1);
      if (this.activeTabId === tabId) {
        const next = this.openNotebooks[idx] ?? this.openNotebooks[idx - 1] ?? null;
        this.activeTabId = next?.tabId ?? null;
      }
      if (this.openNotebooks.length === 0) this.newTab();
      else this._schedulePersist();
    },

    async save() {
      const nb = this.active;
      if (!nb || nb.persistedId === null) return; // panel handles save-as for new
      nb.saving = true;
      try {
        await NotebookApi.update(nb.persistedId, {
          name: nb.name,
          description: nb.description,
          namespace_id: nb.namespaceId,
          cells: toWire(nb.cells),
          default_kernel_id: nb.kernelId,
        });
        nb.dirty = false;
        this._schedulePersist();
        // A list-refresh failure must not surface as a save failure (dirty is already cleared).
        await this.loadList().catch(() => undefined);
      } finally {
        nb.saving = false;
      }
    },

    async saveAs(name: string, namespaceId: number | null) {
      const nb = this.active;
      if (!nb) return;
      nb.saving = true;
      try {
        const created = await NotebookApi.create({
          name,
          namespace_id: namespaceId,
          description: nb.description,
          cells: toWire(nb.cells),
          default_kernel_id: nb.kernelId,
        });
        nb.persistedId = created.id;
        nb.sessionFlowId = -created.id;
        nb.name = created.name;
        nb.namespaceId = created.namespace_id;
        nb.dirty = false;
        await this.loadList();
        this._schedulePersist();
        return created;
      } finally {
        nb.saving = false;
      }
    },

    async deleteNotebook(id: number) {
      await NotebookApi.remove(id);
      // Free the namespace + close any open tab pointing at this notebook so a
      // future notebook reusing the id never inherits stale variables.
      const open = this.openNotebooks.find((n) => n.persistedId === id);
      if (open?.kernelId) {
        await KernelApi.clearNamespace(open.kernelId, -id).catch(() => undefined);
      }
      if (open) this.closeTab(open.tabId);
      await this.loadList();
    },

    setName(name: string) {
      const nb = this.active;
      if (nb) {
        nb.name = name;
        nb.dirty = true;
        this._schedulePersist();
      }
    },

    setKernel(kernelId: string | null) {
      const nb = this.active;
      if (nb) {
        nb.kernelId = kernelId;
        nb.dirty = true;
        this._schedulePersist();
      }
    },

    setCellCode(cellId: string, code: string) {
      const cell = this.active?.cells.find((c) => c.id === cellId);
      if (cell && this.active) {
        cell.code = code;
        this.active.dirty = true;
        this._schedulePersist();
      }
    },

    setCellType(cellId: string, cellType: CellType) {
      const cell = this.active?.cells.find((c) => c.id === cellId);
      if (cell && this.active) {
        cell.cellType = cellType;
        cell.output = null;
        cell.renderedHtml = null;
        cell.execState = "idle";
        cell.editing = cellType === "markdown";
        this.active.dirty = true;
        this._schedulePersist();
      }
    },

    setCellEditing(cellId: string, editing: boolean) {
      const cell = this.active?.cells.find((c) => c.id === cellId);
      if (cell) cell.editing = editing;
    },

    addCell(cellType: CellType, afterIndex?: number) {
      const nb = this.active;
      if (!nb) return;
      const cell = newCell(cellType);
      if (afterIndex === undefined || afterIndex < 0) nb.cells.push(cell);
      else nb.cells.splice(afterIndex + 1, 0, cell);
      nb.dirty = true;
      this._schedulePersist();
      return cell;
    },

    removeCell(cellId: string) {
      const nb = this.active;
      if (!nb) return;
      nb.cells = ensureCells(nb.cells.filter((c) => c.id !== cellId));
      nb.dirty = true;
      this._schedulePersist();
    },

    moveCell(cellId: string, direction: -1 | 1) {
      const nb = this.active;
      if (!nb) return;
      const idx = nb.cells.findIndex((c) => c.id === cellId);
      const target = idx + direction;
      if (idx < 0 || target < 0 || target >= nb.cells.length) return;
      const [cell] = nb.cells.splice(idx, 1);
      nb.cells.splice(target, 0, cell);
      nb.dirty = true;
      this._schedulePersist();
    },

    insertReadCell(tableName: string) {
      const nb = this.active;
      if (!nb) return;
      const cell = newCell("python");
      cell.code = `df = flowfile_ctx.read_catalog_table(${JSON.stringify(tableName)})\ndf`;
      nb.cells.push(cell);
      nb.dirty = true;
      this._schedulePersist();
      return cell;
    },

    async runCell(cellId: string) {
      const cell = this.active?.cells.find((c) => c.id === cellId);
      if (!cell) return;
      if (cell.cellType === "markdown") return this.runMarkdownCell(cell);
      return this.runPythonCell(cell);
    },

    runMarkdownCell(cell: NotebookCellModel) {
      cell.renderedHtml = sanitiseMarkdown(cell.code);
      cell.editing = false;
      cell.execState = "idle";
    },

    async runPythonCell(cell: NotebookCellModel, nb: OpenNotebook | null = null) {
      nb = nb ?? this.active;
      if (cell.execState === "running") return; // re-entrancy guard (also covers Shift+Enter)
      if (!nb) return;
      if (!nb.kernelId) {
        cell.output = {
          stdout: "",
          stderr: "",
          display_outputs: [],
          error: "No kernel selected. Start or select a kernel to run Python cells.",
          execution_time_ms: 0,
          execution_count: 0,
        };
        cell.execState = "error";
        return;
      }
      cell.execState = "running";
      try {
        const res = await KernelApi.executeCell(nb.kernelId, {
          node_id: cellNodeId(cell.id),
          code: cell.code,
          flow_id: nb.sessionFlowId, // negative session id: can't collide with positive flow ids
        });
        nb.executionCount += 1;
        cell.output = {
          stdout: res.stdout,
          stderr: res.stderr,
          display_outputs: res.display_outputs,
          error: res.error,
          execution_time_ms: res.execution_time_ms,
          execution_count: nb.executionCount,
        };
        cell.execState = res.error ? "error" : "idle";
      } catch (e: any) {
        cell.output = {
          stdout: "",
          stderr: "",
          display_outputs: [],
          error: e?.message ?? "Cell execution failed",
          execution_time_ms: 0,
          execution_count: nb.executionCount,
        };
        cell.execState = "error";
      }
    },

    /** Run the active notebook top-to-bottom. Markdown always renders; Python
     * cells are skipped (not errored) when there's no kernel, so the notebook is
     * usable on a default desktop install with no Docker. Stops on first error. */
    async runAll() {
      const nb = this.active;
      if (!nb) return;
      for (const cell of nb.cells) {
        if (cell.cellType === "markdown") {
          this.runMarkdownCell(cell);
          continue;
        }
        if (!nb.kernelId) continue;
        await this.runPythonCell(cell, nb);
        // A mid-run tab switch must not let later cells render on the wrong tab.
        if (this.active !== nb) break;
        if (cell.execState === "error") break;
      }
    },

    clearOutputs() {
      const nb = this.active;
      if (!nb) return;
      for (const cell of nb.cells) {
        cell.output = null;
        cell.execState = "idle";
      }
    },

    async restartKernel() {
      const nb = this.active;
      if (!nb) return;
      this.clearOutputs();
      nb.executionCount = 0;
      if (nb.kernelId) {
        await KernelApi.clearNamespace(nb.kernelId, nb.sessionFlowId).catch(() => undefined);
      }
    },

    /** Free every open notebook's kernel namespace (don't leak them into the
     * 20-slot LRU shared with flow runs). Called when the panel unmounts. */
    async closeAllSessions() {
      for (const nb of this.openNotebooks) {
        if (nb.kernelId) {
          await KernelApi.clearNamespace(nb.kernelId, nb.sessionFlowId).catch(() => undefined);
        }
      }
    },
  },
});
