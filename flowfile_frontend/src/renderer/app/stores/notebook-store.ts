// Notebook store: multi-notebook state + execution routing (python -> KernelApi, markdown -> client-side) for the Catalog notebook tab.
import { defineStore } from "pinia";
import { KernelApi } from "../api/kernel.api";
import { NotebookApi } from "../api/notebook.api";
import type { NotebookCellWire, NotebookSummary } from "../api/notebook.api";
import type { CellType, NotebookCellModel } from "../components/notebook/types";
import {
  disposeOwnerViews,
  getCellView,
  ownerIdForNotebook,
} from "../components/notebook/editorViews";
import {
  applyOperation,
  duplicateCell as duplicateCellOp,
  insertCell,
  moveCell as moveCellOp,
  newCellId,
  removeCell as removeCellOp,
  type CellOperation,
  type OperationResult,
} from "../components/notebook/cellOperations";
import {
  disposeCellHistory,
  getCellHistory,
  type CellHistory,
} from "../components/notebook/useCellHistory";
import {
  disposeCellPresentation,
  disposeOwnerPresentation,
} from "../components/notebook/cellPresentation";
import {
  beginExecution,
  bumpSessionEpoch,
  bumpSourceRevision,
  clearResults,
  disposeOwner,
  ensureOwner,
  invalidateFrom,
  markDownstreamStale,
  runExecutionBatch,
  settleExecution,
  type RuntimeCellRef,
  type SettledMeta,
} from "../components/notebook/notebookRuntimeState";
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
    id: newCellId(),
    cellType,
    code: "",
    metadata: {},
    output: null,
    renderedHtml: null,
    execState: "idle",
    editing: cellType === "markdown",
  };
}

/** renderedHtml/editing ride along so a duplicated markdown cell looks identical. */
function cloneCell(src: NotebookCellModel, id: string): NotebookCellModel {
  return { ...src, id, output: null, execState: "idle", cursor: undefined };
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

/** Python that reads a catalog table; the typed ref chain only fits the
 * catalog.schema.table shape, anything else falls back to the string form. */
export function readTableSnippet(qualifiedName: string): string {
  const parts = qualifiedName.split(".");
  if (parts.length === 3) {
    const [catalog, schema, table] = parts.map((p) => JSON.stringify(p));
    return `df = flowfile_ctx.get_catalog(${catalog}).get_schema(${schema}).get_table_ref(${table}).read()`;
  }
  return `df = flowfile_ctx.read_catalog_table(${JSON.stringify(qualifiedName)})`;
}

/** Splice `snippet` into `code` at `pos`, padded with newlines so it sits on its
 * own line. Returns the text actually inserted and the caret offset after it. */
export function snippetInsertion(code: string, pos: number, snippet: string) {
  const at = Math.max(0, Math.min(pos, code.length));
  const before = at > 0 && code[at - 1] !== "\n" ? "\n" : "";
  const after = at < code.length && code[at] !== "\n" ? "\n" : "";
  const insert = `${before}${snippet}${after}`;
  return { at, insert, cursor: at + before.length + snippet.length };
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
  focusedCellId: string | null; // transient: last cell the caret was in
}

const ownerOf = (nb: OpenNotebook): string => ownerIdForNotebook(nb.tabId);

const refs = (nb: OpenNotebook): RuntimeCellRef[] =>
  nb.cells.map((c) => ({ id: c.id, isPython: c.cellType === "python" }));

/** First position a structural op can have invalidated; a move reaches back to its origin. */
function affectedIndex(op: CellOperation<NotebookCellModel>): number {
  return op.kind === "move" ? Math.min(op.from, op.to) : op.index;
}

/** The generation/revision stamp is optional on older kernels, so never assume it is there. */
function settledMeta(res: unknown): SettledMeta {
  const stamped = (res ?? {}) as { namespace_generation?: string | null; revision?: number | null };
  return {
    namespace_generation: stamped.namespace_generation ?? null,
    revision: stamped.revision ?? null,
  };
}

/** Re-resolve by id: the cells array may have been rebuilt while the request was out. */
function settledCell(nb: OpenNotebook, cell: NotebookCellModel): NotebookCellModel {
  return nb.cells.find((c) => c.id === cell.id) ?? cell;
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
    focusedCellId: null,
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

    _applyStructural(nb: OpenNotebook, result: OperationResult<NotebookCellModel>) {
      nb.cells = result.cells;
      if (result.op.kind === "remove") disposeCellPresentation(ownerOf(nb), result.op.cell.id);
      getCellHistory<NotebookCellModel>(ownerOf(nb)).push({
        op: result.op,
        inverse: result.inverse,
      });
      invalidateFrom(ownerOf(nb), refs(nb), affectedIndex(result.op), "upstream-changed");
      nb.dirty = true;
      this._schedulePersist();
    },

    /** Restore open tabs from browser storage on first use; start one blank
     * notebook if there's nothing persisted. Idempotent. */
    ensureHydrated() {
      if (this.hydrated) return;
      this.hydrated = true;
      const persisted = loadPersistedNotebooks();
      if (persisted.openNotebooks.length) {
        this.openNotebooks = persisted.openNotebooks.map(hydrateTab);
        for (const nb of this.openNotebooks) ensureOwner(ownerOf(nb));
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
        focusedCellId: null,
      };
      this.openNotebooks.push(tab);
      ensureOwner(ownerOf(tab));
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
          focusedCellId: null,
        };
        this.openNotebooks.push(tab);
        ensureOwner(ownerOf(tab));
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
      disposeCellHistory(ownerIdForNotebook(tabId));
      disposeOwnerViews(ownerIdForNotebook(tabId));
      disposeOwnerPresentation(ownerIdForNotebook(tabId));
      disposeOwner(ownerIdForNotebook(tabId));
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
      // closeTab frees the namespace, so a future notebook reusing the id never inherits stale variables.
      const open = this.openNotebooks.find((n) => n.persistedId === id);
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

    /** A different kernel is a different session: retained results are from the previous one. */
    setKernel(kernelId: string | null) {
      const nb = this.active;
      if (!nb || nb.kernelId === kernelId) return;
      nb.kernelId = kernelId;
      nb.dirty = true;
      bumpSessionEpoch(ownerOf(nb));
      this._schedulePersist();
    },

    setCellCode(cellId: string, code: string) {
      const nb = this.active;
      const idx = nb ? nb.cells.findIndex((c) => c.id === cellId) : -1;
      if (!nb || idx < 0) return;
      const cell = nb.cells[idx];
      cell.code = code;
      nb.dirty = true;
      // Markdown edits change only their own preview, so they invalidate nothing.
      if (cell.cellType === "python") {
        bumpSourceRevision(ownerOf(nb), cellId);
        invalidateFrom(ownerOf(nb), refs(nb), idx + 1, "upstream-changed");
      }
      this._schedulePersist();
    },

    setCellType(cellId: string, cellType: CellType) {
      const nb = this.active;
      const idx = nb ? nb.cells.findIndex((c) => c.id === cellId) : -1;
      if (!nb || idx < 0) return;
      const cell = nb.cells[idx];
      cell.cellType = cellType;
      cell.output = null;
      cell.renderedHtml = null;
      cell.execState = "idle";
      cell.editing = cellType === "markdown";
      nb.dirty = true;
      bumpSourceRevision(ownerOf(nb), cellId);
      invalidateFrom(ownerOf(nb), refs(nb), idx + 1, "upstream-changed");
      this._schedulePersist();
    },

    setCellEditing(cellId: string, editing: boolean) {
      const cell = this.active?.cells.find((c) => c.id === cellId);
      if (cell) cell.editing = editing;
    },

    addCell(cellType: CellType, afterIndex?: number) {
      const nb = this.active;
      if (!nb) return;
      const cell = newCell(cellType);
      const at = afterIndex == null || afterIndex < 0 ? nb.cells.length : afterIndex + 1;
      this._applyStructural(nb, insertCell(nb.cells, cell, at));
      return cell;
    },

    /** `index` is the new cell's final index, unlike `addCell`'s after-index. */
    insertCellAt(cellType: CellType, index: number) {
      const nb = this.active;
      if (!nb) return;
      const cell = newCell(cellType);
      this._applyStructural(nb, insertCell(nb.cells, cell, index));
      return cell;
    },

    /** Returns the id to focus next, or `null` when the delete was refused. */
    removeCell(cellId: string): string | null {
      const nb = this.active;
      if (!nb) return null;
      const result = removeCellOp(nb.cells, cellId, { minCells: 1 });
      if (!result) return null;
      const idx = nb.cells.findIndex((c) => c.id === cellId);
      const focusId = (nb.cells[idx + 1] ?? nb.cells[idx - 1])?.id ?? null;
      this._applyStructural(nb, result);
      return focusId;
    },

    moveCell(cellId: string, direction: -1 | 1) {
      const nb = this.active;
      if (!nb) return null;
      const idx = nb.cells.findIndex((c) => c.id === cellId);
      if (idx < 0) return null;
      return this.moveCellToIndex(cellId, idx + direction);
    },

    /** `targetIndex` is the cell's final index; `null` means the move was a no-op. */
    moveCellToIndex(cellId: string, targetIndex: number) {
      const nb = this.active;
      if (!nb) return null;
      const result = moveCellOp(nb.cells, cellId, targetIndex);
      if (!result) return null;
      this._applyStructural(nb, result);
      const op = result.op as Extract<CellOperation<NotebookCellModel>, { kind: "move" }>;
      return { from: op.from, to: op.to, total: nb.cells.length };
    },

    duplicateCell(cellId: string) {
      const nb = this.active;
      if (!nb) return null;
      const result = duplicateCellOp(nb.cells, cellId, cloneCell);
      if (!result) return null;
      this._applyStructural(nb, result);
      const op = result.op as Extract<CellOperation<NotebookCellModel>, { kind: "duplicate" }>;
      return op.cell;
    },

    undoCellAction() {
      const nb = this.active;
      if (!nb) return null;
      const history = getCellHistory<NotebookCellModel>(ownerOf(nb));
      const entry = history.undo();
      if (!entry) return null;
      return this._replayCellAction(nb, history, entry.inverse);
    },

    redoCellAction() {
      const nb = this.active;
      if (!nb) return null;
      const history = getCellHistory<NotebookCellModel>(ownerOf(nb));
      const entry = history.redo();
      if (!entry) return null;
      return this._replayCellAction(nb, history, entry.op);
    },

    /** Replays a recorded op without re-recording it — the history already moved the entry. */
    _replayCellAction(
      nb: OpenNotebook,
      history: CellHistory<NotebookCellModel>,
      op: CellOperation<NotebookCellModel>,
    ) {
      const result = applyOperation(nb.cells, op);
      if (!result) {
        history.clear();
        return null;
      }
      nb.cells = result.cells;
      if (result.op.kind === "remove") disposeCellPresentation(ownerOf(nb), result.op.cell.id);
      invalidateFrom(ownerOf(nb), refs(nb), affectedIndex(result.op), "upstream-changed");
      nb.dirty = true;
      this._schedulePersist();
      return result.op;
    },

    setCellCursor(cellId: string, offset: number) {
      const nb = this.active;
      const cell = nb?.cells.find((c) => c.id === cellId);
      if (!nb || !cell) return;
      cell.cursor = offset;
      nb.focusedCellId = cellId;
    },

    /** Focus without touching the caret: chrome clicks must not reset `cell.cursor`. */
    setFocusedCell(cellId: string) {
      const nb = this.active;
      if (nb?.cells.some((c) => c.id === cellId)) nb.focusedCellId = cellId;
    },

    /** Insert a read of `qualifiedName` (catalog.schema.table) at the caret of the
     * last-focused Python cell. Without one, a trailing blank Python cell is filled
     * in, else a new cell is appended. */
    insertReadCell(qualifiedName: string) {
      const nb = this.active;
      if (!nb) return;
      const snippet = readTableSnippet(qualifiedName);
      const focused = nb.cells.find((c) => c.id === nb.focusedCellId);
      if (focused?.cellType === "python") {
        const { at, insert, cursor } = snippetInsertion(focused.code, focused.cursor ?? 0, snippet);
        const view = getCellView(ownerIdForNotebook(nb.tabId), focused.id);
        if (view) {
          view.dispatch({ changes: { from: at, insert }, selection: { anchor: cursor } });
          view.focus();
        } else {
          focused.code = focused.code.slice(0, at) + insert + focused.code.slice(at);
        }
        focused.cursor = cursor;
        nb.dirty = true;
        this._schedulePersist();
        return focused;
      }
      const last = nb.cells[nb.cells.length - 1];
      const cell = last?.cellType === "python" && !last.code.trim() ? last : newCell("python");
      cell.code = snippet;
      cell.cursor = snippet.length;
      if (cell !== last) nb.cells.push(cell);
      nb.focusedCellId = cell.id;
      nb.dirty = true;
      this._schedulePersist();
      return cell;
    },

    /** Resolves false when the run was refused or failed (no kernel, kernel error, request error). */
    async runCell(cellId: string): Promise<boolean> {
      const nb = this.active;
      const cell = nb?.cells.find((c) => c.id === cellId);
      if (!nb || !cell) return false;
      if (cell.cellType === "markdown") {
        this.runMarkdownCell(cell);
        return true;
      }
      return this._runBatch(nb, [cell]);
    },

    runMarkdownCell(cell: NotebookCellModel) {
      cell.renderedHtml = sanitiseMarkdown(cell.code);
      cell.editing = false;
      cell.execState = "idle";
    },

    async runPythonCell(cell: NotebookCellModel, nb: OpenNotebook): Promise<boolean> {
      if (cell.execState === "running") return false; // re-entrancy guard (also covers Shift+Enter)
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
        return false;
      }
      const ownerId = ownerOf(nb);
      // Marked at submission: a cell that errors part-way has still mutated the namespace.
      markDownstreamStale(ownerId, refs(nb), cell.id);
      const ticket = beginExecution(ownerId, cell.id);
      cell.execState = "running";
      try {
        const res = await KernelApi.executeCell(nb.kernelId, {
          node_id: cellNodeId(cell.id),
          code: cell.code,
          flow_id: nb.sessionFlowId, // negative session id: can't collide with positive flow ids
        });
        if (settleExecution(ticket, settledMeta(res)) === "discard") {
          cell.execState = "idle"; // nothing newer owns this cell (re-entrancy guard), so release it
          return false;
        }
        const target = settledCell(nb, cell);
        nb.executionCount += 1;
        target.output = {
          stdout: res.stdout,
          stderr: res.stderr,
          display_outputs: res.display_outputs,
          error: res.error,
          execution_time_ms: res.execution_time_ms,
          execution_count: nb.executionCount,
        };
        target.execState = res.error ? "error" : "idle";
        return res.success && !res.error;
      } catch (e: any) {
        if (settleExecution(ticket) === "discard") {
          cell.execState = "idle";
          return false;
        }
        const target = settledCell(nb, cell);
        target.output = {
          stdout: "",
          stderr: "",
          display_outputs: [],
          error: e?.message ?? "Cell execution failed",
          execution_time_ms: 0,
          execution_count: nb.executionCount,
        };
        target.execState = "error";
        return false;
      }
    },

    /** One execution batch per notebook (a single run is a batch of one). Resolves
     * false when the batch was refused as a duplicate, or when a cell failed. */
    async _runBatch(
      nb: OpenNotebook,
      cells: NotebookCellModel[],
      opts: { skipPythonWithoutKernel?: boolean } = {},
    ): Promise<boolean> {
      const runnable =
        opts.skipPythonWithoutKernel && !nb.kernelId
          ? cells.filter((c) => c.cellType !== "python")
          : cells;
      let ok = true;
      const started = await runExecutionBatch({
        ownerId: ownerOf(nb),
        cells: runnable.map((c) => ({ id: c.id, isPython: c.cellType === "python" })),
        runOne: async (cellId) => {
          const cell = nb.cells.find((c) => c.id === cellId);
          const result = cell ? await this.runPythonCell(cell, nb) : false;
          if (!result) ok = false;
          return { ok: result };
        },
        onMarkdown: (cellId) => {
          const cell = nb.cells.find((c) => c.id === cellId);
          if (cell) this.runMarkdownCell(cell);
        },
        stillPresent: (cellId) => nb.cells.some((c) => c.id === cellId),
      });
      return started && ok;
    },

    /** Run the notebook top-to-bottom. Markdown always renders; Python cells are
     * skipped (not errored) when there's no kernel, so the notebook is usable on a
     * default desktop install with no Docker. Stops on first error. The notebook is
     * captured once, so a mid-run tab switch neither stops it nor redirects results. */
    async runAll() {
      const nb = this.active;
      if (!nb) return;
      await this._runBatch(nb, nb.cells.slice(), { skipPythonWithoutKernel: true });
    },

    clearOutputs() {
      const nb = this.active;
      if (!nb) return;
      for (const cell of nb.cells) {
        cell.output = null;
        cell.execState = "idle";
      }
      clearResults(ownerOf(nb));
    },

    /** Clear the kernel's variables for this notebook; the kernel keeps running. Rejects
     * (outputs retained) when the namespace clear fails — a failed reset is not a reset. */
    async resetSession() {
      const nb = this.active;
      if (!nb) return;
      if (nb.kernelId) {
        await KernelApi.clearNamespace(nb.kernelId, nb.sessionFlowId);
      }
      bumpSessionEpoch(ownerOf(nb));
      this.clearOutputs();
      nb.executionCount = 0;
    },

    /** Free every open notebook's kernel namespace (don't leak them into the
     * 20-slot LRU shared with flow runs). Called when the panel unmounts. */
    async closeAllSessions() {
      for (const nb of this.openNotebooks) {
        // The namespace is gone, so nothing still on screen can be current.
        bumpSessionEpoch(ownerOf(nb));
        if (nb.kernelId) {
          await KernelApi.clearNamespace(nb.kernelId, nb.sessionFlowId).catch(() => undefined);
        }
      }
    },
  },
});
