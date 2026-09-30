// Notebook store: catalog tabs run python through KernelApi; a flow tab syncs its cells to the canvas and runs them there.
import { defineStore } from "pinia";
import { FlowApi } from "../api/flow.api";
import { KernelApi } from "../api/kernel.api";
import { NodeApi } from "../api/node.api";
import { NotebookApi } from "../api/notebook.api";
import type {
  NotebookCellWire,
  NotebookPlan,
  NotebookPushBody,
  NotebookPushResult,
  NotebookRendering,
  NotebookSummary,
  NotebookSyncErrorDetail,
  RenderedCell,
} from "../api/notebook.api";
import type { FlowParameter, RunInformation } from "../types/flow.types";
import type { DisplayOutput } from "../types/kernel.types";
import type { CellOutput, TableExample } from "../types/node.types";
import { detailMessage } from "../composables/saveError";
import {
  TABLE_MIME,
  type TablePayload,
} from "../components/nodes/node-types/elements/pythonScript/notebookDisplay";
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
  endBatch,
  ensureOwner,
  invalidateFrom,
  markDownstreamStale,
  runExecutionBatch,
  settleExecution,
  settledMeta,
  startBatch,
  type RuntimeCellRef,
  type SyncState,
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
  /** Set for a flow's canvas notebook: an ephemeral tab rendered from the canvas, never persisted. */
  flowId?: number;
  /** Per cell id, the code last rendered from (or pushed to) the canvas; equal code means unedited. */
  generated?: Record<string, string>;
  /** Per cell id, the canvas node ids the rendering (or the last sync) attributed to it. */
  nodeIds?: Record<string, number[]>;
  /** Per cell id, the rendered cell's kind; cells added in the notebook have none. */
  kinds?: Record<string, RenderedCell["kind"]>;
  fingerprint?: string;
  /** Why the last sync was refused (the 422 detail); cleared when the next sync starts. */
  syncError?: FlowSyncError | null;
  /** Core answered a sync with 403: only admins may sync on this server. Cells stay editable. */
  syncForbidden?: boolean;
  /** The latest outcome worth telling the user about; replaced by the next flow action. */
  notice?: FlowNotice | null;
}

export interface FlowSyncError extends NotebookSyncErrorDetail {
  /** The failing cell's code as sent; the error applies while the cell still holds it. */
  code: string | null;
}

export interface FlowNotice {
  tone: "success" | "warning" | "error";
  message: string;
}

/** What Run does for a flow cell: imports and plain cells show nothing, the others an output. */
export type FlowCellKind = "imports" | "parameters" | "node" | "plain";

/** Push reviews every plan; Run's automatic sync asks only before deleting canvas nodes. */
export type FlowSyncTrigger = "push" | "run";

export type FlowSyncStatus =
  | "synced"
  | "cancelled"
  | "busy"
  | "invalid"
  | "conflict"
  | "forbidden"
  | "failed";

/** What a flow tab needs from the designer around a sync or a canvas run. */
export interface FlowNotebookHooks {
  /** Flush pending canvas edits and save the settings drawer; false aborts the action. */
  prepare(): Promise<boolean>;
  /** The designer's node id counter, so new nodes number above ids it handed out. */
  clientMaxNodeId(): number;
  /** Ask before applying `plan`; only called when `planNeedsConfirmation` says so. */
  confirm(plan: NotebookPlan, trigger: FlowSyncTrigger): Promise<boolean>;
  /** A push was applied (seed the node id counter, reload the canvas). */
  pushed(result: NotebookPushResult): void;
  /** Core accepted a canvas run's start; a refused start calls neither run hook. */
  runStarted(): void;
  /** The started canvas run is over; `info` is its final status, `null` when polling failed. */
  runEnded(info: RunInformation | null): void;
}

const NO_HOOKS: FlowNotebookHooks = {
  prepare: async () => true,
  clientMaxNodeId: () => 0,
  confirm: async () => false,
  pushed: () => undefined,
  runStarted: () => undefined,
  runEnded: () => undefined,
};

const flowHooks = new Map<number, Partial<FlowNotebookHooks>>();

/** The panel showing flow `flowId` registers its hooks; returns the unregister function. */
export function registerFlowNotebookHooks(
  flowId: number,
  hooks: Partial<FlowNotebookHooks>,
): () => void {
  flowHooks.set(flowId, hooks);
  return () => {
    if (flowHooks.get(flowId) === hooks) flowHooks.delete(flowId);
  };
}

const hooksFor = (flowId: number): FlowNotebookHooks => ({
  ...NO_HOOKS,
  ...flowHooks.get(flowId),
});

export const SYNC_NEEDS_ADMIN =
  "Syncing the notebook to the canvas needs an admin on this server; your edits stay in the notebook.";
export const CANVAS_CHANGED =
  "The canvas changed since these cells were rendered, so they were refreshed; run again to sync your edits.";
export const PREVIEW_ROW_LIMIT = 100;
export const notOnCanvasText = (nodeId: number): string => `Node #${nodeId} is not on the canvas.`;

/** A placeholder keeps its `fl.canvas_node(...)` text under its reason as a comment. */
function renderedCode(cell: RenderedCell): string {
  return cell.status !== "code" && cell.reason
    ? `# ${cell.reason.replace(/\n/g, " ")}\n${cell.code}`
    : cell.code;
}

/** Markdown cells are notes that never sync; a blank cell the canvas never held is unedited. */
const isEdited = (nb: OpenNotebook, cell: NotebookCellModel): boolean =>
  cell.cellType === "python" && (nb.generated?.[cell.id] ?? "") !== cell.code;

/** Adding or removing a blank cell the canvas never held changes nothing a sync would send. */
const isBlankNewCellOp = (nb: OpenNotebook, op: CellOperation<NotebookCellModel>): boolean =>
  op.kind !== "move" && op.cell.code === "" && !(op.cell.id in (nb.generated ?? {}));

/**
 * Merge a rendering into a flow tab: unedited cells take the new text, edited, user-added and
 * Markdown cells stay, new node cells land in render order and cells of deleted nodes go.
 */
function applyRendering(nb: OpenNotebook, rendering: NotebookRendering): void {
  const previous = nb.generated ?? {};
  const order = new Map(rendering.cells.map((c, i) => [c.cell_id, i]));
  const present = new Set(nb.cells.map((c) => c.id));
  const pending = rendering.cells.filter((c) => !present.has(c.cell_id));
  const next: NotebookCellModel[] = [];
  const kept = (cell: NotebookCellModel) => cell.cellType === "markdown" || isEdited(nb, cell);
  const insertBefore = (index: number) => {
    while (pending.length && order.get(pending[0].cell_id)! < index) {
      const fresh = pending.shift()!;
      next.push({ ...newCell("python"), id: fresh.cell_id, code: renderedCode(fresh) });
    }
  };
  for (const cell of nb.cells) {
    const index = order.get(cell.id);
    if (index === undefined && cell.id in previous && !kept(cell)) continue;
    if (index !== undefined) {
      insertBefore(index);
      if (!kept(cell)) cell.code = renderedCode(rendering.cells[index]);
    }
    next.push(cell);
  }
  insertBefore(Infinity);
  const generated = Object.fromEntries(rendering.cells.map((c) => [c.cell_id, renderedCode(c)]));
  for (const cell of next)
    if (!(cell.id in generated) && cell.id in previous) generated[cell.id] = previous[cell.id];
  nb.generated = generated;
  nb.nodeIds = Object.fromEntries(rendering.cells.map((c) => [c.cell_id, c.node_ids]));
  nb.kinds = Object.fromEntries(rendering.cells.map((c) => [c.cell_id, c.kind]));
  nb.fingerprint = rendering.code_fingerprint;
  nb.cells = ensureCells(next);
  nb.dirty = false;
}

/** The push body: Python cells, the edited ones marked, and their live nodes as `[type, id]`. */
export function flowPushBody(
  nb: OpenNotebook,
  nodeTypes: Map<number, string>,
  clientMaxNodeId: number,
): NotebookPushBody {
  const python = nb.cells.filter((c) => c.cellType === "python");
  const provenance: Record<string, [string, number][]> = {};
  for (const cell of python) {
    const live = (nb.nodeIds?.[cell.id] ?? []).filter((id) => nodeTypes.has(id));
    if (live.length) provenance[cell.id] = live.map((id) => [nodeTypes.get(id)!, id]);
  }
  return {
    flow_id: nb.flowId!,
    cells: python.map((c) => [c.id, c.code]),
    changed_cell_ids: python.filter((c) => isEdited(nb, c)).map((c) => c.id),
    provenance,
    code_fingerprint: nb.fingerprint ?? "",
    client_max_node_id: Math.max(clientMaxNodeId, ...nodeTypes.keys()),
  };
}

/** A structural change or any edited cell means the canvas no longer matches the notebook. */
export const flowNeedsSync = (nb: OpenNotebook): boolean =>
  nb.dirty || nb.cells.some((c) => isEdited(nb, c));

/** Rendered imports/parameters cells keep their kind; other cells are node cells while they build nodes. */
export function flowCellKind(nb: OpenNotebook, cellId: string): FlowCellKind {
  const rendered = nb.kinds?.[cellId];
  if (rendered === "imports" || rendered === "parameters") return rendered;
  return nb.nodeIds?.[cellId]?.length ? "node" : "plain";
}

/** The last sync's error, while `cell` still holds the code it failed on. */
export function syncErrorFor(nb: OpenNotebook, cell: NotebookCellModel): FlowSyncError | null {
  const error = nb.syncError;
  return error && error.cell_id === cell.id && error.code === cell.code ? error : null;
}

export function flowCellSyncState(nb: OpenNotebook, cell: NotebookCellModel): SyncState {
  if (syncErrorFor(nb, cell)) return "error";
  return isEdited(nb, cell) ? "edited" : "synced";
}

/** The lines a sync confirmation lists. */
export function planReview(plan: NotebookPlan): string[] {
  return [
    ...plan.deletions.map((id) => `Delete node #${id}`),
    ...(plan.parameter_changes ? ["Replace the flow parameters"] : []),
    ...plan.warnings,
  ];
}

export function planNeedsConfirmation(plan: NotebookPlan, trigger: FlowSyncTrigger): boolean {
  return trigger === "run" ? plan.deletions.length > 0 : planReview(plan).length > 0;
}

function isSyncErrorDetail(detail: unknown): detail is NotebookSyncErrorDetail {
  return (
    !!detail &&
    typeof detail === "object" &&
    typeof (detail as NotebookSyncErrorDetail).message === "string" &&
    "kind" in detail
  );
}

interface CellResult {
  display_outputs: DisplayOutput[];
  error: string | null;
}

const textResult = (text: string): CellResult => ({
  display_outputs: [{ mime_type: "text/plain", data: text, title: "" }],
  error: null,
});

const errorResult = (error: string): CellResult => ({ display_outputs: [], error });

function tableDisplay(
  columns: string[],
  data: Record<string, any>[],
  title: string,
  total = data.length,
): DisplayOutput {
  const payload: TablePayload = {
    columns,
    fields: [],
    data,
    total_rows: total,
    loaded_rows: data.length,
    truncated: total > data.length,
    max_rows: PREVIEW_ROW_LIMIT,
  };
  return { mime_type: TABLE_MIME, data: JSON.stringify(payload), title };
}

/** A `/node/data` preview as a table output; an unknown row count is never shown as a number. */
export function nodePreviewDisplay(example: TableExample, nodeId: number): DisplayOutput {
  const rows = example.data ?? [];
  const total = example.number_of_records;
  const title = `Node #${nodeId} · preview of up to ${PREVIEW_ROW_LIMIT} rows`;
  return total == null
    ? tableDisplay(example.columns, rows, `${title} · total rows unknown`)
    : tableDisplay(example.columns, rows, title, total);
}

export function parametersDisplay(parameters: FlowParameter[]): DisplayOutput {
  if (!parameters.length) return textResult("No flow parameters.").display_outputs[0];
  const rows = parameters.map((p) => ({
    name: p.name,
    type: p.type ?? "string",
    default: p.default_value,
  }));
  return tableDisplay(["name", "type", "default"], rows, "Flow parameters");
}

async function parametersResult(flowId: number): Promise<CellResult> {
  const settings = await FlowApi.getFlowSettings(flowId);
  if (!settings) return errorResult("Could not read the flow parameters.");
  return { display_outputs: [parametersDisplay(settings.parameters ?? [])], error: null };
}

async function liveNodeIds(flowId: number): Promise<Set<number>> {
  return new Set((await FlowApi.getFlowData(flowId)).node_inputs.map((n) => n.id));
}

const failedStep = (info: RunInformation, nodeId?: number): CellResult | null => {
  const step = info.node_step_result.find(
    (r) => r.success === false && (nodeId == null || r.node_id === nodeId),
  );
  return step ? errorResult(`Node #${step.node_id} failed: ${step.error}`) : null;
};

/** A node the run did not complete: its own or an upstream failure, a cancel, or no run. */
function notRunResult(run: RunInformation, nodeId: number): CellResult {
  const cancelled = run.node_step_result.some((r) => r.success == null);
  return (
    failedStep(run, nodeId) ??
    failedStep(run) ??
    errorResult(`Node #${nodeId} did not run${cancelled ? ": the run was cancelled" : ""}.`)
  );
}

/**
 * The rows of a node the run completed, unless it left the canvas (`/node/data` 500s on a
 * missing node); any other node's `/node/data` can still hold an earlier run's rows. After a
 * Performance-mode run they are fetched first, as the preview's Fetch Data does (a preview
 * fetch, which stores the rows, not Explore Data's plan-only one).
 */
async function nodeResult(
  flowId: number,
  nodeId: number,
  live: Set<number>,
  run: RunInformation,
  hooks: FlowNotebookHooks,
): Promise<CellResult> {
  if (!live.has(nodeId)) return textResult(notOnCanvasText(nodeId));
  const step = run.node_step_result.find((r) => r.node_id === nodeId);
  if (step?.success !== true) return notRunResult(run, nodeId);
  // A closed gate skipped it; a fetch would compute a branch the run left out.
  if (step.skipped) return textResult(`Node #${nodeId} has no result yet.`);
  try {
    let example = await NodeApi.getTableExample(flowId, nodeId);
    if (!example.has_example_data && run.execution_mode === "Performance") {
      const fetch = await runOnCanvas(
        flowId,
        () => FlowApi.triggerNodeFetch(flowId, nodeId),
        hooks,
      );
      const failed = failedStep(fetch, nodeId);
      if (failed) return failed;
      example = await NodeApi.getTableExample(flowId, nodeId);
    }
    if (!example.has_example_data) return textResult(`Node #${nodeId} has no result yet.`);
    return { display_outputs: [nodePreviewDisplay(example, nodeId)], error: null };
  } catch (e) {
    return errorResult(detailMessage(e, `Could not read the rows of node #${nodeId}.`));
  }
}

const RUN_POLL_MS = 500;
const RUN_START_GRACE_MS = 10_000;

/**
 * Start a canvas run and poll `/flow/run_status/` until a run newer than the previous one is
 * over; one that never shows up fails rather than report the previous run. The run hooks fire
 * only once core accepted the start, so a refused start never ends another run's state.
 */
async function runOnCanvas(
  flowId: number,
  start: () => Promise<unknown>,
  hooks: FlowNotebookHooks,
): Promise<RunInformation> {
  let info: RunInformation | null = null;
  let accepted = false;
  try {
    const before = (await FlowApi.getRunStatus(flowId)).start_time;
    await start();
    hooks.runStarted();
    accepted = true;
    const started = Date.now();
    let seenRunning = false;
    for (;;) {
      const status = await FlowApi.getRunStatus(flowId);
      seenRunning ||= status.is_running;
      // The first polls can still report the previous (or the "init") run as idle.
      const newer =
        status.run_type !== "init" && !!status.start_time && status.start_time !== before;
      if (!status.is_running && (seenRunning || newer)) {
        info = status;
        return status;
      }
      if (!seenRunning && !newer && Date.now() - started > RUN_START_GRACE_MS) {
        throw new Error("The run did not start.");
      }
      await new Promise((resolve) => setTimeout(resolve, RUN_POLL_MS));
    }
  } finally {
    if (accepted) hooks.runEnded(info);
  }
}

const ownerOf = (nb: OpenNotebook): string => ownerIdForNotebook(nb.tabId);

const refs = (nb: OpenNotebook): RuntimeCellRef[] =>
  nb.cells.map((c) => ({ id: c.id, isPython: c.cellType === "python" }));

/** Catalog tabs mark later results stale; flow cells show edited/synced/error instead. */
function invalidateAfter(nb: OpenNotebook, fromIndex: number): void {
  if (nb.flowId == null) invalidateFrom(ownerOf(nb), refs(nb), fromIndex, "upstream-changed");
}

/** First position a structural op can have invalidated; a move reaches back to its origin. */
function affectedIndex(op: CellOperation<NotebookCellModel>): number {
  return op.kind === "move" ? Math.min(op.from, op.to) : op.index;
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
    /** A new catalog tab inherits the active catalog tab's kernel. */
    inheritedKernelId(): string | null {
      return this.active?.flowId == null ? (this.active?.kernelId ?? null) : null;
    },
  },

  actions: {
    _snapshot() {
      return {
        openNotebooks: this.openNotebooks
          .filter((n) => n.flowId == null)
          .map((n) => ({
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
      invalidateAfter(nb, affectedIndex(result.op));
      if (nb.flowId == null || !isBlankNewCellOp(nb, result.op)) nb.dirty = true;
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
        kernelId: this.inheritedKernelId,
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
          kernelId: nb.default_kernel_id ?? this.inheritedKernelId,
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

    /** Open (or reuse) the flow's notebook tab, rendered from the canvas, and activate it. */
    async openFlowNotebook(flowId: number, name: string) {
      this.ensureHydrated();
      let nb = this.openNotebooks.find((n) => n.flowId === flowId);
      if (!nb) {
        this.openNotebooks.push({
          tabId: uid("tab"),
          persistedId: null,
          sessionFlowId: flowId,
          name,
          description: null,
          namespaceId: null,
          cells: [],
          kernelId: null,
          dirty: false,
          saving: false,
          executionCount: 0,
          focusedCellId: null,
          flowId,
        });
        nb = this.openNotebooks[this.openNotebooks.length - 1];
        ensureOwner(ownerOf(nb));
      }
      // Before the fetch: another tab must not show meanwhile, nor a late response re-activate this one.
      this.activeTabId = nb.tabId;
      const rendering = await NotebookApi.renderFlowNotebook(flowId);
      if (nb.fingerprint !== rendering.code_fingerprint) applyRendering(nb, rendering);
      return nb;
    },

    /** Re-render a flow tab after a canvas change; an unchanged fingerprint (a layout move) is a no-op. */
    async refreshFlowNotebook(flowId: number) {
      const rendering = await NotebookApi.renderFlowNotebook(flowId);
      const nb = this.openNotebooks.find((n) => n.flowId === flowId);
      if (nb && nb.fingerprint !== rendering.code_fingerprint) applyRendering(nb, rendering);
    },

    /** The canvas now holds the pushed `cells`: they count as unedited until the next rendering. */
    markFlowPushed(nb: OpenNotebook, result: NotebookPushResult, cells: [string, string][]) {
      nb.generated = Object.fromEntries(cells);
      nb.nodeIds = { ...nb.nodeIds, ...result.node_ids_by_cell };
      nb.fingerprint = result.code_fingerprint;
      nb.dirty = false;
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
      // A flow tab compares edits with the canvas; there `dirty` means a structural change.
      if (nb.flowId == null) nb.dirty = true;
      // Markdown edits change only their own preview, so they invalidate nothing.
      if (cell.cellType === "python" && nb.flowId == null) {
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
      invalidateAfter(nb, idx + 1);
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
      invalidateAfter(nb, affectedIndex(result.op));
      if (nb.flowId == null || !isBlankNewCellOp(nb, result.op)) nb.dirty = true;
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

    /** Resolves false when the run was refused or failed (no kernel, sync refused, kernel or request error). */
    async runCell(cellId: string): Promise<boolean> {
      const nb = this.active;
      const cell = nb?.cells.find((c) => c.id === cellId);
      if (!nb || !cell) return false;
      if (cell.cellType === "markdown") {
        this.runMarkdownCell(cell);
        return true;
      }
      if (nb.flowId != null) return this.runFlowCell(cellId);
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
          flow_id: nb.sessionFlowId, // negative, never a real flow id
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
      if (nb.flowId != null) await this.runAllFlow();
      else await this._runBatch(nb, nb.cells.slice(), { skipPythonWithoutKernel: true });
    },

    /** One flow action at a time per tab: it holds the tab's batch, the panel's busy flag. */
    async _withFlowBatch<T>(nb: OpenNotebook, action: () => Promise<T>): Promise<T | null> {
      const owner = ownerOf(nb);
      const batch = startBatch(owner, []);
      if (batch === null) return null;
      nb.notice = null;
      try {
        return await action();
      } finally {
        endBatch(owner, batch);
      }
    },

    /** Push the flow tab's cells onto the canvas (the Push button), reviewing the plan first. */
    async syncFlowNotebook(): Promise<FlowSyncStatus> {
      const nb = this.active;
      if (nb?.flowId == null) return "cancelled";
      const hooks = hooksFor(nb.flowId);
      const status = await this._withFlowBatch(nb, async () =>
        (await hooks.prepare()) ? this._syncFlow(nb, "push", hooks) : "cancelled",
      );
      return status ?? "busy";
    },

    /** Plan, confirm when needed, push; a refusal lands on its cell, the tab or the notice. */
    async _syncFlow(
      nb: OpenNotebook,
      trigger: FlowSyncTrigger,
      hooks: FlowNotebookHooks,
    ): Promise<FlowSyncStatus> {
      const flowId = nb.flowId!;
      this._clearSyncError(nb);
      let cells: [string, string][] = [];
      try {
        // `prepare` may have saved the settings drawer, which moves the canvas fingerprint.
        await this.refreshFlowNotebook(flowId);
        const nodes = (await FlowApi.getFlowData(flowId)).node_inputs;
        const body = flowPushBody(
          nb,
          new Map(nodes.map((n) => [n.id, n.item])),
          hooks.clientMaxNodeId(),
        );
        cells = body.cells;
        const plan = await NotebookApi.planPush(body);
        if (planNeedsConfirmation(plan, trigger) && !(await hooks.confirm(plan, trigger))) {
          return "cancelled";
        }
        const result = await NotebookApi.pushFlowNotebook(body);
        this.markFlowPushed(nb, result, cells);
        nb.syncForbidden = false;
        hooks.pushed(result);
        if (trigger === "push") nb.notice = { tone: "success", message: "Pushed to the canvas" };
        else if (result.warnings.length) {
          nb.notice = { tone: "warning", message: result.warnings.join("\n") };
        }
        return "synced";
      } catch (e) {
        return this._syncFailed(nb, e, cells);
      }
    },

    async _syncFailed(
      nb: OpenNotebook,
      error: unknown,
      cells: [string, string][],
    ): Promise<FlowSyncStatus> {
      const response = (error as { response?: { status?: number; data?: { detail?: unknown } } })
        ?.response;
      const detail = response?.data?.detail;
      if (response?.status === 409) {
        await this.refreshFlowNotebook(nb.flowId!).catch(() => undefined);
        nb.notice = { tone: "warning", message: CANVAS_CHANGED };
        return "conflict";
      }
      if (response?.status === 403) {
        nb.syncForbidden = true;
        nb.notice = { tone: "warning", message: SYNC_NEEDS_ADMIN };
        return "forbidden";
      }
      if (response?.status === 422 && isSyncErrorDetail(detail)) {
        const code = cells.find(([id]) => id === detail.cell_id)?.[1] ?? null;
        nb.syncError = { ...detail, code };
        const cell = nb.cells.find((c) => c.id === detail.cell_id);
        if (!cell) {
          nb.notice = { tone: "error", message: detail.message };
          return "invalid";
        }
        const where = detail.line != null ? `Line ${detail.line}: ` : "";
        cell.output = this._flowOutput(nb, errorResult(`${where}${detail.message}`), Date.now());
        cell.execState = "error";
        return "invalid";
      }
      nb.notice = { tone: "error", message: detailMessage(error, "The sync failed") };
      return "failed";
    },

    /** The previous refusal's output goes with it, so a fixed cell does not keep an old error. */
    _clearSyncError(nb: OpenNotebook) {
      const cell = nb.cells.find((c) => c.id === nb.syncError?.cell_id);
      if (cell) {
        cell.output = null;
        if (cell.execState === "error") cell.execState = "idle";
      }
      nb.syncError = null;
    },

    _flowOutput(nb: OpenNotebook, result: CellResult, started: number): CellOutput {
      nb.executionCount += 1;
      return {
        stdout: "",
        stderr: "",
        display_outputs: result.display_outputs,
        error: result.error,
        execution_time_ms: Date.now() - started,
        execution_count: nb.executionCount,
      };
    },

    /**
     * Run one flow cell: sync first when the canvas no longer matches, then per kind — imports
     * and plain cells show nothing, the parameters cell lists the flow's parameters, a node cell
     * runs its last node's lineage on the canvas and shows up to 100 of its rows.
     */
    async runFlowCell(cellId: string): Promise<boolean> {
      const nb = this.active;
      const cell = nb?.cells.find((c) => c.id === cellId);
      if (!nb || nb.flowId == null || !cell) return false;
      const flowId = nb.flowId;
      const hooks = hooksFor(flowId);
      const ran = await this._withFlowBatch(nb, async () => {
        const started = Date.now();
        const needsSync = flowNeedsSync(nb);
        cell.execState = "running";
        try {
          if (needsSync || flowCellKind(nb, cellId) === "node") {
            if (!(await hooks.prepare())) return false;
          }
          if (needsSync && (await this._syncFlow(nb, "run", hooks)) !== "synced") return false;
          const kind = flowCellKind(nb, cellId);
          if (kind === "imports" || kind === "plain") {
            cell.output = null;
            return true;
          }
          let result: CellResult;
          try {
            result =
              kind === "parameters"
                ? await parametersResult(flowId)
                : await this._runNodeCell(flowId, nb.nodeIds![cellId].at(-1)!, hooks);
          } catch (e) {
            result = errorResult(detailMessage(e, "The run failed"));
          }
          cell.output = this._flowOutput(nb, result, started);
          cell.execState = result.error ? "error" : "idle";
          return !result.error;
        } finally {
          if (cell.execState === "running") cell.execState = "idle";
        }
      });
      return ran ?? false;
    },

    /** The node's lineage on the canvas, then its rows; a failed step reports instead. */
    async _runNodeCell(flowId: number, nodeId: number, hooks: FlowNotebookHooks) {
      const live = await liveNodeIds(flowId);
      if (!live.has(nodeId)) return textResult(notOnCanvasText(nodeId));
      const info = await runOnCanvas(flowId, () => NotebookApi.runLineage(flowId, nodeId), hooks);
      return failedStep(info) ?? nodeResult(flowId, nodeId, live, info, hooks);
    },

    /** Sync, run the whole flow on the canvas, then refresh the parameters and node cells' outputs. */
    async runAllFlow(): Promise<boolean> {
      const nb = this.active;
      if (!nb || nb.flowId == null) return false;
      const flowId = nb.flowId;
      const hooks = hooksFor(flowId);
      const ran = await this._withFlowBatch(nb, async () => {
        const started = Date.now();
        for (const c of nb.cells) if (c.cellType === "markdown") this.runMarkdownCell(c);
        if (!(await hooks.prepare())) return false;
        if (flowNeedsSync(nb) && (await this._syncFlow(nb, "run", hooks)) !== "synced") {
          return false;
        }
        const python = nb.cells.filter((c) => c.cellType === "python");
        const targets = python.filter((c) =>
          ["parameters", "node"].includes(flowCellKind(nb, c.id)),
        );
        for (const c of python) if (!targets.includes(c)) c.output = null;
        for (const c of targets) c.execState = "running";
        try {
          const info = await runOnCanvas(flowId, () => FlowApi.runFlow(flowId), hooks);
          const live = await liveNodeIds(flowId);
          let ok = true;
          for (const cell of targets) {
            const nodeId = nb.nodeIds?.[cell.id]?.at(-1);
            const result =
              flowCellKind(nb, cell.id) === "parameters" || nodeId == null
                ? await parametersResult(flowId)
                : (failedStep(info, nodeId) ??
                  (await nodeResult(flowId, nodeId, live, info, hooks)));
            cell.output = this._flowOutput(nb, result, started);
            cell.execState = result.error ? "error" : "idle";
            ok &&= !result.error;
          }
          return ok;
        } catch (e) {
          nb.notice = { tone: "error", message: detailMessage(e, "The flow run failed") };
          return false;
        } finally {
          for (const c of targets) if (c.execState === "running") c.execState = "idle";
        }
      });
      return ran ?? false;
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

    /** Free every open catalog notebook's kernel namespace (don't leak them into the
     * 20-slot LRU shared with flow runs). Called when the panel unmounts; flow tabs
     * hold no kernel namespace. */
    async closeAllSessions() {
      for (const nb of this.openNotebooks.filter((n) => n.flowId == null)) {
        // The namespace is gone, so nothing still on screen can be current.
        bumpSessionEpoch(ownerOf(nb));
        if (nb.kernelId) {
          await KernelApi.clearNamespace(nb.kernelId, nb.sessionFlowId).catch(() => undefined);
        }
      }
    },
  },
});
