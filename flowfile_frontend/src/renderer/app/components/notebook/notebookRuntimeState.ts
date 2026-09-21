// Execution identity and staleness per owner id + cell id. Not Pinia: the node notebook has no
// store. Transient by design — it never reaches notebook YAML or python_script_input.
import { reactive } from "vue";

export type CellStatus = "idle" | "queued" | "running";

export type StaleReason = "code-changed" | "upstream-changed" | "previous-session";

export interface CellRuntime {
  sourceRevision: number;
  submittedRevision: number | null;
  requestId: number | null;
  epoch: number;
  status: CellStatus;
  staleReason: StaleReason | null;
  staleAt: number;
}

export interface BatchProgress {
  id: number;
  total: number;
  done: number;
  currentCellId: string | null;
}

export interface OwnerRuntime {
  sessionEpoch: number;
  batch: BatchProgress | null;
  cells: Record<string, CellRuntime>;
}

export interface ExecutionTicket {
  ownerId: string;
  cellId: string;
  requestId: number;
  revision: number;
  epoch: number;
}

export type SettleVerdict = "apply" | "apply-stale" | "discard";

export interface RuntimeCellRef {
  id: string;
  isPython: boolean;
}

/** Namespace identity stamped on an ExecuteResult; the schema cache is keyed on it. */
export interface SettledMeta {
  namespace_generation?: string | null;
  revision?: number | null;
}

export type SessionEpochListener = (epoch: number) => void;

export type ExecutionSettledListener = (
  cellId: string,
  verdict: SettleVerdict,
  meta?: SettledMeta,
) => void;

export interface ExecutionBatchOptions {
  ownerId: string;
  cells: RuntimeCellRef[];
  runOne: (cellId: string) => Promise<{ ok: boolean }>;
  onMarkdown?: (cellId: string) => void;
  stillPresent: (cellId: string) => boolean;
}

const owners = reactive<Record<string, OwnerRuntime>>({});
const epochListeners = new Map<string, Set<SessionEpochListener>>();
const settledListeners = new Map<string, Set<ExecutionSettledListener>>();

// One counter behind both request ids and stale marks, so "was this cell marked stale after its
// request went out?" is a single comparison.
let sequence = 0;

function nextSeq(): number {
  sequence += 1;
  return sequence;
}

const STALE_RANK: Record<StaleReason, number> = {
  "upstream-changed": 1,
  "code-changed": 2,
  "previous-session": 3,
};

const STALE_LABELS: Record<StaleReason, string> = {
  "code-changed": "Code changed — rerun",
  "upstream-changed": "Earlier cells changed — rerun",
  "previous-session": "Previous session",
};

const STALE_TITLES: Record<StaleReason, string> = {
  "code-changed":
    "This cell's code changed after this result was produced, so run it again to refresh it.",
  "upstream-changed":
    "An earlier cell changed or ran again after this result was produced, so it may be out of date.",
  "previous-session":
    "This result came from an earlier kernel session, so the variables behind it no longer exist.",
};

export function staleLabel(reason: StaleReason): string {
  return STALE_LABELS[reason];
}

export function staleTitle(reason: StaleReason): string {
  return STALE_TITLES[reason];
}

/** A marker never downgrades: previous-session > code-changed > upstream-changed. */
function applyStale(rt: CellRuntime, reason: StaleReason): void {
  rt.staleAt = nextSeq();
  const current = rt.staleReason ? STALE_RANK[rt.staleReason] : 0;
  if (STALE_RANK[reason] > current) rt.staleReason = reason;
}

function createCellRuntime(epoch: number): CellRuntime {
  return {
    sourceRevision: 0,
    submittedRevision: null,
    requestId: null,
    epoch,
    status: "idle",
    staleReason: null,
    staleAt: 0,
  };
}

function ensureCell(owner: OwnerRuntime, cellId: string): CellRuntime {
  if (!owner.cells[cellId]) owner.cells[cellId] = createCellRuntime(owner.sessionEpoch);
  return owner.cells[cellId];
}

export function ensureOwner(ownerId: string): OwnerRuntime {
  if (!owners[ownerId]) owners[ownerId] = { sessionEpoch: 0, batch: null, cells: {} };
  return owners[ownerId];
}

export function getOwner(ownerId: string): OwnerRuntime | undefined {
  return owners[ownerId];
}

/** Read-only lookup: templates call this, so it must never create an entry. */
export function cellRuntime(ownerId: string, cellId: string): CellRuntime | undefined {
  return owners[ownerId]?.cells[cellId];
}

export function disposeOwner(ownerId: string): void {
  delete owners[ownerId];
}

export function bumpSourceRevision(ownerId: string, cellId: string): void {
  const owner = ensureOwner(ownerId);
  const rt = ensureCell(owner, cellId);
  rt.sourceRevision += 1;
  if (rt.submittedRevision !== null) applyStale(rt, "code-changed");
}

/** Syncs membership to `cells`, then marks python cells at `fromIndex` and later. */
export function invalidateFrom(
  ownerId: string,
  cells: RuntimeCellRef[],
  fromIndex: number,
  reason: StaleReason,
): void {
  const owner = ensureOwner(ownerId);
  const live = new Set(cells.map((c) => c.id));
  for (const id of Object.keys(owner.cells)) {
    if (!live.has(id)) delete owner.cells[id];
  }
  for (let i = 0; i < cells.length; i += 1) {
    const ref = cells[i];
    const rt = ensureCell(owner, ref.id);
    if (i >= fromIndex && ref.isPython) applyStale(rt, reason);
  }
}

export function markDownstreamStale(
  ownerId: string,
  cells: RuntimeCellRef[],
  cellId: string,
): void {
  const index = cells.findIndex((c) => c.id === cellId);
  invalidateFrom(ownerId, cells, index < 0 ? cells.length : index + 1, "upstream-changed");
}

export function beginExecution(ownerId: string, cellId: string): ExecutionTicket {
  const owner = ensureOwner(ownerId);
  const rt = ensureCell(owner, cellId);
  const requestId = nextSeq();
  rt.requestId = requestId;
  rt.status = "running";
  return { ownerId, cellId, requestId, revision: rt.sourceRevision, epoch: owner.sessionEpoch };
}

export function settleExecution(ticket: ExecutionTicket, meta?: SettledMeta): SettleVerdict {
  const owner = owners[ticket.ownerId];
  const rt = owner?.cells[ticket.cellId];
  let verdict: SettleVerdict = "discard";
  if (owner && rt && rt.requestId === ticket.requestId && owner.sessionEpoch === ticket.epoch) {
    rt.status = "idle";
    rt.submittedRevision = ticket.revision;
    rt.epoch = ticket.epoch;
    if (rt.sourceRevision !== ticket.revision) {
      applyStale(rt, "code-changed");
      verdict = "apply-stale";
    } else if (rt.staleAt > ticket.requestId) {
      // An earlier cell ran or changed while this one was in flight; keep the reason it carries.
      verdict = "apply-stale";
    } else {
      rt.staleReason = null;
      verdict = "apply";
    }
  }
  emitSettled(ticket.ownerId, ticket.cellId, verdict, meta);
  return verdict;
}

export function bumpSessionEpoch(ownerId: string): number {
  const owner = ensureOwner(ownerId);
  owner.sessionEpoch += 1;
  owner.batch = null;
  for (const rt of Object.values(owner.cells)) {
    rt.status = "idle";
    if (rt.submittedRevision !== null) applyStale(rt, "previous-session");
  }
  const listeners = epochListeners.get(ownerId);
  if (listeners) for (const cb of Array.from(listeners)) cb(owner.sessionEpoch);
  return owner.sessionEpoch;
}

export function clearResults(ownerId: string): void {
  const owner = owners[ownerId];
  if (!owner) return;
  for (const rt of Object.values(owner.cells)) {
    rt.submittedRevision = null;
    rt.staleReason = null;
  }
}

export function isBatchActive(ownerId: string): boolean {
  return !!owners[ownerId]?.batch;
}

export function batchProgress(ownerId: string): BatchProgress | null {
  return owners[ownerId]?.batch ?? null;
}

/** `null` when a batch is already running — the duplicate Run All guard. */
export function startBatch(ownerId: string, cellIds: string[]): number | null {
  const owner = ensureOwner(ownerId);
  if (owner.batch) return null;
  const id = nextSeq();
  owner.batch = { id, total: cellIds.length, done: 0, currentCellId: null };
  for (const cellId of cellIds) ensureCell(owner, cellId).status = "queued";
  return id;
}

export function endBatch(ownerId: string, batchId: number): void {
  const owner = owners[ownerId];
  if (!owner || owner.batch?.id !== batchId) return;
  owner.batch = null;
  for (const rt of Object.values(owner.cells)) {
    if (rt.status === "queued") rt.status = "idle";
  }
}

export function snapshotRevisions(ownerId: string, ids: string[]): Record<string, number> {
  const owner = owners[ownerId];
  const snapshot: Record<string, number> = {};
  if (!owner) return snapshot;
  for (const id of ids) {
    const rt = owner.cells[id];
    if (rt) snapshot[id] = rt.sourceRevision;
  }
  return snapshot;
}

export function onSessionEpoch(ownerId: string, cb: SessionEpochListener): () => void {
  let listeners = epochListeners.get(ownerId);
  if (!listeners) {
    listeners = new Set();
    epochListeners.set(ownerId, listeners);
  }
  listeners.add(cb);
  return () => {
    listeners?.delete(cb);
    if (listeners && !listeners.size) epochListeners.delete(ownerId);
  };
}

export function onExecutionSettled(ownerId: string, cb: ExecutionSettledListener): () => void {
  let listeners = settledListeners.get(ownerId);
  if (!listeners) {
    listeners = new Set();
    settledListeners.set(ownerId, listeners);
  }
  listeners.add(cb);
  return () => {
    listeners?.delete(cb);
    if (listeners && !listeners.size) settledListeners.delete(ownerId);
  };
}

function emitSettled(
  ownerId: string,
  cellId: string,
  verdict: SettleVerdict,
  meta?: SettledMeta,
): void {
  const listeners = settledListeners.get(ownerId);
  if (!listeners) return;
  for (const cb of Array.from(listeners)) cb(cellId, verdict, meta);
}

/**
 * Runs `cells` (a snapshot, never a live collection) as one batch. Resolves false when another
 * batch is already active, without ever calling `runOne`.
 */
export async function runExecutionBatch(opts: ExecutionBatchOptions): Promise<boolean> {
  const { ownerId } = opts;
  // Own copy: the batch must survive the caller mutating its list mid-run.
  const cells = opts.cells.slice();
  const pythonIds = cells.filter((c) => c.isPython).map((c) => c.id);
  const batchId = startBatch(ownerId, pythonIds);
  if (batchId === null) return false;
  const startEpoch = ensureOwner(ownerId).sessionEpoch;
  const revisions = snapshotRevisions(ownerId, pythonIds);
  try {
    for (const ref of cells) {
      if (!opts.stillPresent(ref.id)) break;
      if (getOwner(ownerId)?.sessionEpoch !== startEpoch) break;
      if (!ref.isPython) {
        opts.onMarkdown?.(ref.id);
        continue;
      }
      const rt = cellRuntime(ownerId, ref.id);
      if (!rt || rt.sourceRevision !== revisions[ref.id]) break;
      const batch = batchProgress(ownerId);
      if (batch?.id === batchId) batch.currentCellId = ref.id;
      const result = await opts.runOne(ref.id);
      const after = batchProgress(ownerId);
      if (after?.id === batchId) after.done += 1;
      if (!result?.ok) break;
    }
  } finally {
    endBatch(ownerId, batchId);
  }
  return true;
}
