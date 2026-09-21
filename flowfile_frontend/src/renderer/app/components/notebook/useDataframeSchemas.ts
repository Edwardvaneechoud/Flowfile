// Module state, not Pinia: the completion source reads this cache synchronously on keystroke.
import { LspApi } from "@/api/lsp.api";
import type {
  CatalogRefLiteral,
  FrameKind,
  SchemaColumn,
} from "../nodes/node-types/elements/pythonScript/dataframeSchemaTypes";
import { resolveCatalogRefs } from "./catalogRefResolver";
import {
  isBatchActive,
  onExecutionSettled,
  onSessionEpoch,
  type SettledMeta,
} from "./notebookRuntimeState";

export interface FrameSchema {
  kind: FrameKind;
  state: string;
  columns: SchemaColumn[];
  truncated: boolean;
}

export interface SchemaCacheEntry {
  kernelId: string;
  flowId: number;
  generation: string;
  revision: number;
  frames: Map<string, FrameSchema>;
}

export interface DataframeSchemaContext {
  kernelId: string | null;
  flowId: number;
  nodeId?: number | null;
  catalogRefs: () => CatalogRefLiteral[];
}

export type DataframeSchemaContextGetter = () => DataframeSchemaContext;

interface OwnerState {
  entry: SchemaCacheEntry | null;
  getCtx: DataframeSchemaContextGetter | null;
  attached: boolean;
  unsubscribes: (() => void)[];
  inFlight: Promise<void> | null;
  lastGeneration: string | null;
  refreshSeq: number;
  deferTimer: ReturnType<typeof setTimeout> | null;
  retryTimer: ReturnType<typeof setTimeout> | null;
}

const BUSY_RETRY_MS = 1500;

const owners = new Map<string, OwnerState>();

export function getSchemas(ownerId: string): SchemaCacheEntry | null {
  return owners.get(ownerId)?.entry ?? null;
}

export function invalidate(ownerId: string): void {
  const state = owners.get(ownerId);
  if (state) state.entry = null;
}

/** A run against a different namespace generation makes the cached frames unrelated. */
export function noteExecution(ownerId: string, meta: SettledMeta): void {
  const state = owners.get(ownerId);
  if (!state) return;
  const generation = meta?.namespace_generation;
  if (!generation) return;
  if (state.entry && state.entry.generation !== generation) state.entry = null;
  state.lastGeneration = generation;
}

export function refresh(ownerId: string): Promise<void> {
  return refreshInternal(ownerId, true);
}

export function attachDataframeSchemas(
  ownerId: string,
  getCtx: DataframeSchemaContextGetter,
): () => void {
  const state = ensureState(ownerId);
  state.getCtx = getCtx;
  state.attached = true;

  const ctx = getCtx();
  if (ctx.kernelId) void refresh(ownerId);
  void resolveCatalogRefs(ctx.catalogRefs());

  state.unsubscribes.push(
    onSessionEpoch(ownerId, () => {
      invalidate(ownerId);
      // The next kernel mints its own generation; keeping the old one rejects every answer.
      state.lastGeneration = null;
      void refresh(ownerId);
    }),
  );
  state.unsubscribes.push(
    onExecutionSettled(ownerId, (_cellId, verdict, meta) => {
      if (verdict === "discard") return;
      noteExecution(ownerId, meta ?? {});
      const current = state.getCtx?.();
      if (current) void resolveCatalogRefs(current.catalogRefs());
      scheduleRefresh(ownerId, state);
    }),
  );

  return () => detach(ownerId);
}

/** Test seam. */
export function resetDataframeSchemas(): void {
  for (const ownerId of Array.from(owners.keys())) detach(ownerId);
  owners.clear();
}

function ensureState(ownerId: string): OwnerState {
  let state = owners.get(ownerId);
  if (!state) {
    state = {
      entry: null,
      getCtx: null,
      attached: false,
      unsubscribes: [],
      inFlight: null,
      lastGeneration: null,
      refreshSeq: 0,
      deferTimer: null,
      retryTimer: null,
    };
    owners.set(ownerId, state);
  }
  return state;
}

function detach(ownerId: string): void {
  const state = owners.get(ownerId);
  if (!state) return;
  for (const off of state.unsubscribes) off();
  state.unsubscribes = [];
  state.attached = false;
  state.getCtx = null;
  clearTimer(state, "deferTimer");
  clearTimer(state, "retryTimer");
  owners.delete(ownerId);
}

function clearTimer(state: OwnerState, key: "deferTimer" | "retryTimer"): void {
  if (state[key] !== null) {
    clearTimeout(state[key] as ReturnType<typeof setTimeout>);
    state[key] = null;
  }
}

// Run All settles once per cell; deferring past an active batch collapses those into one request.
function scheduleRefresh(ownerId: string, state: OwnerState): void {
  if (state.deferTimer !== null) return;
  state.deferTimer = setTimeout(() => {
    state.deferTimer = null;
    if (!state.attached || isBatchActive(ownerId)) return;
    void refresh(ownerId);
  }, 0);
}

function refreshInternal(ownerId: string, allowRetry: boolean): Promise<void> {
  const state = owners.get(ownerId);
  if (!state) return Promise.resolve();
  if (state.inFlight) return state.inFlight;
  const run = doRefresh(ownerId, state, allowRetry).finally(() => {
    if (state.inFlight === run) state.inFlight = null;
  });
  state.inFlight = run;
  return run;
}

async function doRefresh(ownerId: string, state: OwnerState, allowRetry: boolean): Promise<void> {
  const ctx = state.getCtx?.();
  const kernelId = ctx?.kernelId;
  if (!ctx || !kernelId) return;
  const capabilities = await LspApi.capabilities();
  if (!capabilities?.enabled) return;

  const flowId = ctx.flowId;
  const seq = (state.refreshSeq += 1);
  const response = await LspApi.dataframeSchemas(kernelId, {
    flow_id: flowId,
    node_id: ctx.nodeId ?? null,
  });

  if (owners.get(ownerId) !== state) return;
  const now = state.getCtx?.();
  if (!now || now.kernelId !== kernelId || now.flowId !== flowId) return;

  // "No namespace for this flow" needs no generation/revision: there is nothing to be older than.
  if (response.state === "unavailable") {
    state.entry = null;
    return;
  }
  if (state.lastGeneration !== null && response.namespace_generation !== state.lastGeneration) {
    return;
  }
  if (state.entry && response.revision < state.entry.revision) return;

  if (response.state === "busy") {
    if (!allowRetry) return;
    clearTimer(state, "retryTimer");
    state.retryTimer = setTimeout(() => {
      state.retryTimer = null;
      if (!state.attached || state.refreshSeq !== seq) return;
      void refreshInternal(ownerId, false);
    }, BUSY_RETRY_MS);
    return;
  }
  if (response.state !== "ready") return;

  const frames = new Map<string, FrameSchema>();
  for (const frame of response.dataframes ?? []) {
    frames.set(frame.name, {
      kind: toFrameKind(frame.kind),
      state: frame.state,
      columns: (frame.columns ?? []).map((c) => ({ name: c.name, dtype: c.dtype })),
      truncated: !!frame.truncated,
    });
  }
  state.entry = {
    kernelId,
    flowId,
    generation: response.namespace_generation,
    revision: response.revision,
    frames,
  };
}

function toFrameKind(kind: string): FrameKind {
  return kind === "DataFrame" || kind === "LazyFrame" ? kind : "unknown";
}
