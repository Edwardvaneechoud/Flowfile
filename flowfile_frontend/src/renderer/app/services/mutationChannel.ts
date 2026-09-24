/**
 * The single ordered channel for requests that change a flow, or that must see every
 * earlier change (a run): they reach core one at a time, in the order they were issued,
 * and each response's history state is applied in that same order. Free of
 * store/axios.config imports so it can be unit-tested against a bare axios instance.
 */
import type {
  AxiosInstance,
  AxiosRequestTransformer,
  AxiosResponse,
  InternalAxiosRequestConfig,
} from "axios";
import type { HistoryState } from "../types";

declare module "axios" {
  interface AxiosRequestConfig {
    /** The channel slot this request is sent in; a gesture may reserve it before sending. */
    mutationSlot?: number;
  }
}

// Exact (normalized) paths of non-/editor/ routes that must be ordered.
const ORDERED_PATHS = new Set([
  "update_settings",
  "node/description",
  "node/reference",
  "user_defined_components/update_user_defined_node",
  "flow_settings",
  "save_flow",
  "save_flow_to_catalog",
  "overwrite_flow_in_catalog",
  "flow/apply_standard_layout",
  // Runs: core refuses edits while a run is active, so a run must not overtake queued edits.
  "flow/run",
  "node/trigger_fetch_data",
]);

function routePath(url: string, baseURL?: string): string | null {
  let path = url;
  if (/^[a-z][a-z\d+.-]*:\/\//i.test(path)) {
    // An absolute URL is only ours when it points at the configured core base.
    if (!baseURL || !path.startsWith(baseURL)) return null;
    path = path.slice(baseURL.length);
  }
  return path.split(/[?#]/)[0].replace(/^\/+/, "").replace(/\/+$/, "");
}

/**
 * True for requests that must be ordered: every non-GET /editor/* route except code
 * exports, plus node settings, descriptions, references, flow settings, saves, layout and
 * runs. Cancel, uploads, AI and catalog traffic are deliberately left out.
 */
export function isOrderedRequest(
  method: string | undefined,
  url: string | undefined,
  baseURL?: string,
): boolean {
  if (!url || (method ?? "get").toLowerCase() === "get") return false;
  const path = routePath(url, baseURL);
  if (path === null) return false;
  if (path.startsWith("editor/")) return !path.startsWith("editor/code_to_");
  if (path.startsWith("transform/")) return true;
  return ORDERED_PATHS.has(path);
}

/** The history state a mutation response carries, when it names its flow. */
export function historyOf(data: unknown): HistoryState | null {
  if (!data || typeof data !== "object") return null;
  const history = (data as { history?: unknown }).history;
  if (!history || typeof history !== "object") return null;
  const flowId = (history as { flow_id?: unknown }).flow_id;
  return typeof flowId === "number" ? (history as HistoryState) : null;
}

/**
 * A failed ordered request that core answered (and did not just ask to re-authenticate):
 * the channel resyncs the canvas for it, whatever the call site does.
 */
export function isRefusedMutation(error: unknown): boolean {
  const failure = error as {
    config?: { mutationSlot?: number };
    response?: { status?: number };
  } | null;
  return (
    failure?.config?.mutationSlot !== undefined &&
    !!failure.response &&
    failure.response.status !== 401
  );
}

export interface MutationQueue {
  /** Take the next slot. */
  enqueue(): number;
  /** Resolves once every slot taken before `id` has been released. */
  ready(id: number): Promise<void>;
  /** Free a slot; false when it was already free. */
  release(id: number): boolean;
  whenIdle(): Promise<void>;
  generation(): number;
}

export function createMutationQueue(): MutationQueue {
  let tail: Promise<void> = Promise.resolve();
  let generation = 0;
  let nextId = 0;
  const slots = new Map<number, { ready: Promise<void>; free: () => void }>();

  return {
    enqueue() {
      const id = ++nextId;
      generation += 1;
      const ready = tail;
      const done = new Promise<void>((resolve) => slots.set(id, { ready, free: resolve }));
      tail = ready.then(() => done);
      return id;
    },
    ready(id) {
      return slots.get(id)?.ready ?? Promise.resolve();
    },
    release(id) {
      const slot = slots.get(id);
      if (!slot) return false;
      slots.delete(id);
      slot.free();
      return true;
    },
    whenIdle() {
      return tail;
    },
    generation() {
      return generation;
    },
  };
}

// Request transformers that already free their slot when they throw.
const guardedTransforms = new WeakSet<AxiosRequestTransformer>();

/**
 * A request transform that throws never reaches the adapter and its error carries no
 * config, so no response interceptor can tell whose slot it held: free it right there.
 */
function freeSlotOnTransformError(config: InternalAxiosRequestConfig, free: () => void): void {
  const transforms = [config.transformRequest ?? []].flat();
  if (transforms.some((transform) => guardedTransforms.has(transform))) return;
  const guarded: AxiosRequestTransformer = function (
    this: InternalAxiosRequestConfig,
    data,
    headers,
  ) {
    try {
      return transforms.reduce((value, transform) => transform.call(this, value, headers), data);
    } catch (error) {
      free();
      throw error;
    }
  };
  guardedTransforms.add(guarded);
  config.transformRequest = [guarded];
}

/**
 * Route an axios instance's ordered requests through `queue`. Install it AFTER the auth
 * interceptors: request interceptors run in reverse order, so the slot is taken before
 * any async auth work, and the response side then sees a 401 retry complete before it
 * releases. A request carrying `mutationSlot` is sent in that (reserved) slot.
 * `onRefused` runs once for every ordered request core refused (see isRefusedMutation),
 * so an optimistic canvas change is always resynced, whether or not its call site catches.
 */
export function installMutationChannel(
  instance: AxiosInstance,
  queue: MutationQueue,
  applyHistory: (history: HistoryState) => void,
  onRefused?: (error: unknown) => void,
): void {
  instance.interceptors.request.use(async (config: InternalAxiosRequestConfig) => {
    if (config.mutationSlot === undefined) {
      if (!isOrderedRequest(config.method, config.url, config.baseURL)) return config;
      config.mutationSlot = queue.enqueue();
    }
    const slot = config.mutationSlot;
    freeSlotOnTransformError(config, () => queue.release(slot));
    // A 401 retry keeps its slot, which is already ready.
    await queue.ready(slot);
    return config;
  });

  instance.interceptors.response.use(
    (response: AxiosResponse) => {
      const slot = response.config?.mutationSlot;
      // Only the release that frees the slot applies history (a 401 retry passes twice).
      if (slot === undefined || !queue.release(slot)) return response;
      const history = historyOf(response.data);
      if (history) {
        try {
          applyHistory(history);
        } catch (error) {
          console.error("Failed to apply history state:", error);
        }
      }
      return response;
    },
    (error: { config?: InternalAxiosRequestConfig }) => {
      const slot = error?.config?.mutationSlot;
      // As above: only the release that frees the slot reacts.
      if (slot !== undefined && queue.release(slot) && isRefusedMutation(error)) {
        try {
          onRefused?.(error);
        } catch (hookError) {
          console.error("Failed to handle a refused mutation:", hookError);
        }
      }
      return Promise.reject(error);
    },
  );
}

const pendingEditFlushers = new Set<() => void | Promise<void>>();

/**
 * Register a local canvas edit that is debounced before it is sent (arrow-key nudges).
 * Its flusher resolves once the request has been issued, not when it completes.
 */
export function registerPendingEdit(flush: () => void | Promise<void>): () => void {
  pendingEditFlushers.add(flush);
  return () => pendingEditFlushers.delete(flush);
}

/** Issue every debounced local edit now instead of when its timer fires. */
export async function flushPendingEdits(): Promise<void> {
  await Promise.all([...pendingEditFlushers].map((flush) => flush()));
}
