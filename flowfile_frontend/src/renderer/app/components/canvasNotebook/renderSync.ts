import type { NotebookRendering } from "../../api/canvasNotebook.api";

export const RENDER_DEBOUNCE_MS = 400;

export interface RenderSyncDeps {
  getFlowId: () => number | null;
  fetchRendering: (flowId: number) => Promise<NotebookRendering>;
  whenMutationsIdle: () => Promise<void>;
  mutationGeneration: () => number;
  apply: (flowId: number, rendering: NotebookRendering) => void;
  onError: (flowId: number, error: unknown) => void;
  debounceMs?: number;
}

export interface RenderSync {
  /** Debounced refresh, for graph-version bumps. */
  schedule: () => void;
  /** Immediate refresh, for opening the dock or switching flows. */
  refreshNow: () => Promise<void>;
  dispose: () => void;
}

/**
 * Keeps the dock's rendering in step with the canvas.
 *
 * A refresh waits for the mutation channel to go idle and re-fetches when a mutation was
 * enqueued while the request was in flight, so the dock never shows a graph core has
 * already moved past. A newer refresh (or a dispose) supersedes an older one's response.
 */
export function createRenderSync(deps: RenderSyncDeps): RenderSync {
  const debounceMs = deps.debounceMs ?? RENDER_DEBOUNCE_MS;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let token = 0;

  const clearTimer = () => {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  };

  const run = async (myToken: number): Promise<void> => {
    for (;;) {
      await deps.whenMutationsIdle();
      if (myToken !== token) return;
      const flowId = deps.getFlowId();
      if (flowId === null || flowId <= 0) return;
      const generation = deps.mutationGeneration();
      let rendering: NotebookRendering;
      try {
        rendering = await deps.fetchRendering(flowId);
      } catch (error) {
        if (myToken === token) deps.onError(flowId, error);
        return;
      }
      if (myToken !== token || flowId !== deps.getFlowId()) return;
      if (generation !== deps.mutationGeneration()) continue;
      deps.apply(flowId, rendering);
      return;
    }
  };

  return {
    schedule() {
      clearTimer();
      timer = setTimeout(() => {
        timer = null;
        void run(++token);
      }, debounceMs);
    },
    refreshNow() {
      clearTimer();
      return run(++token);
    },
    dispose() {
      clearTimer();
      token += 1;
    },
  };
}
