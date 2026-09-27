import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { NotebookRendering } from "../../api/canvasNotebook.api";
import { createRenderSync, type RenderSyncDeps } from "./renderSync";

const rendering = (fingerprint: string): NotebookRendering => ({
  cells: [],
  warnings: [],
  var_by_node: {},
  code_fingerprint: fingerprint,
});

function harness(overrides: Partial<RenderSyncDeps> = {}) {
  const state = { flowId: 1 as number | null, generation: 0 };
  const apply = vi.fn();
  const onError = vi.fn();
  const fetchRendering = vi.fn(async () => rendering("fp"));
  const deps: RenderSyncDeps = {
    getFlowId: () => state.flowId,
    fetchRendering,
    whenMutationsIdle: () => Promise.resolve(),
    mutationGeneration: () => state.generation,
    apply,
    onError,
    ...overrides,
  };
  return { state, apply, onError, fetchRendering, sync: createRenderSync(deps) };
}

const flush = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve();
};

describe("createRenderSync", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("debounces a burst of schedules into one fetch after 400 ms", async () => {
    const { sync, fetchRendering, apply } = harness();
    sync.schedule();
    vi.advanceTimersByTime(200);
    sync.schedule();
    vi.advanceTimersByTime(399);
    await flush();
    expect(fetchRendering).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    await flush();
    expect(fetchRendering).toHaveBeenCalledTimes(1);
    expect(apply).toHaveBeenCalledWith(1, rendering("fp"));
  });

  it("waits for the mutation channel to go idle before fetching", async () => {
    let release: (() => void) | undefined;
    const idle = new Promise<void>((resolve) => (release = resolve));
    const { sync, fetchRendering } = harness({ whenMutationsIdle: () => idle });
    const done = sync.refreshNow();
    await flush();
    expect(fetchRendering).not.toHaveBeenCalled();
    release?.();
    await done;
    expect(fetchRendering).toHaveBeenCalledTimes(1);
  });

  it("re-fetches when a mutation was enqueued while the request was in flight", async () => {
    const h = harness();
    h.fetchRendering.mockImplementationOnce(async () => {
      h.state.generation += 1;
      return rendering("stale");
    });
    await h.sync.refreshNow();
    expect(h.fetchRendering).toHaveBeenCalledTimes(2);
    expect(h.apply).toHaveBeenCalledTimes(1);
    expect(h.apply).toHaveBeenCalledWith(1, rendering("fp"));
  });

  it("drops a response superseded by a newer refresh", async () => {
    const h = harness();
    let resolveFirst: ((value: NotebookRendering) => void) | undefined;
    h.fetchRendering.mockImplementationOnce(
      () => new Promise<NotebookRendering>((resolve) => (resolveFirst = resolve)),
    );
    const first = h.sync.refreshNow();
    await flush();
    await h.sync.refreshNow();
    resolveFirst?.(rendering("old"));
    await first;
    expect(h.apply).toHaveBeenCalledTimes(1);
    expect(h.apply).toHaveBeenCalledWith(1, rendering("fp"));
  });

  it("drops a response for a flow the canvas has left", async () => {
    const h = harness();
    h.fetchRendering.mockImplementationOnce(async () => {
      h.state.flowId = 2;
      return rendering("fp");
    });
    await h.sync.refreshNow();
    expect(h.apply).not.toHaveBeenCalled();
  });

  it("does nothing without an open flow, and nothing after dispose", async () => {
    const h = harness();
    h.state.flowId = null;
    await h.sync.refreshNow();
    expect(h.fetchRendering).not.toHaveBeenCalled();
    h.state.flowId = 1;
    h.sync.schedule();
    h.sync.dispose();
    vi.advanceTimersByTime(1000);
    await flush();
    expect(h.fetchRendering).not.toHaveBeenCalled();
  });

  it("reports a failed fetch", async () => {
    const h = harness();
    h.fetchRendering.mockRejectedValueOnce(new Error("boom"));
    await h.sync.refreshNow();
    expect(h.onError).toHaveBeenCalledWith(1, expect.any(Error));
    expect(h.apply).not.toHaveBeenCalled();
  });
});
