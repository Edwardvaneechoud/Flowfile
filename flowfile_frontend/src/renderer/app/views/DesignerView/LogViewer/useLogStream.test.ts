// @vitest-environment happy-dom
// The viewer only opens log streams; core ends them. A run here opens one, a run elsewhere
// re-reads the file, a run's end stops nothing, and a retry dies with the viewer.
import { createApp, defineComponent, reactive, type App } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

interface Stream {
  signal: AbortSignal;
  onOpen?: () => void;
  onData: (data: string) => void;
  end: () => void;
}

const h = vi.hoisted(() => ({
  stores: {
    node: null as unknown as { isRunning: boolean; flow_id: number },
    editor: null as unknown as { isShowingLogViewer: boolean },
    flow: null as unknown as { pendingRunStateCounter: number },
  },
  streams: [] as Stream[],
  streamFlowLogs: vi.fn(),
}));

vi.mock("../../../stores/column-store", () => ({ useNodeStore: () => h.stores.node }));
vi.mock("../../../stores/editor-store", () => ({ useEditorStore: () => h.stores.editor }));
vi.mock("../../../stores/flow-store", () => ({ useFlowStore: () => h.stores.flow }));
vi.mock("../../../services/auth.service", () => ({
  default: { getToken: vi.fn(async () => "jwt"), hasValidToken: () => true },
}));
vi.mock("../../../services/logStreamClient", () => ({ streamFlowLogs: h.streamFlowLogs }));

import { useLogStream } from "./useLogStream";

const settle = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve();
};

describe("useLogStream", () => {
  let app: App | null = null;

  const mount = () => {
    let api!: ReturnType<typeof useLogStream>;
    app = createApp(
      defineComponent({
        setup() {
          api = useLogStream();
          return () => null;
        },
      }),
    );
    app.mount(document.createElement("div"));
    return api;
  };

  beforeEach(() => {
    vi.clearAllMocks();
    h.stores.node = reactive({ isRunning: false, flow_id: 4 });
    h.stores.editor = reactive({ isShowingLogViewer: false });
    h.stores.flow = reactive({ pendingRunStateCounter: 0 });
    h.streams.length = 0;
    h.streamFlowLogs.mockImplementation(
      (options: Omit<Stream, "end">) =>
        new Promise<void>((resolve, reject) => {
          h.streams.push({ ...options, end: resolve });
          options.signal.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        }),
    );
    vi.spyOn(console, "error").mockImplementation(() => undefined);
  });

  afterEach(() => {
    app?.unmount();
    app = null;
    vi.useRealTimers();
  });

  it("opens nothing for a dock opened on a preview; shown logs read the file once", async () => {
    const api = mount();
    await settle();
    expect(h.streams).toHaveLength(0);

    h.stores.editor.isShowingLogViewer = true;
    await settle();
    expect(h.streams).toHaveLength(1);

    h.streams[0].onOpen?.();
    h.streams[0].onData('"line one"');
    h.streams[0].end();
    await settle();
    expect(api.logs.value).toBe("line one\n");
    expect(api.connectionStatus.value).toBe("disconnected");
    expect(api.errorMessage.value).toBeNull();
  });

  it("a run started here opens its stream, and the run's end stops nothing", async () => {
    mount();
    await settle();
    h.stores.node.isRunning = true;
    await settle();
    expect(h.streams).toHaveLength(1);

    h.stores.node.isRunning = false;
    await settle();
    expect(h.streams).toHaveLength(1);
    expect(h.streams[0].signal.aborted).toBe(false);
  });

  it("a run elsewhere re-reads the file from the start; a late running read is left alone", async () => {
    const api = mount();
    h.stores.flow.pendingRunStateCounter += 1;
    await settle();
    expect(h.streams).toHaveLength(0); // hidden and idle: nothing to show

    h.stores.editor.isShowingLogViewer = true;
    await settle();
    h.streams[0].onData('"old run"');
    h.stores.flow.pendingRunStateCounter += 1;
    await settle();
    expect(h.streams).toHaveLength(2);
    expect(h.streams[0].signal.aborted).toBe(true);
    expect(api.logs.value).toBe("");

    h.stores.node.isRunning = true;
    await settle();
    h.stores.node.isRunning = false;
    await settle();
    expect(h.streams).toHaveLength(3);
    expect(h.streams[2].signal.aborted).toBe(false);
  });

  it("retries a run's stream that gave no lines, and the retry dies with the viewer", async () => {
    vi.useFakeTimers();
    const api = mount();
    await settle();
    h.stores.node.isRunning = true;
    await settle();
    h.streams[0].end();
    await settle();
    expect(api.errorMessage.value).toMatch(/Retrying \(1\/5\)/);

    await vi.advanceTimersByTimeAsync(1_000);
    await settle();
    expect(h.streams).toHaveLength(2);

    h.streams[1].end();
    await settle();
    expect(api.errorMessage.value).toMatch(/Retrying \(2\/5\)/);
    app!.unmount();
    app = null;
    await vi.advanceTimersByTimeAsync(10_000);
    expect(h.streams).toHaveLength(2);
  });

  it("a stream that ends after its lines while the run goes on is not retried", async () => {
    vi.useFakeTimers();
    const api = mount();
    await settle();
    h.stores.node.isRunning = true;
    await settle();
    h.streams[0].onData('"Flow completed!"');
    h.streams[0].end();
    await settle();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(h.streams).toHaveLength(1);
    expect(api.errorMessage.value).toBeNull();
    expect(api.logs.value).toBe("Flow completed!\n");
  });
});
