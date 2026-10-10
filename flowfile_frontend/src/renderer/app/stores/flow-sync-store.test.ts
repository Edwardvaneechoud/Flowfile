// @vitest-environment happy-dom
// The sibling stores are mocked: flow-store reads sessionStorage and reaches axios.config at import time.
import { createPinia, setActivePinia } from "pinia";
import { nextTick, reactive } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  flowStore: null as any,
  editorStore: { isRunning: false, bumpGraphVersion: vi.fn() },
  getToken: vi.fn(),
  stream: vi.fn(),
}));

vi.mock("../../config/constants", () => ({ flowfileCorebaseURL: "http://127.0.0.1:63578/" }));
vi.mock("./flow-store", () => ({ useFlowStore: () => mocks.flowStore }));
vi.mock("./editor-store", () => ({ useEditorStore: () => mocks.editorStore }));
vi.mock("../services/auth.service", () => ({ default: { getToken: mocks.getToken } }));
vi.mock("../services/flowEventsClient", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../services/flowEventsClient")>()),
  streamFlowEvents: mocks.stream,
}));

import { clientId } from "../services/clientId";
import {
  FlowEventsHttpError,
  type FlowEvent,
  type FlowEventsOptions,
} from "../services/flowEventsClient";
import { RELOAD_DEBOUNCE_MS, RETRY_BASE_MS, useFlowSyncStore } from "./flow-sync-store";

interface OpenStream {
  opts: FlowEventsOptions;
  finish: () => void;
  fail: (error: unknown) => void;
}

let streams: OpenStream[] = [];

const streamImpl = (opts: FlowEventsOptions) =>
  new Promise<void>((resolve, reject) => {
    streams.push({ opts, finish: resolve, fail: reject });
    opts.signal.addEventListener("abort", () =>
      reject(Object.assign(new Error("aborted"), { name: "AbortError" })),
    );
    opts.onOpen?.();
  });

const current = () => streams[streams.length - 1];

const settle = async () => {
  await nextTick();
  for (let i = 0; i < 10; i++) await Promise.resolve();
};

const event = (partial: Partial<FlowEvent>): FlowEvent => ({
  kind: "graph",
  flow_id: 7,
  revision: 2,
  origin: "another-window",
  ...partial,
});

const open = async (flowId = 7) => {
  const store = useFlowSyncStore();
  mocks.flowStore.flowId = flowId;
  await settle();
  return store;
};

beforeEach(() => {
  vi.useFakeTimers();
  setActivePinia(createPinia());
  mocks.flowStore = reactive({
    flowId: -1,
    historyState: { flow_id: null, revision: 0 },
    requestReload: vi.fn(),
    requestRunStateRefresh: vi.fn(),
  });
  mocks.editorStore.isRunning = false;
  mocks.editorStore.bumpGraphVersion.mockReset();
  mocks.getToken.mockReset().mockResolvedValue("jwt");
  mocks.stream.mockReset().mockImplementation(streamImpl);
  streams = [];
});

afterEach(() => vi.useRealTimers());

describe("flow-sync-store", () => {
  it("opens the feed for the active flow with this window's id", async () => {
    const store = await open();
    expect(mocks.stream).toHaveBeenCalledTimes(1);
    expect(current().opts).toMatchObject({ flowId: 7, token: "jwt", clientId });
    expect(store.connected).toBe(true);
  });

  it("turns a burst of foreign graph events into one reload", async () => {
    const store = await open();
    current().opts.onEvent(event({ revision: 2 }));
    current().opts.onEvent(event({ revision: 3 }));
    expect(mocks.flowStore.requestReload).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(RELOAD_DEBOUNCE_MS);
    expect(mocks.flowStore.requestReload).toHaveBeenCalledTimes(1);
    expect(store.lastRevision[7]).toBe(3);
  });

  it("skips the events this window caused but records their revision", async () => {
    const store = await open();
    current().opts.onEvent(event({ revision: 5, origin: clientId }));
    current().opts.onEvent(event({ kind: "run_started", revision: 6, origin: clientId }));
    await vi.advanceTimersByTimeAsync(RELOAD_DEBOUNCE_MS);
    expect(mocks.flowStore.requestReload).not.toHaveBeenCalled();
    expect(mocks.flowStore.requestRunStateRefresh).not.toHaveBeenCalled();
    expect(store.lastRevision[7]).toBe(6);
  });

  it("refreshes the run state when another client starts or ends a run", async () => {
    await open();
    current().opts.onEvent(event({ kind: "run_started", revision: 2 }));
    current().opts.onEvent(event({ kind: "run_ended", revision: 3 }));
    expect(mocks.flowStore.requestRunStateRefresh).toHaveBeenCalledTimes(2);
    expect(mocks.flowStore.requestReload).not.toHaveBeenCalled();
  });

  it("bumps the graph version when another client saves", async () => {
    await open();
    current().opts.onEvent(event({ kind: "saved", revision: 2 }));
    expect(mocks.editorStore.bumpGraphVersion).toHaveBeenCalledTimes(1);
  });

  it("ignores events of another flow", async () => {
    await open();
    current().opts.onEvent(event({ flow_id: 8, revision: 9 }));
    await vi.advanceTimersByTimeAsync(RELOAD_DEBOUNCE_MS);
    expect(mocks.flowStore.requestReload).not.toHaveBeenCalled();
  });

  it("reloads after a reconnect whose hello shows the flow moved meanwhile", async () => {
    await open();
    current().opts.onEvent(
      event({ kind: "hello", revision: 4, origin: undefined, is_running: false }),
    );
    await vi.advanceTimersByTimeAsync(RELOAD_DEBOUNCE_MS);
    expect(mocks.flowStore.requestReload).not.toHaveBeenCalled();

    current().fail(new Error("connection dropped"));
    await vi.advanceTimersByTimeAsync(RETRY_BASE_MS);
    expect(mocks.stream).toHaveBeenCalledTimes(2);
    current().opts.onEvent(
      event({ kind: "hello", revision: 6, origin: undefined, is_running: true }),
    );
    await vi.advanceTimersByTimeAsync(RELOAD_DEBOUNCE_MS);
    expect(mocks.flowStore.requestReload).toHaveBeenCalledTimes(1);
    expect(mocks.flowStore.requestRunStateRefresh).toHaveBeenCalledTimes(1);
  });

  it("records the revision of every history state this window applies", async () => {
    const store = await open();
    mocks.flowStore.historyState = { flow_id: 7, revision: 9 };
    await nextTick();
    expect(store.lastRevision[7]).toBe(9);
  });

  it("reconnects with a growing backoff after a dropped stream", async () => {
    const store = await open();
    current().fail(new Error("connection dropped"));
    await settle();
    expect(store.connected).toBe(false);
    await vi.advanceTimersByTimeAsync(RETRY_BASE_MS - 1);
    expect(mocks.stream).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(mocks.stream).toHaveBeenCalledTimes(2);
    expect(store.connected).toBe(true);

    current().fail(new Error("connection dropped again"));
    await vi.advanceTimersByTimeAsync(RETRY_BASE_MS);
    expect(mocks.stream).toHaveBeenCalledTimes(3);
  });

  it("stops for good when core answers 404", async () => {
    const store = await open();
    current().fail(new FlowEventsHttpError(404));
    await vi.advanceTimersByTimeAsync(60_000);
    expect(mocks.stream).toHaveBeenCalledTimes(1);
    expect(store.connected).toBe(false);
  });

  it("switches streams when the active flow changes and stops when none is open", async () => {
    await open();
    const first = current();
    mocks.flowStore.flowId = 8;
    await settle();
    expect(first.opts.signal.aborted).toBe(true);
    expect(mocks.stream).toHaveBeenCalledTimes(2);
    expect(current().opts.flowId).toBe(8);

    mocks.flowStore.flowId = -1;
    await settle();
    expect(current().opts.signal.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(mocks.stream).toHaveBeenCalledTimes(2);
  });

  it("a closed event ends the stream without reconnecting and says which flow closed", async () => {
    const store = await open();
    expect(store.closedFlowId).toBeNull();
    current().opts.onEvent(event({ kind: "closed", revision: null, origin: null }));
    expect(current().opts.signal.aborted).toBe(true);
    expect(store.closedFlowId).toBe(7);
    expect(store.closeCount).toBe(1);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(mocks.stream).toHaveBeenCalledTimes(1);
    expect(store.connected).toBe(false);
  });

  it("a final event on a stream the window already left keeps the next flow's stream", async () => {
    await open();
    const first = current();
    mocks.flowStore.flowId = 9;
    await settle();
    first.opts.onEvent(event({ kind: "rekeyed", revision: null, origin: null, new_flow_id: 9 }));
    expect(current().opts.flowId).toBe(9);
    expect(current().opts.signal.aborted).toBe(false);
  });

  it("a rekeyed event ends the stream and records where the flow went", async () => {
    const store = await open();
    current().opts.onEvent(
      event({ kind: "rekeyed", revision: null, origin: null, new_flow_id: 9 }),
    );
    expect(current().opts.signal.aborted).toBe(true);
    expect(store.rekeyedTo).toEqual({ from: 7, to: 9 });
    expect(store.closedFlowId).toBeNull();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(mocks.stream).toHaveBeenCalledTimes(1);
    expect(mocks.flowStore.requestReload).not.toHaveBeenCalled();
  });
});
