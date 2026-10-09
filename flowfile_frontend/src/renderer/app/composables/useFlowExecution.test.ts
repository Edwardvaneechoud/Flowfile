// Run saves the open settings drawer before POST /flow/run/; a refused save must not start it.

import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  calls: [] as string[],
  post: vi.fn(),
  get: vi.fn(),
  notify: vi.fn(),
  getFlowSettings: vi.fn(),
  nodeState: {
    nodeId: -1,
    vueFlowInstance: null,
    resetNodeResult: vi.fn(),
    showLogViewer: vi.fn(),
    insertRunResult: vi.fn(),
  },
}));

vi.mock("vue", async (importOriginal) => ({
  ...(await importOriginal<typeof import("vue")>()),
  onUnmounted: vi.fn(),
}));
vi.mock("axios", () => ({ default: { post: mocks.post, get: mocks.get } }));
vi.mock("element-plus", () => ({ ElNotification: mocks.notify, ElMessage: { error: vi.fn() } }));
vi.mock("@element-plus/icons-vue", () => ({ Promotion: {}, DataLine: {} }));
vi.mock("../api", () => ({ FlowApi: { getFlowSettings: mocks.getFlowSettings } }));
vi.mock("../stores/column-store", () => ({ useNodeStore: () => mocks.nodeState }));
vi.mock("../stores/node-store", () => ({ useNodeStore: () => mocks.nodeState }));
vi.mock("../stores/results-store", () => ({ useResultsStore: () => ({}) }));
vi.mock("../stores/tutorial-store", () => ({ useTutorialStore: () => ({ notify: vi.fn() }) }));

import { ref } from "vue";
import { useEditorStore } from "../stores/editor-store";
import { showCancelling, useFlowExecution } from "./useFlowExecution";
import { useNodeSettings } from "./useNodeSettings";

const openDrawerWithSave = (save: () => Promise<unknown>) => {
  const editor = useEditorStore();
  editor.isDrawerOpen = true;
  mocks.nodeState.nodeId = 3;
  editor.setCloseFunction(save);
  return editor;
};

describe("useFlowExecution.runFlow", () => {
  it("derives Cancelling from server state while running, but not after completion", () => {
    expect(showCancelling(true, true, false)).toBe(true);
    expect(showCancelling(true, false, true)).toBe(true);
    expect(showCancelling(false, true, false)).toBe(false);
    expect(showCancelling(true, false, false)).toBe(false);
  });

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.get.mockReset();
    vi.useFakeTimers();
    setActivePinia(createPinia());
    mocks.calls.length = 0;
    mocks.nodeState.nodeId = -1;
    mocks.getFlowSettings.mockImplementation(async () => {
      mocks.calls.push("settings");
      return { name: "flow", execution_mode: "Development", execution_location: "remote" };
    });
    mocks.post.mockImplementation(async (url: string) => {
      mocks.calls.push(url);
      return { status: 200 };
    });
    vi.stubGlobal("document", {
      createElement: () => ({ textContent: "", innerHTML: "" }),
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("awaits the open drawer's save before starting the run", async () => {
    const editor = openDrawerWithSave(async () => {
      await Promise.resolve();
      mocks.calls.push("save");
      return true;
    });

    await useFlowExecution(1).runFlow();

    expect(mocks.calls).toEqual(["save", "settings", "/flow/run/"]);
    expect(editor.isRunning).toBe(true);
    expect(editor.isDrawerOpen).toBe(true);
  });

  it("does not start the run when the drawer's save is refused", async () => {
    const editor = openDrawerWithSave(async () => false);

    await useFlowExecution(1).runFlow();

    expect(mocks.post).not.toHaveBeenCalled();
    expect(mocks.getFlowSettings).not.toHaveBeenCalled();
    expect(editor.isRunning).toBe(false);
    expect(mocks.notify).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Flow not started", type: "error" }),
    );
  });

  it("does not start the run when the drawer's save throws", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    openDrawerWithSave(async () => {
      throw new Error("422 Flow is running");
    });

    await useFlowExecution(1).runFlow();

    expect(mocks.post).not.toHaveBeenCalled();
    expect(mocks.notify).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Flow not started" }),
    );
  });

  it("starts the run when the open drawer's node never loaded (nothing to save)", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { pushNodeData } = useNodeSettings({ nodeRef: ref(null) });
    const editor = openDrawerWithSave(pushNodeData);

    await useFlowExecution(1).runFlow();

    expect(mocks.calls).toEqual(["settings", "/flow/run/"]);
    expect(editor.isRunning).toBe(true);
    expect(mocks.notify).not.toHaveBeenCalledWith(
      expect.objectContaining({ title: "Flow not started" }),
    );
  });

  it("starts one run when Run is clicked twice during the pre-run save", async () => {
    const save = vi.fn(async () => {
      await Promise.resolve();
      mocks.calls.push("save");
      return true;
    });
    openDrawerWithSave(save);
    const execution = useFlowExecution(1);

    await Promise.all([execution.runFlow(), execution.runFlow()]);

    expect(save).toHaveBeenCalledOnce();
    expect(mocks.calls).toEqual(["save", "settings", "/flow/run/"]);
  });

  it("saves the open drawer before running a single node, and refuses like Run", async () => {
    openDrawerWithSave(async () => false);

    await useFlowExecution(1).triggerNodeFetch(3);

    expect(mocks.post).not.toHaveBeenCalled();
    expect(mocks.notify).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Node not run", type: "error" }),
    );
  });

  it("skips the flush when no settings drawer is open", async () => {
    const save = vi.fn(async () => false);
    const editor = useEditorStore();
    editor.setCloseFunction(save);

    await useFlowExecution(1).runFlow();

    expect(save).not.toHaveBeenCalled();
    expect(mocks.calls).toEqual(["settings", "/flow/run/"]);
  });

  it("keeps running until cancellation returns the terminal node results", async () => {
    const running = { flow_id: 1, node_step_result: [{ node_id: 3, is_running: true }] };
    const cancelled = {
      flow_id: 1,
      success: false,
      node_step_result: [{ node_id: 3, is_running: false, success: null }],
    };
    mocks.get
      .mockResolvedValueOnce({ status: 202, data: running })
      .mockResolvedValueOnce({ status: 200, data: cancelled });
    const editor = useEditorStore();
    editor.isRunning = true;
    const execution = useFlowExecution(1);

    await execution.cancelFlow();

    expect(mocks.post).toHaveBeenCalledWith("/flow/cancel/", null, {
      params: { flow_id: 1 },
      headers: { accept: "application/json" },
    });
    expect(editor.isRunning).toBe(true);
    expect(editor.isCanceled).toBe(true);
    expect(execution.isPollingActive()).toBe(true);
    expect(execution.isCancelling.value).toBe(true);
    expect(mocks.nodeState.insertRunResult).toHaveBeenCalledWith(running);

    await vi.advanceTimersByTimeAsync(2000);

    expect(mocks.nodeState.insertRunResult).toHaveBeenLastCalledWith(cancelled);
    expect(editor.isRunning).toBe(false);
    expect(editor.isCanceled).toBe(false);
    expect(execution.isCancelling.value).toBe(false);
    expect(execution.isPollingActive()).toBe(false);
  });

  it("does not send another cancel request while one is pending", async () => {
    let finishRequest!: () => void;
    mocks.post.mockImplementationOnce(
      () => new Promise((resolve) => (finishRequest = () => resolve({ status: 200 }))),
    );
    mocks.get.mockResolvedValue({
      status: 200,
      data: { flow_id: 1, success: false, node_step_result: [] },
    });
    const execution = useFlowExecution(1);

    const first = execution.cancelFlow();
    const second = execution.cancelFlow();
    expect(mocks.post).toHaveBeenCalledTimes(1);
    expect(execution.isCancelling.value).toBe(true);

    finishRequest();
    await Promise.all([first, second]);
    expect(execution.isCancelling.value).toBe(false);
  });

  it("keeps the run active if the cancellation request fails", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    mocks.post.mockRejectedValueOnce(new Error("server unavailable"));
    const editor = useEditorStore();
    editor.isRunning = true;
    const execution = useFlowExecution(1);

    await execution.cancelFlow();

    expect(editor.isRunning).toBe(true);
    expect(execution.isCancelling.value).toBe(false);
    expect(mocks.get).not.toHaveBeenCalled();
    expect(mocks.notify).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Error", type: "error" }),
    );
  });

  it("keeps polling the cancelled flow after changing canvases without corrupting the new canvas", async () => {
    const currentId = ref(1);
    const editor = useEditorStore();
    editor.isRunning = true;
    const execution = useFlowExecution(() => currentId.value);
    const oldRunning = { flow_id: 1, node_step_result: [] };
    mocks.get
      .mockResolvedValueOnce({ status: 202, data: oldRunning })
      .mockResolvedValueOnce({ status: 200, data: { flow_id: 1, success: false } });

    await execution.cancelFlow();
    currentId.value = 2;
    editor.isCanceled = false;
    editor.isRunning = true; // Flow 2 is also running.
    mocks.nodeState.insertRunResult.mockClear();
    mocks.notify.mockClear();

    await vi.advanceTimersByTimeAsync(2000);

    expect(mocks.get).toHaveBeenNthCalledWith(2, "/flow/run_status/", {
      params: { flow_id: 1 },
      headers: { accept: "application/json" },
    });
    expect(mocks.nodeState.insertRunResult).not.toHaveBeenCalled();
    expect(mocks.notify).not.toHaveBeenCalled();
    expect(editor.isRunning).toBe(true);
    expect(editor.isCanceled).toBe(false);
    expect(execution.isPollingActive()).toBe(false);
  });

  it("pins a cancelled flow even when the user switches before the server acknowledges", async () => {
    const currentId = ref(1);
    let acknowledge!: () => void;
    mocks.post.mockImplementationOnce(
      () => new Promise((resolve) => (acknowledge = () => resolve({ status: 200 }))),
    );
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    const editor = useEditorStore();
    const execution = useFlowExecution(() => currentId.value);

    const cancelled = execution.cancelFlow();
    currentId.value = 2;
    editor.isRunning = true;
    acknowledge();
    await cancelled;

    expect(mocks.get).toHaveBeenCalledWith("/flow/run_status/", {
      params: { flow_id: 1 },
      headers: { accept: "application/json" },
    });
    expect(editor.isRunning).toBe(true);
    expect(editor.isCanceled).toBe(false);
    expect(mocks.nodeState.insertRunResult).not.toHaveBeenCalled();
    expect(execution.isCancelling.value).toBe(false);
  });

  it("keeps the existing node-fetch polling path working", async () => {
    const execution = useFlowExecution(
      1,
      { interval: 2000, enabled: true },
      { persistPolling: true },
    );
    const result = { flow_id: 1, success: true, node_step_result: [] };
    mocks.get.mockResolvedValue({ status: 200, data: result });

    await execution.triggerNodeFetch(3);
    expect(execution.isPollingActive("node_3")).toBe(true);
    await vi.advanceTimersByTimeAsync(2000);

    expect(mocks.nodeState.insertRunResult).toHaveBeenCalledWith(result);
    expect(execution.isPollingActive("node_3")).toBe(false);
    expect(useEditorStore().isRunning).toBe(false);
  });

  it("uses the rekeyed flow id for a new run after Save As", async () => {
    const currentId = ref(1);
    const execution = useFlowExecution(() => currentId.value);
    currentId.value = 2;

    await execution.runFlow();

    expect(mocks.getFlowSettings).toHaveBeenCalledWith(2);
    expect(mocks.post).toHaveBeenCalledWith("/flow/run/", null, {
      params: { flow_id: 2 },
      headers: { accept: "application/json" },
    });
  });

  it("ignores a late terminal response from canvas A while canvas B is polling", async () => {
    const currentId = ref(1);
    let finishOld!: (response: unknown) => void;
    mocks.get.mockImplementationOnce(() => new Promise((resolve) => (finishOld = resolve)));
    mocks.get.mockResolvedValueOnce({ status: 202, data: { flow_id: 2 } });
    const editor = useEditorStore();
    editor.isRunning = true;
    const execution = useFlowExecution(() => currentId.value);

    execution.startPolling(execution.checkRunStatus);
    const oldRequest = execution.checkRunStatus();
    currentId.value = 2;
    execution.startPolling(execution.checkRunStatus);
    await execution.checkRunStatus();
    finishOld({ status: 200, data: { flow_id: 1, success: false } });
    await oldRequest;

    expect(mocks.nodeState.insertRunResult).toHaveBeenCalledTimes(1);
    expect(mocks.nodeState.insertRunResult).toHaveBeenCalledWith({ flow_id: 2 });
    expect(editor.isRunning).toBe(true);
    expect(execution.isPollingActive()).toBe(true);
    expect(mocks.notify).not.toHaveBeenCalledWith(expect.objectContaining({ title: "Error" }));
  });

  it("does not start the old flow if Save As rekeys during the settings request", async () => {
    const currentId = ref(1);
    let finishSettings!: (settings: unknown) => void;
    mocks.getFlowSettings.mockImplementationOnce(
      () => new Promise((resolve) => (finishSettings = resolve)),
    );
    const execution = useFlowExecution(() => currentId.value);

    const start = execution.runFlow();
    await Promise.resolve();
    currentId.value = 2;
    finishSettings({ name: "old flow", execution_mode: "Development" });
    await start;

    expect(mocks.getFlowSettings).toHaveBeenCalledWith(1);
    expect(mocks.post).not.toHaveBeenCalled();
    expect(useEditorStore().isRunning).toBe(false);
  });
});
