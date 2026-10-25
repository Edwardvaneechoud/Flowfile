/// <reference types="node" />
// Run saves the open settings drawer before POST /flow/run/; a refused save must not start it.

import { readFileSync } from "node:fs";
import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  calls: [] as string[],
  post: vi.fn(),
  get: vi.fn(),
  notify: vi.fn(),
  tutorial: vi.fn(),
  getFlowSettings: vi.fn(),
  nodeState: {
    flow_id: 1,
    nodeId: -1,
    vueFlowInstance: null as unknown as import("@vue-flow/core").VueFlowStore,
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
vi.mock("../services/auth.service", () => ({ default: { getToken: vi.fn() } }));
vi.mock("../services/flowEventsClient", () => ({
  FlowEventsHttpError: class extends Error {},
  streamFlowEvents: vi.fn(),
}));
vi.mock("../stores/column-store", () => ({ useNodeStore: () => mocks.nodeState }));
vi.mock("../stores/node-store", () => ({ useNodeStore: () => mocks.nodeState }));
vi.mock("../stores/results-store", () => ({ useResultsStore: () => ({}) }));
vi.mock("../stores/tutorial-store", () => ({
  useTutorialStore: () => ({ notify: mocks.tutorial }),
}));

import { compile, onUnmounted, ref, type VNode } from "vue";
import { useEditorStore } from "../stores/editor-store";
import { useFlowSyncStore } from "../stores/flow-sync-store";
import { useFlowExecution, resetFlowExecutionState } from "./useFlowExecution";
import { useNodeSettings } from "./useNodeSettings";

const openDrawerWithSave = (save: () => Promise<unknown>) => {
  const editor = useEditorStore();
  editor.isDrawerOpen = true;
  mocks.nodeState.nodeId = 3;
  editor.setCloseFunction(save);
  return editor;
};

// Exercise the actual header orchestration without mounting its unrelated dialogs.
const createHeaderSettingsLoader = (
  button: ReturnType<typeof useFlowExecution>,
  flowState = { setParameters: vi.fn(), pendingRunStateCounter: 0 },
): (() => Promise<void>) => {
  const source = readFileSync(
    new URL("../components/layout/Header/HeaderButtons.vue", import.meta.url),
    "utf8",
  );
  const body = source
    .slice(source.indexOf("const loadFlowSettings ="), source.indexOf("const pushFlowSettings ="))
    .replace(": Promise<void>", "");
  return new Function(
    "nodeStore",
    "editorStore",
    "flowStore",
    "flowSettings",
    "runButton",
    "getFlowSettings",
    "getRunStatus",
    `let settingsLoad = null; ${body}; return loadFlowSettings;`,
  )(
    mocks.nodeState,
    useEditorStore(),
    flowState,
    ref(null),
    ref(button),
    mocks.getFlowSettings,
    mocks.get,
  );
};

const loadHeaderSettings = (...args: Parameters<typeof createHeaderSettingsLoader>) =>
  createHeaderSettingsLoader(...args)();

const rekey = (from: number, to: number) =>
  useFlowSyncStore().handleEvent(from, {
    kind: "rekeyed",
    flow_id: from,
    revision: null,
    new_flow_id: to,
  });

describe("useFlowExecution.runFlow", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.get.mockReset();
    vi.useFakeTimers();
    setActivePinia(createPinia());
    mocks.calls.length = 0;
    mocks.nodeState.flow_id = 1;
    mocks.nodeState.nodeId = -1;
    mocks.nodeState.vueFlowInstance = {
      nodesDraggable: ref(true),
      nodesConnectable: ref(true),
      elementsSelectable: ref(true),
    } as import("@vue-flow/core").VueFlowStore;
    mocks.getFlowSettings.mockImplementation(async () => {
      mocks.calls.push("settings");
      return { name: "flow", execution_mode: "Development", execution_location: "remote" };
    });
    mocks.post.mockImplementation(async (url: string) => {
      mocks.calls.push(url);
      return { status: 200 };
    });
    vi.stubGlobal("sessionStorage", { getItem: () => null, setItem: vi.fn() });
    vi.stubGlobal("document", {
      createElement: () => ({ textContent: "", innerHTML: "" }),
    });
  });

  afterEach(() => {
    resetFlowExecutionState();
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it.each([true, false])(
    "recovers paused observation when Cancel rejects an already finished run (persistent=%s)",
    async (persistPolling) => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      const onComplete = vi.fn();
      const execution = useFlowExecution(
        1,
        { interval: 2000, enabled: true, maxAttempts: 1 },
        { persistPolling },
      );
      await execution.triggerNodeFetch(3, { focusResultPanels: false, onComplete });
      mocks.get.mockRejectedValueOnce(new Error("status unavailable"));
      await vi.advanceTimersByTimeAsync(2000);
      expect(vi.getTimerCount()).toBe(0);
      expect(onComplete).not.toHaveBeenCalled();

      mocks.post.mockRejectedValueOnce({ response: { status: 422 } });
      mocks.get.mockResolvedValue({
        status: 200,
        data: { flow_id: 1, success: true, node_step_result: [] },
      });
      mocks.notify.mockClear();
      await execution.cancelFlow();

      expect(onComplete).toHaveBeenCalledOnce();
      expect(useEditorStore().isRunning).toBe(false);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(true);
      expect(mocks.notify.mock.calls.map(([notice]) => notice.title)).toEqual(["Success"]);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each(["flow", "node"])(
    "adopts server-confirmed cancellation into an existing %s owner",
    async (kind) => {
      const onComplete = vi.fn();
      const execution = useFlowExecution(1, undefined, { persistPolling: true });
      if (kind === "flow") await execution.runFlow();
      else await execution.triggerNodeFetch(3, { onComplete });
      mocks.getFlowSettings.mockResolvedValue({
        name: "flow",
        is_running: true,
        is_canceled: true,
      });
      await loadHeaderSettings(execution);
      mocks.tutorial.mockClear();
      mocks.get.mockResolvedValue({
        status: 200,
        data: { flow_id: 1, success: false, node_step_result: [] },
      });
      await vi.advanceTimersByTimeAsync(2000);

      expect(mocks.notify).toHaveBeenLastCalledWith(
        expect.objectContaining({ title: "Cancelled" }),
      );
      expect(useEditorStore().showFlowResult).toBe(false);
      expect(useEditorStore().isShowingLogViewer).toBe(false);
      expect(mocks.tutorial).not.toHaveBeenCalled();
      if (kind === "node") expect(onComplete).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each([true, false])(
    "adopts uncancelled replacement metadata while the previous owner is paused (success=%s)",
    async (success) => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      const owner = useFlowExecution(
        1,
        { interval: 2000, enabled: true, maxAttempts: 1 },
        { persistPolling: true },
      );
      await owner.runFlow();
      useEditorStore().isCanceled = true;
      owner.startStatusPolling(1, false);
      mocks.get.mockRejectedValueOnce(new Error("status unavailable"));
      await vi.advanceTimersByTimeAsync(2000);
      expect(vi.getTimerCount()).toBe(0);

      mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
      await loadHeaderSettings(owner);
      mocks.notify.mockClear();
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success } });
      await vi.advanceTimersByTimeAsync(2000);

      expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ title: success ? "Success" : "Error" }),
      );
      expect(useEditorStore().isRunning).toBe(false);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each(["start", "cancel"])(
    "ignores a settings reply overtaken by a local %s",
    async (change) => {
      const owner = useFlowExecution(1, undefined, { persistPolling: true });
      if (change === "cancel") await owner.runFlow();
      let reply!: (settings: unknown) => void;
      mocks.getFlowSettings.mockImplementationOnce(
        () => new Promise((resolve) => (reply = resolve)),
      );
      const load = loadHeaderSettings(owner);
      mocks.get.mockResolvedValue({ status: 202, data: { flow_id: 1 } });
      if (change === "start") await owner.runFlow();
      else await owner.cancelFlow();
      mocks.getFlowSettings.mockResolvedValue({
        is_running: true,
        is_canceled: change === "cancel",
      });
      reply({ is_running: change === "cancel", is_canceled: false });
      await load;

      expect(useEditorStore().isRunning).toBe(true);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
      expect(useEditorStore().isCanceled).toBe(change === "cancel");
      if (change === "cancel") {
        mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
        await vi.advanceTimersByTimeAsync(2000);
        expect(mocks.notify).toHaveBeenLastCalledWith(
          expect.objectContaining({ title: "Cancelled" }),
        );
      }
    },
  );

  it("re-reads settings when a run-state event overtakes the in-flight load", async () => {
    const owner = useFlowExecution(1, undefined, { persistPolling: true });
    await owner.runFlow();
    useEditorStore().isCanceled = true;
    owner.startStatusPolling(1, false);
    const flowState = { setParameters: vi.fn(), pendingRunStateCounter: 0 };
    let reply!: (settings: unknown) => void;
    mocks.getFlowSettings
      .mockClear()
      .mockImplementationOnce(() => new Promise((resolve) => (reply = resolve)));
    const load = loadHeaderSettings(owner, flowState);
    flowState.pendingRunStateCounter += 1;
    mocks.getFlowSettings.mockResolvedValueOnce({ is_running: true, is_canceled: false });
    reply({ is_running: true, is_canceled: true });
    await load;

    expect(mocks.getFlowSettings).toHaveBeenCalledTimes(2);
    expect(useEditorStore().isCanceled).toBe(false);
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(mocks.notify).toHaveBeenLastCalledWith(expect.objectContaining({ title: "Success" }));
  });

  it("ignores settings from a retired owner even when the new run has the same UI flags", async () => {
    const owner = useFlowExecution(1, undefined, { persistPolling: true });
    await owner.runFlow();
    let reply!: (settings: unknown) => void;
    mocks.getFlowSettings.mockImplementationOnce(() => new Promise((resolve) => (reply = resolve)));
    const load = loadHeaderSettings(owner);
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await vi.advanceTimersByTimeAsync(2000);
    await owner.runFlow();
    mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
    reply({ is_running: true, is_canceled: true });
    await load;

    expect(useEditorStore().isRunning).toBe(true);
    expect(useEditorStore().isCanceled).toBe(false);
    expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
  });

  it("refreshes an overtaken settings load before adopting a lost start acknowledgement", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const owner = useFlowExecution(1, undefined, { persistPolling: true });
    const complete = vi.fn();
    let reply!: (settings: unknown) => void;
    let rejectStart!: (error: Error) => void;
    mocks.getFlowSettings.mockImplementationOnce(() => new Promise((resolve) => (reply = resolve)));
    const load = loadHeaderSettings(owner);
    mocks.post.mockImplementationOnce(() => new Promise((_, reject) => (rejectStart = reject)));
    const fetch = owner.triggerNodeFetch(3, { onComplete: complete }).then(
      () => null,
      (error) => error,
    );
    await Promise.resolve();
    mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
    reply({ is_running: true, is_canceled: false });
    await load;
    rejectStart(new Error("accepted start acknowledgement lost"));

    expect(await fetch).toBeNull();
    expect(useEditorStore().isRunning).toBe(true);
    expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(complete).toHaveBeenCalledOnce();
  });

  it("keeps the newest settings after switching A to B and back to A", async () => {
    const button = useFlowExecution(() => mocks.nodeState.flow_id, undefined, {
      persistPolling: true,
    });
    const load = createHeaderSettingsLoader(button);
    const reads: Array<{ id: number; resolve: (settings: unknown) => void }> = [];
    mocks.getFlowSettings.mockImplementation(
      (id) => new Promise((resolve) => reads.push({ id, resolve })),
    );
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1 } });
    const firstA = load();
    mocks.nodeState.flow_id = 2;
    const b = load();
    mocks.nodeState.flow_id = 1;
    const newestA = load();
    const duplicateA = load();
    expect(reads.map(({ id }) => id)).toEqual([1, 2, 1]);

    reads[2].resolve({ is_running: false, is_canceled: false });
    await Promise.all([newestA, duplicateA]);
    reads[0].resolve({ is_running: true, is_canceled: true });
    await firstA;
    reads[1].resolve({ is_running: false, is_canceled: false });
    await b;

    expect(useEditorStore().isRunning).toBe(false);
    expect(useEditorStore().isCanceled).toBe(false);
    expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    expect(mocks.getFlowSettings).toHaveBeenCalledTimes(3);
  });

  it("reports an already running flow for keyboard Run without starting again", async () => {
    useEditorStore().isRunning = true;
    await useFlowExecution(1).runFlow();
    expect(mocks.post).not.toHaveBeenCalled();
    expect(mocks.notify).toHaveBeenCalledWith(
      expect.objectContaining({ message: "Flow is already running" }),
    );
  });

  it.each(["run", "fetch"])(
    "unfreezes the selected idle canvas while a background %s POST fails",
    async (kind) => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      let reject!: (error: Error) => void;
      mocks.post.mockImplementationOnce(() => new Promise((_, fail) => (reject = fail)));
      const execution = useFlowExecution(() => mocks.nodeState.flow_id, undefined, {
        persistPolling: true,
      });
      const pending = kind === "run" ? execution.runFlow() : execution.triggerNodeFetch(3);
      await vi.advanceTimersByTimeAsync(0);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
      mocks.nodeState.flow_id = 2;
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 2, success: true } });
      await loadHeaderSettings(execution);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(true);
      reject(new Error("offline"));
      await pending;
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(true);
      mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
      await loadHeaderSettings(execution);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
    },
  );

  it("lets the shared poller owner finish when settings refresh just before its terminal poll", async () => {
    const visible = useFlowExecution(1, undefined, { persistPolling: true });
    const hidden = useFlowExecution(1, undefined, { persistPolling: true });
    await visible.runFlow();
    mocks.notify.mockClear();
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
    await loadHeaderSettings(hidden);
    await vi.advanceTimersByTimeAsync(4000);
    expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ title: "Success" }),
    );
    expect(useEditorStore().showFlowResult).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
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

  it("does not update another selected flow after its run button unmounts", async () => {
    const props = { flowId: 1 };
    const editor = useEditorStore();
    const execution = useFlowExecution(() => props.flowId, undefined, { persistPolling: true });
    mocks.get.mockResolvedValue({
      status: 200,
      data: { flow_id: 1, success: true, node_step_result: [] },
    });

    await execution.runFlow();
    vi.mocked(onUnmounted).mock.calls.forEach(([cleanup]) => cleanup());
    mocks.nodeState.flow_id = 2;
    editor.isRunning = true;
    mocks.notify.mockClear();
    await vi.advanceTimersByTimeAsync(2000);

    expect(editor.isRunning).toBe(true);
    expect(mocks.nodeState.insertRunResult).not.toHaveBeenCalled();
    expect(editor.showFlowResult).toBe(false);
    expect(editor.isShowingLogViewer).toBe(false);
    expect(mocks.notify).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
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
    expect(execution.isPollingActive()).toBe(true);
    expect(editor.isCanceled).toBe(true);
    expect(execution.isCancelling.value).toBe(false);
    expect(mocks.nodeState.insertRunResult).toHaveBeenCalledWith(running);

    await vi.advanceTimersByTimeAsync(2000);

    expect(mocks.nodeState.insertRunResult).toHaveBeenLastCalledWith(cancelled);
    expect(editor.isRunning).toBe(false);
    expect(execution.isCancelling.value).toBe(false);
    expect(execution.isPollingActive()).toBe(false);
    expect(mocks.notify).not.toHaveBeenCalledWith(expect.objectContaining({ title: "Error" }));
    expect(mocks.tutorial).not.toHaveBeenCalledWith(
      expect.objectContaining({ type: "flow-run-completed" }),
    );
  });

  it.each([200, 202])("keeps panel focus when cancellation first returns %s", async (status) => {
    const editor = useEditorStore();
    const onComplete = vi.fn();
    const panel = useFlowExecution(1, undefined, {
      persistPolling: true,
      pollingKey: "table_flow_1",
    });
    const header = useFlowExecution(1, undefined, { persistPolling: true });
    const terminal = { flow_id: 1, success: false, node_step_result: [] };
    mocks.get
      .mockResolvedValueOnce({ status, data: terminal })
      .mockResolvedValue({ status: 200, data: terminal });

    try {
      await panel.triggerNodeFetch(3, { focusResultPanels: false, onComplete });
      await header.cancelFlow();

      expect(editor.isRunning).toBe(status === 202);
      expect(editor.showFlowResult).toBe(false);
      expect(editor.isShowingLogViewer).toBe(false);
      expect(onComplete).toHaveBeenCalledTimes(status === 200 ? 1 : 0);
      await vi.advanceTimersByTimeAsync(2000);

      expect(editor.isRunning).toBe(false);
      expect(editor.showFlowResult).toBe(false);
      expect(editor.isShowingLogViewer).toBe(false);
      expect(onComplete).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
      const reports = mocks.notify.mock.calls.filter(([notice]) =>
        ["Error", "Success", "Cancelled"].includes(notice.title),
      );
      expect(reports).toHaveLength(1);
      expect(reports[0][0].title).toBe("Cancelled");
      expect(mocks.tutorial).not.toHaveBeenCalledWith(
        expect.objectContaining({ type: "flow-run-completed" }),
      );
    } finally {
      panel.stopPolling("node_3");
      header.stopPolling();
    }
  });

  it.each(["acknowledged", "failed"])(
    "keeps B's pending cancel owned when A's POST is %s",
    async (outcome) => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      const currentId = ref(1);
      const execution = useFlowExecution(currentId, undefined, { persistPolling: true });
      let finishA!: () => void;
      let finishB!: () => void;
      mocks.post
        .mockImplementationOnce(
          () =>
            new Promise((resolve, reject) => {
              finishA = () =>
                outcome === "failed" ? reject(new Error("offline")) : resolve({ status: 200 });
            }),
        )
        .mockImplementationOnce(
          () => new Promise((resolve) => (finishB = () => resolve({ status: 200 }))),
        );
      mocks.get.mockResolvedValue({ status: 202, data: { flow_id: 1 } });
      const first = execution.cancelFlow();
      currentId.value = mocks.nodeState.flow_id = 2;
      const second = execution.cancelFlow();
      finishA();
      await first;
      expect(execution.isCancelling.value).toBe(true);
      await execution.cancelFlow();
      expect(mocks.post).toHaveBeenCalledTimes(2);
      finishB();
      await second;
      expect(execution.isCancelling.value).toBe(false);
    },
  );

  it("keeps Cancel visible during cancellation and disables it only for the POST", () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const source = readFileSync(
      new URL("../components/layout/Header/run.vue", import.meta.url),
      "utf8",
    );
    const template = source.split("<template>")[1].split("</template>")[0];
    const render = compile(template) as unknown as (context: object, cache: unknown[]) => VNode;
    for (const pending of [false, true]) {
      const tree = render(
        {
          nodeStore: { isRunning: true },
          isCancelling: true,
          cancelPending: pending,
          runFlow: vi.fn(),
          cancelFlow: vi.fn(),
        },
        [],
      );
      const cancel = (tree.children as VNode[])[1];
      expect(cancel.type).toBe("el-button");
      expect(cancel.props?.disabled).toBe(pending);
    }
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

  it("keeps polling the cancelled flow when switching before acknowledgement", async () => {
    const currentId = ref(1);
    const editor = useEditorStore();
    editor.isRunning = true;
    const execution = useFlowExecution(() => currentId.value, undefined, { persistPolling: true });
    let acknowledge!: () => void;
    mocks.post.mockImplementationOnce(
      () => new Promise((resolve) => (acknowledge = () => resolve({ status: 200 }))),
    );
    const oldRunning = { flow_id: 1, node_step_result: [] };
    mocks.get
      .mockResolvedValueOnce({ status: 202, data: oldRunning })
      .mockResolvedValueOnce({ status: 200, data: { flow_id: 1, success: false } });

    const cancellation = execution.cancelFlow();
    currentId.value = 2;
    mocks.nodeState.flow_id = 2;
    editor.isCanceled = false;
    editor.isRunning = true; // Flow 2 is also running.
    acknowledge();
    await cancellation;
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
    const requests = mocks.get.mock.calls.length;
    await vi.advanceTimersByTimeAsync(4000);
    expect(mocks.get).toHaveBeenCalledTimes(requests); // In particular, no further request for flow 1.
    expect(vi.getTimerCount()).toBe(0);
  });

  it("completes a background node fetch without changing the selected flow UI", async () => {
    const currentId = ref(11);
    mocks.nodeState.flow_id = 11;
    const editor = useEditorStore();
    const onComplete = vi.fn();
    const execution = useFlowExecution(currentId, undefined, { persistPolling: true });
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 11, success: true } });

    await execution.triggerNodeFetch(3, { focusResultPanels: false, onComplete });
    currentId.value = 12;
    mocks.nodeState.flow_id = 12;
    editor.isRunning = true;
    mocks.notify.mockClear();
    expect(onComplete).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(4000);

    expect(onComplete).toHaveBeenCalledOnce();
    expect(mocks.nodeState.insertRunResult).not.toHaveBeenCalled();
    expect(mocks.notify).not.toHaveBeenCalled();
    expect(editor.isRunning).toBe(true);
    expect(editor.showFlowResult).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["flow", "node"])(
    "pauses %s observation after five consecutive failures and resumes its owner on Cancel",
    async (kind) => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      const execution = useFlowExecution(1, undefined, { persistPolling: true });
      const complete = vi.fn();
      if (kind === "flow") await execution.runFlow();
      else await execution.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
      mocks.notify.mockClear();
      mocks.get.mockRejectedValue(new Error("offline"));
      await vi.advanceTimersByTimeAsync(8000);
      mocks.get.mockResolvedValueOnce({ status: 202, data: { flow_id: 1 } });
      await vi.advanceTimersByTimeAsync(2000);
      await vi.advanceTimersByTimeAsync(8000);
      expect(vi.getTimerCount()).toBe(1); // The healthy 202 reset the failure count.
      await vi.advanceTimersByTimeAsync(2000);
      expect(vi.getTimerCount()).toBe(0);
      expect(useEditorStore().isRunning).toBe(true);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
      expect(complete).not.toHaveBeenCalled();
      expect(execution.isPollingActive(kind === "node" ? "node_3" : "")).toBe(true);
      expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          title: "Status unavailable",
          type: "error",
        }),
      );
      const failedCalls = mocks.get.mock.calls.length;
      await vi.advanceTimersByTimeAsync(6000);
      expect(mocks.get).toHaveBeenCalledTimes(failedCalls);
      mocks.get.mockResolvedValue({ status: 202, data: { flow_id: 1 } });
      await execution.cancelFlow();
      expect(vi.getTimerCount()).toBe(1);
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
      await vi.advanceTimersByTimeAsync(2000);
      expect(vi.getTimerCount()).toBe(0);
      expect(useEditorStore().isRunning).toBe(false);
      expect(complete).toHaveBeenCalledTimes(kind === "node" ? 1 : 0);
    },
  );

  it("resumes a paused local node owner when Cancel comes from another component", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const panel = useFlowExecution(1);
    const header = useFlowExecution(1, undefined, { persistPolling: true });
    const complete = vi.fn();
    await panel.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
    mocks.get.mockRejectedValue(new Error("offline"));
    await vi.advanceTimersByTimeAsync(10000);
    expect(panel.isPollingActive("node_3")).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    mocks.get.mockResolvedValue({ status: 202, data: { flow_id: 1 } });
    await header.cancelFlow();
    expect(vi.getTimerCount()).toBe(1);
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(complete).toHaveBeenCalledOnce();
    expect(panel.isPollingActive("node_3")).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
    expect(useEditorStore().showFlowResult).toBe(false);
  });

  it("keeps the real preview completion loop waiting while status is paused", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const execution = useFlowExecution(1, undefined, {
      persistPolling: true,
      pollingKey: "table_flow_1",
    });
    const source = readFileSync(
      new URL("../features/designer/dataPreview.vue", import.meta.url),
      "utf8",
    );
    const body = source.slice(
      source.indexOf("async function handleFetchData()"),
      source.indexOf("function removeData()"),
    );
    const fetching = ref(false);
    const download = vi.fn();
    const fetch = new Function(
      "currentNodeId",
      "isFetching",
      "isPollingActive",
      "triggerNodeFetch",
      "downloadData",
      `${body.replace("currentNodeId.value!", "currentNodeId.value")}; return handleFetchData;`,
    )(ref(3), fetching, execution.isPollingActive, execution.triggerNodeFetch, download);
    await fetch();
    mocks.get.mockRejectedValue(new Error("offline"));
    await vi.advanceTimersByTimeAsync(10250);
    expect(execution.isPollingActive("node_3")).toBe(true);
    expect(vi.getTimerCount()).toBe(2); // Only the preview check and safety timeout; no status timer.
    expect(fetching.value).toBe(true);
    expect(download).not.toHaveBeenCalled();
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await execution.cancelFlow();
    await vi.advanceTimersByTimeAsync(250);
    expect(execution.isPollingActive("node_3")).toBe(false);
    expect(download).toHaveBeenCalledExactlyOnceWith(3);
    expect(fetching.value).toBe(false);
    vi.clearAllTimers();
  });

  it("restores cancellation metadata without panel focus or a tutorial", async () => {
    const execution = useFlowExecution(1, undefined, { persistPolling: true });
    const start = vi.spyOn(execution, "startStatusPolling");
    mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: true });
    await loadHeaderSettings(execution);
    expect(start).toHaveBeenCalledWith(1, false);
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(useEditorStore().showFlowResult).toBe(false);
    expect(mocks.tutorial).not.toHaveBeenCalled();
    expect(mocks.notify).not.toHaveBeenCalledWith(expect.objectContaining({ title: "Error" }));
  });

  it("restores a running node fetch through its existing owner, not a flow tutorial poller", async () => {
    const execution = useFlowExecution(1, undefined, { persistPolling: true });
    const complete = vi.fn();
    await execution.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
    mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
    await loadHeaderSettings(execution);
    expect(vi.getTimerCount()).toBe(1);
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(complete).toHaveBeenCalledOnce();
    expect(mocks.tutorial).not.toHaveBeenCalled();
    expect(useEditorStore().showFlowResult).toBe(false);
  });

  it.each(["flow", "node"])(
    "waits for the %s POST owner when settings refresh before acknowledgement",
    async (kind) => {
      const execution = useFlowExecution(1, undefined, { persistPolling: true });
      let acknowledge!: () => void;
      mocks.post.mockImplementationOnce(
        () => new Promise((resolve) => (acknowledge = () => resolve({ status: 200 }))),
      );
      const complete = vi.fn();
      const pending =
        kind === "flow"
          ? execution.runFlow()
          : execution.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
      await vi.advanceTimersByTimeAsync(0);
      mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
      await loadHeaderSettings(execution);
      expect(vi.getTimerCount()).toBe(0);
      acknowledge();
      await pending;
      expect(vi.getTimerCount()).toBe(1);
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
      await vi.advanceTimersByTimeAsync(2000);
      expect(complete).toHaveBeenCalledTimes(kind === "node" ? 1 : 0);
      expect(
        mocks.tutorial.mock.calls.filter(([event]) => event.type === "flow-run-completed"),
      ).toHaveLength(kind === "node" ? 0 : 1);
    },
  );

  it.each(["flow", "node"])(
    "refreezes the selected %s when acknowledgement follows an idle settings read",
    async (kind) => {
      const execution = useFlowExecution(1, undefined, { persistPolling: true });
      let acknowledge!: () => void;
      mocks.post.mockImplementationOnce(
        () => new Promise((resolve) => (acknowledge = () => resolve({ status: 200 }))),
      );
      const pending = kind === "flow" ? execution.runFlow() : execution.triggerNodeFetch(3);
      await vi.advanceTimersByTimeAsync(0);
      mocks.getFlowSettings.mockResolvedValue({ is_running: false, is_canceled: false });
      mocks.get.mockResolvedValue({ status: 202, data: { flow_id: 1 } });
      await loadHeaderSettings(execution);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(true);
      acknowledge();
      await pending;
      expect(useEditorStore().isRunning).toBe(true);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
      expect(mocks.nodeState.vueFlowInstance.nodesConnectable.value).toBe(false);
      expect(mocks.nodeState.vueFlowInstance.elementsSelectable.value).toBe(false);
      expect(vi.getTimerCount()).toBe(1);
    },
  );

  it.each(["flow", "node"])(
    "keeps a confirmed running %s observed when its pending POST rejects",
    async (kind) => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      const owner = useFlowExecution(1, undefined, { persistPolling: true });
      const header = useFlowExecution(1, undefined, { persistPolling: true });
      const complete = vi.fn();
      let reject!: (error: Error) => void;
      mocks.post.mockImplementationOnce(() => new Promise((_, fail) => (reject = fail)));
      const pending =
        kind === "flow"
          ? owner.runFlow()
          : owner.triggerNodeFetch(3, {
              focusResultPanels: false,
              onComplete: complete,
            });
      await vi.advanceTimersByTimeAsync(0);
      mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
      await loadHeaderSettings(header);
      expect(useEditorStore().isRunning).toBe(true);
      reject(new Error("response lost"));
      await pending;
      expect(useEditorStore().isRunning).toBe(true);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(false);
      expect(vi.getTimerCount()).toBe(1);
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
      await vi.advanceTimersByTimeAsync(2000);
      expect(useEditorStore().isRunning).toBe(false);
      expect(mocks.nodeState.vueFlowInstance.nodesDraggable.value).toBe(true);
      expect(complete).toHaveBeenCalledTimes(kind === "node" ? 1 : 0);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("lets an idle settings refresh recover a paused owner's terminal path", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const execution = useFlowExecution(1, undefined, { persistPolling: true });
    await execution.runFlow();
    mocks.get.mockRejectedValue(new Error("offline"));
    await vi.advanceTimersByTimeAsync(10000);
    expect(vi.getTimerCount()).toBe(0);
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
    mocks.notify.mockClear();
    await loadHeaderSettings(execution);
    await vi.advanceTimersByTimeAsync(0);
    expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ title: "Success" }),
    );
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["flow", "node"])(
    "does not revive a cancelled %s when its start acknowledgement arrives late",
    async (kind) => {
      const execution = useFlowExecution(1, undefined, { persistPolling: true });
      const complete = vi.fn();
      let acknowledge!: () => void;
      mocks.post.mockImplementationOnce(
        () => new Promise((resolve) => (acknowledge = () => resolve({ status: 200 }))),
      );
      const pending =
        kind === "flow"
          ? execution.runFlow()
          : execution.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
      await vi.advanceTimersByTimeAsync(0);
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
      await execution.cancelFlow();
      expect(useEditorStore().isRunning).toBe(false);
      expect(complete).toHaveBeenCalledTimes(kind === "node" ? 1 : 0);
      expect(vi.getTimerCount()).toBe(0);
      acknowledge();
      await pending;
      expect(useEditorStore().isRunning).toBe(false);
      await vi.advanceTimersByTimeAsync(2000);
      expect(complete).toHaveBeenCalledTimes(kind === "node" ? 1 : 0);
      expect(
        mocks.notify.mock.calls.filter(([notice]) => notice.title === "Cancelled"),
      ).toHaveLength(1);
      expect(mocks.tutorial).not.toHaveBeenCalledWith(
        expect.objectContaining({ type: "flow-run-completed" }),
      );
    },
  );

  it.each(["flow", "node"])(
    "retires a cancelled pending %s before adopting another server run",
    async (kind) => {
      const execution = useFlowExecution(1, undefined, { persistPolling: true });
      const header = useFlowExecution(1, undefined, { persistPolling: true });
      const complete = vi.fn();
      let acknowledge!: () => void;
      mocks.post.mockImplementationOnce(
        () => new Promise((resolve) => (acknowledge = () => resolve({ status: 200 }))),
      );
      const pending =
        kind === "flow"
          ? execution.runFlow()
          : execution.triggerNodeFetch(3, { onComplete: complete });
      await vi.advanceTimersByTimeAsync(0);
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
      await header.cancelFlow();
      expect(complete).toHaveBeenCalledTimes(kind === "node" ? 1 : 0);
      expect(vi.getTimerCount()).toBe(0);
      mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
      await loadHeaderSettings(header);
      expect(vi.getTimerCount()).toBe(1);
      mocks.notify.mockClear();
      acknowledge();
      await pending;
      expect(useEditorStore().isRunning).toBe(true);
      expect(vi.getTimerCount()).toBe(1);
      expect(mocks.notify).not.toHaveBeenCalled();
      expect(complete).toHaveBeenCalledTimes(kind === "node" ? 1 : 0);
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
      await vi.advanceTimersByTimeAsync(2000);
      expect(useEditorStore().isRunning).toBe(false);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("settles the node owner when the cancel status wins before an older owner reply", async () => {
    const panel = useFlowExecution(1, undefined, {
      persistPolling: true,
      pollingKey: "table_flow_1",
    });
    const header = useFlowExecution(1, undefined, { persistPolling: true });
    const complete = vi.fn();
    await panel.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
    const replies: Array<(response: unknown) => void> = [];
    mocks.get.mockImplementation(() => new Promise((resolve) => replies.push(resolve)));
    await vi.advanceTimersByTimeAsync(2000);
    const cancelling = header.cancelFlow();
    await vi.advanceTimersByTimeAsync(0);
    expect(replies).toHaveLength(2);
    replies[1]({ status: 200, data: { flow_id: 1, success: false } });
    await cancelling;
    await vi.advanceTimersByTimeAsync(0);
    expect(complete).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
    expect(mocks.notify.mock.calls.filter(([notice]) => notice.title === "Cancelled")).toHaveLength(
      1,
    );
    await header.runFlow();
    mocks.notify.mockClear();
    mocks.nodeState.insertRunResult.mockClear();
    replies[0]({ status: 200, data: { flow_id: 1, success: false } });
    await vi.advanceTimersByTimeAsync(0);
    expect(useEditorStore().isRunning).toBe(true);
    expect(header.isPollingActive()).toBe(true);
    expect(complete).toHaveBeenCalledOnce();
    expect(mocks.notify).not.toHaveBeenCalled();
    expect(mocks.nodeState.insertRunResult).not.toHaveBeenCalled();
  });

  it("follows Save As without applying an in-flight old-ID result", async () => {
    const currentId = ref(1);
    const editor = useEditorStore();
    const execution = useFlowExecution(() => currentId.value, undefined, { persistPolling: true });
    const terminal = { flow_id: 2, success: true, node_step_result: [] };
    let finishOldStatus!: (response: unknown) => void;
    mocks.get
      .mockImplementationOnce(() => new Promise((resolve) => (finishOldStatus = resolve)))
      .mockResolvedValue({ status: 200, data: terminal });

    await execution.runFlow();
    const oldRequest = execution.checkRunStatus();
    currentId.value = 2;
    mocks.nodeState.flow_id = 2;
    const restored = useFlowExecution(() => currentId.value, undefined, { persistPolling: true });
    restored.startStatusPolling(2);
    rekey(1, 2);
    expect(vi.getTimerCount()).toBe(1);
    finishOldStatus({ status: 200, data: { flow_id: 1, success: false } });
    await oldRequest;
    expect(mocks.nodeState.insertRunResult).not.toHaveBeenCalled();
    expect(editor.isRunning).toBe(true);
    await vi.advanceTimersByTimeAsync(2000);

    expect(mocks.get).toHaveBeenCalledWith("/flow/run_status/", {
      params: { flow_id: 2 },
      headers: { accept: "application/json" },
    });
    expect(mocks.nodeState.insertRunResult).toHaveBeenCalledWith(terminal);
    expect(editor.isRunning).toBe(false);
    expect(execution.isPollingActive()).toBe(false);
  });
  it.each(["acknowledged", "rejected"])(
    "ignores an old %s cancellation after a same-ID replacement starts",
    async (outcome) => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      const execution = useFlowExecution(1, undefined, { persistPolling: true });
      await execution.runFlow();
      let settle!: () => void;
      mocks.post.mockImplementationOnce(
        () =>
          new Promise((resolve, reject) => {
            settle = () =>
              outcome === "acknowledged" ? resolve({ status: 200 }) : reject(new Error("offline"));
          }),
      );
      const pending = execution.cancelFlow();
      expect(execution.isCancelling.value).toBe(true);
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
      await vi.advanceTimersByTimeAsync(2000);
      expect(execution.isCancelling.value).toBe(false);
      await execution.runFlow();
      expect(execution.isCancelling.value).toBe(false);
      mocks.notify.mockClear();
      mocks.get.mockClear();
      settle();
      await pending;
      expect(execution.isCancelling.value).toBe(false);
      expect(useEditorStore().isCanceled).toBe(false);
      expect(mocks.get).not.toHaveBeenCalled();
      expect(mocks.notify).not.toHaveBeenCalled();
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
      await vi.advanceTimersByTimeAsync(2000);
      expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ title: "Success" }),
      );
    },
  );

  it("rolls back a failed pending cancellation without relabelling a node failure", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const panel = useFlowExecution(1, undefined, { persistPolling: true, pollingKey: "panel" });
    const header = useFlowExecution(1, undefined, { persistPolling: true });
    const complete = vi.fn();
    await panel.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
    mocks.post.mockRejectedValueOnce(new Error("offline"));
    await header.cancelFlow();
    expect(header.isCancelling.value).toBe(false);
    mocks.notify.mockClear();
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(complete).toHaveBeenCalledOnce();
    expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ title: "Error" }),
    );
  });

  it("retains acknowledged cancellation when a recovery Cancel request fails", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const execution = useFlowExecution(1, undefined, { persistPolling: true });
    await execution.runFlow();
    mocks.get.mockResolvedValue({ status: 202, data: { flow_id: 1 } });
    await execution.cancelFlow();
    expect(useEditorStore().isCanceled).toBe(true);
    mocks.post.mockRejectedValueOnce(new Error("offline"));
    await execution.cancelFlow();
    expect(useEditorStore().isCanceled).toBe(true);
    mocks.notify.mockClear();
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ title: "Cancelled" }),
    );
  });

  it.each([true, false])(
    "settles a node terminal success=%s before a rejected Cancel without a false cancellation",
    async (success) => {
      const panel = useFlowExecution(1, undefined, { persistPolling: true, pollingKey: "panel" });
      const header = useFlowExecution(1, undefined, { persistPolling: true });
      const complete = vi.fn();
      await panel.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
      let reject!: () => void;
      mocks.post.mockImplementationOnce(
        () =>
          new Promise((_, fail) => {
            reject = () => fail(new Error("offline"));
          }),
      );
      const pending = header.cancelFlow();
      mocks.notify.mockClear();
      mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success } });
      await vi.advanceTimersByTimeAsync(2000);
      expect(mocks.notify.mock.calls.map(([notice]) => notice.title)).toEqual(
        success ? ["Success"] : [],
      );
      reject();
      await pending;
      expect(complete).toHaveBeenCalledOnce();
      expect(mocks.notify.mock.calls.map(([notice]) => notice.title)).toEqual(
        success ? ["Success"] : [],
      );
      expect(useEditorStore().isCanceled).toBe(false);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it("REVIEW: adopts an uncancelled replacement after cancelled run", async () => {
    const owner = useFlowExecution(1, undefined, { persistPolling: true });
    const header = useFlowExecution(1, undefined, { persistPolling: true });
    await owner.runFlow();
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await header.cancelFlow();
    mocks.getFlowSettings.mockResolvedValue({ is_running: true, is_canceled: false });
    await loadHeaderSettings(header);
    mocks.notify.mockClear();
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: true } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(mocks.notify).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ title: "Success" }),
    );
  });
  it("keeps an unsuccessful node terminal silent while cancel ACK is pending", async () => {
    const panel = useFlowExecution(1, undefined, {
      persistPolling: true,
      pollingKey: "table_flow_1",
    });
    const header = useFlowExecution(1, undefined, { persistPolling: true });
    const complete = vi.fn();
    await panel.triggerNodeFetch(3, { focusResultPanels: false, onComplete: complete });
    let acknowledge!: () => void;
    mocks.post.mockImplementationOnce(
      () => new Promise((resolve) => (acknowledge = () => resolve({ status: 200 }))),
    );
    const pending = header.cancelFlow();
    mocks.notify.mockClear();
    mocks.get.mockResolvedValue({ status: 200, data: { flow_id: 1, success: false } });
    await vi.advanceTimersByTimeAsync(2000);
    expect(complete).toHaveBeenCalledOnce();
    const reports = mocks.notify.mock.calls.map(([n]) => n.title);
    acknowledge();
    await pending;
    expect(reports).toEqual([]);
    expect(mocks.notify).not.toHaveBeenCalled();
    expect(complete).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });
});
