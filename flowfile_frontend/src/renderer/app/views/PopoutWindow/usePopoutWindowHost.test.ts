// @vitest-environment happy-dom
// The shared half of a pop-out window: pin the URL's flow before the feed starts, then follow the feed.
import { nextTick, reactive } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
  log: [] as string[],
  route: { query: {} as Record<string, string> },
  router: { replace: vi.fn() },
  flowStore: {} as any,
  editorStore: {} as any,
  flowSync: {} as any,
  getFlowSettings: vi.fn(),
  desktop: {
    setWindowTitle: vi.fn(),
    closeCurrentWindow: vi.fn(),
    returnPopoutToDesigner: vi.fn(),
  },
  messageInfo: vi.fn(),
}));

vi.mock("vue-router", () => ({ useRoute: () => h.route, useRouter: () => h.router }));
vi.mock("../../api", () => ({ FlowApi: { getFlowSettings: h.getFlowSettings } }));
vi.mock("../../../lib/desktop", () => ({ desktop: h.desktop }));
vi.mock("../../stores/flow-store", () => ({ useFlowStore: () => h.flowStore }));
vi.mock("../../stores/editor-store", () => ({ useEditorStore: () => h.editorStore }));
vi.mock("../../stores/flow-sync-store", () => ({
  useFlowSyncStore: () => {
    h.log.push("flowSync");
    return h.flowSync;
  },
}));
vi.mock("element-plus", () => ({ ElMessage: { info: h.messageInfo } }));

import { usePopoutWindowHost } from "./usePopoutWindowHost";

const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
  await nextTick();
};

describe("usePopoutWindowHost", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    h.log.length = 0;
    h.route.query = { flow: "4" };
    h.flowStore = reactive({
      pendingReloadCounter: 0,
      pendingRunStateCounter: 0,
      setFlowId: vi.fn((id: number) => h.log.push(`setFlowId:${id}`)),
    });
    h.editorStore = reactive({ isRunning: false });
    h.flowSync = reactive({
      closedFlowId: null as number | null,
      closeCount: 0,
      rekeyedTo: null as { from: number; to: number } | null,
    });
    h.getFlowSettings.mockResolvedValue({ name: "salary", display_name: null, is_running: true });
    h.desktop.setWindowTitle.mockResolvedValue(undefined);
    h.desktop.closeCurrentWindow.mockResolvedValue(undefined);
    h.desktop.returnPopoutToDesigner.mockResolvedValue(undefined);
  });

  it("pins the URL's flow before the feed starts, then loads it", async () => {
    const host = usePopoutWindowHost({ kind: "notebook" });
    expect(h.log.slice(0, 2)).toEqual(["setFlowId:4", "flowSync"]);
    expect(host.flowId.value).toBe(4);

    await settle();
    expect(h.getFlowSettings).toHaveBeenCalledWith(4);
    expect(host.state.value).toBe("ready");
    expect(host.title.value).toBe("Notebook – salary");
    expect(h.desktop.setWindowTitle).toHaveBeenCalledWith("Notebook – salary");
    expect(h.editorStore.isRunning).toBe(true);
  });

  it("reports a flow core does not have as missing", async () => {
    h.getFlowSettings.mockResolvedValue(null);
    const host = usePopoutWindowHost({ kind: "logs" });
    await settle();
    expect(host.state.value).toBe("missing");
    expect(host.title.value).toBe("Logs");
    expect(h.desktop.setWindowTitle).toHaveBeenCalledWith("Logs");
  });

  it("has no flow without a usable query and does not start the feed on another's", () => {
    h.route.query = { flow: "abc" };
    const host = usePopoutWindowHost({ kind: "notebook" });
    expect(host.flowId.value).toBe(-1);
    expect(host.state.value).toBe("missing");
    expect(h.flowStore.setFlowId).not.toHaveBeenCalled();
    expect(h.getFlowSettings).not.toHaveBeenCalled();
  });

  it("re-reads the run state on the feed's signal and tells the view", async () => {
    const onRunStateChanged = vi.fn();
    usePopoutWindowHost({ kind: "notebook", onRunStateChanged });
    await settle();
    expect(h.editorStore.isRunning).toBe(true);

    h.getFlowSettings.mockResolvedValue({ name: "salary", display_name: null, is_running: false });
    h.flowStore.pendingRunStateCounter += 1;
    await settle();
    expect(h.editorStore.isRunning).toBe(false);
    expect(onRunStateChanged).toHaveBeenCalledWith(false);
  });

  it("turns a reload request into the view's hook", async () => {
    const onReload = vi.fn();
    usePopoutWindowHost({ kind: "notebook", onReload });
    await settle();
    h.flowStore.pendingReloadCounter += 1;
    await settle();
    expect(onReload).toHaveBeenCalledOnce();
  });

  it("closes when its own flow closes elsewhere, not when another does", async () => {
    usePopoutWindowHost({ kind: "notebook" });
    await settle();

    h.flowSync.closedFlowId = 9;
    h.flowSync.closeCount += 1;
    await settle();
    expect(h.desktop.closeCurrentWindow).not.toHaveBeenCalled();

    h.flowSync.closedFlowId = 4;
    h.flowSync.closeCount += 1;
    await settle();
    expect(h.messageInfo).toHaveBeenCalledOnce();
    expect(h.desktop.closeCurrentWindow).toHaveBeenCalledOnce();
  });

  it("follows a Save As of its own flow to the new id", async () => {
    const onRekey = vi.fn();
    usePopoutWindowHost({ kind: "notebook", onRekey });
    await settle();

    h.flowSync.rekeyedTo = { from: 7, to: 9 };
    await settle();
    expect(onRekey).not.toHaveBeenCalled();
    expect(h.router.replace).not.toHaveBeenCalled();

    h.flowSync.rekeyedTo = { from: 4, to: 9 };
    await settle();
    expect(onRekey).toHaveBeenCalledWith({ from: 4, to: 9 });
    expect(h.router.replace).toHaveBeenCalledWith({ query: { flow: "9" } });
  });

  it("returns to the designer unless the view's guard says no", async () => {
    const blocked = usePopoutWindowHost({ kind: "notebook", beforeReturn: async () => false });
    await settle();
    await blocked.returnToDesigner();
    expect(h.desktop.returnPopoutToDesigner).not.toHaveBeenCalled();

    const open = usePopoutWindowHost({ kind: "notebook" });
    await open.returnToDesigner();
    expect(h.desktop.returnPopoutToDesigner).toHaveBeenCalledWith("notebook", 4);
  });

  it("just closes on Return when it has no flow", async () => {
    h.route.query = {};
    const host = usePopoutWindowHost({ kind: "notebook" });
    await host.returnToDesigner();
    expect(h.desktop.returnPopoutToDesigner).not.toHaveBeenCalled();
    expect(h.desktop.closeCurrentWindow).toHaveBeenCalledOnce();
  });
});
