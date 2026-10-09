// @vitest-environment happy-dom
// Popping out is a move: the designer remembers the flow per kind until its window is gone again.
import { beforeEach, describe, expect, it, vi } from "vitest";

type Ref = { kind: string; flowId: number };
type Move = { kind: string; from: number; to: number };

const mocks = vi.hoisted(() => ({
  desktop: {
    openPopoutWindow: vi.fn(),
    focusPopoutWindow: vi.fn(),
    closePopoutWindow: vi.fn(),
    listPopoutWindows: vi.fn(),
    postToPopoutWindow: vi.fn(),
    onPopoutWindowClosed: vi.fn(),
    onPopoutWindowReturned: vi.fn(),
    onPopoutWindowRekeyed: vi.fn(),
    onPopoutWindowReady: vi.fn(),
  },
  editorStore: {
    poppedOut: {} as Record<string, number[]>,
    markPoppedOut: vi.fn((kind: string, id: number) => {
      (mocks.editorStore.poppedOut[kind] ??= []).push(id);
    }),
    clearPoppedOut: vi.fn((kind: string, id: number) => {
      mocks.editorStore.poppedOut[kind] = (mocks.editorStore.poppedOut[kind] ?? []).filter(
        (x) => x !== id,
      );
    }),
    isPoppedOut: vi.fn((kind: string, id: number) =>
      (mocks.editorStore.poppedOut[kind] ?? []).includes(id),
    ),
    setCodeGeneratorVisibility: vi.fn(),
    openCodePane: vi.fn(),
    hideLogViewer: vi.fn(),
  },
  flowStore: { setFlowId: vi.fn(), selectedNodeIds: [] as number[] },
  drawerStore: {
    previewNodeId: null as number | null,
    popoutPreview: null as { flowId: number; nodeId: number; token: number } | null,
    divertPreviewToWindow: vi.fn(),
    requestDock: vi.fn(),
  },
  router: { currentRoute: { value: { name: "designer" as string } }, push: vi.fn() },
  messageError: vi.fn(),
}));

vi.mock("../../lib/desktop", () => ({ desktop: mocks.desktop }));
vi.mock("../stores/editor-store", () => ({ useEditorStore: () => mocks.editorStore }));
vi.mock("../stores/flow-store", () => ({ useFlowStore: () => mocks.flowStore }));
vi.mock("../stores/drawer-store", () => ({ useDrawerStore: () => mocks.drawerStore }));
vi.mock("../router", () => ({ default: mocks.router }));
vi.mock("element-plus", () => ({ ElMessage: { error: mocks.messageError } }));

import {
  _resetForTests,
  broadcastSelection,
  installPopoutListeners,
  usePopout,
} from "./usePopout";

const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
};

describe("usePopout", () => {
  let onClosed: ((popout: Ref) => void) | null = null;
  let onReturned: ((popout: Ref) => void) | null = null;
  let onRekeyed: ((move: Move) => void) | null = null;
  let onReady: ((popout: Ref) => void) | null = null;

  beforeEach(() => {
    vi.clearAllMocks();
    _resetForTests();
    mocks.editorStore.poppedOut = {};
    mocks.flowStore.selectedNodeIds = [];
    mocks.drawerStore.previewNodeId = null;
    mocks.drawerStore.popoutPreview = null;
    mocks.router.currentRoute.value.name = "designer";
    onClosed = null;
    onReturned = null;
    onRekeyed = null;
    onReady = null;
    mocks.desktop.onPopoutWindowClosed.mockImplementation(async (handler) => {
      onClosed = handler;
      return () => undefined;
    });
    mocks.desktop.onPopoutWindowReturned.mockImplementation(async (handler) => {
      onReturned = handler;
      return () => undefined;
    });
    mocks.desktop.onPopoutWindowRekeyed.mockImplementation(async (handler) => {
      onRekeyed = handler;
      return () => undefined;
    });
    mocks.desktop.onPopoutWindowReady.mockImplementation(async (handler) => {
      onReady = handler;
      return () => undefined;
    });
    mocks.desktop.listPopoutWindows.mockResolvedValue([]);
    mocks.desktop.postToPopoutWindow.mockResolvedValue(undefined);
    mocks.desktop.openPopoutWindow.mockResolvedValue(undefined);
    mocks.desktop.closePopoutWindow.mockResolvedValue(undefined);
    mocks.desktop.focusPopoutWindow.mockResolvedValue(undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
  });

  it("opens the window on the kind's route, marks the flow and closes the dock", async () => {
    const { popOut, isPoppedOut } = usePopout("notebook");

    expect(await popOut(4)).toBe(true);
    expect(mocks.desktop.openPopoutWindow).toHaveBeenCalledWith("notebook", 4, {
      hash: "#/notebook?flow=4",
      name: "flowfile-notebook-4",
    });
    expect(isPoppedOut(4)).toBe(true);
    expect(mocks.editorStore.setCodeGeneratorVisibility).toHaveBeenCalledWith(false);
  });

  it("keeps the dock when the window could not open", async () => {
    mocks.desktop.openPopoutWindow.mockRejectedValue(new Error("blocked"));
    const { popOut, isPoppedOut } = usePopout("notebook");

    expect(await popOut(4)).toBe(false);
    expect(isPoppedOut(4)).toBe(false);
    expect(mocks.messageError).toHaveBeenCalledOnce();
    expect(mocks.editorStore.setCodeGeneratorVisibility).not.toHaveBeenCalled();
  });

  it("refuses a kind that has no window yet", async () => {
    const { popOut, isPoppedOut } = usePopout("ai");

    expect(await popOut(4)).toBe(false);
    expect(mocks.desktop.openPopoutWindow).not.toHaveBeenCalled();
    expect(isPoppedOut(4)).toBe(false);
    expect(console.error).toHaveBeenCalledOnce();
    expect(mocks.messageError).not.toHaveBeenCalled();
  });

  it("forgets a flow whose window closed by itself, one kind at a time", async () => {
    mocks.desktop.listPopoutWindows.mockResolvedValue([{ kind: "table", flowId: 4 }]);
    installPopoutListeners();
    const { popOut, isPoppedOut } = usePopout("notebook");
    await popOut(4);
    await settle();
    expect(mocks.editorStore.isPoppedOut("table", 4)).toBe(true);

    expect(onClosed).not.toBeNull();
    onClosed!({ kind: "table", flowId: 4 });
    expect(isPoppedOut(4)).toBe(true);
    expect(mocks.editorStore.isPoppedOut("table", 4)).toBe(false);

    onClosed!({ kind: "notebook", flowId: 4 });
    expect(isPoppedOut(4)).toBe(false);
  });

  it("bringing back closes the window and reopens the pane in notebook mode", async () => {
    const { popOut, bringBack, isPoppedOut } = usePopout("notebook");
    await popOut(4);

    await bringBack(4);
    expect(mocks.desktop.closePopoutWindow).toHaveBeenCalledWith("notebook", 4);
    expect(isPoppedOut(4)).toBe(false);
    expect(mocks.editorStore.openCodePane).toHaveBeenCalledWith("notebook");
  });

  it("focus goes to the kind's window", async () => {
    const { focus } = usePopout("notebook");
    await focus(4);
    expect(mocks.desktop.focusPopoutWindow).toHaveBeenCalledWith("notebook", 4);
  });

  it("a window's Return to designer reopens the pane on its flow", async () => {
    installPopoutListeners();
    const { popOut, isPoppedOut } = usePopout("notebook");
    await popOut(4);
    await settle();

    expect(onReturned).not.toBeNull();
    onReturned!({ kind: "notebook", flowId: 4 });
    expect(isPoppedOut(4)).toBe(false);
    expect(mocks.flowStore.setFlowId).toHaveBeenCalledWith(4);
    expect(mocks.editorStore.openCodePane).toHaveBeenCalledWith("notebook");
    expect(mocks.router.push).not.toHaveBeenCalled();
  });

  it("a returned notebook brings the designer page back when it was left", async () => {
    mocks.router.currentRoute.value.name = "catalog";
    installPopoutListeners();
    await settle();

    onReturned!({ kind: "notebook", flowId: 4 });
    expect(mocks.flowStore.setFlowId).toHaveBeenCalledWith(4);
    expect(mocks.editorStore.openCodePane).toHaveBeenCalledWith("notebook");
    expect(mocks.router.push).toHaveBeenCalledWith({ name: "designer" });
  });

  it("a returned window of a kind without a panel only clears its mark", async () => {
    mocks.desktop.listPopoutWindows.mockResolvedValue([{ kind: "ai", flowId: 4 }]);
    installPopoutListeners();
    await settle();
    expect(mocks.editorStore.isPoppedOut("ai", 4)).toBe(true);

    onReturned!({ kind: "ai", flowId: 4 });
    expect(mocks.editorStore.isPoppedOut("ai", 4)).toBe(false);
    expect(mocks.flowStore.setFlowId).not.toHaveBeenCalled();
    expect(mocks.editorStore.openCodePane).not.toHaveBeenCalled();
  });

  it("the Data tab pops out on the previewed node and takes it along", async () => {
    mocks.drawerStore.previewNodeId = 3;
    const { popOut } = usePopout("table");

    expect(await popOut(4)).toBe(true);
    expect(mocks.desktop.openPopoutWindow).toHaveBeenCalledWith("table", 4, {
      hash: "#/popout/table?flow=4&node=3",
      name: "flowfile-table-4",
    });
    expect(mocks.drawerStore.divertPreviewToWindow).toHaveBeenCalledExactlyOnceWith(4);
    expect(mocks.editorStore.isPoppedOut("table", 4)).toBe(true);
  });

  it("the Data tab pops out without a node when nothing is previewed", async () => {
    const { popOut } = usePopout("table");
    await popOut(4);
    expect(mocks.desktop.openPopoutWindow).toHaveBeenCalledWith("table", 4, {
      hash: "#/popout/table?flow=4",
      name: "flowfile-table-4",
    });
  });

  it("the Logs tab pops out and the dock's logs hide", async () => {
    const { popOut } = usePopout("logs");
    expect(await popOut(4)).toBe(true);
    expect(mocks.desktop.openPopoutWindow).toHaveBeenCalledWith("logs", 4, {
      hash: "#/popout/logs?flow=4",
      name: "flowfile-logs-4",
    });
    expect(mocks.editorStore.hideLogViewer).toHaveBeenCalledOnce();
  });

  it("a Data window that listens is told its node and the selection; other kinds nothing", async () => {
    mocks.drawerStore.popoutPreview = { flowId: 4, nodeId: 3, token: 1 };
    mocks.flowStore.selectedNodeIds = [3, 5];
    installPopoutListeners();
    await settle();

    expect(onReady).not.toBeNull();
    onReady!({ kind: "table", flowId: 4 });
    expect(mocks.desktop.postToPopoutWindow).toHaveBeenCalledExactlyOnceWith("table", 4, {
      type: "selection",
      previewNodeId: 3,
      selectedNodeIds: [3, 5],
    });

    onReady!({ kind: "logs", flowId: 4 });
    onReady!({ kind: "notebook", flowId: 4 });
    expect(mocks.desktop.postToPopoutWindow).toHaveBeenCalledOnce();

    onReady!({ kind: "table", flowId: 9 });
    expect(mocks.desktop.postToPopoutWindow).toHaveBeenLastCalledWith("table", 9, {
      type: "selection",
      previewNodeId: null,
      selectedNodeIds: [3, 5],
    });
  });

  it("a selection change reaches the popped-out Data window of that flow only", () => {
    mocks.editorStore.poppedOut = { table: [4], logs: [4], notebook: [4] };
    broadcastSelection(4);
    expect(mocks.desktop.postToPopoutWindow).toHaveBeenCalledExactlyOnceWith(
      "table",
      4,
      expect.objectContaining({ type: "selection" }),
    );

    broadcastSelection(9);
    broadcastSelection(-1);
    expect(mocks.desktop.postToPopoutWindow).toHaveBeenCalledOnce();
  });

  it("a Data window's Return reopens the dock on its node, a Logs window's on the logs", async () => {
    mocks.drawerStore.popoutPreview = { flowId: 4, nodeId: 3, token: 1 };
    installPopoutListeners();
    await settle();

    onReturned!({ kind: "table", flowId: 4 });
    expect(mocks.flowStore.setFlowId).toHaveBeenCalledWith(4);
    expect(mocks.drawerStore.requestDock).toHaveBeenCalledWith({ flowId: 4, tab: "data", nodeId: 3 });

    onReturned!({ kind: "logs", flowId: 4 });
    expect(mocks.drawerStore.requestDock).toHaveBeenLastCalledWith({ flowId: 4, tab: "logs" });
    expect(mocks.router.push).not.toHaveBeenCalled();
  });

  it("a Data window returned for another flow asks for the dock without a node", async () => {
    mocks.drawerStore.popoutPreview = { flowId: 4, nodeId: 3, token: 1 };
    mocks.router.currentRoute.value.name = "catalog";
    installPopoutListeners();
    await settle();

    onReturned!({ kind: "table", flowId: 9 });
    expect(mocks.flowStore.setFlowId).toHaveBeenCalledWith(9);
    expect(mocks.drawerStore.requestDock).toHaveBeenCalledWith({
      flowId: 9,
      tab: "data",
      nodeId: undefined,
    });
    expect(mocks.router.push).toHaveBeenCalledWith({ name: "designer" });
  });

  it("bringing the Logs window back asks for the dock on the logs", async () => {
    const { popOut, bringBack } = usePopout("logs");
    await popOut(4);
    await bringBack(4);
    expect(mocks.desktop.closePopoutWindow).toHaveBeenCalledWith("logs", 4);
    expect(mocks.drawerStore.requestDock).toHaveBeenCalledWith({ flowId: 4, tab: "logs" });
  });

  it("a window's Save As moves the mark of its kind only", async () => {
    mocks.desktop.listPopoutWindows.mockResolvedValue([
      { kind: "notebook", flowId: 4 },
      { kind: "table", flowId: 4 },
    ]);
    installPopoutListeners();
    await settle();

    expect(onRekeyed).not.toBeNull();
    onRekeyed!({ kind: "notebook", from: 4, to: 9 });
    expect(mocks.editorStore.isPoppedOut("notebook", 4)).toBe(false);
    expect(mocks.editorStore.isPoppedOut("notebook", 9)).toBe(true);
    expect(mocks.editorStore.isPoppedOut("table", 4)).toBe(true);
    expect(mocks.editorStore.isPoppedOut("table", 9)).toBe(false);
  });

  it("an untracked window's Save As marks nothing", async () => {
    installPopoutListeners();
    await settle();

    onRekeyed!({ kind: "logs", from: 4, to: 9 });
    expect(mocks.editorStore.isPoppedOut("logs", 9)).toBe(false);
    expect(mocks.editorStore.markPoppedOut).not.toHaveBeenCalled();
  });

  it("leaves the listeners to the layout", () => {
    usePopout("notebook");
    expect(mocks.desktop.onPopoutWindowClosed).not.toHaveBeenCalled();
    expect(mocks.desktop.onPopoutWindowRekeyed).not.toHaveBeenCalled();
    expect(mocks.desktop.listPopoutWindows).not.toHaveBeenCalled();
  });

  it("adopts the windows already open and installs the listeners once", async () => {
    mocks.desktop.listPopoutWindows.mockResolvedValue([
      { kind: "notebook", flowId: 2 },
      { kind: "notebook", flowId: 3 },
    ]);
    installPopoutListeners();
    const first = usePopout("notebook");
    await settle();
    expect(first.isPoppedOut(2)).toBe(true);
    expect(first.isPoppedOut(3)).toBe(true);

    installPopoutListeners();
    expect(mocks.desktop.onPopoutWindowClosed).toHaveBeenCalledTimes(1);
    expect(mocks.desktop.onPopoutWindowReturned).toHaveBeenCalledTimes(1);
    expect(mocks.desktop.onPopoutWindowRekeyed).toHaveBeenCalledTimes(1);
    expect(mocks.desktop.onPopoutWindowReady).toHaveBeenCalledTimes(1);
    expect(mocks.desktop.listPopoutWindows).toHaveBeenCalledTimes(1);
  });
});
