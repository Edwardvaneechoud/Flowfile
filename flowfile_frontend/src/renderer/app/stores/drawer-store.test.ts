// @vitest-environment happy-dom
// The preview selection: the canvas centres on the node without a zoom jump, and a flow whose Data
// is in its own window sends the node there instead of opening the dock.
import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

// flow-store reaches axios.config at import time; only its canvas handle and flow id matter here.
const mocks = vi.hoisted(() => ({ vueFlowInstance: null as Record<string, unknown> | null }));
vi.mock("./flow-store", () => ({
  useFlowStore: () => ({ flowId: 4, vueFlowInstance: mocks.vueFlowInstance }),
}));
vi.mock("element-plus", () => ({ ElMessage: { warning: vi.fn(), info: vi.fn() } }));

import { useDrawerStore } from "./drawer-store";
import { useEditorStore } from "./editor-store";

const canvasAt = (zoom: number) => {
  const fitView = vi.fn();
  mocks.vueFlowInstance = { fitView, getViewport: () => ({ x: 0, y: 0, zoom }) };
  return fitView;
};

describe("selectNodeForPreview", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    mocks.vueFlowInstance = null;
  });

  it("animates to the node and never zooms past 1:1 from a zoomed-out canvas", () => {
    const fitView = canvasAt(0.5);
    useDrawerStore().selectNodeForPreview(3);
    expect(fitView).toHaveBeenCalledWith({ nodes: ["3"], padding: 0.3, duration: 300, maxZoom: 1 });
  });

  it("keeps a closer zoom the user already chose", () => {
    const fitView = canvasAt(1.5);
    useDrawerStore().selectNodeForPreview(3);
    expect(fitView).toHaveBeenCalledWith(expect.objectContaining({ maxZoom: 1.5 }));
  });

  it("still opens the data tab without a canvas", () => {
    const drawer = useDrawerStore();
    drawer.selectNodeForPreview(3);
    expect(drawer.previewNodeId).toBe(3);
    expect(drawer.activeTab.bottomDock).toBe("data");
  });
});

describe("setPreviewNode while the flow's Data window is out", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    mocks.vueFlowInstance = null;
  });

  it("sends the node to the window and leaves the dock as it is", () => {
    useEditorStore().markPoppedOut("table", 4);
    const drawer = useDrawerStore();
    drawer.setPreviewNode(3);
    expect(drawer.previewNodeId).toBeNull();
    expect(drawer.popoutPreview).toEqual({ 4: { nodeId: 3, token: 1 } });

    drawer.setPreviewNode(3);
    expect(drawer.popoutPreview[4]).toEqual({ nodeId: 3, token: 2 });
    expect(drawer.previewRefreshToken).toBe(0);
  });

  it("previews in the dock when the window is another flow's", () => {
    useEditorStore().markPoppedOut("table", 9);
    const drawer = useDrawerStore();
    drawer.setPreviewNode(3);
    expect(drawer.previewNodeId).toBe(3);
    expect(drawer.popoutPreview).toEqual({});
  });

  it("a canvas deselect never blanks the window", () => {
    useEditorStore().markPoppedOut("table", 4);
    const drawer = useDrawerStore();
    drawer.setPreviewNode(3);
    drawer.clearPreview();
    drawer.setPreviewNode(null);
    expect(drawer.popoutPreview[4]?.nodeId).toBe(3);
  });

  it("a results row click goes to the window too", () => {
    useEditorStore().markPoppedOut("table", 4);
    const drawer = useDrawerStore();
    drawer.selectNodeForPreview(3);
    expect(drawer.previewNodeId).toBeNull();
    expect(drawer.popoutPreview[4]?.nodeId).toBe(3);
  });
});

describe("the Data tab's move, a Save As and a window's Return", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("takes the previewed node along when the tab pops out, and nothing when there is none", () => {
    const drawer = useDrawerStore();
    drawer.setPreviewNode(3);
    drawer.divertPreviewToWindow(4);
    expect(drawer.previewNodeId).toBeNull();
    expect(drawer.popoutPreview).toEqual({ 4: { nodeId: 3, token: 1 } });

    drawer.divertPreviewToWindow(4);
    expect(drawer.popoutPreview).toEqual({});
  });

  it("keeps each flow's window on its own node", () => {
    const drawer = useDrawerStore();
    drawer.setPreviewNode(3);
    drawer.divertPreviewToWindow(4);
    drawer.setPreviewNode(5);
    drawer.divertPreviewToWindow(9);
    expect(drawer.popoutPreview).toEqual({
      4: { nodeId: 3, token: 1 },
      9: { nodeId: 5, token: 1 },
    });
  });

  it("moves the window's node with a Save As and forgets it with the window", () => {
    const drawer = useDrawerStore();
    drawer.setPreviewNode(3);
    drawer.divertPreviewToWindow(4);
    drawer.movePopoutPreview(4, 9);
    expect(drawer.popoutPreview).toEqual({ 9: { nodeId: 3, token: 1 } });

    drawer.movePopoutPreview(4, 11);
    expect(drawer.popoutPreview).toEqual({ 9: { nodeId: 3, token: 1 } });

    drawer.forgetPopoutPreview(9);
    expect(drawer.popoutPreview).toEqual({});
  });

  it("numbers dock requests and forgets a consumed one", () => {
    const drawer = useDrawerStore();
    drawer.requestDock({ flowId: 4, tab: "data", nodeId: 3 });
    expect(drawer.dockRequest).toEqual({ flowId: 4, tab: "data", nodeId: 3, token: 1 });
    drawer.requestDock({ flowId: 4, tab: "logs" });
    expect(drawer.dockRequest).toEqual({ flowId: 4, tab: "logs", token: 2 });
    drawer.consumeDockRequest();
    expect(drawer.dockRequest).toBeNull();
  });
});
