// @vitest-environment happy-dom
// Unit tests for the preview selection: the canvas centres on the node without a zoom jump.
import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

// flow-store reaches axios.config at import time; only its canvas handle matters here.
const mocks = vi.hoisted(() => ({ vueFlowInstance: null as Record<string, unknown> | null }));
vi.mock("./flow-store", () => ({
  useFlowStore: () => ({ vueFlowInstance: mocks.vueFlowInstance }),
}));

import { useDrawerStore } from "./drawer-store";

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
