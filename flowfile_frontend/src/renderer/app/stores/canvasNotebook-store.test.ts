// @vitest-environment happy-dom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { NotebookRendering, RenderedCell } from "../api/canvasNotebook.api";

const { getStatusMock } = vi.hoisted(() => ({ getStatusMock: vi.fn() }));

vi.mock("../api/canvasNotebook.api", () => ({
  CanvasNotebookApi: { getStatus: getStatusMock, render: vi.fn() },
}));

import {
  DOCK_DEFAULT_WIDTH,
  DOCK_MIN_WIDTH,
  DOCK_WIDTH_KEY,
  clampDockWidth,
  toDockCells,
  useCanvasNotebookStore,
} from "./canvasNotebook-store";
import { useEditorStore } from "./editor-store";
import { CANVAS_PANEL_SELECTOR, isCanvasClipboardTarget } from "../composables/useFlowClipboard";

const cell = (overrides: Partial<RenderedCell>): RenderedCell => ({
  cell_id: "node-1",
  node_ids: [1],
  kind: "node",
  code: "x = 1",
  defines: [],
  uses: [],
  status: "code",
  reason: null,
  ...overrides,
});

const rendering = (fingerprint: string, cells: RenderedCell[] = []): NotebookRendering => ({
  cells,
  warnings: [],
  var_by_node: {},
  code_fingerprint: fingerprint,
});

const memoryStorage = () => {
  const map = new Map<string, string>();
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    value: {
      getItem: (key: string) => map.get(key) ?? null,
      setItem: (key: string, value: string) => void map.set(key, String(value)),
      removeItem: (key: string) => void map.delete(key),
      clear: () => map.clear(),
    },
  });
  return map;
};

describe("canvasNotebook-store", () => {
  let storage: Map<string, string>;

  beforeEach(() => {
    storage = memoryStorage();
    setActivePinia(createPinia());
    getStatusMock.mockReset();
  });

  it("is available only when the status route answers", async () => {
    getStatusMock.mockResolvedValue(null);
    const store = useCanvasNotebookStore();
    await store.loadStatus();
    expect(store.available).toBe(false);
    store.toggle();
    expect(store.open).toBe(false);

    getStatusMock.mockResolvedValue({ canvas_notebook: true, sessions: false });
    await store.loadStatus(true);
    expect(store.available).toBe(true);
    expect(store.sessionsEnabled).toBe(false);
    store.toggle();
    expect(store.open).toBe(true);
  });

  it("loads the status once and treats a failure as unavailable", async () => {
    getStatusMock.mockRejectedValue(new Error("down"));
    const store = useCanvasNotebookStore();
    await store.loadStatus();
    await store.loadStatus();
    expect(getStatusMock).toHaveBeenCalledTimes(1);
    expect(store.statusLoaded).toBe(true);
    expect(store.available).toBe(false);
  });

  it("orders imports and parameters first and keeps node cells in render order", () => {
    const cells = toDockCells(
      rendering("fp", [
        cell({ cell_id: "node-2", node_ids: [2] }),
        cell({ cell_id: "parameters", kind: "parameters", node_ids: [] }),
        cell({ cell_id: "node-1", node_ids: [1], status: "placeholder", reason: "not set up" }),
        cell({ cell_id: "imports", kind: "imports", node_ids: [], code: "import flowfile as fl" }),
      ]),
    );
    expect(cells.map((c) => c.cellId)).toEqual(["imports", "parameters", "node-2", "node-1"]);
    expect(cells[3]).toMatchObject({ status: "placeholder", reason: "not set up", base: "x = 1" });
  });

  it("ignores a rendering whose fingerprint is unchanged or whose flow is stale", () => {
    const store = useCanvasNotebookStore();
    store.resetForFlow(1);
    expect(store.applyRendering(1, rendering("a", [cell({})]))).toBe(true);
    expect(store.renderCount).toBe(1);
    expect(store.applyRendering(1, rendering("a", []))).toBe(false);
    expect(store.cells).toHaveLength(1);
    expect(store.applyRendering(2, rendering("b"))).toBe(false);
    expect(store.applyRendering(1, rendering("b"))).toBe(true);
    expect(store.renderCount).toBe(2);
  });

  it("forgets the fingerprint when the flow changes", () => {
    const store = useCanvasNotebookStore();
    store.resetForFlow(1);
    store.applyRendering(1, rendering("same", [cell({})]));
    store.resetForFlow(2);
    expect(store.cells).toEqual([]);
    expect(store.applyRendering(2, rendering("same"))).toBe(true);
  });

  it("persists the width only at gesture end and clamps it", () => {
    const store = useCanvasNotebookStore();
    expect(store.width).toBe(DOCK_DEFAULT_WIDTH);
    store.setWidth(500);
    expect(storage.has(DOCK_WIDTH_KEY)).toBe(false);
    store.setWidth(520, true);
    expect(storage.get(DOCK_WIDTH_KEY)).toBe("520");
    setActivePinia(createPinia());
    expect(useCanvasNotebookStore().width).toBe(520);
    expect(clampDockWidth(10, 1000)).toBe(DOCK_MIN_WIDTH);
    expect(clampDockWidth(5000, 1000)).toBe(700);
  });

  it("stays open through hideAllPanels", async () => {
    getStatusMock.mockResolvedValue({ canvas_notebook: true, sessions: true });
    const store = useCanvasNotebookStore();
    await store.loadStatus();
    store.setOpen(true);
    useEditorStore().hideAllPanels();
    expect(store.open).toBe(true);
  });

  it("keeps canvas paste out of the dock", () => {
    expect(CANVAS_PANEL_SELECTOR).toContain("[data-canvas-notebook]");
    const dock = document.createElement("aside");
    dock.setAttribute("data-canvas-notebook", "");
    const inner = document.createElement("span");
    dock.appendChild(inner);
    document.body.appendChild(dock);
    expect(isCanvasClipboardTarget(inner, null)).toBe(false);
    dock.remove();
  });
});
