// Unit tests for the multi-notebook catalog store (kernel/markdown/notebook APIs mocked, Pinia per-test).
import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  executeCell: vi.fn(),
  clearNamespace: vi.fn(),
  nbList: vi.fn(),
  nbGet: vi.fn(),
  nbCreate: vi.fn(),
  nbUpdate: vi.fn(),
  nbRemove: vi.fn(),
}));

vi.mock("../api/kernel.api", () => ({
  KernelApi: { executeCell: mocks.executeCell, clearNamespace: mocks.clearNamespace },
}));
vi.mock("../api/notebook.api", () => ({
  NotebookApi: {
    list: mocks.nbList,
    get: mocks.nbGet,
    create: mocks.nbCreate,
    update: mocks.nbUpdate,
    remove: mocks.nbRemove,
  },
}));
vi.mock("../features/ai/markdown", () => ({
  sanitiseMarkdown: (s: string) => `<md>${s}</md>`,
}));

import { useNotebookStore, cellNodeId } from "./notebook-store";

const okExecResult = {
  success: true,
  output_paths: [],
  artifacts_published: [],
  artifacts_deleted: [],
  display_outputs: [],
  stdout: "hello",
  stderr: "",
  error: null,
  execution_time_ms: 7,
};

beforeEach(() => {
  setActivePinia(createPinia());
  vi.clearAllMocks();
  mocks.executeCell.mockResolvedValue(okExecResult);
  mocks.clearNamespace.mockResolvedValue(undefined);
  mocks.nbList.mockResolvedValue([]);
});

describe("cellNodeId", () => {
  it("is stable and non-negative", () => {
    const id = "a1b2-c3d4-e5f6";
    expect(cellNodeId(id)).toBe(cellNodeId(id));
    expect(cellNodeId(id)).toBeGreaterThanOrEqual(0);
    expect(cellNodeId("other")).not.toBe(cellNodeId(id));
  });
});

describe("tab lifecycle", () => {
  it("ensureHydrated starts one blank tab when nothing is persisted", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    expect(store.openNotebooks).toHaveLength(1);
    expect(store.active).not.toBeNull();
    expect(store.active!.cells[0].cellType).toBe("python");
    expect(store.active!.dirty).toBe(false);
  });

  it("opens multiple notebooks as separate tabs with distinct sessions", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const a = store.active!.tabId;
    const b = store.newTab().tabId;
    expect(store.openNotebooks).toHaveLength(2);
    expect(store.activeTabId).toBe(b);
    const sessions = store.openNotebooks.map((n) => n.sessionFlowId);
    expect(new Set(sessions).size).toBe(2); // distinct, collision-free
    expect(sessions.every((s) => s < 0)).toBe(true);
    store.setActiveTab(a);
    expect(store.activeTabId).toBe(a);
  });

  it("closeTab activates a neighbour and never leaves zero tabs", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const first = store.active!.tabId;
    store.newTab();
    store.closeTab(first);
    expect(store.openNotebooks.some((n) => n.tabId === first)).toBe(false);
    expect(store.openNotebooks.length).toBeGreaterThanOrEqual(1);
    // Closing the last remaining tab spawns a fresh blank one.
    store.closeTab(store.activeTabId!);
    expect(store.openNotebooks).toHaveLength(1);
  });

  it("openNotebook focuses an already-open tab instead of duplicating", async () => {
    mocks.nbGet.mockResolvedValue({
      id: 7,
      name: "nb7",
      description: null,
      namespace_id: null,
      default_kernel_id: null,
      owner_id: 1,
      created_at: "",
      updated_at: "",
      namespace_name: null,
      access: null,
      cells: [{ id: "c1", type: "python", source: "x=1", metadata: {} }],
    });
    const store = useNotebookStore();
    store.ensureHydrated();
    await store.openNotebook(7);
    const count = store.openNotebooks.length;
    await store.openNotebook(7); // again
    expect(store.openNotebooks.length).toBe(count); // no duplicate tab
    expect(store.active!.persistedId).toBe(7);
    expect(store.active!.sessionFlowId).toBe(-7);
  });
});

describe("editing flips dirty (active tab)", () => {
  it("edits dirty the active notebook only", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    expect(store.active!.dirty).toBe(false);
    const cellId = store.active!.cells[0].id;
    store.setCellCode(cellId, "print(1)");
    expect(store.active!.dirty).toBe(true);
    expect(store.active!.cells[0].code).toBe("print(1)");
  });

  it("setCellType to markdown clears output", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const cellId = store.active!.cells[0].id;
    store.active!.cells[0].output = { ...okExecResult, execution_count: 1 } as any;
    store.setCellType(cellId, "markdown");
    expect(store.active!.cells[0].cellType).toBe("markdown");
    expect(store.active!.cells[0].output).toBeNull();
  });
});

describe("run routing", () => {
  it("python cell with no kernel errors without calling the kernel", async () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    store.active!.kernelId = null;
    await store.runCell(store.active!.cells[0].id);
    expect(mocks.executeCell).not.toHaveBeenCalled();
    expect(store.active!.cells[0].execState).toBe("error");
    expect(store.active!.cells[0].output?.error).toMatch(/kernel/i);
  });

  it("python cell with a kernel sends the tab's negative session + hashed node_id", async () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setKernel("kern-1");
    const cell = store.active!.cells[0];
    await store.runCell(cell.id);
    expect(mocks.executeCell).toHaveBeenCalledTimes(1);
    const [kernelId, req] = mocks.executeCell.mock.calls[0];
    expect(kernelId).toBe("kern-1");
    expect(req.flow_id).toBe(store.active!.sessionFlowId);
    expect(req.flow_id).toBeLessThan(0);
    expect(req.node_id).toBe(cellNodeId(cell.id));
    expect(store.active!.cells[0].output?.execution_count).toBe(1);
  });

  it("markdown renders client-side, no API call", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const id = store.active!.cells[0].id;
    store.setCellType(id, "markdown");
    store.setCellCode(id, "# Title");
    store.runMarkdownCell(store.active!.cells[0]);
    expect(store.active!.cells[0].renderedHtml).toBe("<md># Title</md>");
    expect(store.active!.cells[0].editing).toBe(false);
    expect(mocks.executeCell).not.toHaveBeenCalled();
  });

  it("runAll renders markdown and skips python when there's no kernel", async () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    store.active!.cells = [];
    const py = store.addCell("python")!;
    const md = store.addCell("markdown")!;
    store.setCellCode(md.id, "## hi");
    store.active!.kernelId = null;
    await store.runAll();
    expect(mocks.executeCell).not.toHaveBeenCalled();
    expect(md.renderedHtml).toBe("<md>## hi</md>");
    expect(py.output).toBeNull();
  });

  it("executeCell rejection sets execState=error and surfaces the message", async () => {
    mocks.executeCell.mockRejectedValueOnce(new Error("kernel exploded"));
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setKernel("kern-1");
    const cell = store.active!.cells[0];
    await store.runCell(cell.id);
    expect(cell.execState).toBe("error");
    expect(cell.output?.error).toBe("kernel exploded");
  });

  it("a non-empty display_outputs result lands on cell.output", async () => {
    const display = [{ mime_type: "text/plain", data: "boom" }];
    mocks.executeCell.mockResolvedValueOnce({ ...okExecResult, display_outputs: display });
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setKernel("kern-1");
    const cell = store.active!.cells[0];
    await store.runCell(cell.id);
    expect(cell.output?.display_outputs).toEqual(display);
  });

  it("a python cell error mid-runAll halts the loop before a later cell", async () => {
    mocks.executeCell
      .mockResolvedValueOnce({ ...okExecResult, error: "boom" })
      .mockResolvedValueOnce(okExecResult);
    const store = useNotebookStore();
    store.ensureHydrated();
    store.active!.cells = [];
    const first = store.addCell("python")!;
    const second = store.addCell("python")!;
    store.setKernel("kern-1");
    await store.runAll();
    expect(mocks.executeCell).toHaveBeenCalledTimes(1); // stopped after the failing cell
    expect(first.execState).toBe("error");
    expect(second.execState).toBe("idle");
    expect(second.output).toBeNull();
  });

  it("double-invoking runCell on a running cell does not double-run (NB-03)", async () => {
    let resolve!: (v: unknown) => void;
    mocks.executeCell.mockReturnValueOnce(new Promise((r) => (resolve = r)));
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setKernel("kern-1");
    const cell = store.active!.cells[0];
    const first = store.runCell(cell.id);
    expect(cell.execState).toBe("running");
    await store.runCell(cell.id); // re-entrant call while running
    expect(mocks.executeCell).toHaveBeenCalledTimes(1);
    resolve(okExecResult);
    await first;
    expect(cell.execState).toBe("idle");
  });

  it("a mid-run tab switch keeps output on the originating tab (NB-04)", async () => {
    let resolve!: (v: unknown) => void;
    mocks.executeCell
      .mockReturnValueOnce(new Promise((r) => (resolve = r)))
      .mockResolvedValue(okExecResult);
    const store = useNotebookStore();
    store.ensureHydrated();
    const originTab = store.active!;
    originTab.cells = [];
    const c1 = store.addCell("python")!;
    const c2 = store.addCell("python")!;
    store.setKernel("kern-1");
    const otherTab = store.newTab(); // switches active away from origin
    store.setActiveTab(originTab.tabId);
    const run = store.runAll();
    store.setActiveTab(otherTab.tabId); // switch tabs mid-run
    resolve(okExecResult);
    await run;
    // First cell finished before the switch; the second must not have executed.
    expect(mocks.executeCell).toHaveBeenCalledTimes(1);
    expect(c1.output).not.toBeNull();
    expect(c2.output).toBeNull();
  });
});

describe("insertReadCell", () => {
  it("inserts a Python read cell", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const cell = store.insertReadCell("orders")!;
    expect(cell.cellType).toBe("python");
    expect(cell.code).toBe('df = flowfile_ctx.read_catalog_table("orders")\ndf');
  });
});

describe("persistence mapping + legacy coercion", () => {
  it("openNotebook coerces a legacy sql cell to python", async () => {
    mocks.nbGet.mockResolvedValue({
      id: 12,
      name: "legacy",
      description: null,
      namespace_id: null,
      default_kernel_id: null,
      owner_id: 1,
      created_at: "",
      updated_at: "",
      namespace_name: null,
      access: null,
      cells: [
        { id: "c1", type: "sql", source: "SELECT 1", metadata: {} },
        { id: "c2", type: "markdown", source: "# md", metadata: {} },
      ],
    });
    const store = useNotebookStore();
    store.ensureHydrated();
    await store.openNotebook(12);
    expect(store.active!.cells[0].cellType).toBe("python"); // sql -> python
    expect(store.active!.cells[0].code).toBe("SELECT 1");
    expect(store.active!.cells[1].cellType).toBe("markdown");
    expect(store.active!.cells[1].renderedHtml).toBe("<md># md</md>");
  });

  it("saveAs maps {cellType,code} to wire {type,source} and binds the tab", async () => {
    mocks.nbCreate.mockResolvedValue({
      id: 99,
      name: "saved",
      description: null,
      namespace_id: null,
      default_kernel_id: null,
      owner_id: 1,
      created_at: "",
      updated_at: "",
      namespace_name: null,
      access: null,
      cells: [],
    });
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setCellCode(store.active!.cells[0].id, "print(1)");
    await store.saveAs("saved", null);
    const payload = mocks.nbCreate.mock.calls[0][0];
    expect(payload.cells[0]).toMatchObject({ type: "python", source: "print(1)" });
    expect(payload.cells[0]).not.toHaveProperty("cellType");
    expect(store.active!.persistedId).toBe(99);
    expect(store.active!.sessionFlowId).toBe(-99);
    expect(store.active!.dirty).toBe(false);
  });

  it("save (update path) sends wire cells to NotebookApi.update and clears dirty", async () => {
    mocks.nbGet.mockResolvedValue({
      id: 21,
      name: "nb21",
      description: null,
      namespace_id: null,
      default_kernel_id: null,
      owner_id: 1,
      created_at: "",
      updated_at: "",
      namespace_name: null,
      access: null,
      cells: [{ id: "c1", type: "python", source: "x=1", metadata: {} }],
    });
    mocks.nbUpdate.mockResolvedValue(undefined);
    const store = useNotebookStore();
    store.ensureHydrated();
    await store.openNotebook(21);
    store.setCellCode(store.active!.cells[0].id, "print(2)");
    expect(store.active!.dirty).toBe(true);
    await store.save();
    expect(mocks.nbUpdate).toHaveBeenCalledTimes(1);
    const [id, payload] = mocks.nbUpdate.mock.calls[0];
    expect(id).toBe(21);
    expect(payload.cells[0]).toMatchObject({ type: "python", source: "print(2)" });
    expect(payload.cells[0]).not.toHaveProperty("cellType");
    expect(store.active!.dirty).toBe(false);
  });

  it("save does not surface a loadList failure as a save failure (dirty stays cleared)", async () => {
    mocks.nbGet.mockResolvedValue({
      id: 22,
      name: "nb22",
      description: null,
      namespace_id: null,
      default_kernel_id: null,
      owner_id: 1,
      created_at: "",
      updated_at: "",
      namespace_name: null,
      access: null,
      cells: [{ id: "c1", type: "python", source: "x=1", metadata: {} }],
    });
    mocks.nbUpdate.mockResolvedValue(undefined);
    mocks.nbList.mockRejectedValueOnce(new Error("network down"));
    const store = useNotebookStore();
    store.ensureHydrated();
    await store.openNotebook(22);
    store.setCellCode(store.active!.cells[0].id, "print(3)");
    await expect(store.save()).resolves.toBeUndefined();
    expect(store.active!.dirty).toBe(false);
  });

  it("deleteNotebook closes its tab and clears the namespace", async () => {
    mocks.nbGet.mockResolvedValue({
      id: 5,
      name: "nb5",
      description: null,
      namespace_id: null,
      default_kernel_id: null,
      owner_id: 1,
      created_at: "",
      updated_at: "",
      namespace_name: null,
      access: null,
      cells: [],
    });
    mocks.nbRemove.mockResolvedValue(undefined);
    const store = useNotebookStore();
    store.ensureHydrated();
    await store.openNotebook(5);
    store.setKernel("kx");
    await store.deleteNotebook(5);
    expect(mocks.nbRemove).toHaveBeenCalledWith(5);
    expect(mocks.clearNamespace).toHaveBeenCalledWith("kx", -5);
    expect(store.openNotebooks.some((n) => n.persistedId === 5)).toBe(false);
  });
});
