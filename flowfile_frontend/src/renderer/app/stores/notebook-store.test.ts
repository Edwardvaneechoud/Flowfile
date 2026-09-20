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
import { getCellHistory } from "../components/notebook/useCellHistory";

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

describe("structural cell actions", () => {
  /** Fresh tab with `n` python cells, dirty cleared so an assertion can see the next edit. */
  function withCells(n: number) {
    const store = useNotebookStore();
    store.ensureHydrated();
    store.active!.cells = [];
    const cells = Array.from({ length: n }, () => store.addCell("python")!);
    store.active!.dirty = false;
    return { store, cells };
  }

  it("addCell appends without an index and inserts after the given one", () => {
    const { store, cells } = withCells(2);
    const appended = store.addCell("python")!;
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[0].id, cells[1].id, appended.id]);
    const inserted = store.addCell("markdown", 0)!;
    expect(store.active!.cells.map((c) => c.id)).toEqual([
      cells[0].id,
      inserted.id,
      cells[1].id,
      appended.id,
    ]);
    expect(store.active!.dirty).toBe(true);
  });

  it("insertCellAt puts the cell at index 0", () => {
    const { store, cells } = withCells(2);
    const inserted = store.insertCellAt("python", 0)!;
    expect(store.active!.cells.map((c) => c.id)).toEqual([inserted.id, cells[0].id, cells[1].id]);
    expect(store.active!.dirty).toBe(true);
  });

  it("insertCellAt puts the cell at a middle index", () => {
    const { store, cells } = withCells(3);
    const inserted = store.insertCellAt("markdown", 2)!;
    expect(store.active!.cells.map((c) => c.id)).toEqual([
      cells[0].id,
      cells[1].id,
      inserted.id,
      cells[2].id,
    ]);
    expect(inserted.cellType).toBe("markdown");
  });

  it("removeCell refuses to delete the last remaining cell", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const only = store.active!.cells[0];
    expect(store.removeCell(only.id)).toBeNull();
    expect(store.active!.cells).toHaveLength(1);
    expect(store.active!.cells[0].id).toBe(only.id);
    expect(store.active!.dirty).toBe(false);
  });

  it("removeCell returns the next surviving cell, else the previous one", () => {
    const { store, cells } = withCells(3);
    expect(store.removeCell(cells[0].id)).toBe(cells[1].id);
    expect(store.removeCell(cells[2].id)).toBe(cells[1].id);
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[1].id]);
  });

  it("moveCellToIndex no-ops without dirtying the notebook", () => {
    const { store, cells } = withCells(3);
    expect(store.moveCellToIndex(cells[0].id, 0)).toBeNull();
    expect(store.moveCellToIndex("nope", 1)).toBeNull();
    expect(store.active!.dirty).toBe(false);
    expect(store.active!.cells.map((c) => c.id)).toEqual(cells.map((c) => c.id));
  });

  it("moveCellToIndex moves first to last and last to first", () => {
    const { store, cells } = withCells(3);
    expect(store.moveCellToIndex(cells[0].id, 2)).toEqual({ from: 0, to: 2, total: 3 });
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[1].id, cells[2].id, cells[0].id]);
    expect(store.moveCellToIndex(cells[0].id, 0)).toEqual({ from: 2, to: 0, total: 3 });
    expect(store.active!.cells.map((c) => c.id)).toEqual(cells.map((c) => c.id));
    expect(store.active!.dirty).toBe(true);
  });

  it("moveCell by direction still works", () => {
    const { store, cells } = withCells(3);
    expect(store.moveCell(cells[0].id, 1)).toEqual({ from: 0, to: 1, total: 3 });
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[1].id, cells[0].id, cells[2].id]);
  });

  it("duplicateCell copies code and type below the source with a fresh id and no output", () => {
    const { store, cells } = withCells(2);
    store.setCellCode(cells[0].id, "print(1)");
    store.active!.cells[0].output = { ...okExecResult, execution_count: 1 } as any;
    const copy = store.duplicateCell(cells[0].id)!;
    expect(copy.id).not.toBe(cells[0].id);
    expect(copy.code).toBe("print(1)");
    expect(copy.cellType).toBe("python");
    expect(copy.output).toBeNull();
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[0].id, copy.id, cells[1].id]);
  });

  it("undoCellAction reverses a delete (output included) and then the move before it", () => {
    const { store, cells } = withCells(3);
    store.active!.cells[1].output = { ...okExecResult, execution_count: 4 } as any;
    const outputRef = store.active!.cells[1].output;
    store.moveCellToIndex(cells[0].id, 2);
    store.removeCell(cells[1].id);
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[2].id, cells[0].id]);

    store.undoCellAction();
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[1].id, cells[2].id, cells[0].id]);
    expect(store.active!.cells[0].output).toBe(outputRef);

    store.undoCellAction();
    expect(store.active!.cells.map((c) => c.id)).toEqual(cells.map((c) => c.id));
  });

  it("redoCellAction re-applies an undone move", () => {
    const { store, cells } = withCells(3);
    store.moveCellToIndex(cells[0].id, 2);
    store.undoCellAction();
    expect(store.active!.cells.map((c) => c.id)).toEqual(cells.map((c) => c.id));
    store.redoCellAction();
    expect(store.active!.cells.map((c) => c.id)).toEqual([cells[1].id, cells[2].id, cells[0].id]);
  });

  it("undoCellAction with nothing recorded returns null", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    expect(store.undoCellAction()).toBeNull();
    expect(store.redoCellAction()).toBeNull();
  });

  it("closeTab disposes the tab's cell history", () => {
    const { store } = withCells(2);
    const tabId = store.active!.tabId;
    expect(getCellHistory(tabId).canUndo.value).toBe(true);
    store.closeTab(tabId);
    expect(getCellHistory(tabId).canUndo.value).toBe(false);
  });
});

describe("insertReadCell", () => {
  it("reuses a trailing blank Python cell with a typed ref chain", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const before = store.active!.cells.length;
    const cell = store.insertReadCell("Demo.market.fx_rates")!;
    expect(store.active!.cells.length).toBe(before);
    expect(cell.cellType).toBe("python");
    expect(cell.code).toBe(
      'df = flowfile_ctx.get_catalog("Demo").get_schema("market").get_table_ref("fx_rates").read()',
    );
  });

  it("inserts at the caret of the focused cell on its own line", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const cell = store.active!.cells[0];
    store.setCellCode(cell.id, "x = 1\ny = 2");
    store.setCellCursor(cell.id, 5);
    const target = store.insertReadCell("orders")!;
    expect(target.id).toBe(cell.id);
    expect(cell.code).toBe('x = 1\ndf = flowfile_ctx.read_catalog_table("orders")\ny = 2');
    expect(cell.cursor).toBe(5 + 1 + 'df = flowfile_ctx.read_catalog_table("orders")'.length);
    expect(store.active!.cells.length).toBe(1);
  });

  it("ignores a focused markdown cell and appends instead", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const md = store.addCell("markdown")!;
    store.setCellCursor(md.id, 0);
    const before = store.active!.cells.length;
    const cell = store.insertReadCell("orders")!;
    expect(cell.cellType).toBe("python");
    expect(store.active!.cells.length).toBe(before + 1);
    expect(store.active!.focusedCellId).toBe(cell.id);
  });

  it("still inserts at the caret after the cell's chrome took focus", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const cell = store.active!.cells[0];
    store.setCellCode(cell.id, "x = 1\ny = 2");
    store.setCellCursor(cell.id, 5);
    store.setFocusedCell(cell.id);
    expect(cell.cursor).toBe(5);
    store.insertReadCell("orders");
    expect(cell.code).toBe('x = 1\ndf = flowfile_ctx.read_catalog_table("orders")\ny = 2');
  });

  it("appends a new cell when the last one has code", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const last = store.active!.cells[store.active!.cells.length - 1];
    store.setCellCode(last.id, "x = 1");
    const before = store.active!.cells.length;
    const cell = store.insertReadCell("orders")!;
    expect(store.active!.cells.length).toBe(before + 1);
    expect(cell.code).toBe('df = flowfile_ctx.read_catalog_table("orders")');
  });
});

describe("setFocusedCell", () => {
  it("records focus, leaves the caret alone and ignores unknown ids", () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const first = store.active!.cells[0];
    const second = store.addCell("python")!;
    store.setCellCursor(first.id, 4);

    store.setFocusedCell(second.id);
    expect(store.active!.focusedCellId).toBe(second.id);
    expect(first.cursor).toBe(4);

    store.setFocusedCell("missing-cell");
    expect(store.active!.focusedCellId).toBe(second.id);
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
