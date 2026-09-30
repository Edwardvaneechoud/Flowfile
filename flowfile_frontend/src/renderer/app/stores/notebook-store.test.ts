// Unit tests for the multi-notebook store (kernel/markdown/notebook/flow APIs mocked, Pinia per-test).
import { setActivePinia, createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  executeCell: vi.fn(),
  clearNamespace: vi.fn(),
  nbList: vi.fn(),
  nbGet: vi.fn(),
  nbCreate: vi.fn(),
  nbUpdate: vi.fn(),
  nbRemove: vi.fn(),
  render: vi.fn(),
  push: vi.fn(),
  runLineage: vi.fn(),
  getFlowData: vi.fn(),
  getRunStatus: vi.fn(),
  runFlow: vi.fn(),
  triggerNodeFetch: vi.fn(),
  getFlowSettings: vi.fn(),
  getTableExample: vi.fn(),
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
    renderFlowNotebook: mocks.render,
    pushFlowNotebook: mocks.push,
    runLineage: mocks.runLineage,
  },
}));
vi.mock("../api/flow.api", () => ({
  FlowApi: {
    getFlowData: mocks.getFlowData,
    getRunStatus: mocks.getRunStatus,
    runFlow: mocks.runFlow,
    triggerNodeFetch: mocks.triggerNodeFetch,
    getFlowSettings: mocks.getFlowSettings,
  },
}));
vi.mock("../api/node.api", () => ({
  NodeApi: { getTableExample: mocks.getTableExample },
}));
vi.mock("../features/ai/markdown", () => ({
  sanitiseMarkdown: (s: string) => `<md>${s}</md>`,
}));

import {
  CANVAS_CHANGED,
  SYNC_NEEDS_ADMIN,
  useNotebookStore,
  cellNodeId,
  flowCellKind,
  flowCellSyncState,
  flowNeedsSync,
  flowPushBody,
  nodePreviewDisplay,
  notOnCanvasText,
  registerFlowNotebookHooks,
  syncErrorFor,
  type FlowNotebookHooks,
} from "./notebook-store";
import { TABLE_MIME } from "../components/nodes/node-types/elements/pythonScript/notebookDisplay";
import { getCellHistory } from "../components/notebook/useCellHistory";
import { ownerIdForNotebook } from "../components/notebook/editorViews";
import { batchProgress, cellRuntime, getOwner } from "../components/notebook/notebookRuntimeState";

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

  it("double-invoking runCell on a running cell does not double-run", async () => {
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

  it("a mid-run tab switch keeps the batch and its results on the origin tab", async () => {
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
    // The batch is bound to the captured notebook, so it runs to the end on the origin tab.
    expect(mocks.executeCell).toHaveBeenCalledTimes(2);
    expect(c1.output).not.toBeNull();
    expect(c2.output).not.toBeNull();
    expect(otherTab.cells.every((c) => c.output === null)).toBe(true);
  });
});

describe("runCell result", () => {
  // Earlier describes leave unconsumed *Once entries queued on the shared mock.
  beforeEach(() => {
    mocks.executeCell.mockReset();
    mocks.executeCell.mockResolvedValue(okExecResult);
  });

  it("resolves true for a markdown cell", async () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    const id = store.active!.cells[0].id;
    store.setCellType(id, "markdown");
    store.setCellCode(id, "# Title");
    await expect(store.runCell(id)).resolves.toBe(true);
    expect(store.active!.cells[0].renderedHtml).toBe("<md># Title</md>");
  });

  it("resolves false with no kernel and still writes the error output", async () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    store.active!.kernelId = null;
    const cell = store.active!.cells[0];
    await expect(store.runCell(cell.id)).resolves.toBe(false);
    expect(cell.execState).toBe("error");
    expect(cell.output?.error).toMatch(/kernel/i);
  });

  it("resolves false when the kernel reports success:false, output still written", async () => {
    mocks.executeCell.mockResolvedValueOnce({ ...okExecResult, success: false, error: "boom" });
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setKernel("kern-1");
    const cell = store.active!.cells[0];
    await expect(store.runCell(cell.id)).resolves.toBe(false);
    expect(cell.execState).toBe("error");
    expect(cell.output?.error).toBe("boom");
  });

  it("resolves false when executeCell rejects", async () => {
    mocks.executeCell.mockRejectedValueOnce(new Error("kernel exploded"));
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setKernel("kern-1");
    const cell = store.active!.cells[0];
    await expect(store.runCell(cell.id)).resolves.toBe(false);
    expect(cell.output?.error).toBe("kernel exploded");
  });

  it("resolves true after a successful execute", async () => {
    const store = useNotebookStore();
    store.ensureHydrated();
    store.setKernel("kern-1");
    const cell = store.active!.cells[0];
    await expect(store.runCell(cell.id)).resolves.toBe(true);
    expect(cell.execState).toBe("idle");
  });
});

describe("execution identity and staleness", () => {
  // Earlier describes leave unconsumed *Once entries queued on the shared mock.
  beforeEach(() => {
    mocks.executeCell.mockReset();
    mocks.executeCell.mockResolvedValue(okExecResult);
    mocks.clearNamespace.mockReset();
    mocks.clearNamespace.mockResolvedValue(undefined);
  });

  /** Fresh tab with `n` python cells and a kernel, plus its runtime owner id. */
  function withKernel(n = 1) {
    const store = useNotebookStore();
    store.ensureHydrated();
    store.active!.cells = [];
    const cells = Array.from({ length: n }, () => store.addCell("python")!);
    store.setKernel("kern-1");
    return { store, cells, owner: ownerIdForNotebook(store.active!.tabId) };
  }

  it("a second runAll while one is active is refused, so each cell runs once", async () => {
    let resolve!: (v: unknown) => void;
    mocks.executeCell
      .mockReturnValueOnce(new Promise((r) => (resolve = r)))
      .mockResolvedValue(okExecResult);
    const { store, owner } = withKernel(2);
    const first = store.runAll();
    const batchId = batchProgress(owner)!.id;
    const second = store.runAll(); // duplicate click while the batch is active
    expect(batchProgress(owner)!.id).toBe(batchId); // refused, not restarted
    await second;
    resolve(okExecResult);
    await first;
    expect(mocks.executeCell).toHaveBeenCalledTimes(2);
  });

  it("an edit during a run keeps the returned output and labels it code-changed", async () => {
    let resolve!: (v: unknown) => void;
    mocks.executeCell.mockReturnValueOnce(new Promise((r) => (resolve = r)));
    const { store, cells, owner } = withKernel();
    const run = store.runCell(cells[0].id);
    store.setCellCode(cells[0].id, "print('edited')");
    resolve(okExecResult);
    await run;
    expect(cells[0].output).not.toBeNull();
    expect(cellRuntime(owner, cells[0].id)!.staleReason).toBe("code-changed");
  });

  it("editing A marks A and B; rerunning A clears only A", async () => {
    const { store, cells, owner } = withKernel(2);
    const [a, b] = cells;
    await store.runCell(a.id);
    await store.runCell(b.id);
    expect(cellRuntime(owner, a.id)!.staleReason).toBeNull();
    expect(cellRuntime(owner, b.id)!.staleReason).toBeNull();

    store.setCellCode(a.id, "x = 2");
    expect(cellRuntime(owner, a.id)!.staleReason).toBe("code-changed");
    expect(cellRuntime(owner, b.id)!.staleReason).toBe("upstream-changed");

    await store.runCell(a.id);
    expect(cellRuntime(owner, a.id)!.staleReason).toBeNull();
    expect(cellRuntime(owner, b.id)!.staleReason).toBe("upstream-changed");
  });

  it("clearOutputs also drops the stale marks", async () => {
    const { store, cells, owner } = withKernel();
    await store.runCell(cells[0].id);
    store.setCellCode(cells[0].id, "x = 2");
    expect(cellRuntime(owner, cells[0].id)!.staleReason).toBe("code-changed");
    store.clearOutputs();
    expect(cells[0].output).toBeNull();
    expect(cellRuntime(owner, cells[0].id)!.staleReason).toBeNull();
  });

  it("a failed resetSession rejects, keeps outputs and leaves the session epoch alone", async () => {
    const { store, cells, owner } = withKernel();
    await store.runCell(cells[0].id);
    const epoch = getOwner(owner)!.sessionEpoch;
    mocks.clearNamespace.mockRejectedValueOnce(new Error("namespace busy"));
    await expect(store.resetSession()).rejects.toThrow("namespace busy");
    expect(cells[0].output).not.toBeNull();
    expect(getOwner(owner)!.sessionEpoch).toBe(epoch);
  });

  it("a successful resetSession clears outputs and bumps the session epoch", async () => {
    const { store, cells, owner } = withKernel();
    await store.runCell(cells[0].id);
    const epoch = getOwner(owner)!.sessionEpoch;
    await store.resetSession();
    expect(mocks.clearNamespace).toHaveBeenCalledWith("kern-1", store.active!.sessionFlowId);
    expect(cells[0].output).toBeNull();
    expect(store.active!.executionCount).toBe(0);
    expect(getOwner(owner)!.sessionEpoch).toBe(epoch + 1);
    expect(cellRuntime(owner, cells[0].id)!.staleReason).toBeNull();
  });

  it("setKernel ignores the same id and marks retained results previous-session", async () => {
    const { store, cells, owner } = withKernel();
    await store.runCell(cells[0].id);
    const epoch = getOwner(owner)!.sessionEpoch;

    store.setKernel("kern-1");
    expect(getOwner(owner)!.sessionEpoch).toBe(epoch);
    expect(cellRuntime(owner, cells[0].id)!.staleReason).toBeNull();

    store.setKernel("kern-2");
    expect(getOwner(owner)!.sessionEpoch).toBe(epoch + 1);
    expect(cellRuntime(owner, cells[0].id)!.staleReason).toBe("previous-session");
    expect(cells[0].output).not.toBeNull();
  });

  it("a response arriving after a kernel switch never writes its output", async () => {
    let resolve!: (v: unknown) => void;
    mocks.executeCell.mockReturnValueOnce(new Promise((r) => (resolve = r)));
    const { store, cells } = withKernel();
    const run = store.runCell(cells[0].id);
    store.setKernel("kern-2");
    resolve(okExecResult);
    await expect(run).resolves.toBe(false);
    expect(cells[0].output).toBeNull();
    // The discarded run must release the cell, or it stays "running" and refuses every later run.
    expect(cells[0].execState).toBe("idle");
    const calls = mocks.executeCell.mock.calls.length;
    await expect(store.runCell(cells[0].id)).resolves.toBe(true);
    expect(mocks.executeCell.mock.calls.length).toBe(calls + 1);
    expect(cells[0].output).not.toBeNull();
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

describe("flow notebook", () => {
  const cell = (id: number, code: string, extra = {}) => ({
    cell_id: `node-${id}`,
    node_ids: [id],
    kind: "node",
    code,
    status: "code",
    reason: null,
    ...extra,
  });
  const rendering = (fingerprint: string, cells: unknown[]) => ({
    cells,
    warnings: [],
    code_fingerprint: fingerprint,
  });

  it("opens an ephemeral tab from the rendering with no kernel, never persisted", async () => {
    mocks.render.mockResolvedValue(
      rendering("f1", [
        cell(1, "a = fl.canvas_node(1)", { status: "placeholder", reason: "Not configured" }),
        cell(2, "b = a.filter(x)"),
      ]),
    );
    mocks.getFlowData.mockResolvedValue({ node_inputs: [{ id: 1 }, { id: 2 }] });
    mocks.getRunStatus
      .mockReset()
      .mockResolvedValueOnce({ start_time: "t0", is_running: false, run_type: "full_run" })
      .mockResolvedValue({
        start_time: "t1",
        is_running: false,
        run_type: "fetch_one",
        success: true,
        node_step_result: [],
      });
    mocks.getTableExample.mockResolvedValue({ columns: [], data: [], has_example_data: false });
    const store = useNotebookStore();
    const nb = await store.openFlowNotebook(7, "flow");
    expect(nb.kernelId).toBeNull();
    expect(nb.sessionFlowId).toBe(7);
    expect(nb.cells.map((c) => [c.id, c.code])).toEqual([
      ["node-1", "# Not configured\na = fl.canvas_node(1)"],
      ["node-2", "b = a.filter(x)"],
    ]);
    expect(store._snapshot().openNotebooks.some((n) => n.tabId === nb.tabId)).toBe(false);
    expect((await store.openFlowNotebook(7, "flow")).tabId).toBe(nb.tabId);
    await store.runCell("node-2");
    expect(mocks.executeCell).not.toHaveBeenCalled();
    expect(mocks.runLineage).toHaveBeenCalledWith(7, 2);
  });

  it("refresh keeps edited cells, updates the rest, inserts and removes in render order", async () => {
    mocks.render.mockResolvedValue(
      rendering("f1", [cell(1, "a = 1"), cell(2, "b = a"), cell(3, "c = b")]),
    );
    const store = useNotebookStore();
    const nb = await store.openFlowNotebook(7, "flow");
    store.setCellCode("node-2", "b = a * 2");
    const extra = store.addCell("python")!;
    store.setCellCode(extra.id, "e = b");
    mocks.render.mockResolvedValue(
      rendering("f2", [cell(1, "a = 10"), cell(4, "d = a"), cell(2, "b = a + 1")]),
    );
    await store.refreshFlowNotebook(7);
    expect(nb.cells.map((c) => [c.id, c.code])).toEqual([
      ["node-1", "a = 10"],
      ["node-4", "d = a"],
      ["node-2", "b = a * 2"],
      [extra.id, "e = b"],
    ]);
    expect(flowNeedsSync(nb)).toBe(true);

    const body = flowPushBody(
      nb,
      new Map([
        [1, "manual_input"],
        [2, "filter"],
        [4, "select"],
      ]),
      9,
    );
    expect(body.changed_cell_ids).toEqual(["node-2", extra.id]);
    expect(body.provenance).toEqual({
      "node-1": [["manual_input", 1]],
      "node-4": [["select", 4]],
      "node-2": [["filter", 2]],
    });
    expect(body.code_fingerprint).toBe("f2");
    expect(body.client_max_node_id).toBe(9);

    store.markFlowPushed(
      nb,
      {
        history: {} as never,
        code_fingerprint: "f2b",
        max_node_id: 4,
        node_ids_by_cell: {},
        warnings: [],
        applied: true,
        deletions: [],
        parameter_changes: false,
      },
      nb.cells.map((c) => [c.id, c.code]),
    );
    mocks.render.mockResolvedValue(
      rendering("f3", [cell(1, "a = 10"), cell(4, "d = a"), cell(2, "b = a * 2  # canonical")]),
    );
    await store.refreshFlowNotebook(7);
    expect(nb.cells.map((c) => c.code)).toEqual(["a = 10", "d = a", "b = a * 2  # canonical"]);
    expect(nb.dirty).toBe(false);
  });

  it("a cell spanning several nodes carries them all, in order", async () => {
    const fused = (ids: number[], code: string) => ({
      ...cell(ids[0], code),
      cell_id: `cell-${ids[0]}`,
      node_ids: ids,
    });
    mocks.render.mockResolvedValue(
      rendering("f1", [fused([1, 2], "a = 1\nb = a"), cell(3, "c = b")]),
    );
    mocks.getFlowData.mockResolvedValue({ node_inputs: [{ id: 1 }, { id: 2 }, { id: 3 }] });
    mocks.getRunStatus
      .mockReset()
      .mockResolvedValueOnce({ start_time: "t0", is_running: false, run_type: "full_run" })
      .mockResolvedValue({
        start_time: "t1",
        is_running: false,
        run_type: "fetch_one",
        success: true,
        node_step_result: [],
      });
    mocks.getTableExample.mockResolvedValue({ columns: [], data: [], has_example_data: false });
    const store = useNotebookStore();
    const nb = await store.openFlowNotebook(7, "flow");
    await store.runCell("cell-1");
    expect(mocks.runLineage).toHaveBeenLastCalledWith(7, 2);
    mocks.render.mockResolvedValue(
      rendering("f2", [fused([1, 4, 2], "a = 1\nb = a"), cell(3, "c = b")]),
    );
    await store.refreshFlowNotebook(7);
    expect(nb.dirty).toBe(false);
    const types = new Map([
      [1, "manual_input"],
      [2, "filter"],
      [3, "select"],
      [4, "sort"],
    ]);
    expect(flowPushBody(nb, types, 4).provenance).toEqual({
      "cell-1": [
        ["manual_input", 1],
        ["sort", 4],
        ["filter", 2],
      ],
      "node-3": [["select", 3]],
    });
  });

  it("an unchanged fingerprint leaves the cells alone", async () => {
    mocks.render.mockResolvedValue(rendering("f1", [cell(1, "a = 1")]));
    const store = useNotebookStore();
    const nb = await store.openFlowNotebook(7, "flow");
    const before = nb.cells;
    await store.refreshFlowNotebook(7);
    expect(nb.cells).toBe(before);
  });

  it("a flow switch never shows the other flow's cells while the render is out", async () => {
    const pending = () => {
      let resolve!: (v: unknown) => void;
      mocks.render.mockReturnValueOnce(new Promise((r) => (resolve = r)));
      return (v: unknown) => resolve(v);
    };
    mocks.render.mockResolvedValueOnce(rendering("a1", [cell(1, "a = 1")]));
    const store = useNotebookStore();
    const a = await store.openFlowNotebook(7, "flow 7");
    store.setCellCode("node-1", "a = 2");

    const renderB = pending();
    const openB = store.openFlowNotebook(8, "flow 8");
    expect(store.active!.flowId).toBe(8);
    expect(store.active!.cells).toEqual([]);
    renderB(rendering("b1", [cell(5, "b = 1")]));
    const b = await openB;
    expect(store.active!.cells.map((c) => c.code)).toEqual(["b = 1"]);

    const renderA = pending();
    const openA = store.openFlowNotebook(7, "flow 7");
    expect(store.active!.tabId).toBe(a.tabId);
    expect(store.active!.cells.map((c) => c.code)).toEqual(["a = 2"]);

    const renderB2 = pending();
    const openB2 = store.openFlowNotebook(8, "flow 8");
    expect(store.active!.tabId).toBe(b.tabId);
    renderA(rendering("a1", [cell(1, "a = 1")]));
    await openA;
    expect(store.active!.tabId).toBe(b.tabId);
    expect(store.active!.cells.map((c) => c.code)).toEqual(["b = 1"]);
    expect(a.cells.map((c) => c.code)).toEqual(["a = 2"]);
    renderB2(rendering("b1", [cell(5, "b = 1")]));
    await openB2;
    expect(store.active!.tabId).toBe(b.tabId);
  });
});

describe("flow notebook run", () => {
  const FLOW = 7;
  const importsCell = {
    cell_id: "imports",
    node_ids: [],
    kind: "imports",
    code: "import flowfile as fl",
    status: "code",
    reason: null,
  };
  const paramsCell = {
    cell_id: "parameters",
    node_ids: [],
    kind: "parameters",
    code: 'n = fl.add_flow_parameter(flow, fl.Parameter("n", default=8, type="integer"))',
    status: "code",
    reason: null,
  };
  const nodeCell = (ids: number[], code: string) => ({
    cell_id: `cell-${ids[0]}`,
    node_ids: ids,
    kind: "node",
    code,
    status: "code",
    reason: null,
  });
  const rendered = (fingerprint: string, cells: unknown[] = defaultCells()) => ({
    cells,
    warnings: [],
    var_by_node: {},
    code_fingerprint: fingerprint,
  });
  const defaultCells = () => [
    importsCell,
    paramsCell,
    nodeCell([1], 'source_1 = fl.read_csv("a.csv")'),
    nodeCell([2, 3], "filtered_2 = source_1.filter(fl.col('q') >= n).head(5)"),
  ];
  const pushed = (extra = {}) => ({
    history: { flow_id: FLOW },
    code_fingerprint: "f2",
    max_node_id: 3,
    node_ids_by_cell: {},
    warnings: [],
    applied: true,
    deletions: [],
    parameter_changes: false,
    ...extra,
  });
  /** What core answers a push it holds for review: nothing applied, the fingerprint unchanged. */
  const held = (extra = {}) => pushed({ applied: false, code_fingerprint: "f1", ...extra });
  const example = (extra = {}) => ({
    node_id: 3,
    number_of_records: null,
    number_of_columns: 1,
    name: "3",
    table_schema: [],
    columns: ["q"],
    data: [{ q: 8 }, { q: 9 }],
    has_example_data: true,
    has_run_with_current_setup: true,
    ...extra,
  });
  const httpError = (status: number, detail: unknown) =>
    Object.assign(new Error(`Request failed with status code ${status}`), {
      response: { status, data: { detail } },
    });
  const finishedRun = (extra = {}) => ({
    flow_id: FLOW,
    start_time: "t1",
    end_time: "t1",
    success: true,
    is_running: false,
    run_type: "fetch_one",
    node_step_result: [1, 2, 3].map((node_id) => ({ node_id, success: true })),
    ...extra,
  });

  let hooks: FlowNotebookHooks & { [K in keyof FlowNotebookHooks]: ReturnType<typeof vi.fn> };
  let unregister: () => void;

  beforeEach(() => {
    for (const m of [
      mocks.render,
      mocks.push,
      mocks.runLineage,
      mocks.getFlowData,
      mocks.getRunStatus,
      mocks.runFlow,
      mocks.triggerNodeFetch,
      mocks.getFlowSettings,
      mocks.getTableExample,
    ]) {
      m.mockReset();
    }
    mocks.render.mockResolvedValue(rendered("f1"));
    mocks.getFlowData.mockResolvedValue({
      node_inputs: [1, 2, 3].map((id) => ({ id, item: "filter" })),
    });
    mocks.getRunStatus
      .mockResolvedValueOnce({ start_time: "t0", is_running: false, run_type: "full_run" })
      .mockResolvedValue(finishedRun());
    mocks.runLineage.mockResolvedValue({ message: "Data started", flow_id: FLOW, node_ids: [] });
    mocks.runFlow.mockResolvedValue(undefined);
    mocks.getTableExample.mockResolvedValue(example());
    mocks.getFlowSettings.mockResolvedValue({
      flow_id: FLOW,
      name: "flow",
      parameters: [{ name: "n", default_value: "10", description: "", type: "integer" }],
    });
    mocks.push.mockResolvedValue(pushed());
    hooks = {
      prepare: vi.fn(async () => true),
      clientMaxNodeId: vi.fn(() => 3),
      confirm: vi.fn(async () => true),
      pushed: vi.fn(),
      runStarted: vi.fn(),
      runEnded: vi.fn(),
    } as typeof hooks;
    unregister = registerFlowNotebookHooks(FLOW, hooks);
  });

  afterEach(() => unregister());

  async function openFlow() {
    const store = useNotebookStore();
    const nb = await store.openFlowNotebook(FLOW, "flow");
    return { store, nb };
  }

  const tableOf = (output: { display_outputs: { mime_type: string; data: string }[] }) => {
    expect(output.display_outputs[0].mime_type).toBe(TABLE_MIME);
    return JSON.parse(output.display_outputs[0].data);
  };

  it("runs an unedited node cell's last node on the canvas without syncing", async () => {
    const { store, nb } = await openFlow();
    expect(await store.runCell("cell-2")).toBe(true);
    expect(mocks.push).not.toHaveBeenCalled();
    expect(mocks.runLineage).toHaveBeenCalledWith(FLOW, 3);
    expect(mocks.getTableExample).toHaveBeenCalledWith(FLOW, 3);
    expect(hooks.runStarted).toHaveBeenCalledTimes(1);
    expect(hooks.runEnded).toHaveBeenCalledWith(expect.objectContaining({ start_time: "t1" }));
    const cell = nb.cells.find((c) => c.id === "cell-2")!;
    expect(cell.execState).toBe("idle");
    expect(flowCellSyncState(nb, cell)).toBe("synced");
  });

  it("shows the node's rows as a table and never a number for an unknown row count", async () => {
    const { store, nb } = await openFlow();
    await store.runCell("cell-2");
    const output = nb.cells.find((c) => c.id === "cell-2")!.output!;
    expect(output.error).toBeNull();
    const payload = tableOf(output);
    expect(payload.columns).toEqual(["q"]);
    expect(payload.data).toEqual([{ q: 8 }, { q: 9 }]);
    expect(payload.total_rows).toBe(2);
    expect(payload.truncated).toBe(false);
    expect(output.display_outputs[0].title).toContain("up to 100 rows");
    expect(output.display_outputs[0].title).toContain("unknown");
  });

  it("marks a preview truncated when the known row count exceeds the rows sent", () => {
    const display = nodePreviewDisplay(example({ number_of_records: 1234 }) as never, 3);
    const payload = JSON.parse(display.data);
    expect(payload.total_rows).toBe(1234);
    expect(payload.loaded_rows).toBe(2);
    expect(payload.truncated).toBe(true);
    expect(display.title).not.toContain("unknown");
  });

  it("syncs first when a cell is edited, then runs the node the push attributed", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "filtered_2 = source_1.filter(fl.col('q') >= n)");
    mocks.push.mockResolvedValue(pushed({ node_ids_by_cell: { "cell-2": [2, 4] } }));
    mocks.getFlowData.mockResolvedValue({
      node_inputs: [1, 2, 4].map((id) => ({ id, item: "filter" })),
    });
    mocks.getRunStatus.mockResolvedValue(
      finishedRun({ node_step_result: [1, 2, 4].map((node_id) => ({ node_id, success: true })) }),
    );
    expect(await store.runCell("cell-2")).toBe(true);
    expect(mocks.push).toHaveBeenCalledTimes(1);
    const body = mocks.push.mock.calls[0][0];
    expect(body.changed_cell_ids).toEqual(["cell-2"]);
    expect(body.code_fingerprint).toBe("f1");
    expect(body.trigger).toBe("run");
    expect(hooks.confirm).not.toHaveBeenCalled();
    expect(hooks.pushed).toHaveBeenCalledWith(expect.objectContaining({ code_fingerprint: "f2" }));
    expect(mocks.runLineage).toHaveBeenCalledWith(FLOW, 4);
    expect(nb.fingerprint).toBe("f2");
    expect(nb.dirty).toBe(false);
    expect(flowCellSyncState(nb, nb.cells.find((c) => c.id === "cell-2")!)).toBe("synced");
  });

  it("asks before a run's sync deletes canvas nodes, and a cancel stops the run", async () => {
    const { store } = await openFlow();
    store.setCellCode("cell-2", "filtered_2 = source_1");
    mocks.push.mockResolvedValue(held({ deletions: [3] }));
    hooks.confirm.mockResolvedValue(false);
    expect(await store.runCell("cell-2")).toBe(false);
    expect(hooks.confirm).toHaveBeenCalledWith(expect.objectContaining({ deletions: [3] }), "run");
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(mocks.push.mock.calls[0][0].trigger).toBe("run");
    expect(hooks.pushed).not.toHaveBeenCalled();
    expect(mocks.runLineage).not.toHaveBeenCalled();
  });

  it("reports a node the sync removed as not on the canvas without reading its data", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-1", 'source_1 = fl.read_csv("b.csv")');
    mocks.getFlowData
      .mockResolvedValueOnce({ node_inputs: [1, 2, 3].map((id) => ({ id, item: "filter" })) })
      .mockResolvedValue({ node_inputs: [1, 2].map((id) => ({ id, item: "filter" })) });
    mocks.push.mockResolvedValueOnce(held({ deletions: [3] }));
    expect(await store.runCell("cell-2")).toBe(true);
    expect(hooks.confirm).toHaveBeenCalledTimes(1);
    expect(mocks.push).toHaveBeenCalledTimes(2);
    expect(mocks.push.mock.calls[1][0]).not.toHaveProperty("trigger");
    expect(mocks.runLineage).not.toHaveBeenCalled();
    expect(mocks.getTableExample).not.toHaveBeenCalled();
    const output = nb.cells.find((c) => c.id === "cell-2")!.output!;
    expect(output.display_outputs[0]).toMatchObject({
      mime_type: "text/plain",
      data: notOnCanvasText(3),
    });
  });

  it("lists the flow parameters with type and default, read after the sync", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("parameters", paramsCell.code.replace("default=8", "default=10"));
    expect(await store.runCell("parameters")).toBe(true);
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(mocks.push.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.getFlowSettings.mock.invocationCallOrder[0],
    );
    expect(mocks.runLineage).not.toHaveBeenCalled();
    const payload = tableOf(nb.cells.find((c) => c.id === "parameters")!.output!);
    expect(payload.columns).toEqual(["name", "type", "default"]);
    expect(payload.data).toEqual([{ name: "n", type: "integer", default: "10" }]);
  });

  it("imports and plain cells show no output and stay synced", async () => {
    const { store, nb } = await openFlow();
    expect(await store.runCell("imports")).toBe(true);
    const imports = nb.cells.find((c) => c.id === "imports")!;
    expect(imports.output).toBeNull();
    expect(flowCellSyncState(nb, imports)).toBe("synced");
    expect(flowCellKind(nb, "imports")).toBe("imports");
    expect(mocks.push).not.toHaveBeenCalled();
    expect(mocks.runLineage).not.toHaveBeenCalled();
    expect(mocks.getFlowSettings).not.toHaveBeenCalled();

    const plain = store.addCell("python")!;
    store.setCellCode(plain.id, "threshold = 8");
    expect(flowCellSyncState(nb, plain)).toBe("edited");
    mocks.push.mockResolvedValue(pushed({ node_ids_by_cell: { [plain.id]: [] } }));
    expect(await store.runCell(plain.id)).toBe(true);
    expect(flowCellKind(nb, plain.id)).toBe("plain");
    expect(plain.output).toBeNull();
    expect(flowCellSyncState(nb, plain)).toBe("synced");
    expect(mocks.runLineage).not.toHaveBeenCalled();
  });

  it("puts a 422 on its cell and line, blocking the sync and the run", async () => {
    const { store, nb } = await openFlow();
    const bad = "filtered_2 = source_1.filter(fl.col('q') >= n)\nprint(filtered_2)";
    store.setCellCode("cell-2", bad);
    const detail = {
      message: "print is not part of the notebook's flow code; this needs a kernel",
      cell_id: "cell-2",
      line: 2,
      kind: "needs_kernel",
    };
    mocks.push.mockRejectedValue(httpError(422, detail));
    expect(await store.runCell("parameters")).toBe(false);
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(hooks.pushed).not.toHaveBeenCalled();
    expect(mocks.getFlowSettings).not.toHaveBeenCalled();
    const failing = nb.cells.find((c) => c.id === "cell-2")!;
    expect(nb.syncError).toEqual({ ...detail, code: bad });
    expect(syncErrorFor(nb, failing)).toMatchObject({ line: 2, kind: "needs_kernel" });
    expect(flowCellSyncState(nb, failing)).toBe("error");
    expect(failing.execState).toBe("error");
    expect(failing.output!.error).toBe(`Line 2: ${detail.message}`);
    const params = nb.cells.find((c) => c.id === "parameters")!;
    expect(params.execState).toBe("idle");
    expect(flowCellSyncState(nb, params)).toBe("synced");

    store.setCellCode("cell-2", "filtered_2 = source_1");
    expect(flowCellSyncState(nb, failing)).toBe("edited");
    expect(syncErrorFor(nb, failing)).toBeNull();
  });

  it("clears the previous refusal when the next sync starts", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "print(1)");
    mocks.push.mockRejectedValueOnce(
      httpError(422, {
        message: "needs a kernel",
        cell_id: "cell-2",
        line: 1,
        kind: "needs_kernel",
      }),
    );
    await store.runCell("cell-2");
    expect(nb.syncError).not.toBeNull();
    store.setCellCode("cell-2", "filtered_2 = source_1");
    mocks.push.mockResolvedValue(pushed({ node_ids_by_cell: { "cell-2": [2, 3] } }));
    expect(await store.runCell("cell-2")).toBe(true);
    expect(nb.syncError).toBeNull();
    expect(nb.cells.find((c) => c.id === "cell-2")!.output!.error).toBeNull();
  });

  it("a sync that stops at another cell drops the running cell's old refusal", async () => {
    const { store, nb } = await openFlow();
    const refusal = (cellId: string, message: string) =>
      httpError(422, { message, cell_id: cellId, line: 1, kind: "needs_kernel" });
    store.setCellCode("cell-2", "print(1)");
    mocks.push.mockRejectedValueOnce(refusal("cell-2", "print needs a kernel"));
    await store.runCell("cell-2");
    store.setCellCode("cell-2", "filtered_2 = source_1");
    store.setCellCode("cell-1", "import os");
    mocks.push.mockRejectedValueOnce(refusal("cell-1", "import os needs a kernel"));
    expect(await store.runCell("cell-2")).toBe(false);
    const byId = (id: string) => nb.cells.find((c) => c.id === id)!;
    expect(byId("cell-2").output).toBeNull();
    expect(byId("cell-2").execState).toBe("idle");
    expect(byId("cell-1").output!.error).toBe("Line 1: import os needs a kernel");
    expect(flowCellSyncState(nb, byId("cell-1"))).toBe("error");
  });

  it("says so and does not run when a 422 names no cell", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "x = 1");
    mocks.push.mockRejectedValue(
      httpError(422, {
        message: "The notebook is too large",
        cell_id: null,
        line: null,
        kind: "error",
      }),
    );
    expect(await store.runCell("cell-2")).toBe(false);
    expect(nb.notice).toEqual({ tone: "error", message: "The notebook is too large" });
    expect(mocks.runLineage).not.toHaveBeenCalled();
  });

  it("re-renders on a 409 and says the canvas changed", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "filtered_2 = source_1");
    mocks.push.mockRejectedValue(
      httpError(409, {
        message: "The canvas changed since these cells were rendered.",
        code_fingerprint: "f9",
      }),
    );
    mocks.render.mockResolvedValueOnce(rendered("f1")).mockResolvedValue(rendered("f9"));
    expect(await store.runCell("cell-2")).toBe(false);
    expect(mocks.render).toHaveBeenCalledTimes(3);
    expect(nb.fingerprint).toBe("f9");
    expect(nb.notice).toEqual({ tone: "warning", message: CANVAS_CHANGED });
    expect(nb.cells.find((c) => c.id === "cell-2")!.code).toBe("filtered_2 = source_1");
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(hooks.pushed).not.toHaveBeenCalled();
    expect(mocks.runLineage).not.toHaveBeenCalled();
  });

  it("flags a 403 as needing an admin and keeps the cells editable", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "filtered_2 = source_1");
    mocks.push.mockRejectedValue(httpError(403, "Admin privileges required"));
    expect(await store.runCell("cell-2")).toBe(false);
    expect(nb.syncForbidden).toBe(true);
    expect(nb.notice).toEqual({ tone: "warning", message: SYNC_NEEDS_ADMIN });
    expect(mocks.runLineage).not.toHaveBeenCalled();
    const cell = nb.cells.find((c) => c.id === "cell-2")!;
    expect(batchProgress(ownerIdForNotebook(nb.tabId))).toBeNull();
    expect(flowCellSyncState(nb, cell)).toBe("edited");
    expect(cell.execState).toBe("idle");
    expect(await store.syncFlowNotebook()).toBe("forbidden");
  });

  it("run all after a refused sync starts no run", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "filtered_2 = source_1");
    mocks.push.mockRejectedValue(httpError(403, "Admin privileges required"));
    await store.runAll();
    expect(nb.syncForbidden).toBe(true);
    expect(mocks.runFlow).not.toHaveBeenCalled();
    expect(hooks.runStarted).not.toHaveBeenCalled();
  });

  it("shows a failed lineage step instead of rows", async () => {
    const { store, nb } = await openFlow();
    mocks.getRunStatus
      .mockReset()
      .mockResolvedValueOnce({ start_time: "t0", is_running: false, run_type: "full_run" })
      .mockResolvedValue(
        finishedRun({
          success: false,
          node_step_result: [{ node_id: 1, success: false, error: "file not found" }],
        }),
      );
    expect(await store.runCell("cell-2")).toBe(false);
    const cell = nb.cells.find((c) => c.id === "cell-2")!;
    expect(cell.output!.error).toBe("Node #1 failed: file not found");
    expect(cell.execState).toBe("error");
    expect(mocks.getTableExample).not.toHaveBeenCalled();
  });

  it("an aborted prepare runs nothing", async () => {
    const { store } = await openFlow();
    hooks.prepare.mockResolvedValue(false);
    expect(await store.runCell("cell-2")).toBe(false);
    expect(mocks.runLineage).not.toHaveBeenCalled();
  });

  it("refuses a second flow action while one is in flight", async () => {
    const { store } = await openFlow();
    let release!: () => void;
    hooks.prepare.mockImplementationOnce(
      () => new Promise<boolean>((r) => (release = () => r(true))),
    );
    const first = store.runCell("cell-2");
    expect(await store.runCell("cell-1")).toBe(false);
    expect(await store.syncFlowNotebook()).toBe("busy");
    release();
    expect(await first).toBe(true);
    expect(mocks.runLineage).toHaveBeenCalledTimes(1);
  });

  it("run all syncs, runs the whole flow, then refreshes every parameters and node cell", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "filtered_2 = source_1.filter(fl.col('q') >= n)");
    mocks.push.mockResolvedValue(pushed({ node_ids_by_cell: { "cell-2": [2, 3] } }));
    await store.runAll();
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(mocks.runFlow).toHaveBeenCalledWith(FLOW);
    expect(mocks.push.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.runFlow.mock.invocationCallOrder[0],
    );
    expect(mocks.runLineage).not.toHaveBeenCalled();
    expect(mocks.getTableExample.mock.calls.map((c) => c[1])).toEqual([1, 3]);
    const byId = (id: string) => nb.cells.find((c) => c.id === id)!;
    expect(byId("imports").output).toBeNull();
    expect(tableOf(byId("parameters").output!).data).toEqual([
      { name: "n", type: "integer", default: "10" },
    ]);
    expect(tableOf(byId("cell-1").output!).columns).toEqual(["q"]);
    expect(tableOf(byId("cell-2").output!).columns).toEqual(["q"]);
    expect(nb.cells.every((c) => c.execState === "idle")).toBe(true);
    expect(hooks.runEnded).toHaveBeenCalledTimes(1);
  });

  it("the push button reviews warnings and reports the push", async () => {
    const { store, nb } = await openFlow();
    const warning = "Node 4 reads from a node its cell did not rebuild; that input is dropped.";
    mocks.push.mockResolvedValueOnce(held({ warnings: [warning] }));
    expect(await store.syncFlowNotebook()).toBe("synced");
    expect(hooks.confirm).toHaveBeenCalledWith(
      expect.objectContaining({ warnings: [warning], deletions: [], parameter_changes: false }),
      "push",
    );
    expect(mocks.push.mock.calls.map(([body]) => body.trigger)).toEqual(["push", undefined]);
    expect(hooks.pushed).toHaveBeenCalledTimes(1);
    expect(nb.notice).toEqual({ tone: "success", message: "Pushed to the canvas" });

    mocks.push
      .mockResolvedValueOnce(held({ parameter_changes: true }))
      .mockResolvedValueOnce(pushed({ parameter_changes: true }));
    expect(await store.syncFlowNotebook()).toBe("synced");
    expect(hooks.confirm).toHaveBeenLastCalledWith(
      expect.objectContaining({ parameter_changes: true }),
      "push",
    );
    expect(mocks.push).toHaveBeenCalledTimes(4);
  });

  it("a push with nothing to review applies in one request", async () => {
    const { store, nb } = await openFlow();
    expect(await store.syncFlowNotebook()).toBe("synced");
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(mocks.push.mock.calls[0][0].trigger).toBe("push");
    expect(hooks.confirm).not.toHaveBeenCalled();
    expect(hooks.pushed).toHaveBeenCalledTimes(1);
    expect(nb.notice).toEqual({ tone: "success", message: "Pushed to the canvas" });
  });

  it("a cancelled push review pushes nothing", async () => {
    const { store, nb } = await openFlow();
    mocks.push.mockResolvedValue(held({ warnings: ["Changes the flow parameters"] }));
    hooks.confirm.mockResolvedValue(false);
    expect(await store.syncFlowNotebook()).toBe("cancelled");
    expect(hooks.confirm).toHaveBeenCalledWith(
      expect.objectContaining({ warnings: ["Changes the flow parameters"] }),
      "push",
    );
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(hooks.pushed).not.toHaveBeenCalled();
    expect(nb.notice).toBeNull();
  });

  it("re-renders once the run is over after a sync that changes the parameters", async () => {
    const source = nodeCell([1], "source_1 = 1");
    const headCell = nodeCell([4], "head_4 = source_1.head(5)");
    mocks.render.mockResolvedValue(rendered("f1", [importsCell, source]));
    const { store, nb } = await openFlow();
    const added = store.addCell("python")!;
    store.setCellCode(added.id, paramsCell.code);
    const head = store.addCell("python")!;
    store.setCellCode(head.id, headCell.code);
    mocks.getFlowData.mockResolvedValue({
      node_inputs: [1, 4].map((id) => ({ id, item: "filter" })),
    });
    mocks.getRunStatus.mockResolvedValue(
      finishedRun({ node_step_result: [1, 4].map((node_id) => ({ node_id, success: true })) }),
    );
    mocks.push.mockImplementation(async () => {
      mocks.render.mockResolvedValue(rendered("f2", [importsCell, paramsCell, source, headCell]));
      return pushed({
        parameter_changes: true,
        node_ids_by_cell: { [added.id]: [], [head.id]: [4] },
      });
    });
    expect(await store.runCell(head.id)).toBe(true);
    expect(mocks.runLineage).toHaveBeenCalledWith(FLOW, 4);
    expect(mocks.runLineage.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.render.mock.invocationCallOrder.at(-1)!,
    );
    expect(nb.cells.some((c) => c.id === added.id)).toBe(false);
    expect(flowCellKind(nb, "parameters")).toBe("parameters");
    expect(await store.runCell("parameters")).toBe(true);
    expect(tableOf(nb.cells.find((c) => c.id === "parameters")!.output!).columns).toEqual([
      "name",
      "type",
      "default",
    ]);
  });

  it("tells a rendering's warnings through the notice, once until they change", async () => {
    const failed = "The flow could not be rendered as code: boom";
    mocks.render.mockResolvedValue({ ...rendered("f1", [importsCell]), warnings: [failed] });
    const { store, nb } = await openFlow();
    expect(nb.notice).toEqual({ tone: "warning", message: failed });
    nb.notice = null;
    mocks.render.mockResolvedValue({ ...rendered("f2", [importsCell]), warnings: [failed] });
    await store.refreshFlowNotebook(FLOW);
    expect(nb.notice).toBeNull();
  });

  it("a re-render that fails after a parameter-changing push keeps the cells and warns", async () => {
    const { store, nb } = await openFlow();
    const codes = nb.cells.map((c) => c.code);
    const failed = "The flow could not be rendered as code: boom";
    mocks.push.mockImplementation(async () => {
      mocks.render.mockRejectedValue(httpError(422, failed));
      return pushed({ parameter_changes: true });
    });
    expect(await store.syncFlowNotebook()).toBe("synced");
    expect(nb.cells.map((c) => c.code)).toEqual(codes);
    expect(nb.notice).toEqual({ tone: "warning", message: failed });
  });

  it("an edit made while a push is in flight stays edited", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-2", "filtered_2 = source_1");
    mocks.push.mockImplementation(async () => {
      store.setCellCode("cell-2", "filtered_2 = source_1.head(3)");
      return pushed({ node_ids_by_cell: { "cell-2": [2] } });
    });
    await store.syncFlowNotebook();
    expect(flowCellSyncState(nb, nb.cells.find((c) => c.id === "cell-2")!)).toBe("edited");
    expect(flowNeedsSync(nb)).toBe(true);
  });

  it("re-renders before the push, so a drawer save in prepare is no stale fingerprint", async () => {
    const { store, nb } = await openFlow();
    const edited = "filtered_2 = source_1.filter(fl.col('q') >= n)";
    store.setCellCode("cell-2", edited);
    hooks.prepare.mockImplementationOnce(async () => {
      mocks.render.mockResolvedValue(rendered("f1b"));
      return true;
    });
    mocks.push.mockImplementation(async (body: { code_fingerprint: string }) => {
      if (body.code_fingerprint !== "f1b") throw httpError(409, { message: "canvas changed" });
      return pushed();
    });
    expect(await store.runCell("cell-2")).toBe(true);
    expect(mocks.push).toHaveBeenCalledTimes(1);
    expect(mocks.push.mock.calls[0][0].code_fingerprint).toBe("f1b");
    expect(nb.notice).toBeNull();
    expect(nb.cells.find((c) => c.id === "cell-2")!.code).toBe(edited);
  });

  it("an edit typed back to the canvas text leaves nothing to sync", async () => {
    const { store, nb } = await openFlow();
    const original = nb.cells.find((c) => c.id === "cell-2")!.code;
    store.setCellCode("cell-2", `${original}x`);
    expect(flowNeedsSync(nb)).toBe(true);
    store.setCellCode("cell-2", original);
    expect(flowNeedsSync(nb)).toBe(false);
    expect(await store.runCell("cell-2")).toBe(true);
    expect(mocks.push).not.toHaveBeenCalled();
    expect(mocks.runLineage).toHaveBeenCalledWith(FLOW, 3);
  });

  it("a blank cell appended after the last one needs no sync, even for a non-admin", async () => {
    const { store, nb } = await openFlow();
    mocks.push.mockRejectedValue(httpError(403, "Admin privileges required"));
    const blank = store.addCell("python")!;
    expect(flowCellSyncState(nb, blank)).toBe("synced");
    expect(flowNeedsSync(nb)).toBe(false);
    expect(await store.runCell("cell-2")).toBe(true);
    expect(mocks.push).not.toHaveBeenCalled();
    expect(mocks.runLineage).toHaveBeenCalledWith(FLOW, 3);
    expect(nb.syncForbidden).toBeFalsy();
    store.removeCell(blank.id);
    expect(flowNeedsSync(nb)).toBe(false);
    store.moveCell("cell-2", -1);
    expect(flowNeedsSync(nb)).toBe(true);
  });

  it("a markdown note never enters the push body nor makes Run sync", async () => {
    const { store, nb } = await openFlow();
    const note = store.addCell("markdown")!;
    store.setCellCode(note.id, "Some note about the flow");
    expect(flowNeedsSync(nb)).toBe(false);
    expect(flowCellSyncState(nb, note)).not.toBe("edited");

    store.setCellCode("cell-2", "filtered_2 = source_1");
    expect(await store.runCell("cell-2")).toBe(true);
    const body = mocks.push.mock.calls[0][0];
    expect(body.cells.map(([id]: [string, string]) => id)).not.toContain(note.id);
    expect(body.changed_cell_ids).toEqual(["cell-2"]);
  });

  it("a re-render keeps a rendered cell that was turned into a markdown note", async () => {
    const { store, nb } = await openFlow();
    store.setCellType("cell-1", "markdown");
    store.setCellCode("cell-1", "Reads the source file");
    mocks.render.mockResolvedValue(rendered("f2"));
    await store.refreshFlowNotebook(FLOW);
    expect(nb.cells.find((c) => c.id === "cell-1")!.code).toBe("Reads the source file");
    const body = flowPushBody(nb, new Map([[1, "read"]]), 3);
    expect(body.cells.map(([id]) => id)).not.toContain("cell-1");
    expect(body.provenance).not.toHaveProperty("cell-1");
  });

  it("a refused canvas run touches no run hook", async () => {
    mocks.runLineage.mockRejectedValue(httpError(422, "Flow is already running"));
    const { store, nb } = await openFlow();
    expect(await store.runCell("cell-2")).toBe(false);
    expect(nb.cells.find((c) => c.id === "cell-2")!.output!.error).toBe("Flow is already running");
    expect(hooks.runStarted).not.toHaveBeenCalled();
    expect(hooks.runEnded).not.toHaveBeenCalled();
  });

  it("fails the cell when no newer run shows up instead of reporting the previous one", async () => {
    vi.useFakeTimers();
    try {
      mocks.getRunStatus
        .mockReset()
        .mockResolvedValue(finishedRun({ start_time: "t0", run_type: "full_run" }));
      const { store, nb } = await openFlow();
      const ran = store.runCell("cell-2");
      await vi.advanceTimersByTimeAsync(11_000);
      expect(await ran).toBe(false);
      const cell = nb.cells.find((c) => c.id === "cell-2")!;
      expect(cell.output!.error).toBe("The run did not start.");
      expect(mocks.getTableExample).not.toHaveBeenCalled();
      expect(hooks.runStarted).toHaveBeenCalledTimes(1);
      expect(hooks.runEnded).toHaveBeenCalledWith(null);
    } finally {
      vi.useRealTimers();
    }
  });

  it("a cancelled run says the node did not run and reads no rows", async () => {
    mocks.getRunStatus
      .mockReset()
      .mockResolvedValueOnce({ start_time: "t0", is_running: false, run_type: "full_run" })
      .mockResolvedValue(
        finishedRun({
          success: false,
          node_step_result: [
            { node_id: 1, success: true },
            { node_id: 2, success: null, error: "" },
          ],
        }),
      );
    const { store, nb } = await openFlow();
    expect(await store.runCell("cell-2")).toBe(false);
    const cell = nb.cells.find((c) => c.id === "cell-2")!;
    expect(cell.output!.error).toBe("Node #3 did not run: the run was cancelled.");
    expect(cell.execState).toBe("error");
    expect(mocks.getTableExample).not.toHaveBeenCalled();
    expect(mocks.triggerNodeFetch).not.toHaveBeenCalled();
  });

  it("run all shows the failed step on a node that did not run, reading and fetching nothing", async () => {
    mocks.getRunStatus
      .mockReset()
      .mockResolvedValueOnce({ start_time: "t0", is_running: false, run_type: "full_run" })
      .mockResolvedValue(
        finishedRun({
          success: false,
          run_type: "full_run",
          execution_mode: "Performance",
          node_step_result: [
            { node_id: 1, success: true },
            { node_id: 2, success: false, error: "boom" },
          ],
        }),
      );
    const { store, nb } = await openFlow();
    await store.runAll();
    const byId = (id: string) => nb.cells.find((c) => c.id === id)!;
    expect(byId("cell-2").output!.error).toBe("Node #2 failed: boom");
    expect(byId("cell-2").execState).toBe("error");
    expect(tableOf(byId("cell-1").output!).columns).toEqual(["q"]);
    expect(mocks.getTableExample.mock.calls.map((c) => c[1])).toEqual([1]);
    expect(mocks.triggerNodeFetch).not.toHaveBeenCalled();
  });

  it("flow cells carry no kernel staleness", async () => {
    const { store, nb } = await openFlow();
    store.setCellCode("cell-1", "source_1 = fl.read_csv('b.csv')");
    store.moveCell("cell-2", -1);
    const owner = ownerIdForNotebook(nb.tabId);
    for (const cell of nb.cells)
      expect(cellRuntime(owner, cell.id)?.staleReason ?? null).toBeNull();
  });

  describe("after a Performance-mode run, which stores no rows", () => {
    let runs: number;
    let latest: Record<string, unknown>;
    const fetched = new Set<number>();
    const finish = (extra: Record<string, unknown> = {}) => {
      runs += 1;
      latest = finishedRun({
        start_time: `p${runs}`,
        end_time: `p${runs}`,
        execution_mode: "Performance",
        run_type: "full_run",
        ...extra,
      });
    };
    const fetchDone = (nodeId: number) =>
      finish({ run_type: "fetch_one", node_step_result: [{ node_id: nodeId, success: true }] });
    type Cells = { cells: { id: string; output?: { display_outputs: unknown[] } | null }[] };
    const textOf = (id: string, nb: Cells) =>
      nb.cells.find((c) => c.id === id)!.output!.display_outputs[0];

    beforeEach(() => {
      runs = 0;
      fetched.clear();
      latest = { start_time: "t0", is_running: false, run_type: "full_run" };
      mocks.getRunStatus.mockReset().mockImplementation(async () => latest);
      mocks.runLineage.mockImplementation(async () => {
        finish();
        return { message: "Data started", flow_id: FLOW, node_ids: [] };
      });
      mocks.runFlow.mockImplementation(async () => finish());
      mocks.triggerNodeFetch.mockImplementation(async (_flow: number, nodeId: number) => {
        fetched.add(nodeId);
        fetchDone(nodeId);
      });
      mocks.getTableExample.mockImplementation(async (_flow: number, nodeId: number) =>
        fetched.has(nodeId)
          ? example({ node_id: nodeId })
          : example({ node_id: nodeId, has_example_data: false, data: [] }),
      );
    });

    it("fetches the node's rows once the run is over, like the preview's Fetch Data", async () => {
      const { store, nb } = await openFlow();
      expect(await store.runCell("cell-2")).toBe(true);
      expect(mocks.runLineage).toHaveBeenCalledWith(FLOW, 3);
      expect(mocks.triggerNodeFetch).toHaveBeenCalledTimes(1);
      expect(mocks.triggerNodeFetch).toHaveBeenCalledWith(FLOW, 3);
      expect(mocks.runLineage.mock.invocationCallOrder[0]).toBeLessThan(
        mocks.triggerNodeFetch.mock.invocationCallOrder[0],
      );
      expect(mocks.getTableExample.mock.calls.map((c) => c[1])).toEqual([3, 3]);
      const cell = nb.cells.find((c) => c.id === "cell-2")!;
      expect(cell.output!.error).toBeNull();
      expect(tableOf(cell.output!).data).toEqual([{ q: 8 }, { q: 9 }]);
      expect(cell.execState).toBe("idle");
      expect(hooks.runStarted).toHaveBeenCalledTimes(2);
      expect(hooks.runEnded).toHaveBeenLastCalledWith(
        expect.objectContaining({ start_time: "p2", run_type: "fetch_one" }),
      );
    });

    it("run all fetches every node cell's rows, and none for the parameters cell", async () => {
      const { store, nb } = await openFlow();
      await store.runAll();
      expect(mocks.runFlow).toHaveBeenCalledTimes(1);
      expect(mocks.runLineage).not.toHaveBeenCalled();
      expect(mocks.triggerNodeFetch.mock.calls).toEqual([
        [FLOW, 1],
        [FLOW, 3],
      ]);
      expect(tableOf(nb.cells.find((c) => c.id === "cell-1")!.output!).data).toHaveLength(2);
      expect(tableOf(nb.cells.find((c) => c.id === "cell-2")!.output!).data).toHaveLength(2);
      expect(tableOf(nb.cells.find((c) => c.id === "parameters")!.output!).columns).toEqual([
        "name",
        "type",
        "default",
      ]);
      expect(nb.cells.every((c) => c.execState === "idle")).toBe(true);
    });

    it("never fetches after a Development-mode run", async () => {
      mocks.runLineage.mockImplementation(async () => {
        finish({ execution_mode: "Development" });
        return { message: "Data started", flow_id: FLOW, node_ids: [] };
      });
      const { store, nb } = await openFlow();
      expect(await store.runCell("cell-2")).toBe(true);
      expect(mocks.triggerNodeFetch).not.toHaveBeenCalled();
      expect(textOf("cell-2", nb)).toMatchObject({ data: "Node #3 has no result yet." });
    });

    it("never fetches a node a closed gate skipped", async () => {
      mocks.runLineage.mockImplementation(async () => {
        finish({ node_step_result: [{ node_id: 3, success: true, skipped: true }] });
        return { message: "Data started", flow_id: FLOW, node_ids: [] };
      });
      const { store, nb } = await openFlow();
      expect(await store.runCell("cell-2")).toBe(true);
      expect(mocks.triggerNodeFetch).not.toHaveBeenCalled();
      expect(textOf("cell-2", nb)).toMatchObject({ data: "Node #3 has no result yet." });
    });

    it("shows why the fetch was refused on the cell", async () => {
      mocks.triggerNodeFetch.mockRejectedValue(httpError(422, "Flow is already running"));
      const { store, nb } = await openFlow();
      expect(await store.runCell("cell-2")).toBe(false);
      const cell = nb.cells.find((c) => c.id === "cell-2")!;
      expect(cell.output!.error).toBe("Flow is already running");
      expect(cell.output!.display_outputs).toEqual([]);
      expect(cell.execState).toBe("error");
      expect(mocks.getTableExample).toHaveBeenCalledTimes(1);
      expect(hooks.runStarted).toHaveBeenCalledTimes(1);
      expect(hooks.runEnded).toHaveBeenCalledTimes(1);
      expect(hooks.runEnded).toHaveBeenLastCalledWith(
        expect.objectContaining({ run_type: "full_run" }),
      );
    });

    it("shows a failed fetch step instead of rows", async () => {
      mocks.triggerNodeFetch.mockImplementation(async () =>
        finish({
          run_type: "fetch_one",
          success: false,
          node_step_result: [{ node_id: 3, success: false, error: "worker lost" }],
        }),
      );
      const { store, nb } = await openFlow();
      expect(await store.runCell("cell-2")).toBe(false);
      expect(nb.cells.find((c) => c.id === "cell-2")!.output!.error).toBe(
        "Node #3 failed: worker lost",
      );
      expect(mocks.getTableExample).toHaveBeenCalledTimes(1);
    });

    it("says the node has no result only when a successful fetch still stored none", async () => {
      mocks.triggerNodeFetch.mockImplementation(async (_flow: number, nodeId: number) =>
        fetchDone(nodeId),
      );
      const { store, nb } = await openFlow();
      expect(await store.runCell("cell-2")).toBe(true);
      expect(mocks.triggerNodeFetch).toHaveBeenCalledTimes(1);
      expect(mocks.getTableExample).toHaveBeenCalledTimes(2);
      expect(textOf("cell-2", nb)).toMatchObject({
        mime_type: "text/plain",
        data: "Node #3 has no result yet.",
      });
    });
  });
});
