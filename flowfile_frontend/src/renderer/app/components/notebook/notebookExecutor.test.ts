import { beforeEach, describe, expect, it, vi } from "vitest";
import { ref } from "vue";
import type { CellOutput } from "../../types/node.types";

vi.mock("../../api/kernel.api", () => ({
  KernelApi: { executeCell: vi.fn(), clearNamespace: vi.fn() },
}));

import { KernelApi } from "../../api/kernel.api";
import {
  createKernelExecutor,
  isLockedCell,
  runNotebookCell,
  runtimeRefs,
  type CellRunResult,
  type NotebookExecutor,
  type NotebookViewCell,
} from "./notebookExecutor";
import { bumpSessionEpoch, cellRuntime, ensureOwner } from "./notebookRuntimeState";

const executeCell = vi.mocked(KernelApi.executeCell);
const clearNamespace = vi.mocked(KernelApi.clearNamespace);

let ownerSeq = 0;
function newOwner(): string {
  ownerSeq += 1;
  const id = `canvas:exec-test-${ownerSeq}`;
  ensureOwner(id);
  return id;
}

const okResult = (over: Partial<CellRunResult> = {}): CellRunResult => ({
  success: true,
  stdout: "hi\n",
  stderr: "",
  display_outputs: [],
  error: null,
  execution_time_ms: 5,
  ...over,
});

function fakeExecutor(run: NotebookExecutor["run"], canRun = true): NotebookExecutor {
  return {
    run: vi.fn(run),
    reset: vi.fn(() => Promise.resolve()),
    canRun: ref(canRun),
  };
}

const cell = (
  id: string,
  code: string,
  extra: Partial<NotebookViewCell> = {},
): NotebookViewCell => ({
  id,
  code,
  output: null,
  ...extra,
});

function harness(executor: NotebookExecutor, cells: NotebookViewCell[], cellId: string) {
  const outputs: Record<string, CellOutput> = {};
  let count = 1;
  const ownerId = newOwner();
  const run = () =>
    runNotebookCell({
      ownerId,
      executor,
      cells,
      cellId,
      nextExecutionCount: () => count++,
      onOutput: (id, output) => {
        outputs[id] = output;
      },
    });
  return { ownerId, outputs, run };
}

describe("createKernelExecutor", () => {
  beforeEach(() => {
    executeCell.mockReset();
    clearNamespace.mockReset();
  });

  it("can run only while a kernel is selected, tracking the selection live", () => {
    const kernelId = ref<string | null>(null);
    const executor = createKernelExecutor({
      getKernelId: () => kernelId.value,
      getFlowId: () => 7,
      getNodeId: () => 3,
    });
    expect(executor.canRun.value).toBe(false);
    kernelId.value = "k1";
    expect(executor.canRun.value).toBe(true);
  });

  it("sends the code with the node's logical ids to the current kernel", async () => {
    executeCell.mockResolvedValue(okResult() as never);
    const kernelId = ref<string | null>("k1");
    const executor = createKernelExecutor({
      getKernelId: () => kernelId.value,
      getFlowId: () => 7,
      getNodeId: () => 3,
    });
    kernelId.value = "k2";
    await executor.run("c1", "x = 1");
    expect(executeCell).toHaveBeenCalledWith("k2", { node_id: 3, code: "x = 1", flow_id: 7 });
  });

  it("refuses to run without a kernel", async () => {
    const executor = createKernelExecutor({
      getKernelId: () => null,
      getFlowId: () => 7,
      getNodeId: () => 3,
    });
    await expect(executor.run("c1", "x = 1")).rejects.toThrow("No kernel selected");
    expect(executeCell).not.toHaveBeenCalled();
  });

  it("resets the flow's namespace on the kernel, and does nothing without one", async () => {
    const kernelId = ref<string | null>(null);
    const executor = createKernelExecutor({
      getKernelId: () => kernelId.value,
      getFlowId: () => 7,
      getNodeId: () => 3,
    });
    await executor.reset();
    expect(clearNamespace).not.toHaveBeenCalled();
    kernelId.value = "k1";
    await executor.reset();
    expect(clearNamespace).toHaveBeenCalledWith("k1", 7);
  });

  it("hands the LSP the kernel, flow and node", () => {
    const executor = createKernelExecutor({
      getKernelId: () => "k1",
      getFlowId: () => 7,
      getNodeId: () => 3,
    });
    expect(executor.lspContext?.()).toEqual({ kernelId: "k1", flowId: 7, nodeId: 3 });
  });
});

describe("runNotebookCell", () => {
  it("routes the cell's code through the executor and reports a numbered output", async () => {
    const executor = fakeExecutor(async () => okResult());
    const h = harness(executor, [cell("a", "print('hi')")], "a");
    expect(await h.run()).toBe(true);
    expect(executor.run).toHaveBeenCalledWith("a", "print('hi')");
    expect(h.outputs.a).toMatchObject({ stdout: "hi\n", error: null, execution_count: 1 });
    expect(cellRuntime(h.ownerId, "a")?.status).toBe("idle");
  });

  it("reports a failed run as unsuccessful but still shows its output", async () => {
    const executor = fakeExecutor(async () => okResult({ success: false, error: "boom" }));
    const h = harness(executor, [cell("a", "1/0")], "a");
    expect(await h.run()).toBe(false);
    expect(h.outputs.a.error).toBe("boom");
  });

  it("turns an executor rejection into an error output", async () => {
    const executor = fakeExecutor(async () => {
      throw new Error("session gone");
    });
    const h = harness(executor, [cell("a", "x")], "a");
    expect(await h.run()).toBe(false);
    expect(h.outputs.a).toMatchObject({ error: "session gone", execution_time_ms: 0 });
  });

  it("does not call the executor when it cannot run", async () => {
    const executor = fakeExecutor(async () => okResult(), false);
    const h = harness(executor, [cell("a", "x")], "a");
    expect(await h.run()).toBe(false);
    expect(executor.run).not.toHaveBeenCalled();
  });

  it("skips empty and locked cells as done without running them", async () => {
    const executor = fakeExecutor(async () => okResult());
    const cells = [
      cell("blank", "   "),
      cell("lock", "df = ...", { status: "placeholder", reason: "reads a database" }),
    ];
    expect(await harness(executor, cells, "blank").run()).toBe(true);
    expect(await harness(executor, cells, "lock").run()).toBe(true);
    expect(executor.run).not.toHaveBeenCalled();
  });

  it("discards a result that a session reset overtook", async () => {
    let release: ((r: CellRunResult) => void) | undefined;
    const executor = fakeExecutor(() => new Promise<CellRunResult>((res) => (release = res)));
    const h = harness(executor, [cell("a", "x = 1")], "a");
    const pending = h.run();
    await Promise.resolve();
    bumpSessionEpoch(h.ownerId);
    release?.(okResult());
    expect(await pending).toBe(true);
    expect(h.outputs.a).toBeUndefined();
  });
});

describe("locked cells", () => {
  it("treats only a non-code status as locked", () => {
    expect(isLockedCell(cell("a", ""))).toBe(false);
    expect(isLockedCell(cell("a", "", { status: "code" }))).toBe(false);
    expect(isLockedCell(cell("a", "", { status: "placeholder" }))).toBe(true);
    expect(isLockedCell(cell("a", "", { status: "unsupported" }))).toBe(true);
  });

  it("keeps locked cells out of the python runtime membership", () => {
    const refs = runtimeRefs([cell("a", "x"), cell("b", "y", { status: "unsupported" })]);
    expect(refs).toEqual([
      { id: "a", isPython: true },
      { id: "b", isPython: false },
    ]);
  });
});
