import { computed, type Ref } from "vue";
import { KernelApi } from "../../api/kernel.api";
import type { DisplayOutput } from "../../types/kernel.types";
import type { CellOutput, NotebookCell } from "../../types/node.types";
import type { LspContext } from "../nodes/node-types/elements/pythonScript/lspCompletionSource";
import {
  beginExecution,
  markDownstreamStale,
  settleExecution,
  settledMeta,
  type RuntimeCellRef,
  type SettledMeta,
} from "./notebookRuntimeState";

/** What one cell run hands back to the notebook; the identity fields feed the stale-result check. */
export interface CellRunResult extends SettledMeta {
  success: boolean;
  stdout: string;
  stderr: string;
  display_outputs: DisplayOutput[];
  error: string | null;
  execution_time_ms: number;
}

/**
 * Where a `<Notebook>` sends its code. The notebook owns cells, tickets and undo; the executor
 * owns the backend (the Python Script node's kernel).
 */
export interface NotebookExecutor {
  run(cellId: string, code: string): Promise<CellRunResult>;
  reset(): Promise<void>;
  canRun: Ref<boolean>;
  /** Jedi code intelligence; absent means the static completion sources serve. */
  lspContext?: () => LspContext;
}

/** Rendered cells carry a status; anything but `code` is a locked placeholder with a reason. */
export type NotebookCellStatus = "code" | "placeholder" | "unsupported";

export interface NotebookViewCell extends NotebookCell {
  status?: NotebookCellStatus;
  reason?: string | null;
}

export const isLockedCell = (cell: NotebookViewCell): boolean =>
  !!cell.status && cell.status !== "code";

/** Locked cells never run, so they join the runtime as non-python members the batch skips. */
export const runtimeRefs = (cells: NotebookViewCell[]): RuntimeCellRef[] =>
  cells.map((c) => ({ id: c.id, isPython: !isLockedCell(c) }));

export interface KernelExecutorOptions {
  getKernelId: () => string | null;
  getFlowId: () => number;
  getNodeId: () => number;
}

/**
 * The Python Script node's executor: cells run in the selected kernel's per-flow namespace.
 * Reads the ids through getters, so a kernel switch takes effect on the next run.
 */
export function createKernelExecutor(opts: KernelExecutorOptions): NotebookExecutor {
  return {
    canRun: computed(() => !!opts.getKernelId()),
    async run(_cellId, code) {
      const kernelId = opts.getKernelId();
      if (!kernelId) throw new Error("No kernel selected");
      // Logical identifiers only: the backend resolves filesystem paths.
      return KernelApi.executeCell(kernelId, {
        node_id: opts.getNodeId(),
        code,
        flow_id: opts.getFlowId(),
      });
    },
    async reset() {
      const kernelId = opts.getKernelId();
      if (kernelId) await KernelApi.clearNamespace(kernelId, opts.getFlowId());
    },
    lspContext: () => ({
      kernelId: opts.getKernelId(),
      flowId: opts.getFlowId(),
      nodeId: opts.getNodeId(),
    }),
  };
}

export const NO_LSP_CONTEXT: LspContext = { kernelId: null, flowId: 0 };

const outputOf = (result: CellRunResult, executionCount: number): CellOutput => ({
  stdout: result.stdout,
  stderr: result.stderr,
  display_outputs: result.display_outputs,
  error: result.error,
  execution_time_ms: result.execution_time_ms,
  execution_count: executionCount,
});

const failureOutput = (error: unknown, executionCount: number): CellOutput => ({
  stdout: "",
  stderr: "",
  display_outputs: [],
  error: error instanceof Error ? error.message : String(error),
  execution_time_ms: 0,
  execution_count: executionCount,
});

export interface RunNotebookCellArgs {
  ownerId: string;
  executor: NotebookExecutor;
  /** The cell list at submission. */
  cells: NotebookViewCell[];
  cellId: string;
  nextExecutionCount: () => number;
  onOutput: (cellId: string, output: CellOutput) => void;
}

/**
 * Runs one cell through the executor and reports its output, unless a newer run or a session
 * reset superseded it in the meantime. Resolves whether the cell succeeded; an empty, missing or
 * locked cell counts as done so a batch carries on past it.
 */
export async function runNotebookCell(args: RunNotebookCellArgs): Promise<boolean> {
  const { ownerId, executor, cellId } = args;
  if (!executor.canRun.value) return false;
  // Captured at start so an edit during the run cannot change what was sent.
  const cell = args.cells.find((c) => c.id === cellId);
  if (!cell || isLockedCell(cell)) return true;
  const code = cell.code;
  if (!code.trim()) return true;

  // Marked at submission: a cell that fails part-way has still mutated the namespace.
  markDownstreamStale(ownerId, runtimeRefs(args.cells), cellId);
  const ticket = beginExecution(ownerId, cellId);
  try {
    const result = await executor.run(cellId, code);
    // Kernel images before 0.6.0 stamp neither identity field.
    const verdict = settleExecution(ticket, settledMeta(result));
    if (verdict === "discard") return result.success;
    args.onOutput(cellId, outputOf(result, args.nextExecutionCount()));
    return result.success;
  } catch (error) {
    if (settleExecution(ticket) === "discard") return false;
    args.onOutput(cellId, failureOutput(error, args.nextExecutionCount()));
    return false;
  }
}
