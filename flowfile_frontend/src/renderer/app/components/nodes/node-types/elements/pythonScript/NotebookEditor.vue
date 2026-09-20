<template>
  <div class="notebook-editor">
    <!-- Toolbar -->
    <div class="notebook-toolbar">
      <button :disabled="!kernelId || isAnyExecuting" title="Run All Cells" @click="runAllCells">
        <i class="fa-solid fa-play"></i> Run All
      </button>
      <button title="Clear All Outputs" @click="clearAllOutputs">
        <i class="fa-solid fa-eraser"></i> Clear
      </button>
      <button
        :disabled="!kernelId || isAnyExecuting"
        title="Restart Kernel (clear all variables)"
        @click="restartKernel"
      >
        <i class="fa-solid fa-rotate-right"></i> Restart
      </button>
      <button
        :disabled="structuralDisabled || !canUndo"
        title="Undo cell action (insert, delete, move, duplicate)"
        @click="undoCellAction"
      >
        <i class="fa-solid fa-arrow-rotate-left"></i> Undo
      </button>
      <button
        :disabled="structuralDisabled || !canRedo"
        title="Redo cell action"
        @click="redoCellAction"
      >
        <i class="fa-solid fa-arrow-rotate-right"></i> Redo
      </button>
      <span class="notebook-info">{{ cells.length }} cell{{ cells.length !== 1 ? "s" : "" }}</span>
    </div>

    <!-- Cell list -->
    <div ref="hostRef" class="notebook-cells">
      <div
        v-if="drag.indicatorTop.value !== null"
        class="nb-drop-line"
        :style="{ top: `${drag.indicatorTop.value}px` }"
        aria-hidden="true"
      ></div>
      <template v-for="(cell, index) in cells" :key="cell.id">
        <NotebookCellComponent
          :data-cell-id="cell.id"
          :cell="cell"
          :owner-id="ownerId"
          :cell-index="index"
          :is-executing="executingCellId === cell.id"
          :is-last-cell="index === cells.length - 1"
          :cell-count="cells.length"
          :structural-disabled="structuralDisabled"
          :dragging="drag.draggingId.value === cell.id"
          :active="activeCellId === cell.id"
          :input-names="inputNames"
          :upstream-columns="upstreamColumns"
          :prior-cell-codes="cells.slice(0, index).map((c) => c.code)"
          :kernel-id="kernelId"
          :flow-id="flowId"
          :node-id="nodeId"
          @update:code="(code) => updateCellCode(cell.id, code)"
          @run-cell="() => runCell(cell.id)"
          @run-cell-and-advance="() => runCellAndAdvance(cell.id)"
          @drag-start="(ev: PointerEvent) => drag.onHandlePointerDown(cell.id, ev)"
          @move-key="(dir: -1 | 1) => onMoveKey(index, dir)"
          @move-up="() => onMoveKey(index, -1)"
          @move-down="() => onMoveKey(index, 1)"
          @duplicate="() => duplicateCell(cell.id)"
          @insert-above="() => insertAbove(index)"
          @insert-below="() => insertBelow(index)"
          @focus="activeCellId = cell.id"
          @delete="() => deleteCell(cell.id)"
        />

        <!-- Hover-to-insert: a faint "+" appears between cells; click to add a
             cell at this position. -->
        <div
          v-if="index < cells.length - 1"
          class="nb-insert-zone"
          :class="{ 'is-disabled': structuralDisabled }"
          title="Add cell here"
          @click="insertBelow(index)"
        >
          <span class="nb-insert-plus"><i class="fa-solid fa-plus"></i></span>
        </div>
      </template>
    </div>

    <!-- Add cell button -->
    <button class="add-cell-button" :disabled="structuralDisabled" @click="addCell">
      <i class="fa-solid fa-plus"></i> Add Cell
    </button>

    <div class="nb-sr-only" role="status" aria-live="polite">{{ announcement }}</div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, nextTick } from "vue";
import { KernelApi } from "../../../../../api/kernel.api";
import type { NotebookCell } from "../../../../../types/node.types";
import {
  applyOperation,
  cellMoveAnnouncement,
  duplicateCell as duplicateCellOp,
  insertCell,
  moveCell as moveCellOp,
  moveCellBy,
  newCellId,
  removeCell,
  type OperationResult,
} from "../../../../notebook/cellOperations";
import { cellPresentation } from "../../../../notebook/cellPresentation";
import { cellSelector, focusCell, ownerIdForNode } from "../../../../notebook/editorViews";
import { findScrollParent, useCellDrag } from "../../../../notebook/useCellDrag";
import { getCellHistory } from "../../../../notebook/useCellHistory";
import NotebookCellComponent from "./NotebookCell.vue";
import type { UpstreamColumn } from "./useUpstreamColumns";

interface Props {
  cells: NotebookCell[];
  kernelId: string | null;
  flowId: number;
  nodeId: number;
  dependingOnIds: number[];
  inputNames?: string[];
  upstreamColumns?: UpstreamColumn[];
}

const props = withDefaults(defineProps<Props>(), {
  inputNames: () => [],
  upstreamColumns: () => [],
});
const emit = defineEmits<{
  (e: "update:cells", cells: NotebookCell[]): void;
}>();

const executingCellId = ref<string | null>(null);
const executionCounter = ref(1);
const isAnyExecuting = computed(() => executingCellId.value !== null);
const hostRef = ref<HTMLElement | null>(null);
const announcement = ref("");
const activeCellId = ref<string | null>(null);

const ownerId = computed(() => ownerIdForNode(props.flowId, props.nodeId));
// Change 3 swaps this for isBatchActive(ownerId).
const structuralDisabled = computed(() => isAnyExecuting.value);
const canUndo = computed(() => getCellHistory<NotebookCell>(ownerId.value).canUndo.value);
const canRedo = computed(() => getCellHistory<NotebookCell>(ownerId.value).canRedo.value);

// ─── Cell Operations ──────────────────────────────────────────────────────────

interface MoveInfo {
  from: number;
  to: number;
  total: number;
}

const makeCell = (): NotebookCell => ({ id: newCellId(), code: "", output: null });

const applyStructural = (result: OperationResult<NotebookCell>) => {
  emit("update:cells", result.cells);
  getCellHistory<NotebookCell>(ownerId.value).push({ op: result.op, inverse: result.inverse });
};

const applyMove = (result: OperationResult<NotebookCell> | null): MoveInfo | null => {
  if (!result || result.op.kind !== "move") return null;
  applyStructural(result);
  return { from: result.op.from, to: result.op.to, total: result.cells.length };
};

/** Keep the handle focused after a keyboard move so repeated Alt+↑/↓ keeps working. */
const focusDragHandle = (cellId: string) => {
  hostRef.value?.querySelector<HTMLElement>(`${cellSelector(cellId)} .nb-drag-handle`)?.focus();
};

// The parent re-renders (and a new cell registers its view) during the flush nextTick awaits.
const focusAfterTick = (cellId: string | null) => {
  if (!cellId) return;
  // A collapsed editor is display:none and cannot take the caret, so reveal it first.
  cellPresentation(ownerId.value, cellId).codeCollapsed = false;
  nextTick(() => {
    focusCell(ownerId.value, cellId, hostRef.value);
  });
};

const updateCellCode = (cellId: string, code: string) => {
  emit(
    "update:cells",
    props.cells.map((c) => (c.id === cellId ? { ...c, code } : c)),
  );
};

const insertAt = (index: number) => {
  if (structuralDisabled.value) return;
  const cell = makeCell();
  applyStructural(insertCell(props.cells, cell, index));
  focusAfterTick(cell.id);
};

const addCell = () => insertAt(props.cells.length);

const insertAbove = (index: number) => insertAt(index);

const insertBelow = (index: number) => insertAt(index + 1);

const duplicateCell = (cellId: string) => {
  if (structuralDisabled.value) return;
  const result = duplicateCellOp(props.cells, cellId, (src, id) => ({
    id,
    code: src.code,
    output: null,
  }));
  if (!result || result.op.kind !== "duplicate") return;
  applyStructural(result);
  focusAfterTick(result.op.cell.id);
};

const deleteCell = (cellId: string) => {
  const index = props.cells.findIndex((c) => c.id === cellId);
  const result = removeCell(props.cells, cellId, { minCells: 1 });
  if (!result) return;
  // The next surviving cell takes the removed index; at the end, fall back to the previous one.
  const nextFocusId = result.cells[Math.min(index, result.cells.length - 1)]?.id ?? null;
  applyStructural(result);
  focusAfterTick(nextFocusId);
};

const moveCell = (index: number, direction: -1 | 1): MoveInfo | null => {
  const cellId = props.cells[index]?.id;
  return cellId ? applyMove(moveCellBy(props.cells, cellId, direction)) : null;
};

const moveCellToIndex = (cellId: string, targetIndex: number): MoveInfo | null =>
  applyMove(moveCellOp(props.cells, cellId, targetIndex));

const undoCellAction = () => {
  const history = getCellHistory<NotebookCell>(ownerId.value);
  const entry = history.undo();
  if (!entry) return;
  const result = applyOperation(props.cells, entry.inverse);
  if (!result) {
    history.clear();
    return;
  }
  emit("update:cells", result.cells);
};

const redoCellAction = () => {
  const history = getCellHistory<NotebookCell>(ownerId.value);
  const entry = history.redo();
  if (!entry) return;
  const result = applyOperation(props.cells, entry.op);
  if (!result) {
    history.clear();
    return;
  }
  emit("update:cells", result.cells);
};

const onMoveKey = (index: number, direction: -1 | 1) => {
  const cellId = props.cells[index]?.id;
  if (!cellId) return;
  const moved = moveCell(index, direction);
  if (!moved) return;
  announcement.value = cellMoveAnnouncement(moved.from, moved.to, moved.total);
  nextTick(() => focusDragHandle(cellId));
};

const drag = useCellDrag({
  getHost: () => hostRef.value,
  // The node cell list is not its own scroller — the settings drawer / dialog body is.
  getScrollContainer: () => findScrollParent(hostRef.value),
  onCommit: (cellId, targetIndex) => {
    const moved = moveCellToIndex(cellId, targetIndex);
    if (moved) announcement.value = cellMoveAnnouncement(moved.from, moved.to, moved.total);
  },
  isDisabled: () => structuralDisabled.value,
});

const clearAllOutputs = () => {
  emit(
    "update:cells",
    props.cells.map((c) => ({ ...c, output: null })),
  );
};

// ─── Execution ────────────────────────────────────────────────────────────────

const updateCellOutput = (cellId: string, output: NotebookCell["output"]) => {
  emit(
    "update:cells",
    props.cells.map((c) => (c.id === cellId ? { ...c, output } : c)),
  );
};

const runCell = async (cellId: string): Promise<boolean> => {
  if (!props.kernelId) return false;

  // Capture code at start to avoid race conditions if cells change during execution
  const cell = props.cells.find((c) => c.id === cellId);
  if (!cell) return true;
  const codeToRun = cell.code;
  if (!codeToRun.trim()) return true;

  executingCellId.value = cellId;
  try {
    // Send only logical identifiers — the backend resolves filesystem paths
    const result = await KernelApi.executeCell(props.kernelId, {
      node_id: props.nodeId,
      code: codeToRun,
      flow_id: props.flowId,
    });

    const execCount = executionCounter.value++;
    updateCellOutput(cellId, {
      stdout: result.stdout,
      stderr: result.stderr,
      display_outputs: result.display_outputs,
      error: result.error,
      execution_time_ms: result.execution_time_ms,
      execution_count: execCount,
    });
    return result.success;
  } catch (error) {
    const execCount = executionCounter.value++;
    updateCellOutput(cellId, {
      stdout: "",
      stderr: "",
      display_outputs: [],
      error: error instanceof Error ? error.message : String(error),
      execution_time_ms: 0,
      execution_count: execCount,
    });
    return false;
  } finally {
    executingCellId.value = null;
  }
};

const runAllCells = async () => {
  // Sequential execution — MUST NOT use Promise.all
  // Cells depend on state from earlier cells (variables, imports)
  for (const cell of props.cells) {
    const success = await runCell(cell.id);
    if (!success) break; // stop on first error
  }
};

const runCellAndAdvance = (cellId: string) => {
  const index = props.cells.findIndex((c) => c.id === cellId);
  if (index < 0) return;
  // Advance on submit, Jupyter-style — before the run, whose executing-cell guard blocks inserts.
  if (index >= props.cells.length - 1) addCell();
  else focusAfterTick(props.cells[index + 1].id);
  void runCell(cellId);
};

const restartKernel = async () => {
  if (!props.kernelId) return;
  try {
    await KernelApi.clearNamespace(props.kernelId, props.flowId);
    clearAllOutputs();
    executionCounter.value = 1;
  } catch (error) {
    console.error("Failed to restart kernel:", error);
  }
};
</script>

<style scoped>
.notebook-editor {
  display: flex;
  flex-direction: column;
  gap: 0;
}

.notebook-toolbar {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.35rem 0.5rem;
  background: var(--el-fill-color-lighter);
  border: 1px solid var(--el-border-color);
  border-radius: 3px 3px 0 0;
  font-size: 0.75rem;
}

.notebook-toolbar button {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  padding: 0.2rem 0.5rem;
  border: 1px solid var(--el-border-color);
  border-radius: 3px;
  background: var(--el-bg-color);
  cursor: pointer;
  font-size: 0.75rem;
  color: var(--el-text-color-regular);
}

.notebook-toolbar button:hover:not(:disabled) {
  background: var(--el-fill-color);
  color: var(--el-text-color-primary);
}

.notebook-toolbar button:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.notebook-info {
  margin-left: auto;
  color: var(--el-text-color-secondary);
  font-size: 0.7rem;
}

.notebook-cells {
  position: relative;
  border-left: 1px solid var(--el-border-color-lighter);
  border-right: 1px solid var(--el-border-color-lighter);
}

.nb-drop-line {
  position: absolute;
  left: 0;
  right: 0;
  height: 2px;
  background: var(--el-color-primary);
  pointer-events: none;
  z-index: 2;
}

.nb-sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  margin: -1px;
  padding: 0;
  border: 0;
  overflow: hidden;
  white-space: nowrap;
  clip: rect(0 0 0 0);
}

/* Hover-to-insert zone between cells: a thin gap that reveals a centered "+"
   (with a faint connecting line) only on hover. */
.nb-insert-zone {
  position: relative;
  height: 12px;
  cursor: pointer;
}
.nb-insert-zone::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 0;
  right: 0;
  height: 1px;
  background: var(--el-color-primary);
  opacity: 0;
  transition: opacity 0.15s;
}
.nb-insert-plus {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  border-radius: 50%;
  background: var(--el-color-primary);
  color: #fff;
  font-size: 9px;
  opacity: 0;
  transition: opacity 0.15s;
}
.nb-insert-zone:hover::before {
  opacity: 0.35;
}
.nb-insert-zone:hover .nb-insert-plus {
  opacity: 0.85;
}
.nb-insert-zone.is-disabled {
  pointer-events: none;
}

.add-cell-button {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.3rem;
  width: 100%;
  padding: 0.3rem;
  border: 1px dashed var(--el-border-color);
  border-radius: 0 0 3px 3px;
  background: transparent;
  cursor: pointer;
  font-size: 0.75rem;
  color: var(--el-text-color-secondary);
  transition: all 0.15s;
}

.add-cell-button:hover:not(:disabled) {
  border-color: var(--el-color-primary);
  color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
}

.add-cell-button:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
</style>
