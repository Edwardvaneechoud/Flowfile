<template>
  <div class="notebook-editor">
    <!-- Toolbar -->
    <div class="notebook-toolbar">
      <button @click="runAllCells" :disabled="!kernelId || isAnyExecuting" title="Run All Cells">
        <i class="fa-solid fa-play"></i> Run All
      </button>
      <button @click="clearAllOutputs" title="Clear All Outputs">
        <i class="fa-solid fa-eraser"></i> Clear
      </button>
      <span class="notebook-info">{{ cells.length }} cell{{ cells.length !== 1 ? 's' : '' }}</span>
    </div>

    <!-- Cell list -->
    <div class="notebook-cells">
      <NotebookCellComponent
        v-for="(cell, index) in cells"
        :key="cell.id"
        :cell="cell"
        :cell-index="index"
        :is-executing="executingCellId === cell.id"
        :is-last-cell="index === cells.length - 1"
        :cell-count="cells.length"
        @update:code="(code) => updateCellCode(cell.id, code)"
        @run-cell="() => runCell(cell.id)"
        @run-cell-and-advance="() => runCellAndAdvance(cell.id, index)"
        @move-up="() => moveCell(index, -1)"
        @move-down="() => moveCell(index, 1)"
        @delete="() => deleteCell(cell.id)"
      />
    </div>

    <!-- Add cell button -->
    <button class="add-cell-button" @click="addCell">
      <i class="fa-solid fa-plus"></i> Add Cell
    </button>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { KernelApi } from "../../../../../api/kernel.api";
import { NodeApi } from "../../../../../api/node.api";
import type { NotebookCell, CellOutput } from "../../../../../types/node.types";
import NotebookCellComponent from "./NotebookCell.vue";

interface Props {
  cells: NotebookCell[];
  kernelId: string | null;
  flowId: number;
  nodeId: number;
}

const props = defineProps<Props>();
const emit = defineEmits<{
  (e: 'update:cells', cells: NotebookCell[]): void;
}>();

const executingCellId = ref<string | null>(null);
const executionCounter = ref(0);
const isAnyExecuting = computed(() => executingCellId.value !== null);

// ─── Cell Operations ──────────────────────────────────────────────────────────

const emitUpdate = () => {
  emit('update:cells', [...props.cells]);
};

const updateCellCode = (cellId: string, code: string) => {
  const cell = props.cells.find(c => c.id === cellId);
  if (cell) {
    cell.code = code;
    emitUpdate();
  }
};

const addCell = () => {
  props.cells.push({
    id: crypto.randomUUID(),
    code: "",
    output: null,
  });
  emitUpdate();
};

const deleteCell = (cellId: string) => {
  if (props.cells.length <= 1) return; // never delete the last cell
  const index = props.cells.findIndex(c => c.id === cellId);
  if (index !== -1) {
    props.cells.splice(index, 1);
    emitUpdate();
  }
};

const moveCell = (index: number, direction: number) => {
  const newIndex = index + direction;
  if (newIndex < 0 || newIndex >= props.cells.length) return;
  const [cell] = props.cells.splice(index, 1);
  props.cells.splice(newIndex, 0, cell);
  emitUpdate();
};

const clearAllOutputs = () => {
  props.cells.forEach(cell => { cell.output = null; });
  emitUpdate();
};

// ─── Execution ────────────────────────────────────────────────────────────────

const runCell = async (cellId: string): Promise<boolean> => {
  if (!props.kernelId) return false;

  const cell = props.cells.find(c => c.id === cellId);
  if (!cell || !cell.code.trim()) return true; // empty cell is success

  executingCellId.value = cellId;
  try {
    // Prepare inputs from upstream nodes (writes parquet files)
    const { input_paths, output_dir } = await NodeApi.prepareInputs(
      props.flowId,
      props.nodeId,
    );

    const result = await KernelApi.executeCell(props.kernelId, {
      node_id: props.nodeId,
      code: cell.code,
      input_paths,
      output_dir,
      flow_id: props.flowId,
    });

    executionCounter.value++;
    cell.output = {
      stdout: result.stdout,
      stderr: result.stderr,
      display_outputs: result.display_outputs,
      error: result.error,
      execution_time_ms: result.execution_time_ms,
      execution_count: executionCounter.value,
    };
    emitUpdate();
    return result.success;
  } catch (error) {
    executionCounter.value++;
    cell.output = {
      stdout: "",
      stderr: "",
      display_outputs: [],
      error: error instanceof Error ? error.message : String(error),
      execution_time_ms: 0,
      execution_count: executionCounter.value,
    };
    emitUpdate();
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

const runCellAndAdvance = async (cellId: string, index: number) => {
  await runCell(cellId);
  // Focus next cell or add new one
  if (index >= props.cells.length - 1) {
    addCell();
  }
  // Note: actual focus management of the next cell's CodeMirror editor
  // would require ref tracking. For v1, just adding the cell is sufficient.
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
  border-left: 1px solid var(--el-border-color-lighter);
  border-right: 1px solid var(--el-border-color-lighter);
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

.add-cell-button:hover {
  border-color: var(--el-color-primary);
  color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
}
</style>
