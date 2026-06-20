<template>
  <div
    class="nb-cell"
    :class="[`nb-cell--${cell.cellType}`, { running: cell.execState === 'running' }]"
  >
    <!-- Cell toolbar -->
    <div class="nb-cell-bar">
      <button
        class="nb-run"
        :disabled="cell.execState === 'running'"
        :title="cell.cellType === 'markdown' ? 'Render (Shift+Enter)' : 'Run (Shift+Enter)'"
        @click="emit('run')"
      >
        <i v-if="cell.execState === 'running'" class="fa-solid fa-spinner fa-spin"></i>
        <i v-else-if="cell.cellType === 'markdown'" class="fa-solid fa-eye"></i>
        <i v-else class="fa-solid fa-play"></i>
      </button>

      <el-select
        v-if="allowedTypes.length > 1"
        :model-value="cell.cellType"
        size="small"
        class="nb-type-select"
        @change="(v: CellType) => emit('update:type', v)"
      >
        <el-option v-for="t in allowedTypes" :key="t" :label="TYPE_LABELS[t]" :value="t" />
      </el-select>
      <span v-else class="nb-cell-type-badge">{{ TYPE_LABELS[cell.cellType] }}</span>

      <div class="nb-cell-bar-spacer"></div>

      <div class="nb-cell-actions">
        <button class="nb-act" :disabled="index === 0" title="Move up" @click="emit('move', -1)">
          <i class="fa-solid fa-arrow-up"></i>
        </button>
        <button
          class="nb-act"
          :disabled="index === cellCount - 1"
          title="Move down"
          @click="emit('move', 1)"
        >
          <i class="fa-solid fa-arrow-down"></i>
        </button>
        <button class="nb-act nb-act--danger" title="Delete cell" @click="emit('remove')">
          <i class="fa-solid fa-trash"></i>
        </button>
      </div>
    </div>

    <!-- Editor -->
    <div class="nb-cell-editor">
      <!-- Markdown preview (double-click to edit). Content is sanitised via
           DOMPurify in sanitiseMarkdown before reaching v-html. -->
      <!-- eslint-disable vue/no-v-html -->
      <div
        v-if="cell.cellType === 'markdown' && !cell.editing"
        class="nb-md-rendered"
        @dblclick="emit('update:editing', true)"
        v-html="cell.renderedHtml || '<em>Empty markdown cell — double-click to edit</em>'"
      ></div>
      <!-- eslint-enable vue/no-v-html -->
      <el-input
        v-else-if="cell.cellType === 'markdown'"
        :model-value="cell.code"
        type="textarea"
        :autosize="{ minRows: 3 }"
        placeholder="# Markdown — Render (Shift+Enter) to preview"
        @update:model-value="(v: string) => emit('update:code', v)"
        @keydown.shift.enter.prevent="emit('run')"
      />
      <!-- Python code -->
      <codemirror
        v-else
        :model-value="cell.code"
        placeholder="# Python — Shift+Enter to run"
        :indent-with-tab="false"
        :tab-size="4"
        :extensions="extensions"
        @update:model-value="(v: string) => emit('update:code', v)"
      />
    </div>

    <!-- Output -->
    <CellOutput v-if="cell.cellType === 'python' && cell.output" :output="cell.output" />
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { Codemirror } from "vue-codemirror";
import CellOutput from "../nodes/node-types/elements/pythonScript/CellOutput.vue";
import { buildNotebookEditorExtensions } from "../nodes/node-types/elements/pythonScript/notebookEditor";
import type { CellType, NotebookCellModel } from "./types";

const props = defineProps<{
  cell: NotebookCellModel;
  index: number;
  cellCount: number;
  allowedTypes?: CellType[];
  /** Code of cells before this one, for scope/ref completions. */
  priorCellCodes?: string[];
}>();

const emit = defineEmits<{
  (e: "run"): void;
  (e: "update:code", code: string): void;
  (e: "update:type", cellType: CellType): void;
  (e: "update:editing", editing: boolean): void;
  (e: "move", direction: -1 | 1): void;
  (e: "remove"): void;
}>();

const allowedTypes = computed<CellType[]>(() => props.allowedTypes ?? ["python", "markdown"]);

const TYPE_LABELS: Record<CellType, string> = {
  python: "Python",
  markdown: "Markdown",
};

// Flow-graph completions stay empty — a catalog notebook has no graph.
const extensions = buildNotebookEditorExtensions({
  onRun: () => emit("run"),
  getPriorCellCodes: () => props.priorCellCodes ?? [],
});
</script>

<style scoped>
.nb-cell {
  border: 1px solid var(--el-border-color-lighter, #ebeef5);
  border-radius: 6px;
  margin-bottom: 8px;
  background: var(--el-bg-color, #fff);
  overflow: hidden;
  transition:
    border-color 0.12s,
    box-shadow 0.12s;
}
.nb-cell:hover {
  border-color: var(--el-border-color, #dcdfe6);
}
.nb-cell.running {
  border-color: var(--el-color-primary, #409eff);
  box-shadow: inset 3px 0 0 var(--el-color-primary, #409eff);
}

/* Toolbar: thin, flat, no filled background */
.nb-cell-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 3px 6px;
}
.nb-cell-bar-spacer {
  flex: 1;
}

/* Compact ghost run button (replaces the big primary button) */
.nb-run {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--el-color-primary, #409eff);
  cursor: pointer;
  font-size: 12px;
  transition:
    background 0.12s,
    color 0.12s;
}
.nb-run:hover {
  background: var(--el-color-primary-light-9, #ecf5ff);
}
.nb-run .fa-play {
  margin-left: 1px; /* optical-center the triangle */
}

/* Type selector — lighter, borderless until hover */
.nb-type-select {
  width: 92px;
}
.nb-type-select :deep(.el-input__wrapper) {
  box-shadow: none;
  background: transparent;
  padding-left: 6px;
}
.nb-type-select :deep(.el-input__wrapper:hover),
.nb-type-select :deep(.el-input__wrapper.is-focus) {
  background: var(--el-fill-color-light, #f5f7fa);
}
.nb-cell-type-badge {
  font-size: 12px;
  font-weight: 600;
  color: var(--el-text-color-secondary, #909399);
}

/* Secondary actions — revealed on hover/focus */
.nb-cell-actions {
  display: flex;
  gap: 2px;
  opacity: 0;
  transition: opacity 0.12s;
}
.nb-cell:hover .nb-cell-actions,
.nb-cell:focus-within .nb-cell-actions {
  opacity: 1;
}
.nb-act {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--el-text-color-secondary, #909399);
  cursor: pointer;
  font-size: 12px;
  transition:
    background 0.12s,
    color 0.12s;
}
.nb-act:hover:not(:disabled) {
  background: var(--el-fill-color, #f0f2f5);
  color: var(--el-text-color-primary, #303133);
}
.nb-act:disabled {
  opacity: 0.35;
  cursor: not-allowed;
}
.nb-act--danger:hover:not(:disabled) {
  color: var(--el-color-danger, #f56c6c);
  background: var(--el-color-danger-light-9, #fef0f0);
}

.nb-cell-editor {
  padding: 2px 4px 4px;
}
.nb-md-rendered {
  padding: 6px 10px;
  cursor: text;
  line-height: 1.5;
}
</style>
