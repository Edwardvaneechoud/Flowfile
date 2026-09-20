<template>
  <div
    class="nb-cell"
    :class="[
      `nb-cell--${cell.cellType}`,
      {
        running: cell.execState === 'running',
        'is-dragging': dragging,
        'nb-cell--active': active,
      },
    ]"
    tabindex="-1"
    @focus="emit('activate')"
    @keydown.enter.self.exact.prevent="onRootEnter"
    @keydown.shift.enter.self.prevent="emit('run')"
    @keydown.meta.enter.self.prevent="emit('run-advance')"
    @keydown.ctrl.enter.self.prevent="emit('run-advance')"
  >
    <!-- Cell toolbar -->
    <div class="nb-cell-bar">
      <button
        type="button"
        class="nb-drag-handle"
        :aria-label="`Reorder cell ${index + 1} of ${cellCount}`"
        title="Drag to reorder · Alt+↑/↓ to move"
        :disabled="structuralDisabled"
        @pointerdown="emit('drag-start', $event)"
        @keydown.alt.up.prevent="emit('move-key', -1)"
        @keydown.alt.down.prevent="emit('move-key', 1)"
      >
        <i class="fa-solid fa-grip-vertical"></i>
      </button>

      <button
        class="nb-run"
        :disabled="cell.execState === 'running'"
        :title="
          cell.cellType === 'markdown'
            ? 'Render (Shift+Enter) · Render and advance (Cmd/Ctrl+Enter)'
            : 'Run (Shift+Enter) · Run and advance (Cmd/Ctrl+Enter)'
        "
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
        <button
          class="nb-act"
          :disabled="index === 0 || structuralDisabled"
          title="Move up"
          @click="emit('move', -1)"
        >
          <i class="fa-solid fa-arrow-up"></i>
        </button>
        <button
          class="nb-act"
          :disabled="index === cellCount - 1 || structuralDisabled"
          title="Move down"
          @click="emit('move', 1)"
        >
          <i class="fa-solid fa-arrow-down"></i>
        </button>
        <CellActionMenu
          :disabled="structuralDisabled"
          :code-collapsed="pres.codeCollapsed"
          :output-collapsed="pres.outputCollapsed"
          :has-output="cell.cellType === 'python' && !!cell.output"
          :can-delete="cellCount > 1"
          @insert-above="emit('insert-above')"
          @insert-below="emit('insert-below')"
          @duplicate="emit('duplicate')"
          @toggle-code="toggleCodeCollapsed(ownerId, cell.id)"
          @toggle-output="toggleOutputCollapsed(ownerId, cell.id)"
          @delete="emit('remove')"
        />
      </div>
    </div>

    <!-- Editor. Collapse uses v-show so the EditorView (and its text undo history) survives. -->
    <div v-show="!pres.codeCollapsed" class="nb-cell-editor">
      <!-- Markdown preview (double-click to edit). Content is sanitised via
           DOMPurify in sanitiseMarkdown before reaching v-html. -->
      <!-- eslint-disable vue/no-v-html -->
      <div
        v-if="cell.cellType === 'markdown' && !cell.editing"
        class="nb-md-rendered"
        @click="emit('activate')"
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
        @focus="emit('activate')"
        @keydown.shift.enter.prevent="emit('run')"
        @keydown.meta.enter.prevent="emit('run-advance')"
        @keydown.ctrl.enter.prevent="emit('run-advance')"
      />
      <!-- Python code -->
      <codemirror
        v-else
        :model-value="cell.code"
        placeholder="# Python — Shift+Enter to run"
        :indent-with-tab="false"
        :tab-size="4"
        :extensions="extensions"
        @ready="onReady"
        @update:model-value="(v: string) => emit('update:code', v)"
      />
    </div>
    <button
      v-if="pres.codeCollapsed"
      type="button"
      class="nb-cell-collapsed"
      @click="toggleCodeCollapsed(ownerId, cell.id)"
    >
      Code hidden · {{ codeLineCount }} {{ codeLineCount === 1 ? "line" : "lines" }}
    </button>

    <!-- Output -->
    <template v-if="cell.cellType === 'python' && cell.output">
      <CellOutput v-show="!pres.outputCollapsed" :output="cell.output" />
      <button
        v-if="pres.outputCollapsed"
        type="button"
        class="nb-cell-collapsed"
        @click="toggleOutputCollapsed(ownerId, cell.id)"
      >
        Output hidden
      </button>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import { EditorView } from "@codemirror/view";
import { registerCellView, unregisterCellView } from "./editorViews";
import { cellPresentation, toggleCodeCollapsed, toggleOutputCollapsed } from "./cellPresentation";
import CellActionMenu from "./CellActionMenu.vue";
import CellOutput from "../nodes/node-types/elements/pythonScript/CellOutput.vue";
import { buildNotebookEditorExtensions } from "../nodes/node-types/elements/pythonScript/notebookEditor";
import type { CellType, NotebookCellModel } from "./types";

const props = defineProps<{
  cell: NotebookCellModel;
  /** Identifies the open notebook this cell's editor view belongs to. */
  ownerId: string;
  index: number;
  cellCount: number;
  allowedTypes?: CellType[];
  /** Structural edits (reorder, delete) are blocked while the notebook is running. */
  structuralDisabled?: boolean;
  /** The cell the caret last landed in — draws the active border. */
  active?: boolean;
  dragging?: boolean;
  /** Code of cells before this one, for scope/ref completions. */
  priorCellCodes?: string[];
  /** Kernel + namespace identity for Jedi code intelligence (sessionFlowId as flow_id). */
  kernelId?: string | null;
  flowId?: number;
  nodeId?: number;
}>();

const emit = defineEmits<{
  (e: "run"): void;
  (e: "run-advance"): void;
  (e: "update:code", code: string): void;
  (e: "update:type", cellType: CellType): void;
  (e: "update:editing", editing: boolean): void;
  (e: "move", direction: -1 | 1): void;
  (e: "move-key", direction: -1 | 1): void;
  (e: "drag-start", ev: PointerEvent): void;
  (e: "remove"): void;
  (e: "duplicate"): void;
  (e: "insert-above"): void;
  (e: "insert-below"): void;
  (e: "activate"): void;
  (e: "cursor", offset: number): void;
}>();

const allowedTypes = computed<CellType[]>(() => props.allowedTypes ?? ["python", "markdown"]);

// Keyed by the live prop: Vue reuses cell instances across owners when two notebooks share ids.
const pres = computed(() => cellPresentation(props.ownerId, props.cell.id));
const codeLineCount = computed(() => props.cell.code.split("\n").length);

// Jupyter command mode: Enter on a rendered markdown cell's root opens it for editing.
function onRootEnter() {
  if (props.cell.cellType === "markdown" && !props.cell.editing) emit("update:editing", true);
}

const TYPE_LABELS: Record<CellType, string> = {
  python: "Python",
  markdown: "Markdown",
};

// Flow-graph completions stay empty — a catalog notebook has no graph — but Jedi code
// intelligence works against the notebook's kernel session namespace.
const extensions = [
  ...buildNotebookEditorExtensions({
    onRun: () => emit("run"),
    onRunAdvance: () => emit("run-advance"),
    getPriorCellCodes: () => props.priorCellCodes ?? [],
    getKernelId: () => props.kernelId ?? null,
    getFlowId: () => props.flowId ?? 0,
    getNodeId: () => props.nodeId ?? 0,
  }),
  // Report caret moves so the store knows where "insert at cursor" should land.
  EditorView.updateListener.of((update) => {
    if (update.selectionSet || (update.focusChanged && update.view.hasFocus)) {
      emit("cursor", update.state.selection.main.head);
    }
  }),
];

let view: EditorView | null = null;
let viewOwnerId: string | null = null;

function onReady(payload: { view: EditorView }) {
  view = payload.view;
  viewOwnerId = props.ownerId;
  registerCellView(viewOwnerId, props.cell.id, view);
}

// A reused instance has to take its registered view to the new owner.
watch(
  () => props.ownerId,
  (next) => {
    if (!view || viewOwnerId === next) return;
    if (viewOwnerId) unregisterCellView(viewOwnerId, props.cell.id, view);
    viewOwnerId = next;
    registerCellView(next, props.cell.id, view);
  },
);

onBeforeUnmount(() => {
  if (view && viewOwnerId) unregisterCellView(viewOwnerId, props.cell.id, view);
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
.nb-cell.is-dragging {
  opacity: 0.55;
}
/* Declared before .running so a running cell keeps the stronger accent. */
.nb-cell.nb-cell--active {
  border-color: var(--el-color-primary, #409eff);
  box-shadow: inset 3px 0 0 var(--el-color-primary-light-5, #a0cfff);
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

/* Six-dot reorder handle: always rendered (so it stays keyboard-reachable) but
   faint until the cell is hovered or focused. */
.nb-drag-handle {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--el-text-color-secondary, #909399);
  font-size: 12px;
  opacity: 0.35;
  cursor: grab;
  touch-action: none;
  user-select: none;
  transition:
    background 0.12s,
    opacity 0.12s;
}
.nb-cell:hover .nb-drag-handle,
.nb-cell:focus-within .nb-drag-handle {
  opacity: 1;
}
.nb-drag-handle:hover:not(:disabled) {
  background: var(--el-fill-color, #f0f2f5);
  color: var(--el-text-color-primary, #303133);
}
.nb-cell.is-dragging .nb-drag-handle {
  cursor: grabbing;
}
.nb-drag-handle:disabled {
  opacity: 0.35;
  cursor: not-allowed;
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

.nb-cell-editor {
  padding: 2px 4px 4px;
}
.nb-md-rendered {
  padding: 6px 10px;
  cursor: text;
  line-height: 1.5;
}

/* Stub row standing in for hidden code/output; click restores it. */
.nb-cell-collapsed {
  display: block;
  width: calc(100% - 8px);
  margin: 2px 4px 4px;
  padding: 5px 10px;
  border: 1px dashed var(--el-border-color, #dcdfe6);
  border-radius: 5px;
  background: var(--el-fill-color-lighter, #f5f7fa);
  color: var(--el-text-color-secondary, #909399);
  font-size: 12px;
  text-align: left;
  cursor: pointer;
}
.nb-cell-collapsed:hover {
  color: var(--el-text-color-primary, #303133);
  border-color: var(--el-color-primary, #409eff);
}
</style>
