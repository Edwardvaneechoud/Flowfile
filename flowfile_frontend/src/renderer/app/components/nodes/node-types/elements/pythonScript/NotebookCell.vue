<template>
  <div class="cell-wrapper" :class="cellClasses" tabindex="-1">
    <!-- Gutter: execution count or cell index -->
    <div class="cell-gutter">[{{ cell.output?.execution_count ?? cellIndex + 1 }}]</div>

    <!-- Main content area -->
    <div class="cell-content">
      <!-- Toolbar (shown on hover/focus) -->
      <div class="cell-toolbar">
        <button
          type="button"
          class="nb-drag-handle"
          :aria-label="`Reorder cell ${cellIndex + 1} of ${cellCount}`"
          title="Drag to reorder · Alt+↑/↓ to move"
          :disabled="structuralDisabled"
          @pointerdown="emit('drag-start', $event)"
          @keydown.alt.up.prevent="emit('move-key', -1)"
          @keydown.alt.down.prevent="emit('move-key', 1)"
        >
          <i class="fa-solid fa-grip-vertical"></i>
        </button>
        <button
          :disabled="busy"
          title="Run and advance (Shift+Enter) · Run (Cmd/Ctrl+Enter)"
          @click="emit('run-cell')"
        >
          <i class="fa-solid fa-play"></i>
        </button>
        <button
          :disabled="cellIndex === 0 || structuralDisabled"
          title="Move up"
          @click="emit('move-up')"
        >
          <i class="fa-solid fa-chevron-up"></i>
        </button>
        <button
          :disabled="isLastCell || structuralDisabled"
          title="Move down"
          @click="emit('move-down')"
        >
          <i class="fa-solid fa-chevron-down"></i>
        </button>
        <CellActionMenu
          :disabled="structuralDisabled"
          :code-collapsed="pres.codeCollapsed"
          :output-collapsed="pres.outputCollapsed"
          :has-output="!!cell.output"
          :can-delete="cellCount > 1"
          @insert-above="emit('insert-above')"
          @insert-below="emit('insert-below')"
          @duplicate="emit('duplicate')"
          @toggle-code="toggleCodeCollapsed(ownerId, cell.id)"
          @toggle-output="toggleOutputCollapsed(ownerId, cell.id)"
          @delete="emit('delete')"
        />
      </div>

      <!-- CodeMirror editor: v-show keeps the EditorView (and its undo history) alive -->
      <div v-show="!pres.codeCollapsed" class="cell-editor-wrapper">
        <codemirror
          :model-value="cell.code"
          placeholder="# Enter code..."
          :autofocus="false"
          :indent-with-tab="false"
          :tab-size="4"
          :extensions="cellExtensions"
          @ready="onReady"
          @update:model-value="(val: string) => emit('update:code', val)"
        />
      </div>
      <button
        v-if="pres.codeCollapsed"
        type="button"
        class="nb-cell-collapsed"
        @click="toggleCodeCollapsed(ownerId, cell.id)"
      >
        <i class="fa-solid fa-chevron-right"></i> {{ collapsedCodeLabel }}
      </button>

      <!-- Output area: the hover-only toolbar cannot carry a badge, so it sits with the result. -->
      <CellStatusBadge class="cell-status-badge" :runtime="runtime" :has-output="!!cell.output" />
      <CellOutput v-if="cell.output" v-show="!pres.outputCollapsed" :output="cell.output" />
      <button
        v-if="cell.output && pres.outputCollapsed"
        type="button"
        class="nb-cell-collapsed"
        @click="toggleOutputCollapsed(ownerId, cell.id)"
      >
        <i class="fa-solid fa-chevron-right"></i> Output hidden
      </button>

      <!-- Executing indicator -->
      <div v-if="status === 'running'" class="cell-executing">
        <i class="fas fa-spinner fa-spin"></i> Running...
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, onBeforeUnmount, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import { EditorView } from "@codemirror/view";

import CellActionMenu from "../../../../notebook/CellActionMenu.vue";
import CellStatusBadge from "../../../../notebook/CellStatusBadge.vue";
import {
  cellPresentation,
  toggleCodeCollapsed,
  toggleOutputCollapsed,
} from "../../../../notebook/cellPresentation";
import { registerCellView, unregisterCellView } from "../../../../notebook/editorViews";
import type { CellRuntime } from "../../../../notebook/notebookRuntimeState";
import type { NotebookCell } from "../../../../../types/node.types";
import CellOutput from "./CellOutput.vue";
import { buildNotebookEditorExtensions } from "./notebookEditor";
import type { UpstreamColumn } from "./useUpstreamColumns";

interface Props {
  cell: NotebookCell;
  /** Identifies the open notebook this cell's editor view belongs to. */
  ownerId: string;
  cellIndex: number;
  /** Execution identity and staleness for this cell; absent until it is first touched. */
  runtime?: CellRuntime | null;
  /** True while any batch is running in this notebook. */
  busy?: boolean;
  isLastCell: boolean;
  cellCount: number;
  /** Structural edits (reorder, insert, delete) are blocked while the notebook is running. */
  structuralDisabled?: boolean;
  dragging?: boolean;
  active?: boolean;
  inputNames?: string[];
  upstreamColumns?: UpstreamColumn[];
  priorCellCodes?: string[];
  kernelId?: string | null;
  flowId?: number;
  nodeId?: number;
}

const props = withDefaults(defineProps<Props>(), {
  runtime: null,
  busy: false,
  structuralDisabled: false,
  dragging: false,
  active: false,
  inputNames: () => [],
  upstreamColumns: () => [],
  priorCellCodes: () => [],
  kernelId: null,
  flowId: 0,
  nodeId: 0,
});

const emit = defineEmits<{
  (e: "update:code", code: string): void;
  (e: "run-cell"): void;
  (e: "run-cell-and-advance"): void;
  (e: "move-up"): void;
  (e: "move-down"): void;
  (e: "move-key", direction: -1 | 1): void;
  (e: "drag-start", ev: PointerEvent): void;
  (e: "duplicate"): void;
  (e: "insert-above"): void;
  (e: "insert-below"): void;
  (e: "focus"): void;
  (e: "delete"): void;
}>();

// Keyed by the live prop: Vue reuses cell instances across owners when two notebooks share ids.
const pres = computed(() => cellPresentation(props.ownerId, props.cell.id));

const status = computed(() => props.runtime?.status ?? "idle");

const cellClasses = computed(() => ({
  "cell--active": props.active,
  "cell--executing": status.value === "running",
  "cell--queued": status.value === "queued",
  "cell--error": props.cell.output?.error,
  "is-dragging": props.dragging,
}));

const collapsedCodeLabel = computed(() => {
  const lines = props.cell.code.split("\n").length;
  return `Code hidden · ${lines} line${lines === 1 ? "" : "s"}`;
});

const cellExtensions = [
  ...buildNotebookEditorExtensions({
    onRun: () => emit("run-cell"),
    onRunAdvance: () => emit("run-cell-and-advance"),
    getInputNames: () => props.inputNames,
    getUpstreamColumns: () => props.upstreamColumns,
    getPriorCellCodes: () => props.priorCellCodes,
    getKernelId: () => props.kernelId,
    getFlowId: () => props.flowId,
    getNodeId: () => props.nodeId,
  }),
  EditorView.updateListener.of((u) => {
    if (u.focusChanged && u.view.hasFocus) emit("focus");
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
.cell-wrapper {
  display: flex;
  border: 1px solid transparent;
  border-left: 3px solid transparent;
  transition: border-color 0.15s;
  margin-bottom: 2px;
}

.cell-wrapper:hover,
.cell-wrapper:focus-within {
  border-color: var(--el-border-color);
  border-left-color: var(--el-color-primary);
}

.cell-wrapper.cell--active {
  border-color: var(--el-border-color);
  border-left-color: var(--el-color-primary);
}

.cell-wrapper.cell--queued {
  border-left-color: var(--el-text-color-placeholder);
}

.cell-wrapper.cell--executing {
  border-left-color: var(--el-color-warning);
}

.cell-wrapper.cell--error {
  border-left-color: var(--el-color-danger);
}

.cell-wrapper.is-dragging {
  opacity: 0.55;
}

.cell-gutter {
  width: 36px;
  flex-shrink: 0;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding-top: 0.4rem;
  font-family: "Fira Code", monospace;
  font-size: 0.65rem;
  color: var(--el-text-color-placeholder);
  user-select: none;
}

.cell-content {
  flex: 1;
  min-width: 0;
}

.cell-toolbar {
  display: flex;
  align-items: center;
  gap: 0.2rem;
  padding: 0.1rem 0.25rem;
  opacity: 0;
  transition: opacity 0.15s;
  font-size: 0.7rem;
}

.cell-wrapper:hover .cell-toolbar,
.cell-wrapper:focus-within .cell-toolbar {
  opacity: 1;
}

.cell-toolbar button {
  background: none;
  border: none;
  cursor: pointer;
  padding: 0.15rem 0.3rem;
  border-radius: 2px;
  color: var(--el-text-color-secondary);
  font-size: 0.65rem;
  line-height: 1;
}

.cell-toolbar button:hover:not(:disabled) {
  background: var(--el-fill-color);
  color: var(--el-text-color-primary);
}

.cell-toolbar button:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

.cell-toolbar .nb-drag-handle {
  touch-action: none;
  cursor: grab;
  user-select: none;
}

.cell-wrapper.is-dragging .nb-drag-handle {
  cursor: grabbing;
}

.cell-toolbar .nb-drag-handle:disabled {
  cursor: not-allowed;
}

/* The shared menu button ships at 24px; the node toolbar is denser. */
.cell-toolbar :deep(.nb-cell-menu) {
  width: 20px;
  height: 20px;
  font-size: 0.65rem;
}

.nb-cell-collapsed {
  display: flex;
  align-items: center;
  gap: 0.35rem;
  width: 100%;
  padding: 0.25rem 0.5rem;
  border: 1px dashed var(--el-border-color-lighter);
  border-radius: 3px;
  background: var(--el-fill-color-lighter);
  cursor: pointer;
  font-size: 0.7rem;
  color: var(--el-text-color-secondary);
}

.nb-cell-collapsed:hover {
  border-color: var(--el-color-primary);
  color: var(--el-color-primary);
}

.cell-editor-wrapper {
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 3px;
  overflow: hidden;
}

.cell-status-badge {
  margin: 0.15rem 0 0.15rem 0.25rem;
}

.cell-executing {
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  color: var(--el-color-warning);
}
</style>
