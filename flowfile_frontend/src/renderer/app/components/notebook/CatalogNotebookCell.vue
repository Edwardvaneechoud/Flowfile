<template>
  <div
    class="nb-cell"
    :class="[
      `nb-cell--${cell.cellType}`,
      {
        running: cell.execState === 'running',
        queued: runtime?.status === 'queued',
        'is-dragging': dragging,
        'nb-cell--active': active,
        'has-meta': !!execMeta,
      },
    ]"
    tabindex="-1"
    @focus="emit('activate')"
    @keydown.enter.self.exact.prevent="onRootEnter"
    @keydown.shift.enter.self.prevent="emit('run-advance')"
    @keydown.meta.enter.self.prevent="emit('run')"
    @keydown.ctrl.enter.self.prevent="emit('run')"
  >
    <!-- Left rail: run on top, reorder handle below it. -->
    <div class="nb-cell-rail">
      <button
        class="nb-run"
        :disabled="busy || cell.execState === 'running'"
        :title="
          cell.cellType === 'markdown'
            ? 'Render and advance (Shift+Enter) · Render (Cmd/Ctrl+Enter)'
            : 'Run and advance (Shift+Enter) · Run (Cmd/Ctrl+Enter)'
        "
        @click="emit('run')"
      >
        <i v-if="cell.execState === 'running'" class="fa-solid fa-spinner fa-spin"></i>
        <i v-else-if="cell.cellType === 'markdown'" class="fa-solid fa-eye"></i>
        <i v-else class="fa-solid fa-play"></i>
      </button>
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
    </div>

    <div class="nb-cell-main">
      <!-- Floating actions, shown on hover/focus/active. -->
      <div class="nb-cell-actions">
        <el-select
          v-if="!typeInMenu && allowedTypes.length > 1"
          :model-value="cell.cellType"
          size="small"
          class="nb-type-select"
          @change="(v: CellType) => emit('update:type', v)"
        >
          <el-option v-for="t in allowedTypes" :key="t" :label="TYPE_LABELS[t]" :value="t" />
        </el-select>
        <span v-else-if="!typeInMenu" class="nb-cell-type-badge">{{
          TYPE_LABELS[cell.cellType]
        }}</span>
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
        >
          <el-dropdown-item
            v-if="typeInMenu"
            data-action="toggle-type"
            :disabled="readOnly"
            @click="emit('update:type', cell.cellType === 'python' ? 'markdown' : 'python')"
          >
            <i
              class="nb-menu-icon"
              :class="cell.cellType === 'python' ? 'fa-solid fa-paragraph' : 'fa-brands fa-python'"
            ></i>
            {{ cell.cellType === "python" ? "Convert to Markdown" : "Convert to Python" }}
          </el-dropdown-item>
          <slot name="menu-extra" />
        </CellActionMenu>
      </div>

      <!-- Editor. Collapse uses v-show so the EditorView (and its text undo history) survives. -->
      <div v-show="!pres.codeCollapsed" class="nb-cell-editor">
        <span v-if="execMeta" class="nb-cell-meta" :title="execMeta.title">
          <span class="nb-cell-meta__count">[{{ execMeta.count }}]</span>
          <span>{{ execMeta.time }}</span>
        </span>
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
          placeholder="# Markdown — Render (Cmd/Ctrl+Enter) to preview"
          @update:model-value="(v: string) => emit('update:code', v)"
          @focus="emit('activate')"
          @keydown.shift.enter.prevent="emit('run-advance')"
          @keydown.meta.enter.prevent="emit('run')"
          @keydown.ctrl.enter.prevent="emit('run')"
        />
        <!-- Python code -->
        <codemirror
          v-else
          :model-value="cell.code"
          :disabled="readOnly"
          placeholder="# Python — Cmd/Ctrl+Enter to run"
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

      <div class="nb-cell-status">
        <CellStatusBadge
          :runtime="runtime"
          :has-output="cell.cellType === 'python' && !!cell.output"
        />
      </div>

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
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import { EditorView } from "@codemirror/view";
import { registerCellView, unregisterCellView } from "./editorViews";
import { cellPresentation, toggleCodeCollapsed, toggleOutputCollapsed } from "./cellPresentation";
import CellActionMenu from "./CellActionMenu.vue";
import CellStatusBadge from "./CellStatusBadge.vue";
import type { CellRuntime } from "./notebookRuntimeState";
import CellOutput from "../nodes/node-types/elements/pythonScript/CellOutput.vue";
import { buildNotebookEditorExtensions } from "../nodes/node-types/elements/pythonScript/notebookEditor";
import { formatExecutionTime } from "../nodes/node-types/elements/pythonScript/notebookDisplay";
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
  /** Execution identity + staleness for this cell; absent until it is first touched. */
  runtime?: CellRuntime | null;
  /** An execution batch is active in this notebook, so single runs are refused. */
  busy?: boolean;
  /** The cell the caret last landed in — draws the active border. */
  active?: boolean;
  dragging?: boolean;
  /** Code of cells before this one, for scope/ref completions. */
  priorCellCodes?: string[];
  /** Cells before this one with their ids, so column inference can date each assignment. */
  priorCells?: { id: string; code: string }[];
  /** Kernel + namespace identity for Jedi code intelligence (sessionFlowId as flow_id). */
  kernelId?: string | null;
  flowId?: number;
  nodeId?: number;
  readOnly?: boolean;
  /** Hide the type select; the cell menu switches between Python and Markdown instead. */
  typeInMenu?: boolean;
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

// Shown under the editor's last line, so the output block itself carries no meta row.
const execMeta = computed(() => {
  const out = props.cell.cellType === "python" ? props.cell.output : null;
  if (!out?.execution_count) return null;
  const time = formatExecutionTime(out.execution_time_ms);
  return {
    count: out.execution_count,
    time,
    title: `Execution ${out.execution_count} · ${time}`,
  };
});

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
    getPriorCells: () => props.priorCells ?? [],
    getOwnerId: () => props.ownerId,
    getCellId: () => props.cell.id,
    getSurface: () => "catalog",
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
  position: relative;
  display: flex;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background: var(--color-background-primary);
  box-shadow: var(--shadow-xs);
  transition:
    border-color var(--transition-base),
    box-shadow var(--transition-base);
}
.nb-cell:hover {
  border-color: var(--color-border-secondary);
}
.nb-cell.is-dragging {
  opacity: 0.55;
}
/* State bar on the rail edge: an inset shadow follows the card's rounded corners. */
.nb-cell.queued {
  box-shadow:
    inset 2px 0 0 var(--color-border-secondary),
    var(--shadow-xs);
}
.nb-cell.nb-cell--active,
.nb-cell.running {
  border-color: var(--color-border-secondary);
  box-shadow:
    inset 2px 0 0 var(--color-accent),
    var(--shadow-xs);
}

.nb-cell-rail {
  display: flex;
  flex: 0 0 28px;
  flex-direction: column;
  align-items: center;
  gap: 2px;
  /* Centres the run button on the first code line. */
  padding-top: calc(var(--spacing-2) + 5px);
}
.nb-cell-main {
  position: relative;
  flex: 1;
  min-width: 0;
  padding: var(--spacing-2) var(--spacing-2) var(--spacing-2) 0;
}

/* Ghost icon buttons share one recipe. */
.nb-drag-handle,
.nb-run,
.nb-act {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  padding: 0;
  border: none;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
  cursor: pointer;
  transition:
    background-color var(--transition-fast),
    color var(--transition-fast),
    opacity var(--transition-fast);
}

/* Reorder handle: stays in the tab order, visible on hover, focus or when active. */
.nb-drag-handle {
  opacity: 0;
  cursor: grab;
  touch-action: none;
  user-select: none;
}
.nb-cell:hover .nb-drag-handle,
.nb-cell:focus-within .nb-drag-handle,
.nb-cell--active .nb-drag-handle {
  opacity: 1;
  color: var(--color-text-tertiary);
}
.nb-drag-handle:hover:not(:disabled) {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}
.nb-cell.is-dragging .nb-drag-handle {
  cursor: grabbing;
}
.nb-cell .nb-drag-handle:disabled {
  opacity: 0.35;
  cursor: not-allowed;
}

.nb-run {
  color: var(--color-text-secondary);
}
.nb-cell:hover .nb-run:not(:disabled),
.nb-cell:focus-within .nb-run:not(:disabled),
.nb-cell--active .nb-run:not(:disabled),
.nb-cell.running .nb-run {
  color: var(--color-accent);
}
.nb-run:hover:not(:disabled) {
  background: var(--color-accent-subtle);
  color: var(--color-accent-dark);
}
.nb-run:disabled {
  cursor: not-allowed;
}
.nb-run .fa-play {
  margin-left: 1px; /* optical-center the triangle */
}

/* Floating toolbar on the card's top-right edge, clear of the code. */
.nb-cell-actions {
  position: absolute;
  top: -12px;
  right: var(--spacing-3);
  z-index: 2;
  display: flex;
  align-items: center;
  gap: 1px;
  padding: 1px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  box-shadow: var(--shadow-xs);
  opacity: 0;
  transition: opacity var(--transition-fast);
}
.nb-cell:hover .nb-cell-actions,
.nb-cell:focus-within .nb-cell-actions,
.nb-cell--active .nb-cell-actions {
  opacity: 1;
}
.nb-act {
  color: var(--color-text-tertiary);
}
.nb-act:hover:not(:disabled) {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}
.nb-act:disabled {
  opacity: 0.35;
  cursor: not-allowed;
}

/* Catalog type selector: a compact ghost select inside the toolbar. */
.nb-type-select {
  width: 96px;
}
.nb-type-select :deep(.el-select__wrapper) {
  min-height: 22px;
  padding: 0 6px 0 8px;
  border-radius: var(--border-radius-sm);
  background: transparent;
  box-shadow: none;
  font-size: var(--font-size-xs);
}
.nb-type-select :deep(.el-select__wrapper:hover),
.nb-type-select :deep(.el-select__wrapper.is-focused) {
  background: var(--color-background-tertiary);
  box-shadow: none;
}
.nb-type-select :deep(.el-select__placeholder) {
  color: var(--color-text-secondary);
  font-weight: var(--font-weight-medium);
}
.nb-cell-type-badge {
  padding: 0 6px;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-tertiary);
}

.nb-cell-editor {
  position: relative;
}
.nb-cell-editor :deep(.cm-editor) {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}
.nb-cell-editor :deep(.cm-content) {
  min-height: 0;
}
.nb-cell-editor :deep(.cm-gutters) {
  min-width: 0;
}
.nb-cell-editor :deep(.cm-lineNumbers .cm-gutterElement) {
  min-width: calc(2ch + 8px);
  padding: 0 4px;
}
/* The lint gutter only takes room while it has a marker to show. */
.nb-cell-editor :deep(.cm-gutter-lint:not(:has(.cm-lint-marker))) {
  width: 0;
  overflow: hidden;
}
.nb-cell-editor :deep(.el-textarea__inner) {
  font-family: var(--font-family-mono);
  font-size: 12.5px;
}
.nb-cell-meta {
  position: absolute;
  right: var(--spacing-2);
  bottom: 3px;
  z-index: 1;
  display: inline-flex;
  align-items: baseline;
  gap: var(--spacing-1);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  font-variant-numeric: tabular-nums;
  line-height: 16px;
  white-space: nowrap;
  pointer-events: none;
}
.nb-cell-meta__count {
  font-family: var(--font-family-mono);
}
/* Room under the last line for the bottom-right meta. */
.nb-cell.has-meta .nb-cell-editor :deep(.cm-content) {
  padding-bottom: 20px;
}

.nb-md-rendered {
  padding: 2px 4px;
  color: var(--color-text-primary);
  font-size: var(--font-size-base);
  line-height: var(--line-height-relaxed);
  cursor: text;
}
.nb-md-rendered :deep(h1),
.nb-md-rendered :deep(h2),
.nb-md-rendered :deep(h3) {
  margin: 0.4em 0 0.3em;
  font-weight: var(--font-weight-semibold);
  line-height: var(--line-height-tight);
}
.nb-md-rendered :deep(h1) {
  font-size: var(--font-size-2xl);
}
.nb-md-rendered :deep(h2) {
  font-size: var(--font-size-xl);
}
.nb-md-rendered :deep(strong) {
  font-weight: var(--font-weight-semibold);
}
.nb-md-rendered :deep(p) {
  margin: 0.3em 0;
}
.nb-md-rendered :deep(code) {
  padding: 1px 5px;
  border-radius: var(--border-radius-sm);
  background: var(--color-background-tertiary);
  font-family: var(--font-family-mono);
  font-size: 0.9em;
}
.nb-md-rendered :deep(em:only-child) {
  color: var(--color-text-muted);
}

.nb-cell-status {
  display: flex;
  padding-top: var(--spacing-1-5);
}
.nb-cell-status:empty {
  display: none;
}

/* The editor carries count + timing, so the output drops its own meta row. */
.nb-cell-main > :deep(.cell-output) {
  padding: var(--spacing-2) 0 0;
}
.nb-cell-main > :deep(.cell-output .output-meta) {
  display: none;
}

/* Stub row standing in for hidden code/output; click restores it. */
.nb-cell-collapsed {
  display: flex;
  align-items: center;
  width: 100%;
  padding: 6px 10px;
  border: 1px dashed var(--color-border-secondary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  color: var(--color-text-tertiary);
  font-family: inherit;
  font-size: var(--font-size-sm);
  text-align: left;
  cursor: pointer;
  transition:
    border-color var(--transition-fast),
    color var(--transition-fast);
}
.cell-output + .nb-cell-collapsed {
  margin-top: var(--spacing-2);
}
.nb-cell-collapsed:hover {
  border-color: var(--color-accent);
  color: var(--color-text-primary);
}
</style>
