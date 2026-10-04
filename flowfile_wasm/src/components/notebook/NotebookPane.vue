<template>
  <div class="notebook">
    <div v-if="!pyodideStore.isReady" class="notebook-note">
      <span class="nb-spinner" aria-hidden="true"></span>
      <span>Python is still starting up. The notebook appears as soon as it is ready.</span>
    </div>
    <div v-else-if="notebook.error" class="notebook-note notebook-note--error" role="alert">
      <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 8v5M12 16.5v.5" /></svg>
      <span>The notebook could not be rendered: {{ notebook.error }}</span>
      <button class="note-action" @click="notebook.render()">Try again</button>
    </div>
    <template v-else>
      <header v-if="hasCells" class="nb-toolbar">
        <button
          class="nb-btn nb-btn--run"
          data-action="run-all"
          :disabled="!notebook.canRun"
          title="Run the whole flow and show every cell's rows. Changed cells are pushed first."
          @click="notebook.runAll()"
        >
          <svg class="nb-icon nb-icon--solid" viewBox="0 0 24 24" aria-hidden="true"><path d="M7 4.5v15l12-7.5z" /></svg>
          Run all
        </button>
        <button
          class="nb-btn nb-btn--push"
          :class="{ 'is-armed': notebook.changedCount > 0 }"
          data-action="push"
          :disabled="!notebook.canPush"
          title="Apply the changed cells to the canvas, as one step you can undo. Nothing runs."
          @click="notebook.push()"
        >
          <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 16V4M7 9l5-5 5 5M5 20h14" /></svg>
          Push
          <span v-if="notebook.changedCount" class="nb-btn-count">{{ notebook.changedCount }}</span>
        </button>
        <span class="nb-status" :data-status="status.kind" role="status">
          <span v-if="status.kind === 'busy'" class="nb-spinner" aria-hidden="true"></span>
          <span v-else class="nb-status-dot" aria-hidden="true"></span>
          {{ status.text }}
        </span>
        <span class="nb-keys" title="Shift+Enter runs a cell and moves on · Ctrl/Cmd+Enter runs it in place">
          <kbd>Shift</kbd><kbd>Enter</kbd> to run
        </span>
      </header>
      <div v-if="notebook.notice" class="notebook-note notebook-note--warning notebook-note--notice" role="status">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4 3 19h18zM12 10v4M12 16.5v.5" /></svg>
        <span class="notice-text">{{ notebook.notice }}</span>
        <button class="nb-icon-btn" data-action="dismiss-notice" title="Dismiss" aria-label="Dismiss" @click="notebook.dismissNotice()">
          <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18" /></svg>
        </button>
      </div>
      <div v-if="!hasCells && !notebook.loading" class="nb-empty">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 6 3 12l5 6M16 6l5 6-5 6" /></svg>
        <p>Add a node to the canvas and it appears here as code, or write the first step here.</p>
      </div>
      <div v-for="warning in notebook.warnings" :key="warning" class="notebook-note notebook-note--warning">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4 3 19h18zM12 10v4M12 16.5v.5" /></svg>
        <span>{{ warning }}</span>
      </div>
      <article
        v-for="cell in notebook.shownCells"
        :key="cell.cell_id"
        class="cell"
        :class="cellClasses(cell)"
        :data-cell-id="cell.cell_id"
      >
        <div class="cell-rail">
          <button
            v-if="cell.kind === 'node'"
            class="cell-run"
            data-action="run"
            :disabled="!notebook.canRun"
            title="Run this step and show its rows (Shift+Enter)"
            aria-label="Run this step"
            @click="notebook.runCell(cell.cell_id)"
          >
            <span v-if="notebook.outputs[cell.cell_id]?.state === 'running'" class="nb-spinner" aria-hidden="true"></span>
            <svg v-else class="nb-icon nb-icon--solid" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5v14l11-7z" /></svg>
          </button>
        </div>
        <div class="cell-main">
          <header class="cell-head" @click="focusCell(cell)">
            <button
              class="cell-label"
              type="button"
              :disabled="cell.kind !== 'node'"
              :title="cell.kind === 'node' ? 'Show this step on the canvas' : undefined"
            >
              <template v-if="cell.kind === 'imports'">Imports</template>
              <template v-else-if="cell.fresh">New cell</template>
              <template v-for="(step, index) in cellSteps(cell)" v-else :key="step.id">
                <span v-if="index" class="cell-step-arrow">{{ ' → ' }}</span>
                <span class="cell-step"><span class="cell-step-id">#{{ step.id }}</span> {{ step.name }}</span>
              </template>
            </button>
            <span v-if="cell.status === 'placeholder'" class="cell-badge cell-badge--canvas" :title="cell.reason ?? ''">
              <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="11" width="14" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" /></svg>
              stays on the canvas
            </span>
            <span
              v-if="notebook.cellSyncState(cell.cell_id)"
              class="cell-badge"
              :class="`cell-badge--${notebook.cellSyncState(cell.cell_id)}`"
              data-sync-state
            >
              <span class="cell-badge-dot" aria-hidden="true"></span>
              {{ SYNC_LABELS[notebook.cellSyncState(cell.cell_id)!] }}
            </span>
            <span class="cell-actions">
              <button
                v-if="cell.fresh"
                class="nb-icon-btn"
                data-action="discard"
                title="Remove this cell; nothing of it is on the canvas"
                aria-label="Remove this cell"
                @click.stop="notebook.discardCell(cell.cell_id)"
              >
                <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M5 7h14M10 7V5h4v2M7 7l1 12h8l1-12" /></svg>
              </button>
              <button
                v-if="cell.cell_id in notebook.drafts"
                class="nb-icon-btn"
                data-action="revert"
                title="Drop the change and show what the canvas says"
                aria-label="Revert this cell"
                @click.stop="notebook.setCellCode(cell.cell_id, cell.code)"
              >
                <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9h10a5 5 0 0 1 0 10H9M4 9l4-4M4 9l4 4" /></svg>
              </button>
              <button
                class="nb-icon-btn"
                data-action="copy"
                :data-copied="copiedCell === cell.cell_id"
                :title="copiedCell === cell.cell_id ? 'Copied' : `Copy this cell's code`"
                aria-label="Copy this cell's code"
                @click.stop="copyCell(cell)"
              >
                <svg v-if="copiedCell === cell.cell_id" class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="m5 12 5 5 9-10" /></svg>
                <svg v-else class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2" /><path d="M5 15V6a2 2 0 0 1 2-2h9" /></svg>
              </button>
            </span>
          </header>
          <!-- indent-with-tab off: Tab must move focus, not indent (WCAG 2.1.2). -->
          <div class="cell-editor">
            <Codemirror
              :model-value="notebook.cellCode(cell)"
              :extensions="extensionsFor(cell)"
              :disabled="!notebook.isEditable(cell)"
              :indent-with-tab="false"
              @update:model-value="notebook.setCellCode(cell.cell_id, $event)"
              @ready="registerView(cell.cell_id, $event.view)"
            />
          </div>
          <p v-if="notebook.syncError?.cellId === cell.cell_id" class="cell-error" role="alert">
            <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 8v5M12 16.5v.5" /></svg>
            <span>
              {{ syncErrorText(notebook.syncError) }}
              <a
                v-if="notebook.syncError.kind === 'needs_kernel'"
                class="nb-link"
                data-full-app
                :href="FULL_APP_NOTEBOOK_URL"
                target="_blank"
                rel="noopener"
              >
                The full Flowfile app runs any Python cell
                <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 16 16 8M9 8h7v7" /></svg>
              </a>
            </span>
          </p>
          <CellOutput v-if="notebook.outputs[cell.cell_id]" :output="notebook.outputs[cell.cell_id]" />
        </div>
        <button
          class="cell-add"
          data-action="add-below"
          title="Add a cell here"
          aria-label="Add a cell below this one"
          @click="addCell(cell.cell_id)"
        >
          <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 6v12M6 12h12" /></svg>
          <span class="cell-add-label">Code</span>
        </button>
      </article>
      <button class="nb-add" data-action="add-cell" @click="addCell(null)">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 6v12M6 12h12" /></svg>
        Code cell
      </button>
      <footer v-if="hasCells" class="nb-footnote">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 11v5M12 7.5v.5" /></svg>
        <span>
          Cells here describe the flow and are never run as Python. In the full Flowfile app a notebook can also
          run on a kernel, where any Python cell works.
          <a class="nb-link" data-full-app :href="FULL_APP_NOTEBOOK_URL" target="_blank" rel="noopener">
            See the full notebook
            <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 16 16 8M9 8h7v7" /></svg>
          </a>
        </span>
      </footer>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { Prec, type Extension } from '@codemirror/state'
import { EditorView, keymap } from '@codemirror/view'
import { useFlowStore } from '../../stores/flow-store'
import {
  useNotebookStore,
  type CellSyncError,
  type CellSyncState,
  type NotebookCell
} from '../../stores/notebook-store'
import { usePyodideStore } from '../../stores/pyodide-store'
import { getNodeDescription } from '../../config/nodeDescriptions'
import CellOutput from './CellOutput.vue'
import { notebookEditorLook } from './notebookEditor'
import { setSyncErrorMark, syncErrorLineField } from './syncErrorLine'

const props = defineProps<{
  /** The Notebook tab is the one showing; nothing renders while it is not. */
  active: boolean
}>()

const emit = defineEmits<{
  /** A cell was picked: the canvas should bring this node into view. */
  'focus-node': [nodeId: number]
}>()

const flowStore = useFlowStore()
const notebook = useNotebookStore()
const pyodideStore = usePyodideStore()

const SYNC_LABELS: Record<CellSyncState, string> = {
  new: 'New',
  edited: 'Edited',
  synced: 'Synced',
  failed: 'Sync failed'
}
/** Where the full app's notebook is described: what the browser build points to for cells it cannot run. */
const FULL_APP_NOTEBOOK_URL = 'https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/notebook.html'

const baseExtensions: Extension[] = [python(), notebookEditorLook, EditorView.lineWrapping, syncErrorLineField]
const cellExtensions = new Map<string, Extension[]>()
const views = new Map<string, EditorView>()
const copiedCell = ref<string | null>(null)
let copiedTimer: ReturnType<typeof setTimeout> | null = null
let renderTimer: ReturnType<typeof setTimeout> | null = null

const hasCells = computed(() => notebook.shownCells.some(cell => cell.kind === 'node'))
/** The cell just added, to put the caret in as soon as its editor is there. */
let awaitingFocus: string | null = null

function addCell(afterCellId: string | null) {
  awaitingFocus = notebook.addCell(afterCellId)
}

// A push that put nodes on the canvas brings the last one into view there.
watch(
  () => notebook.added.pushes,
  () => {
    const nodes = notebook.added.nodes
    if (nodes.length) emit('focus-node', nodes[nodes.length - 1])
  }
)

/** What the notebook is doing, or how it stands against the canvas. */
const status = computed<{ kind: 'busy' | 'changed' | 'synced'; text: string }>(() => {
  if (notebook.syncing) return { kind: 'busy', text: 'Pushing to the canvas…' }
  if (notebook.running) return { kind: 'busy', text: 'Running…' }
  const changed = notebook.changedCount
  if (changed) return { kind: 'changed', text: `${changed} changed ${changed === 1 ? 'cell' : 'cells'}, not on the canvas yet` }
  return { kind: 'synced', text: 'In step with the canvas' }
})

function cellClasses(cell: NotebookCell): Record<string, boolean> {
  const state = notebook.cellSyncState(cell.cell_id)
  return {
    'cell--placeholder': cell.status === 'placeholder',
    'cell--selected': isSelected(cell),
    'cell--editable': notebook.isEditable(cell),
    'cell--new': state === 'new',
    'cell--edited': state === 'edited',
    'cell--failed': state === 'failed'
  }
}

/** One extension list per cell, so the editor is configured once: Shift-Enter runs and moves on, Mod-Enter runs. */
function extensionsFor(cell: NotebookCell): Extension[] {
  if (!notebook.isEditable(cell)) return baseExtensions
  let extensions = cellExtensions.get(cell.cell_id)
  if (!extensions) {
    const cellId = cell.cell_id
    const keys = keymap.of([
      { key: 'Shift-Enter', run: () => (runAndAdvance(cellId), true) },
      { key: 'Mod-Enter', run: () => (void notebook.runCell(cellId), true) }
    ])
    extensions = [...baseExtensions, Prec.highest(keys)]
    cellExtensions.set(cellId, extensions)
  }
  return extensions
}

function runAndAdvance(cellId: string) {
  void notebook.runCell(cellId)
  const index = notebook.shownCells.findIndex(cell => cell.cell_id === cellId)
  const next = notebook.shownCells.slice(index + 1).find(cell => notebook.isEditable(cell))
  if (next) views.get(next.cell_id)?.focus()
}

function markSyncError(cellId: string, view: EditorView) {
  const failure = notebook.syncError
  const mark = failure?.cellId === cellId ? { line: failure.line, message: failure.message } : null
  view.dispatch({ effects: setSyncErrorMark.of(mark) })
}

function registerView(cellId: string, view: EditorView) {
  views.set(cellId, view)
  markSyncError(cellId, view)
  if (awaitingFocus === cellId) {
    awaitingFocus = null
    view.focus()
    view.dom.scrollIntoView({ block: 'nearest' })
  }
}

watch(
  () => notebook.syncError,
  () => views.forEach((view, cellId) => markSyncError(cellId, view))
)

// A cell that is gone takes its editor with it.
watch(
  () => notebook.shownCells,
  cells => {
    const shown = new Set(cells.map(cell => cell.cell_id))
    for (const cellId of [...views.keys()]) {
      if (!shown.has(cellId)) {
        views.delete(cellId)
        cellExtensions.delete(cellId)
      }
    }
  }
)

function syncErrorText(failure: CellSyncError): string {
  return failure.line ? `Line ${failure.line}: ${failure.message}` : failure.message
}

// The cells follow the flow: re-render when the tab is showing and the flow moved on.
watch(
  () => [props.active, pyodideStore.isReady, notebook.liveFingerprint] as const,
  ([active, ready]) => {
    if (!active || !ready || !notebook.stale) return
    if (renderTimer) clearTimeout(renderTimer)
    renderTimer = setTimeout(() => {
      renderTimer = null
      void notebook.render()
    }, 150)
  },
  { immediate: true }
)

onBeforeUnmount(() => {
  if (renderTimer) clearTimeout(renderTimer)
  if (copiedTimer) clearTimeout(copiedTimer)
})

/** The steps a cell stands for, each by its id and the name it has on the canvas. */
function cellSteps(cell: NotebookCell): Array<{ id: number; name: string }> {
  return cell.node_ids.map(id => {
    const node = flowStore.getNode(id)
    return { id, name: node ? node.description || getNodeDescription(node.type).title || node.type : '' }
  })
}

function isSelected(cell: NotebookCell): boolean {
  return flowStore.selectedNodeId !== null && cell.node_ids.includes(flowStore.selectedNodeId)
}

/** Picking a cell shows its last node on the canvas: selectNode keeps the preview bookkeeping right. */
function focusCell(cell: NotebookCell) {
  const nodeId = cell.node_ids[cell.node_ids.length - 1]
  if (nodeId === undefined || !flowStore.nodes.has(nodeId)) return
  flowStore.selectNode(nodeId)
  emit('focus-node', nodeId)
}

async function copyCell(cell: NotebookCell) {
  try {
    await navigator.clipboard.writeText(notebook.cellCode(cell))
  } catch {
    return
  }
  copiedCell.value = cell.cell_id
  if (copiedTimer) clearTimeout(copiedTimer)
  copiedTimer = setTimeout(() => (copiedCell.value = null), 1500)
}
</script>

<style scoped>
.notebook {
  /* Light mode syntax; dark mode follows below. */
  --nb-syntax-keyword: #7c3aed;
  --nb-syntax-string: #047857;
  --nb-syntax-number: #b45309;
  --nb-syntax-call: #0369a1;
  --nb-syntax-property: #334155;
  --nb-syntax-operator: #64748b;
  --nb-syntax-comment: #94a3b8;
  --nb-selection: color-mix(in srgb, var(--color-accent) 22%, transparent);
  --nb-active-line: color-mix(in srgb, var(--color-accent) 6%, transparent);
  --nb-surface: var(--color-background-secondary);
  --nb-card: var(--color-background-primary);
  --nb-danger-tint: color-mix(in srgb, var(--color-danger) 10%, transparent);

  display: flex;
  flex-direction: column;
  gap: 12px;
  height: 100%;
  /* Room at the end, so the last lines clear the layout button in the corner of the canvas. */
  padding: 0 14px 64px;
  overflow-y: auto;
  box-sizing: border-box;
  background: var(--nb-surface);
  font-family: var(--font-family-base);
}

[data-theme='dark'] .notebook {
  --nb-syntax-keyword: #c4b5fd;
  --nb-syntax-string: #86efac;
  --nb-syntax-number: #fdba74;
  --nb-syntax-call: #7dd3fc;
  --nb-syntax-property: #cbd5e1;
  --nb-syntax-operator: #94a3b8;
  --nb-syntax-comment: #64748b;
  --nb-surface: #131a2c;
  --nb-card: #1b2338;
  --nb-danger-tint: color-mix(in srgb, var(--color-danger) 16%, transparent);
}

.notebook > :first-child:not(.nb-toolbar) {
  margin-top: 14px;
}

.nb-icon {
  flex: 0 0 auto;
  width: 14px;
  height: 14px;
  fill: none;
  stroke: currentColor;
  stroke-width: 2;
  stroke-linecap: round;
  stroke-linejoin: round;
}

.nb-icon--solid {
  fill: currentColor;
  stroke: none;
}

.nb-spinner {
  flex: 0 0 auto;
  width: 12px;
  height: 12px;
  border: 2px solid currentColor;
  border-top-color: transparent;
  border-radius: 50%;
  animation: nb-spin 0.7s linear infinite;
}

@keyframes nb-spin {
  to {
    transform: rotate(360deg);
  }
}

/* Toolbar */
.nb-toolbar {
  position: sticky;
  top: 0;
  z-index: 3;
  flex: 0 0 auto;
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 0 -14px;
  padding: 9px 14px;
  border-bottom: 1px solid var(--color-border-primary);
  background: color-mix(in srgb, var(--nb-surface) 88%, transparent);
  backdrop-filter: blur(8px);
}

.nb-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 28px;
  padding: 0 12px;
  border: 1px solid transparent;
  border-radius: var(--border-radius-md);
  font: inherit;
  font-size: 12.5px;
  font-weight: var(--font-weight-medium);
  line-height: 1;
  cursor: pointer;
  transition:
    background-color var(--transition-base),
    border-color var(--transition-base),
    color var(--transition-base),
    box-shadow var(--transition-base);
}

.nb-btn:disabled {
  cursor: default;
  opacity: 0.5;
}

.nb-btn:focus-visible,
.nb-icon-btn:focus-visible,
.cell-run:focus-visible,
.cell-label:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
}

.nb-btn--run {
  background: var(--color-accent-purple);
  color: #fff;
  box-shadow: var(--shadow-xs);
}

.nb-btn--run:hover:not(:disabled) {
  background: var(--color-accent-purple-hover);
}

.nb-btn--push {
  border-color: var(--color-border-primary);
  background: var(--nb-card);
  color: var(--color-text-secondary);
}

.nb-btn--push.is-armed {
  border-color: var(--color-accent);
  background: var(--color-accent);
  color: #fff;
  box-shadow: var(--shadow-xs);
}

.nb-btn--push.is-armed:hover:not(:disabled) {
  border-color: var(--color-accent-hover);
  background: var(--color-accent-hover);
}

.nb-btn-count {
  min-width: 17px;
  padding: 2px 5px;
  border-radius: var(--border-radius-full);
  background: rgba(255, 255, 255, 0.25);
  font-size: 11px;
  font-variant-numeric: tabular-nums;
  text-align: center;
}

.nb-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  margin-left: 4px;
  overflow: hidden;
  font-size: 12px;
  color: var(--color-text-tertiary);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.nb-status-dot {
  flex: 0 0 auto;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--color-success);
}

.nb-status[data-status='changed'] {
  color: var(--color-text-secondary);
}

.nb-status[data-status='changed'] .nb-status-dot {
  background: var(--color-warning);
}

.nb-status[data-status='busy'] {
  color: var(--color-accent);
}

.nb-keys {
  flex: 0 0 auto;
  margin-left: auto;
  font-size: 11px;
  color: var(--color-text-muted);
  white-space: nowrap;
}

.nb-keys kbd {
  display: inline-block;
  margin-right: 3px;
  padding: 1px 5px;
  border: 1px solid var(--color-border-primary);
  border-bottom-width: 2px;
  border-radius: var(--border-radius-sm);
  background: var(--nb-card);
  font-family: inherit;
  font-size: 10.5px;
  color: var(--color-text-tertiary);
}

/* Notes */
.notebook-note {
  flex: 0 0 auto;
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin: 0;
  padding: 10px 12px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background: var(--nb-card);
  font-size: 12.5px;
  line-height: 1.45;
  color: var(--color-text-secondary);
}

.notebook-note > .nb-icon,
.notebook-note > .nb-spinner {
  margin-top: 2px;
}

.notebook-note > span {
  flex: 1 1 auto;
  min-width: 0;
  white-space: pre-line;
}

.notebook-note--warning {
  border-color: color-mix(in srgb, var(--color-warning) 45%, transparent);
  background: color-mix(in srgb, var(--color-warning) 9%, var(--nb-card));
  color: var(--color-text-primary);
}

.notebook-note--warning > .nb-icon {
  color: var(--color-warning);
}

.notebook-note--error {
  border-color: color-mix(in srgb, var(--color-danger) 45%, transparent);
  background: color-mix(in srgb, var(--color-danger) 8%, var(--nb-card));
  color: var(--color-text-primary);
}

.notebook-note--error > .nb-icon {
  color: var(--color-danger);
}

.note-action {
  flex: 0 0 auto;
  padding: 3px 10px;
  border: 1px solid var(--color-border-secondary);
  border-radius: var(--border-radius-md);
  background: transparent;
  font: inherit;
  font-size: 12px;
  color: var(--color-text-primary);
  cursor: pointer;
}

.note-action:hover {
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.nb-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 10px;
  padding: 36px 16px;
  border: 1px dashed var(--color-border-secondary);
  border-radius: var(--border-radius-xl);
  color: var(--color-text-tertiary);
  font-size: 13px;
  text-align: center;
}

.nb-empty .nb-icon {
  width: 22px;
  height: 22px;
  color: var(--color-text-muted);
}

.nb-empty p {
  margin: 0;
}

/* Ghost icon buttons share one recipe. */
.nb-icon-btn,
.cell-run {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  padding: 0;
  border: none;
  border-radius: var(--border-radius-md);
  background: transparent;
  color: var(--color-text-tertiary);
  cursor: pointer;
  transition:
    background-color var(--transition-fast),
    color var(--transition-fast);
}

.nb-icon-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.nb-icon-btn[data-copied='true'] {
  color: var(--color-success);
}

/* Cells */
.cell {
  position: relative;
  flex: 0 0 auto;
  display: flex;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background: var(--nb-card);
  box-shadow: var(--shadow-xs);
  transition:
    border-color var(--transition-base),
    box-shadow var(--transition-base);
}

.cell:hover {
  border-color: var(--color-border-secondary);
}

/* The state bar on the rail edge: an inset shadow follows the rounded corners. */
.cell:focus-within,
.cell--selected {
  border-color: var(--color-border-secondary);
  box-shadow:
    inset 3px 0 0 var(--color-accent),
    var(--shadow-sm);
}

.cell--edited {
  box-shadow:
    inset 3px 0 0 var(--color-warning),
    var(--shadow-xs);
}

.cell--failed,
.cell--failed:focus-within {
  border-color: color-mix(in srgb, var(--color-danger) 55%, transparent);
  box-shadow:
    inset 3px 0 0 var(--color-danger),
    var(--shadow-xs);
}

.cell--placeholder {
  border-style: dashed;
  box-shadow: none;
}

.cell--new {
  border-style: dashed;
  border-color: color-mix(in srgb, var(--color-accent) 55%, transparent);
}

/* Adding a cell: a plus between every two cells, always there and plain to see under the pointer,
   and a button at the end (so the last cell needs none). */
.cell-add {
  position: absolute;
  bottom: -11px;
  left: 50%;
  z-index: 2;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 4px;
  height: 20px;
  min-width: 20px;
  padding: 0 3px;
  border: 1px solid var(--color-border-secondary);
  border-radius: var(--border-radius-full);
  background: var(--nb-card);
  font: inherit;
  font-size: 11px;
  font-weight: var(--font-weight-medium);
  line-height: 1;
  color: var(--color-text-tertiary);
  cursor: pointer;
  opacity: 0.7;
  transform: translateX(-50%);
  transition:
    opacity var(--transition-fast),
    border-color var(--transition-fast),
    background-color var(--transition-fast),
    color var(--transition-fast);
}

.cell-add .nb-icon {
  width: 12px;
  height: 12px;
}

.cell-add-label {
  display: none;
  padding-right: 4px;
}

.cell:hover .cell-add,
.cell-add:focus-visible {
  opacity: 1;
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.cell .cell-add:hover,
.cell .cell-add:focus-visible {
  border-color: var(--color-accent);
  background: var(--color-accent);
  color: #fff;
}

.cell-add:hover .cell-add-label,
.cell-add:focus-visible .cell-add-label {
  display: inline;
}

.cell:last-of-type .cell-add {
  display: none;
}

.nb-add {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  height: 34px;
  border: 1px dashed var(--color-border-secondary);
  border-radius: var(--border-radius-lg);
  background: transparent;
  font: inherit;
  font-size: 12.5px;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-tertiary);
  cursor: pointer;
  transition:
    border-color var(--transition-base),
    color var(--transition-base),
    background-color var(--transition-base);
}

.nb-add:hover {
  border-color: var(--color-accent);
  background: color-mix(in srgb, var(--color-accent) 7%, transparent);
  color: var(--color-accent);
}

.cell-rail {
  flex: 0 0 42px;
  display: flex;
  justify-content: center;
  padding-top: 5px;
}

.cell-run {
  width: 26px;
  height: 26px;
  border-radius: 50%;
  background: color-mix(in srgb, var(--color-accent-purple) 12%, transparent);
  color: var(--color-accent-purple);
}

.cell-run:hover:not(:disabled) {
  background: var(--color-accent-purple);
  color: #fff;
}

.cell-run:disabled {
  cursor: default;
  opacity: 0.55;
}

.cell-run .nb-icon {
  width: 15px;
  height: 15px;
}

.cell-main {
  flex: 1 1 auto;
  min-width: 0;
}

.cell-head {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 36px;
  padding: 0 8px 0 0;
}

.cell-label {
  flex: 0 1 auto;
  min-width: 0;
  padding: 2px 0;
  border: none;
  background: none;
  overflow: hidden;
  font: inherit;
  font-size: 12.5px;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: pointer;
}

.cell-label:disabled {
  color: var(--color-text-tertiary);
  cursor: default;
}

.cell-label:hover:not(:disabled) .cell-step {
  color: var(--color-accent);
}

.cell-step-id {
  margin-right: 1px;
  font-family: var(--font-family-mono);
  font-size: 11px;
  font-weight: var(--font-weight-normal);
  color: var(--color-text-muted);
}

.cell-step-arrow {
  color: var(--color-text-muted);
}

.cell-badge {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  background: var(--color-background-tertiary);
  font-size: 11px;
  line-height: 1.5;
  color: var(--color-text-secondary);
  white-space: nowrap;
}

.cell-badge .nb-icon {
  width: 11px;
  height: 11px;
}

.cell-badge-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: currentColor;
}

.cell-badge--new {
  background: color-mix(in srgb, var(--color-accent) 16%, transparent);
  color: var(--color-accent);
}

.cell-badge--edited {
  background: color-mix(in srgb, var(--color-warning) 16%, transparent);
  color: var(--color-warning-hover);
}

.cell-badge--synced {
  background: color-mix(in srgb, var(--color-success) 16%, transparent);
  color: var(--color-success-hover);
}

.cell-badge--failed {
  background: color-mix(in srgb, var(--color-danger) 16%, transparent);
  color: var(--color-danger-hover);
}

[data-theme='dark'] .cell-badge--edited {
  color: #fcd34d;
}

[data-theme='dark'] .cell-badge--synced {
  color: #6ee7b7;
}

[data-theme='dark'] .cell-badge--failed {
  color: #fca5a5;
}

/* Actions wait for the pointer or the keyboard; an edited cell always shows its Revert. */
.cell-actions {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  margin-left: auto;
  opacity: 0;
  transition: opacity var(--transition-fast);
}

.cell:hover .cell-actions,
.cell:focus-within .cell-actions,
.cell--new .cell-actions,
.cell--edited .cell-actions,
.cell--failed .cell-actions {
  opacity: 1;
}

.cell-editor {
  display: block;
  margin: 0 8px 8px 0;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--nb-surface);
  overflow: hidden;
  transition: border-color var(--transition-base);
}

.cell--editable .cell-editor:hover {
  border-color: var(--color-border-primary);
}

.cell--editable .cell-editor:focus-within {
  border-color: var(--color-accent);
}

.cell--placeholder .cell-editor {
  opacity: 0.8;
}

.cell-editor :deep(.cm-editor) {
  cursor: text;
}

.cell:not(.cell--editable) .cell-editor :deep(.cm-editor) {
  cursor: default;
}

.cell-editor :deep(.nb-tok-keyword) {
  color: var(--nb-syntax-keyword);
}

.cell-editor :deep(.nb-tok-string) {
  color: var(--nb-syntax-string);
}

.cell-editor :deep(.nb-tok-number) {
  color: var(--nb-syntax-number);
}

.cell-editor :deep(.nb-tok-call) {
  color: var(--nb-syntax-call);
}

.cell-editor :deep(.nb-tok-property) {
  color: var(--nb-syntax-property);
}

.cell-editor :deep(.nb-tok-operator) {
  color: var(--nb-syntax-operator);
}

.cell-editor :deep(.nb-tok-comment) {
  color: var(--nb-syntax-comment);
  font-style: italic;
}

.cell-editor :deep(.nb-tok-name) {
  font-weight: var(--font-weight-medium);
}

.cell-editor :deep(.nb-sync-error-line) {
  background: var(--nb-danger-tint);
  box-shadow: inset 2px 0 0 var(--color-danger);
}

.cell-error {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin: 0 8px 8px 0;
  padding: 8px 10px;
  border-radius: var(--border-radius-md);
  background: var(--nb-danger-tint);
  font-size: 12px;
  line-height: 1.45;
  color: var(--color-danger-hover);
}

[data-theme='dark'] .cell-error {
  color: #fca5a5;
}

.cell-error .nb-icon {
  margin-top: 1px;
}

.cell-error span {
  min-width: 0;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.nb-link {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  font-weight: var(--font-weight-medium);
  color: var(--color-accent);
  text-decoration: none;
  white-space: nowrap;
}

.nb-link:hover {
  text-decoration: underline;
}

.nb-link .nb-icon {
  width: 12px;
  height: 12px;
}

.cell-error .nb-link {
  display: flex;
  margin-top: 4px;
}

.nb-footnote {
  flex: 0 0 auto;
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 10px 12px;
  border: 1px dashed var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  font-size: 12px;
  line-height: 1.5;
  color: var(--color-text-tertiary);
}

.nb-footnote > .nb-icon {
  margin-top: 2px;
}
</style>
