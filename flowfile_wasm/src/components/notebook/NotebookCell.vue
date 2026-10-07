<template>
  <article class="cell" :class="classes" :data-cell-id="cell.cell_id">
    <div class="cell-rail">
      <button
        v-if="hasRun"
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
      <header class="cell-head" @click="emit('pick')">
        <button
          class="cell-label"
          type="button"
          :disabled="cell.kind !== 'node'"
          :title="cell.kind === 'node' ? 'Show this step on the canvas' : undefined"
        >
          <template v-if="cell.kind === 'imports'">Imports</template>
          <template v-else-if="cell.fresh">New cell</template>
          <template v-else-if="cell.detached">Detached cell</template>
          <template v-for="(step, index) in steps" v-else :key="step.id">
            <span v-if="index" class="cell-step-arrow">{{ ' → ' }}</span>
            <span class="cell-step"><span class="cell-step-id">#{{ step.id }}</span> {{ step.name }}</span>
          </template>
        </button>
        <span
          v-if="notebook.canvasOnly(cell)"
          class="cell-badge cell-badge--canvas"
          data-badge="canvas-only"
          title="A push cannot change this step yet: change it on the canvas, or write a new step in a cell below"
        >
          <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="11" width="14" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" /></svg>
          edit on the canvas
        </span>
        <span v-if="cell.status === 'placeholder'" class="cell-badge cell-badge--canvas" :title="cell.reason ?? ''">
          <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="11" width="14" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" /></svg>
          stays on the canvas
        </span>
        <span
          v-if="syncState"
          class="cell-badge"
          :class="`cell-badge--${syncState}`"
          data-sync-state
        >
          <span class="cell-badge-dot" aria-hidden="true"></span>
          {{ SYNC_LABELS[syncState] }}
        </span>
        <span class="cell-actions">
          <button
            v-if="cell.detached"
            class="note-action"
            data-action="adopt"
            title="Make this text a new cell: a push adds every step in it"
            @click.stop="notebook.adoptDetached(cell.cell_id)"
          >
            Use as a new cell
          </button>
          <button
            v-if="cell.fresh || cell.detached"
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
            :data-copied="copied"
            :title="copied ? 'Copied' : `Copy this cell's code`"
            aria-label="Copy this cell's code"
            @click.stop="emit('copy')"
          >
            <svg v-if="copied" class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="m5 12 5 5 9-10" /></svg>
            <svg v-else class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2" /><path d="M5 15V6a2 2 0 0 1 2-2h9" /></svg>
          </button>
        </span>
      </header>
      <!-- indent-with-tab off: Tab takes a completion or moves focus, never indents (WCAG 2.1.2). -->
      <div class="cell-editor">
        <Codemirror
          :model-value="notebook.cellCode(cell)"
          :extensions="extensions"
          :disabled="!notebook.isEditable(cell)"
          :indent-with-tab="false"
          @update:model-value="notebook.setCellCode(cell.cell_id, $event)"
          @ready="emit('ready', $event.view)"
        />
      </div>
      <p v-if="notebook.syncError?.cellId === cell.cell_id" class="cell-error" role="alert">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 8v5M12 16.5v.5" /></svg>
        <span>
          {{ syncErrorText(notebook.syncError) }}
          <span v-if="notebook.syncError.kind === 'needs_kernel'" class="nb-links">
            <a class="nb-link" data-full-app="kernel" :href="FULL_APP_KERNEL_URL" target="_blank" rel="noopener">
              The full version runs any Python cell on a kernel
              <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 16 16 8M9 8h7v7" /></svg>
            </a>
            <a class="nb-link" data-full-app="download" :href="FULL_APP_INSTALL_URL" target="_blank" rel="noopener">
              <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4v11M7 10l5 5 5-5M5 20h14" /></svg>
              Download the full version
            </a>
          </span>
        </span>
      </p>
      <p v-else-if="cell.detached" class="cell-note" data-note="detached">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 11v6M12 7.5v.5" /></svg>
        <span>
          The canvas changed while this cell was being edited, so this text no longer matches a step. It is kept
          here and never pushed: copy what you need, or use it as a new cell.
        </span>
      </p>
      <p v-else-if="syncState === 'plain'" class="cell-note" data-note="plain">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 11v6M12 7.5v.5" /></svg>
        <span>
          No frame here is given a name, so nothing is added to the flow. Write <code>name = …</code> to add a step.
        </span>
      </p>
      <CellOutput
        v-if="notebook.outputs[cell.cell_id]"
        :output="notebook.outputs[cell.cell_id]"
        :stale="notebook.outputStale(cell.cell_id)"
      />
    </div>
    <button
      class="cell-add"
      data-action="add-below"
      title="Add a cell here"
      aria-label="Add a cell below this one"
      @click="emit('add-below')"
    >
      <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 6v12M6 12h12" /></svg>
      <span class="cell-add-label">Code</span>
    </button>
  </article>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Codemirror } from 'vue-codemirror'
import type { Extension } from '@codemirror/state'
import type { EditorView } from '@codemirror/view'
import { useFlowStore } from '../../stores/flow-store'
import { useNotebookStore, type CellSyncError, type CellSyncState, type NotebookCell } from '../../stores/notebook-store'
import { getNodeDescription } from '../../config/nodeDescriptions'
import CellOutput from './CellOutput.vue'
import { FULL_APP_INSTALL_URL, FULL_APP_KERNEL_URL } from './fullAppLinks'

/** One cell of the notebook: its steps, its state, its editor and what its last run showed. */
const props = defineProps<{
  cell: NotebookCell
  extensions: Extension[]
  /** Its code was just copied. */
  copied: boolean
}>()

const emit = defineEmits<{
  /** The header was picked: show the cell's step on the canvas. */
  pick: []
  copy: []
  'add-below': []
  ready: [view: EditorView]
}>()

const flowStore = useFlowStore()
const notebook = useNotebookStore()

const SYNC_LABELS: Record<CellSyncState, string> = {
  new: 'New',
  edited: 'Edited',
  synced: 'Synced',
  failed: "Can't push",
  plain: 'Not a step',
  detached: 'Not on the canvas'
}

const syncState = computed(() => notebook.cellSyncState(props.cell.cell_id))

const classes = computed<Record<string, boolean>>(() => {
  const state = syncState.value
  const selected = flowStore.selectedNodeId !== null && props.cell.node_ids.includes(flowStore.selectedNodeId)
  return {
    'cell--placeholder': props.cell.status === 'placeholder',
    'cell--selected': selected,
    'cell--editable': notebook.isEditable(props.cell),
    'cell--new': state === 'new',
    'cell--plain': state === 'plain',
    'cell--edited': state === 'edited',
    'cell--failed': state === 'failed'
  }
})

/** A cell has a Run when there is a step to run: one the canvas holds, or one written in a new cell. */
const hasRun = computed(() => {
  const cell = props.cell
  if (cell.kind !== 'node' || cell.detached) return false
  if (cell.node_ids.length) return true
  return !!cell.fresh && notebook.cellCode(cell).trim() !== '' && syncState.value !== 'plain'
})

/** The steps the cell stands for, each by its id and the name it has on the canvas. */
const steps = computed(() =>
  props.cell.node_ids.map(id => {
    const node = flowStore.getNode(id)
    return { id, name: node ? node.description || getNodeDescription(node.type).title || node.type : '' }
  })
)

function syncErrorText(failure: CellSyncError): string {
  return failure.line ? `Line ${failure.line}: ${failure.message}` : failure.message
}
</script>

<style scoped>
.cell-run:focus-visible,
.cell-label:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
}

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

.cell--plain {
  border-style: dashed;
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
.cell--plain .cell-actions,
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

.cell-note {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin: 0 8px 8px 0;
  padding: 7px 10px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--nb-surface);
  font-size: 12px;
  line-height: 1.45;
  color: var(--color-text-secondary);
}

.cell-note .nb-icon {
  flex: 0 0 auto;
  margin-top: 1px;
  color: var(--color-text-muted);
}

.cell-note code {
  padding: 1px 5px;
  border-radius: var(--border-radius-sm);
  background: var(--nb-card);
  font-family: var(--font-family-mono);
  font-size: 11.5px;
  color: var(--color-text-primary);
}

.cell-error span {
  min-width: 0;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
</style>
