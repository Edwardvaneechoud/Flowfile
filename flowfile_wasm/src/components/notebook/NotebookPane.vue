<template>
  <div ref="root" class="notebook">
    <div v-if="pyodideStore.error && !pyodideStore.isReady" class="notebook-note notebook-note--error" role="alert">
      <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 8v5M12 16.5v.5" /></svg>
      <span>Python could not start, so the notebook cannot be shown: {{ pyodideStore.error }}</span>
      <button class="note-action" data-action="start-python" @click="pyodideStore.initialize()">Try again</button>
    </div>
    <div v-else-if="!pyodideStore.isReady && !pyodideStore.isLoading" class="notebook-note">
      <span>The notebook is written by Python running in this page, which has not started yet.</span>
      <button class="note-action" data-action="start-python" @click="pyodideStore.initialize()">Start Python</button>
    </div>
    <div v-else-if="!pyodideStore.isReady" class="notebook-note">
      <span class="nb-spinner" aria-hidden="true"></span>
      <span>Python is still starting up. The notebook appears as soon as it is ready.</span>
    </div>
    <div v-else-if="notebook.error" class="notebook-note notebook-note--error" role="alert">
      <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 8v5M12 16.5v.5" /></svg>
      <span>The notebook could not be rendered: {{ notebook.error }}</span>
      <button class="note-action" @click="notebook.render()">Try again</button>
    </div>
    <template v-else>
      <NotebookToolbar v-if="hasCells" />
      <div v-if="notebook.notice" class="notebook-note notebook-note--warning notebook-note--notice" role="status">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4 3 19h18zM12 10v4M12 16.5v.5" /></svg>
        <span class="notice-text">{{ notebook.notice }}</span>
        <button class="nb-icon-btn" data-action="dismiss-notice" title="Dismiss" aria-label="Dismiss" @click="notebook.dismissNotice()">
          <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18" /></svg>
        </button>
      </div>
      <div v-if="notebook.loading && notebook.fingerprint === null" class="notebook-note" data-note="rendering">
        <span class="nb-spinner" aria-hidden="true"></span>
        <span>Rendering the notebook…</span>
      </div>
      <div v-if="!hasCells && !notebook.loading && notebook.fingerprint !== null" class="nb-empty">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 6 3 12l5 6M16 6l5 6-5 6" /></svg>
        <p>
          Add a node to the canvas and it appears here as code, or start from data written in a cell:
          <code>sales = ff.DataFrame({"product": ["a", "b"], "revenue": [1, 2]})</code>
        </p>
      </div>
      <div v-for="warning in notebook.warnings" :key="warning" class="notebook-note notebook-note--warning">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4 3 19h18zM12 10v4M12 16.5v.5" /></svg>
        <span>{{ warning }}</span>
      </div>
      <NotebookCellView
        v-for="cell in notebook.shownCells"
        :key="cell.cell_id"
        :cell="cell"
        :extensions="extensionsFor(cell)"
        :copied="copiedCell === cell.cell_id"
        @pick="focusCell(cell)"
        @copy="copyCell(cell)"
        @add-below="addCell(cell.cell_id)"
        @ready="registerView(cell.cell_id, $event)"
      />
      <button class="nb-add" data-action="add-cell" @click="addCell(null)">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 6v12M6 12h12" /></svg>
        Code cell
      </button>
      <footer v-if="hasCells" class="nb-footnote">
        <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 11v5M12 7.5v.5" /></svg>
        <div>
          <p>
            Cells here describe the flow and are never run as Python. In the full version of Flowfile a notebook can
            also run on a kernel, where any Python cell works: loops, <code>print</code>, other imports.
          </p>
          <p class="nb-links">
            <a class="nb-link" data-full-app="kernel" :href="FULL_APP_KERNEL_URL" target="_blank" rel="noopener">
              Notebook kernels in the full version
              <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M8 16 16 8M9 8h7v7" /></svg>
            </a>
            <a
              class="nb-link nb-link--button"
              data-full-app="download"
              :href="FULL_APP_INSTALL_URL"
              target="_blank"
              rel="noopener"
            >
              <svg class="nb-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4v11M7 10l5 5 5-5M5 20h14" /></svg>
              Download the full version
            </a>
          </p>
        </div>
      </footer>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import { useNotebookStore, type NotebookCell } from '../../stores/notebook-store'
import { usePyodideStore } from '../../stores/pyodide-store'
import NotebookCellView from './NotebookCell.vue'
import NotebookToolbar from './NotebookToolbar.vue'
import { FULL_APP_INSTALL_URL, FULL_APP_KERNEL_URL } from './fullAppLinks'
import { useCellEditors } from './useCellEditors'

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

const root = ref<HTMLElement | null>(null)
const { addCell, extensionsFor, registerView } = useCellEditors(root)
const copiedCell = ref<string | null>(null)
let copiedTimer: ReturnType<typeof setTimeout> | null = null
let renderTimer: ReturnType<typeof setTimeout> | null = null

const hasCells = computed(() => notebook.shownCells.some(cell => cell.kind === 'node'))

// A push that put nodes on the canvas brings the last one into view there.
watch(
  () => notebook.added.pushes,
  () => {
    const nodes = notebook.added.nodes
    if (nodes.length) emit('focus-node', nodes[nodes.length - 1])
  }
)

// The cells follow the flow: re-render when the tab is showing and the flow moved on. A run clears and
// refills the schemas a render reads, so the cells render once when it ends, not at every step of it.
watch(
  () => [props.active, pyodideStore.isReady, notebook.liveFingerprint, flowStore.isExecuting] as const,
  ([active, ready, , executing]) => {
    if (!active || !ready || executing || !notebook.stale) return
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

/* The text takes the room; a spinner keeps its own size (it is a span too). */
.notebook-note > span:not(.nb-spinner) {
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

.nb-footnote p {
  margin: 0;
}

.nb-footnote .nb-links {
  margin-top: 8px;
}

.nb-footnote code {
  font-family: var(--font-family-mono);
  font-size: 11.5px;
}
</style>

<!-- What the pane, its toolbar and its cells share: icons, the spinner, ghost buttons and links. -->
<style>
.notebook .nb-icon {
  flex: 0 0 auto;
  width: 14px;
  height: 14px;
  fill: none;
  stroke: currentColor;
  stroke-width: 2;
  stroke-linecap: round;
  stroke-linejoin: round;
}

.notebook .nb-icon--solid {
  fill: currentColor;
  stroke: none;
}

.notebook .nb-spinner {
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

.notebook .nb-icon-btn:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
}

.notebook .note-action {
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

.notebook .note-action:hover {
  border-color: var(--color-accent);
  color: var(--color-accent);
}

/* Ghost icon buttons share one recipe (the cell's Run button is one too). */
.notebook .nb-icon-btn {
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

.notebook .nb-icon-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.notebook .nb-icon-btn[data-copied='true'] {
  color: var(--color-success);
}

.notebook .nb-link {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  font-weight: var(--font-weight-medium);
  color: var(--color-accent);
  text-decoration: none;
  white-space: nowrap;
}

.notebook .nb-link:hover {
  text-decoration: underline;
}

.notebook .nb-link .nb-icon {
  width: 12px;
  height: 12px;
}

.notebook .nb-link--button {
  gap: 5px;
  padding: 3px 9px;
  border: 1px solid color-mix(in srgb, var(--color-accent) 45%, transparent);
  border-radius: var(--border-radius-md);
  background: var(--nb-card);
}

.notebook .nb-link--button:hover {
  border-color: var(--color-accent);
  text-decoration: none;
}

.notebook .nb-link:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
  border-radius: var(--border-radius-sm);
}

.notebook .nb-links {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px 14px;
  margin: 4px 0 0;
}
</style>
