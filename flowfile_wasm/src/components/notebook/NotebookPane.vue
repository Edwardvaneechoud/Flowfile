<template>
  <div class="notebook">
    <p v-if="!pyodideStore.isReady" class="notebook-note">
      Python is still starting up. The notebook appears as soon as it is ready.
    </p>
    <div v-else-if="notebook.error" class="notebook-note notebook-note--error" role="alert">
      <span>The notebook could not be rendered: {{ notebook.error }}</span>
      <button class="note-action" @click="notebook.render()">Try again</button>
    </div>
    <template v-else>
      <div v-if="hasNodeCells" class="notebook-bar">
        <button
          class="note-action"
          data-action="run-all"
          :disabled="!notebook.canRun"
          title="Run the whole flow and show every cell's rows"
          @click="notebook.runAll()"
        >
          Run all
        </button>
        <span v-if="notebook.running" class="bar-note">Running…</span>
      </div>
      <p v-if="!hasNodeCells && !notebook.loading" class="notebook-note">
        Add a node to the canvas and it appears here as code.
      </p>
      <p v-for="warning in notebook.warnings" :key="warning" class="notebook-note notebook-note--warning">
        {{ warning }}
      </p>
      <article
        v-for="cell in notebook.cells"
        :key="cell.cell_id"
        class="cell"
        :class="{ 'cell--placeholder': cell.status === 'placeholder', 'cell--selected': isSelected(cell) }"
        :data-cell-id="cell.cell_id"
        @click="focusCell(cell)"
      >
        <header class="cell-head">
          <span class="cell-label">{{ cellLabel(cell) }}</span>
          <span v-if="cell.status === 'placeholder'" class="cell-badge" :title="cell.reason ?? ''">
            stays on the canvas
          </span>
          <button
            v-if="cell.kind === 'node'"
            class="cell-action"
            data-action="run"
            :disabled="!notebook.canRun"
            title="Run this step and show its rows"
            @click.stop="notebook.runCell(cell.cell_id)"
          >
            Run
          </button>
          <button class="cell-action" data-action="copy" :title="`Copy this cell's code`" @click.stop="copyCell(cell)">
            {{ copiedCell === cell.cell_id ? 'Copied' : 'Copy' }}
          </button>
        </header>
        <!-- indent-with-tab off: Tab must move focus, not indent (WCAG 2.1.2). -->
        <Codemirror
          :model-value="cell.code"
          :extensions="extensions"
          :disabled="true"
          :indent-with-tab="false"
          :style="{ fontSize: '13px' }"
        />
        <CellOutput v-if="notebook.outputs[cell.cell_id]" :output="notebook.outputs[cell.cell_id]" @click.stop />
      </article>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView } from '@codemirror/view'
import { useFlowStore } from '../../stores/flow-store'
import { useNotebookStore, type NotebookCell } from '../../stores/notebook-store'
import { usePyodideStore } from '../../stores/pyodide-store'
import { getNodeDescription } from '../../config/nodeDescriptions'
import CellOutput from './CellOutput.vue'

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

const extensions = [python(), oneDark, EditorView.lineWrapping]
const copiedCell = ref<string | null>(null)
let copiedTimer: ReturnType<typeof setTimeout> | null = null
let renderTimer: ReturnType<typeof setTimeout> | null = null

const hasNodeCells = computed(() => notebook.cells.some(cell => cell.kind === 'node'))

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

function nodeName(nodeId: number): string {
  const node = flowStore.getNode(nodeId)
  if (!node) return `#${nodeId}`
  return `#${nodeId} ${node.description || getNodeDescription(node.type).title || node.type}`
}

function cellLabel(cell: NotebookCell): string {
  if (cell.kind === 'imports') return 'Imports'
  return cell.node_ids.map(nodeName).join(' → ')
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
    await navigator.clipboard.writeText(cell.code)
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
  display: flex;
  flex-direction: column;
  gap: 10px;
  height: 100%;
  padding: 12px;
  overflow-y: auto;
  box-sizing: border-box;
}

.notebook-note {
  margin: 0;
  padding: 10px 12px;
  border: 1px dashed var(--color-border-primary);
  border-radius: 6px;
  font-size: 13px;
  color: var(--color-text-secondary);
}

.notebook-note--error {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  border-style: solid;
  border-color: color-mix(in srgb, #ef4444 45%, transparent);
  color: var(--color-text-primary);
}

.notebook-note--warning {
  border-style: solid;
  border-color: color-mix(in srgb, #f59e0b 45%, transparent);
}

.note-action,
.cell-action {
  padding: 2px 8px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background: none;
  font: inherit;
  font-size: 12px;
  color: var(--color-text-secondary);
  cursor: pointer;
}

.note-action:hover:not(:disabled),
.cell-action:hover:not(:disabled) {
  color: var(--color-text-primary);
  border-color: var(--color-border-focus);
}

.note-action:disabled,
.cell-action:disabled {
  opacity: 0.5;
  cursor: default;
}

.notebook-bar {
  position: sticky;
  top: 0;
  z-index: 2;
  flex: 0 0 auto;
  display: flex;
  align-items: center;
  gap: 10px;
  margin: -12px -12px 0;
  padding: 8px 12px;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
}

.bar-note {
  font-size: 12px;
  color: var(--color-text-secondary);
}

.cell {
  flex: 0 0 auto;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  overflow: hidden;
  cursor: pointer;
}

.cell--selected {
  border-color: var(--color-border-focus);
  box-shadow: 0 0 0 1px var(--color-border-focus);
}

.cell--placeholder {
  border-style: dashed;
}

.cell-head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 5px 8px;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  font-size: 12px;
}

.cell-label {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--color-text-secondary);
}

.cell-badge {
  flex: 0 0 auto;
  padding: 1px 6px;
  border-radius: 999px;
  background: color-mix(in srgb, #f59e0b 18%, transparent);
  color: var(--color-text-primary);
}

.cell :deep(.cm-editor) {
  cursor: text;
}

.cell :deep(.cm-editor.cm-focused) {
  outline: none;
}
</style>
