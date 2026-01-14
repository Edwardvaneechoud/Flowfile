<template>
  <div
    class="flowfile-editor"
    :class="[containerClass, { 'flowfile-dark': theme === 'dark' }]"
    :style="{ height, width }"
  >
    <!-- Loading Overlay -->
    <div v-if="!pyodideReady" class="loading-overlay">
      <div class="loading-content">
        <div class="spinner"></div>
        <span class="loading-text">{{ loadingStatus }}</span>
      </div>
    </div>

    <!-- Header (optional) -->
    <header v-if="showHeader" class="editor-header">
      <div class="header-left">
        <slot name="header-left">
          <div class="header-brand">
            <span class="brand-text">Flowfile Editor</span>
          </div>
        </slot>
        <div class="header-divider"></div>
        <div v-if="pyodideReady" class="ready-indicator">
          <span class="ready-dot"></span>
          <span>Ready</span>
        </div>
      </div>
      <div class="header-right">
        <slot name="header-right"></slot>
      </div>
    </header>

    <!-- Main Editor -->
    <main class="editor-main" :class="{ 'no-header': !showHeader }">
      <Canvas />
    </main>

    <!-- Theme Toggle (optional) -->
    <button
      v-if="showThemeToggle"
      class="theme-toggle-floating"
      @click="toggleTheme"
      :title="isDark ? 'Switch to light mode' : 'Switch to dark mode'"
    >
      <svg v-if="isDark" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="12" cy="12" r="5"/>
        <line x1="12" y1="1" x2="12" y2="3"/>
        <line x1="12" y1="21" x2="12" y2="23"/>
        <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/>
        <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
        <line x1="1" y1="12" x2="3" y2="12"/>
        <line x1="21" y1="12" x2="23" y2="12"/>
        <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/>
        <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
      </svg>
      <svg v-else xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
      </svg>
    </button>
  </div>
</template>

<script setup lang="ts">
import { watch, onMounted } from 'vue'
import { storeToRefs } from 'pinia'
import Canvas from './Canvas.vue'
import { useFlowStore } from '../stores/flow-store'
import { usePyodideStore } from '../stores/pyodide-store'
import { useThemeStore } from '../stores/theme-store'
import { useTheme } from '../composables/useTheme'
import type { FlowfileData, NodeResult, DataPreview } from '../types'

// =============================================================================
// Props
// =============================================================================

export interface FlowfileEditorProps {
  /**
   * Initial flow data to load (FlowfileData format)
   */
  initialFlow?: FlowfileData

  /**
   * Data to inject into source nodes (read/manual_input).
   *
   * Can be:
   * - A string: Injected into the first source node
   * - { name, content }: Injected into the first source node with filename
   * - { [nodeId]: string }: Map of node IDs to CSV content
   * - { [nodeId]: { name, content } }: Map of node IDs to named content
   *
   * This allows you to design flows with placeholder data and inject real data at runtime.
   */
  initialData?: string | { name: string; content: string } | Record<number, string | { name: string; content: string }>

  /**
   * Named input bindings - maps binding names to data.
   * Much cleaner than using node IDs!
   *
   * Example: { customers: "csv...", orders: "csv..." }
   *
   * The key matches the node's "binding_name" field (set in the flow designer).
   * This is the recommended way to bind data in embedded flows.
   */
  inputs?: Record<string, string | { name: string; content: string }>

  /**
   * v-model for named output bindings.
   * After execution, this contains results keyed by node binding_name.
   *
   * Example output: { summary: { columns: [...], data: [...] } }
   */
  outputs?: Record<string, DataPreview | null>

  /**
   * Whether to auto-execute the flow after loading initial data
   */
  autoExecute?: boolean

  /**
   * Whether to show the header bar
   */
  showHeader?: boolean

  /**
   * Whether to show the theme toggle button
   */
  showThemeToggle?: boolean

  /**
   * Theme: 'light' or 'dark' (default: 'light')
   */
  theme?: 'light' | 'dark'

  /**
   * Custom CSS class for the editor container
   */
  containerClass?: string

  /**
   * Height of the editor (default: '100%')
   */
  height?: string

  /**
   * Width of the editor (default: '100%')
   */
  width?: string
}

const props = withDefaults(defineProps<FlowfileEditorProps>(), {
  autoExecute: false,
  showHeader: false,
  showThemeToggle: true,
  theme: 'light',
  containerClass: '',
  height: '100%',
  width: '100%'
})

// =============================================================================
// Emits
// =============================================================================

const emit = defineEmits<{
  /**
   * Emitted when the flow structure changes
   */
  (e: 'flow-change', data: FlowfileData): void

  /**
   * Emitted when flow execution completes
   */
  (e: 'execution-complete', results: Map<number, NodeResult>): void

  /**
   * Emitted when execution fails
   */
  (e: 'execution-error', error: string): void

  /**
   * Emitted when Pyodide is ready
   */
  (e: 'pyodide-ready'): void

  /**
   * Emitted when a node is selected
   */
  (e: 'node-selected', nodeId: number | null): void

  /**
   * Emitted when data is loaded into a source node
   */
  (e: 'data-loaded', nodeId: number, fileName: string): void

  /**
   * v-model update for outputs - emits named results after execution
   */
  (e: 'update:outputs', outputs: Record<string, DataPreview | null>): void
}>()

// =============================================================================
// Store Setup
// =============================================================================

const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()
const themeStore = useThemeStore()

const { isReady: pyodideReady, loadingStatus } = storeToRefs(pyodideStore)
const { selectedNodeId, nodeResults, isExecuting, executionError } = storeToRefs(flowStore)

const { isDark, toggleTheme } = useTheme()

// =============================================================================
// Lifecycle
// =============================================================================

onMounted(async () => {
  // Initialize theme
  themeStore.initialize()

  // Apply theme prop
  if (props.theme === 'dark' && !isDark.value) {
    toggleTheme()
  } else if (props.theme === 'light' && isDark.value) {
    toggleTheme()
  }

  // Initialize Pyodide
  await pyodideStore.initialize()

  if (pyodideReady.value) {
    emit('pyodide-ready')

    // Load initial flow if provided
    if (props.initialFlow) {
      flowStore.importFromFlowfile(props.initialFlow)
    }

    // Load initial data if provided (legacy node ID approach)
    if (props.initialData) {
      await loadInitialData()
    }

    // Load named inputs (modern approach - uses node descriptions)
    if (props.inputs) {
      await injectNamedInputs()
    }
  }
})

// Watch for theme prop changes
watch(() => props.theme, (newTheme) => {
  if (newTheme === 'dark' && !isDark.value) {
    toggleTheme()
  } else if (newTheme === 'light' && isDark.value) {
    toggleTheme()
  }
})

// Watch for flow changes
watch(
  () => flowStore.exportToFlowfile('Current Flow'),
  (data) => {
    emit('flow-change', data)
  },
  { deep: true }
)

// Watch for execution completion
watch(isExecuting, (executing, wasExecuting) => {
  if (wasExecuting && !executing) {
    if (executionError.value) {
      emit('execution-error', executionError.value)
    } else {
      emit('execution-complete', nodeResults.value)

      // Emit named outputs for v-model binding
      const namedOutputs = buildNamedOutputs()
      emit('update:outputs', namedOutputs)
    }
  }
})

// Watch for inputs prop changes (reactive data binding)
watch(() => props.inputs, async (newInputs) => {
  if (newInputs && pyodideReady.value) {
    await injectNamedInputs()
  }
}, { deep: true })

// Watch for node selection
watch(selectedNodeId, (nodeId) => {
  emit('node-selected', nodeId)
})

// =============================================================================
// Methods
// =============================================================================

/**
 * Find all source nodes (read/manual_input) in the flow
 */
function getSourceNodes(): Array<{ id: number; type: string; binding_name?: string }> {
  return flowStore.nodeList
    .filter(node => node.type === 'read' || node.type === 'manual_input')
    .map(node => ({ id: node.id, type: node.type, binding_name: node.binding_name }))
}

/**
 * Find a node by its binding_name (for named bindings)
 */
function findNodeByBindingName(bindingName: string): number | null {
  const node = flowStore.nodeList.find(
    n => n.binding_name?.toLowerCase() === bindingName.toLowerCase()
  )
  return node?.id ?? null
}

/**
 * Inject data using named bindings (matches node binding_name)
 */
async function injectNamedInputs() {
  const inputs = props.inputs
  if (!inputs) return

  for (const [name, data] of Object.entries(inputs)) {
    const nodeId = findNodeByBindingName(name)
    if (nodeId === null) {
      console.warn(`[FlowfileEditor] No node found with binding_name "${name}"`)
      continue
    }

    if (typeof data === 'string') {
      injectDataIntoNode(nodeId, data)
    } else {
      injectDataIntoNode(nodeId, data.content, data.name)
    }
  }

  if (props.autoExecute) {
    await flowStore.executeFlow()
  }
}

/**
 * Build named outputs from execution results (keyed by node binding_name)
 */
function buildNamedOutputs(): Record<string, DataPreview | null> {
  const outputs: Record<string, DataPreview | null> = {}

  for (const node of flowStore.nodeList) {
    if (!node.binding_name) continue

    const result = flowStore.getNodeResult(node.id)
    if (result?.success && result.data) {
      outputs[node.binding_name] = result.data
    }
  }

  return outputs
}

/**
 * Inject data into a specific node
 */
function injectDataIntoNode(nodeId: number, content: string, fileName?: string) {
  const node = flowStore.getNode(nodeId)
  if (!node) {
    console.warn(`[FlowfileEditor] Node ${nodeId} not found for data injection`)
    return false
  }

  // Set the file content
  flowStore.setFileContent(nodeId, content)

  // Update filename in settings if provided
  if (fileName) {
    const settings = node.settings as any
    if (node.type === 'read') {
      flowStore.updateNodeSettings(nodeId, {
        ...settings,
        file_name: fileName,
        received_file: {
          ...settings.received_file,
          name: fileName,
          path: fileName
        }
      })
    }
  }

  emit('data-loaded', nodeId, fileName || 'data.csv')
  return true
}

/**
 * Load initial data into the flow.
 *
 * Supports multiple formats:
 * - string: Inject into first source node
 * - { name, content }: Inject into first source node with filename
 * - { [nodeId]: ... }: Inject into specific nodes by ID
 */
async function loadInitialData() {
  const data = props.initialData
  if (!data) return

  const sourceNodes = getSourceNodes()

  // Case 1: Data is a record mapping node IDs to content
  if (typeof data === 'object' && !('content' in data)) {
    // It's a Record<number, ...>
    const dataMap = data as Record<number, string | { name: string; content: string }>

    for (const [nodeIdStr, nodeData] of Object.entries(dataMap)) {
      const nodeId = parseInt(nodeIdStr, 10)

      if (typeof nodeData === 'string') {
        injectDataIntoNode(nodeId, nodeData)
      } else {
        injectDataIntoNode(nodeId, nodeData.content, nodeData.name)
      }
    }
  }
  // Case 2: Data is a single string - inject into first source node
  else if (typeof data === 'string') {
    if (sourceNodes.length > 0) {
      injectDataIntoNode(sourceNodes[0].id, data)
    } else {
      // No source nodes exist - create one
      const nodeId = flowStore.addNode('read', 100, 100)
      injectDataIntoNode(nodeId, data)
    }
  }
  // Case 3: Data is { name, content } - inject into first source node
  else if ('content' in data) {
    const namedData = data as { name: string; content: string }
    if (sourceNodes.length > 0) {
      injectDataIntoNode(sourceNodes[0].id, namedData.content, namedData.name)
    } else {
      // No source nodes exist - create one
      const nodeId = flowStore.addNode('read', 100, 100)
      injectDataIntoNode(nodeId, namedData.content, namedData.name)
    }
  }

  // Auto-execute if configured
  if (props.autoExecute) {
    await flowStore.executeFlow()
  }
}

// =============================================================================
// Expose Public API
// =============================================================================

/**
 * Public API for controlling the editor programmatically
 */
defineExpose({
  /**
   * Execute the entire flow
   */
  executeFlow: () => flowStore.executeFlow(),

  /**
   * Execute a specific node
   */
  executeNode: (nodeId: number) => flowStore.executeNode(nodeId),

  /**
   * Export the current flow as FlowfileData
   */
  exportFlow: (name?: string) => flowStore.exportToFlowfile(name || 'Exported Flow'),

  /**
   * Import a flow from FlowfileData
   */
  importFlow: (data: FlowfileData) => flowStore.importFromFlowfile(data),

  /**
   * Clear the entire flow
   */
  clearFlow: () => flowStore.clearFlow(),

  /**
   * Load CSV data into a specific node (keeps existing node settings)
   */
  loadData: (nodeId: number, content: string, fileName?: string) => {
    return injectDataIntoNode(nodeId, content, fileName)
  },

  /**
   * Inject data into source nodes.
   * - If dataMap is provided: inject into specific nodes by ID
   * - If dataMap is undefined: inject into all source nodes from props.initialData
   */
  injectData: async (
    dataMap?: Record<number, string | { name: string; content: string }>,
    autoExecute?: boolean
  ) => {
    if (dataMap) {
      for (const [nodeIdStr, nodeData] of Object.entries(dataMap)) {
        const nodeId = parseInt(nodeIdStr, 10)
        if (typeof nodeData === 'string') {
          injectDataIntoNode(nodeId, nodeData)
        } else {
          injectDataIntoNode(nodeId, nodeData.content, nodeData.name)
        }
      }
    }

    if (autoExecute) {
      await flowStore.executeFlow()
    }
  },

  /**
   * Get all source nodes (read/manual_input) in the flow
   */
  getSourceNodes,

  /**
   * Add a new node to the flow
   */
  addNode: (type: string, x: number, y: number) => flowStore.addNode(type, x, y),

  /**
   * Remove a node from the flow
   */
  removeNode: (nodeId: number) => flowStore.removeNode(nodeId),

  /**
   * Select a node
   */
  selectNode: (nodeId: number | null) => flowStore.selectNode(nodeId),

  /**
   * Get the result of a specific node
   */
  getNodeResult: (nodeId: number) => flowStore.getNodeResult(nodeId),

  /**
   * Get all node results
   */
  getAllResults: () => nodeResults.value,

  /**
   * Check if Pyodide is ready
   */
  isPyodideReady: () => pyodideReady.value,

  /**
   * Get the flow store for advanced usage
   */
  getFlowStore: () => flowStore,

  /**
   * Get the pyodide store for advanced usage
   */
  getPyodideStore: () => pyodideStore
})
</script>

<style scoped>
.flowfile-editor {
  display: flex;
  flex-direction: column;
  position: relative;
  background: var(--bg-primary, #ffffff);
  overflow: hidden;
}

.flowfile-dark {
  --bg-primary: #1a1a2e;
  --bg-secondary: #16213e;
  --text-primary: #e0e0e0;
  --text-secondary: #a0a0a0;
  --border-color: #2d3748;
}

.loading-overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(255, 255, 255, 0.9);
  z-index: 1000;
}

.flowfile-dark .loading-overlay {
  background: rgba(26, 26, 46, 0.9);
}

.loading-content {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 16px;
}

.loading-text {
  font-size: 14px;
  color: var(--text-secondary, #666);
}

.spinner {
  width: 40px;
  height: 40px;
  border: 3px solid var(--border-color, #e0e0e0);
  border-top-color: var(--accent-color, #3b82f6);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 16px;
  background: var(--bg-secondary, #f5f5f5);
  border-bottom: 1px solid var(--border-color, #e0e0e0);
  height: 50px;
  flex-shrink: 0;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 10px;
}

.header-right {
  display: flex;
  align-items: center;
  gap: 8px;
}

.header-brand {
  display: flex;
  align-items: center;
  gap: 8px;
}

.brand-text {
  font-size: 16px;
  font-weight: 600;
  color: var(--text-primary, #333);
}

.header-divider {
  width: 1px;
  height: 24px;
  background: var(--border-color, #e0e0e0);
  margin: 0 6px;
}

.ready-indicator {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--text-secondary, #666);
}

.ready-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #10b981;
}

.editor-main {
  flex: 1;
  overflow: hidden;
}

.editor-main.no-header {
  height: 100%;
}

.theme-toggle-floating {
  position: absolute;
  bottom: 20px;
  left: 20px;
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 50%;
  background: var(--bg-secondary, #ffffff);
  color: var(--text-primary, #333);
  cursor: pointer;
  transition: all 0.2s;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.theme-toggle-floating:hover {
  background: var(--bg-hover, #f0f0f0);
  transform: scale(1.05);
}

.theme-toggle-floating svg {
  width: 20px;
  height: 20px;
}
</style>
