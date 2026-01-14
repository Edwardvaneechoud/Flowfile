<template>
  <div class="canvas-container">
    <!-- Node List Sidebar - Using DraggablePanel -->
    <DraggablePanel
      title="Data Actions"
      initial-position="left"
      :initial-width="200"
      :initial-top="toolbarHeight"
    >
      <div class="nodes-wrapper">
        <input
          v-model="searchQuery"
          type="text"
          placeholder="Search nodes..."
          class="search-input"
        />

        <div
          v-for="category in filteredCategories"
          :key="category.name"
          class="category"
        >
          <div class="category-header" @click="category.isOpen = !category.isOpen">
            <span class="category-title">{{ category.name }}</span>
            <span class="arrow">{{ category.isOpen ? '▼' : '▶' }}</span>
          </div>
          <div v-if="category.isOpen" class="category-nodes">
            <div
              v-for="node in category.nodes"
              :key="node.type"
              class="node-item"
              draggable="true"
              @dragstart="onDragStart($event, node)"
            >
              <img :src="getIconUrl(node.icon)" :alt="node.name" class="node-icon-img" />
              <span class="node-name">{{ node.name }}</span>
            </div>
          </div>
        </div>
      </div>
    </DraggablePanel>

    <!-- Toolbar -->
    <div ref="toolbarRef" class="toolbar">
      <div class="action-buttons">
        <button
          class="action-btn run-btn"
          @click="handleRunFlow"
          :disabled="isExecuting"
          title="Run Flow (Ctrl+E)"
        >
          <span class="material-icons btn-icon">play_arrow</span>
          <span class="btn-text">{{ isExecuting ? 'Running...' : 'Run' }}</span>
        </button>
        <div class="toolbar-divider"></div>
        <button class="action-btn" @click="handleSaveFlow" title="Save Flow">
          <span class="material-icons btn-icon">save</span>
          <span class="btn-text">Save</span>
        </button>
        <button class="action-btn" @click="triggerLoadFlow" title="Load Flow">
          <span class="material-icons btn-icon">folder_open</span>
          <span class="btn-text">Open</span>
        </button>
        <input
          ref="fileInputRef"
          type="file"
          accept=".json,.yaml,.yml"
          @change="handleLoadFlow"
          style="display: none"
        />
        <DemoButton v-if="hasSeenDemo" />
        <button
          class="action-btn"
          :class="{ active: showCodeGenerator }"
          title="Generate Python Code"
          @click="showCodeGenerator = true"
        >
          <span class="material-icons btn-icon">code</span>
          <span class="btn-text">Generate code</span>
        </button>
        <div class="toolbar-divider"></div>
        <button class="action-btn danger" @click="handleClearFlow" title="Clear Flow">
          <span class="material-icons btn-icon">delete</span>
          <span class="btn-text">Clear</span>
        </button>
      </div>
    </div>

    <!-- Vue Flow Canvas -->
    <div class="flow-canvas animated-bg-gradient" @drop="onDrop" @dragover="onDragOver">
      <VueFlow
        ref="vueFlowRef"
        v-model:nodes="vueNodes"
        v-model:edges="vueEdges"
        :node-types="nodeTypes"
        :default-viewport="{ zoom: 0.5 }"
        :connection-mode="ConnectionMode.Strict"
        class="custom-node-flow"
        fit-view-on-init
        @connect="onConnect"
        @node-click="onNodeClick"
        @pane-click="onPaneClick"
        @edges-change="onEdgesChange"
        @nodes-change="onNodesChange"
      >
        <template #node-flow-node="nodeProps">
          <FlowNode
            :data="nodeProps.data"
            @delete="handleDeleteNode"
            @run="handleRunNode"
          />
        </template>
        <MiniMap />
        <Controls />
      </VueFlow>
    </div>

    <!-- Node Settings Panel -->
    <DraggablePanel
      v-if="selectedNode"
      :title="getNodeDescription(selectedNode.type).title"
      initial-position="right"
      :initial-width="450"
      :initial-top="toolbarHeight"
      :on-close="() => flowStore.selectNode(null)"
    >
      <NodeTitle
        :title="getNodeDescription(selectedNode.type).title"
        :intro="getNodeDescription(selectedNode.type).intro"
      />
      <component
        :is="getSettingsComponent(selectedNode.type)"
        :key="selectedNode.id"
        :node-id="selectedNode.id"
        :settings="selectedNode.settings"
        @update:settings="updateSettings"
      />
    </DraggablePanel>

    <!-- Data Preview Panel -->
    <DraggablePanel
      v-if="selectedNodeId !== null"
      title="Table Preview"
      initial-position="bottom"
      :initial-height="280"
      :initial-left="200"
    >
      <div class="data-preview">
        <!-- Loading state -->
        <div v-if="isPreviewLoading" class="preview-loading">
          <div class="spinner small"></div>
          <span>Loading preview...</span>
        </div>

        <!-- Data grid -->
        <template v-else-if="selectedNodeResult?.success && selectedNodeResult?.data">
          <div class="preview-header">
            <span class="row-count">{{ selectedNodeResult.data.total_rows }} rows</span>
            <span class="col-count">{{ selectedNodeResult.data.columns?.length }} columns</span>
            <button class="auto-size-btn" @click="autoSizeColumns" title="Auto-size columns">
              <span class="material-icons" style="font-size: 14px;">fit_screen</span>
            </button>
          </div>
          <div class="preview-grid-container">
            <AgGridVue
              class="ag-theme-balham"
              :rowData="rowData"
              :columnDefs="columnDefs"
              :defaultColDef="defaultColDef"
              :pagination="true"
              :paginationPageSize="100"
              :enableCellTextSelection="true"
              :suppressMenuHide="true"
              :suppressMovableColumns="false"
              :animateRows="true"
              @grid-ready="onGridReady"
              style="width: 100%; height: 100%;"
            />
          </div>
        </template>

        <!-- Error state -->
        <div v-else-if="selectedNodeResult?.error" class="error-message">
          {{ selectedNodeResult.error }}
        </div>

        <!-- Empty state -->
        <div v-else class="no-data">
          No data available. Run the flow to see results.
        </div>
      </div>
    </DraggablePanel>

    <!-- Code Generator Modal -->
    <CodeGenerator
      :is-visible="showCodeGenerator"
      @close="showCodeGenerator = false"
    />
    <!-- Missing Files Modal -->
    <MissingFilesModal
      :is-open="showMissingFilesModal"
      :missing-files="missingFiles"
      @close="showMissingFilesModal = false"
      @complete="showMissingFilesModal = false"
    />

  </div>
</template>

<script setup lang="ts">
import { ref, computed, markRaw, onMounted, onUnmounted, nextTick } from 'vue'
import { VueFlow, useVueFlow, ConnectionMode } from '@vue-flow/core'
import type { Node, Edge, Connection, NodeChange, EdgeChange } from '@vue-flow/core'
import { MiniMap } from '@vue-flow/minimap'
import { Controls } from '@vue-flow/controls'
import { useFlowStore } from '../stores/flow-store'
import { storeToRefs } from 'pinia'
import type { NodeSettings, FlowEdge, ColumnSchema } from '../types'

// AG Grid imports
import { AgGridVue } from '@ag-grid-community/vue3'
import { ClientSideRowModelModule } from '@ag-grid-community/client-side-row-model'
import { ModuleRegistry } from '@ag-grid-community/core'
import type { ColDef, GridReadyEvent, GridApi } from '@ag-grid-community/core'

// Register AG Grid modules
ModuleRegistry.registerModules([ClientSideRowModelModule])

// Components
import DraggablePanel from './common/DraggablePanel.vue'
import FlowNode from './nodes/FlowNode.vue'
import NodeTitle from './nodes/NodeTitle.vue'
import ReadCsvSettings from './nodes/ReadCsvSettings.vue'
import ManualInputSettings from './nodes/ManualInputSettings.vue'
import FilterSettings from './nodes/FilterSettings.vue'
import SelectSettings from './nodes/SelectSettings.vue'
import GroupBySettings from './nodes/GroupBySettings.vue'
import JoinSettings from './nodes/JoinSettings.vue'
import SortSettings from './nodes/SortSettings.vue'
import PolarsCodeSettings from './nodes/PolarsCodeSettings.vue'
import UniqueSettings from './nodes/UniqueSettings.vue'
import HeadSettings from './nodes/HeadSettings.vue'
import PreviewSettings from './nodes/PreviewSettings.vue'
import CodeGenerator from './CodeGenerator.vue'
import PivotSettings from './nodes/PivotSettings.vue'
import UnpivotSettings from './nodes/UnpivotSettings.vue'
import OutputSettings from './nodes/OutputSettings.vue'
import { getNodeDescription } from '../config/nodeDescriptions'
import MissingFilesModal from './MissingFilesModal.vue'
import DemoButton from './DemoButton.vue'
import { useDemo } from '../composables/useDemo'

const flowStore = useFlowStore()
const { nodes: flowNodes, edges: flowEdges, selectedNodeId, nodeResults, isExecuting } = storeToRefs(flowStore)

// Demo state
const { hasSeenDemo } = useDemo()

const fileInputRef = ref<HTMLInputElement | null>(null)
const toolbarRef = ref<HTMLElement | null>(null)
const toolbarHeight = ref(52)
const { screenToFlowCoordinate, removeNodes, updateNode } = useVueFlow()
const searchQuery = ref('')
const showCodeGenerator = ref(false)
const pendingNodeAdjustment = ref<number | null>(null)
const showMissingFilesModal = ref(false)
const missingFiles = ref<Array<{nodeId: number, fileName: string}>>([])

// Node types for Vue Flow
const nodeTypes: Record<string, any> = {
  'flow-node': markRaw(FlowNode)
}

// Get icon URL
function getIconUrl(iconFile: string): string {
  return new URL(`../assets/icons/${iconFile}`, import.meta.url).href
}

// Node definition interface
interface NodeDefinition {
  type: string
  name: string
  icon: string
  inputs: number
  outputs: number
}

interface NodeCategory {
  name: string
  isOpen: boolean
  nodes: NodeDefinition[]
}

const nodeCategories = ref<NodeCategory[]>([
  {
    name: 'Input Sources',
    isOpen: true,
    nodes: [
      { type: 'read_csv', name: 'Read CSV', icon: 'input_data.png', inputs: 0, outputs: 1 },
      { type: 'manual_input', name: 'Manual Input', icon: 'manual_input.png', inputs: 0, outputs: 1 }
    ]
  },
  {
    name: 'Transformations',
    isOpen: true,
    nodes: [
      { type: 'filter', name: 'Filter', icon: 'filter.png', inputs: 1, outputs: 1 },
      { type: 'select', name: 'Select', icon: 'select.png', inputs: 1, outputs: 1 },
      { type: 'sort', name: 'Sort', icon: 'sort.png', inputs: 1, outputs: 1 },
      { type: 'polars_code', name: 'Polars Code', icon: 'polars_code.png', inputs: 1, outputs: 1 },
      { type: 'unique', name: 'Unique', icon: 'unique.png', inputs: 1, outputs: 1 },
      { type: 'head', name: 'Take Sample', icon: 'sample.png', inputs: 1, outputs: 1 }
    ]
  },
  {
    name: 'Combine Operations',
    isOpen: true,
    nodes: [
      { type: 'join', name: 'Join', icon: 'join.png', inputs: 2, outputs: 1 }
    ]
  },
  {
    name: 'Aggregations',
    isOpen: true,
    nodes: [
      { type: 'group_by', name: 'Group By', icon: 'group_by.png', inputs: 1, outputs: 1 },
      { type: 'pivot', name: 'Pivot', icon: 'pivot.png', inputs: 1, outputs: 1 },
      { type: 'unpivot', name: 'Unpivot', icon: 'unpivot.png', inputs: 1, outputs: 1 }
    ]
  },
  {
    name: 'Output Operations',
    isOpen: true,
    nodes: [
      { type: 'preview', name: 'Preview', icon: 'view.png', inputs: 1, outputs: 0 },
      { type: 'output', name: 'Write Data', icon: 'output.png', inputs: 1, outputs: 0 }
    ]
  }
])

// Filtered categories based on search
const filteredCategories = computed(() => {
  if (!searchQuery.value) return nodeCategories.value

  const query = searchQuery.value.toLowerCase()
  return nodeCategories.value.map(cat => ({
    ...cat,
    nodes: cat.nodes.filter(n => n.name.toLowerCase().includes(query))
  })).filter(cat => cat.nodes.length > 0)
})

// Find node definition
function findNodeDef(type: string): NodeDefinition | undefined {
  for (const cat of nodeCategories.value) {
    const node = cat.nodes.find(n => n.type === type)
    if (node) return node
  }
  return undefined
}

// Convert flow nodes to Vue Flow nodes
const vueNodes = computed<Node[]>({
  get() {
    return Array.from(flowNodes.value.values()).map(node => {
      const def = findNodeDef(node.type)
      return {
        id: String(node.id),
        type: 'flow-node',
        position: { x: node.x, y: node.y },
        data: {
          id: node.id,
          type: node.type,
          label: def?.name || node.type,
          inputs: def?.inputs ?? 1,
          outputs: def?.outputs ?? 1,
          result: nodeResults.value.get(node.id)
        }
      }
    })
  },
  set(newNodes) {
    newNodes.forEach(node => {
      const id = parseInt(node.id)
      flowStore.updateNode(id, { x: node.position.x, y: node.position.y })
    })
  }
})

// Convert flow edges to Vue Flow edges
const vueEdges = computed<Edge[]>({
  get() {
    return flowEdges.value.map(edge => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourceHandle,
      targetHandle: edge.targetHandle
    }))
  },
  set() {}
})

// Selected node
const selectedNode = computed(() => {
  if (selectedNodeId.value === null) return null
  return flowNodes.value.get(selectedNodeId.value) || null
})

const selectedNodeResult = computed(() => {
  if (selectedNodeId.value === null) return null
  return nodeResults.value.get(selectedNodeId.value) || null
})

// Drag and drop handlers
let draggedNodeDef: NodeDefinition | null = null

function onDragStart(event: DragEvent, node: NodeDefinition) {
  draggedNodeDef = node
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = 'move'
    event.dataTransfer.setData('application/json', JSON.stringify(node))
  }
}

function onDragOver(event: DragEvent) {
  event.preventDefault()
  if (event.dataTransfer) {
    event.dataTransfer.dropEffect = 'move'
  }
}

function onDrop(event: DragEvent) {
  event.preventDefault()

  if (!draggedNodeDef) return

  const position = screenToFlowCoordinate({
    x: event.clientX,
    y: event.clientY
  })

  const nodeId = flowStore.addNode(draggedNodeDef.type, position.x, position.y)
  flowStore.selectNode(nodeId)

  pendingNodeAdjustment.value = nodeId
  draggedNodeDef = null
}

// Connection handler
function onConnect(connection: Connection) {
  if (!connection.source || !connection.target) return

  const edge: FlowEdge = {
    id: `e${connection.source}-${connection.target}-${connection.sourceHandle}-${connection.targetHandle}`,
    source: connection.source,
    target: connection.target,
    sourceHandle: connection.sourceHandle || 'output-0',
    targetHandle: connection.targetHandle || 'input-0'
  }

  flowStore.addEdge(edge)
}

// Node click handler
function onNodeClick(event: { node: Node }) {
  flowStore.selectNode(parseInt(event.node.id))
}

// Pane click handler
function onPaneClick() {
  flowStore.selectNode(null)
}

// Handle edge changes (deletion)
function onEdgesChange(changes: EdgeChange[]) {
  changes.forEach(change => {
    if (change.type === 'remove') {
      flowStore.removeEdge(change.id)
    }
  })
}

// Handle node changes (deletion, position)
function onNodesChange(changes: NodeChange[]) {
  changes.forEach(change => {
    if (change.type === 'remove') {
      flowStore.removeNode(parseInt(change.id))
    } else if (change.type === 'position' && change.position) {
      flowStore.updateNode(parseInt(change.id), {
        x: change.position.x,
        y: change.position.y
      })
    } else if (change.type === 'dimensions' && pendingNodeAdjustment.value === parseInt(change.id)) {
      const nodeId = change.id
      updateNode(nodeId, (node) => {
        const width = node.dimensions?.width || 0
        const height = node.dimensions?.height || 0
        return {
          position: {
            x: node.position.x - width / 55,
            y: node.position.y - height / 55
          }
        }
      })
      pendingNodeAdjustment.value = null
    }
  })
}

// Handle delete from context menu
function handleDeleteNode(nodeId: number) {
  removeNodes(String(nodeId))
  flowStore.removeNode(nodeId)
}

// Handle run from context menu
async function handleRunNode(nodeId: number) {
  await flowStore.executeNode(nodeId)
}

// Update settings
function updateSettings(settings: NodeSettings) {
  if (selectedNodeId.value !== null) {
    flowStore.updateNodeSettings(selectedNodeId.value, settings)
  }
}

// Get settings component for node type
function getSettingsComponent(type: string) {
  const components: Record<string, any> = {
    read_csv: ReadCsvSettings,
    manual_input: ManualInputSettings,
    filter: FilterSettings,
    select: SelectSettings,
    group_by: GroupBySettings,
    join: JoinSettings,
    sort: SortSettings,
    polars_code: PolarsCodeSettings,
    unique: UniqueSettings,
    head: HeadSettings,
    preview: PreviewSettings,
    pivot: PivotSettings,
    unpivot: UnpivotSettings,
    output: OutputSettings
  }
  return components[type] || null
}

// AG Grid references and state
const gridApi = ref<GridApi | null>(null)
const isPreviewLoading = computed(() => {
  if (selectedNodeId.value === null) return false
  return flowStore.isPreviewLoading(selectedNodeId.value)
})

// AG Grid column definitions generated from schema
const columnDefs = computed<ColDef[]>(() => {
  const result = selectedNodeResult.value
  if (!result?.data?.columns) return []

  // Use schema for data types if available, otherwise use columns array
  const schemaMap = new Map<string, ColumnSchema>()
  if (result.schema) {
    result.schema.forEach(col => schemaMap.set(col.name, col))
  }

  return result.data.columns.map((colName: string) => {
    const schema = schemaMap.get(colName)
    const dataType = schema?.data_type || 'Unknown'
    const isNumeric = dataType.toLowerCase().includes('float') ||
                      dataType.toLowerCase().includes('int') ||
                      dataType.toLowerCase().includes('numeric')

    return {
      field: colName,
      headerName: colName,
      headerTooltip: `Type: ${dataType}`,
      sortable: true,
      filter: true,
      resizable: true,
      minWidth: 80,
      flex: 1,
      // Format numbers nicely
      valueFormatter: isNumeric ? (params: any) => {
        if (params.value === null || params.value === undefined) return 'null'
        if (typeof params.value === 'number') {
          // Check if it's a float with decimals
          if (!Number.isInteger(params.value)) {
            return params.value.toFixed(2)
          }
        }
        return String(params.value)
      } : (params: any) => {
        if (params.value === null || params.value === undefined) return 'null'
        if (typeof params.value === 'object') return JSON.stringify(params.value)
        return String(params.value)
      }
    }
  })
})

// AG Grid row data - convert from array of arrays to array of objects
const rowData = computed(() => {
  const result = selectedNodeResult.value
  if (!result?.data?.data || !result?.data?.columns) return []

  const columns = result.data.columns
  return result.data.data.map((row: any[]) => {
    const obj: Record<string, any> = {}
    columns.forEach((colName: string, index: number) => {
      obj[colName] = row[index]
    })
    return obj
  })
})

// AG Grid default column definitions
const defaultColDef: ColDef = {
  sortable: true,
  filter: true,
  resizable: true,
  minWidth: 80,
  flex: 1,
}

// AG Grid event handlers
function onGridReady(params: GridReadyEvent) {
  gridApi.value = params.api
}

// Auto-size columns to fit content
function autoSizeColumns() {
  if (gridApi.value) {
    gridApi.value.autoSizeAllColumns()
  }
}

// Toolbar handlers
async function handleRunFlow() {
  await flowStore.executeFlow()
}

function handleSaveFlow() {
  const name = prompt('Enter flow name:', 'my_flow')
  if (name) {
    flowStore.downloadFlowfile(name)
  }
}

function triggerLoadFlow() {
  fileInputRef.value?.click()
}

async function handleLoadFlow(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]

  if (!file) return
  const result = await flowStore.loadFlowfile(file)

  if (result.success && result.missingFiles?.length) {
    showMissingFilesModal.value = true
    missingFiles.value = result.missingFiles
    console.log('[Canvas] Flow loaded successfully')
  } else {
    alert('Failed to load flow file. Please check the file format.')
  }

  input.value = ''
}

function handleClearFlow() {
  if (confirm('Are you sure you want to clear the entire flow? This cannot be undone.')) {
    flowStore.clearFlow()
  }
}

// Keyboard shortcut handler
function handleKeyDown(event: KeyboardEvent) {
  // Ctrl+E or Cmd+E to run flow
  if ((event.ctrlKey || event.metaKey) && (event.key === 'e' || event.key === 'E')) {
    event.preventDefault()
    if (!isExecuting.value) {
      handleRunFlow()
    }
  }
}

// Register keyboard shortcuts
onMounted(async () => {
  window.addEventListener('keydown', handleKeyDown)

  // Wait for DOM to be fully rendered
  await nextTick()
  // Calculate toolbar bottom position for panel positioning
  if (toolbarRef.value) {
    const rect = toolbarRef.value.getBoundingClientRect()
    toolbarHeight.value = rect.bottom
  }
})

onUnmounted(() => {
  window.removeEventListener('keydown', handleKeyDown)
})
</script>

<style scoped>
/* Vue Flow imports */
@import '@vue-flow/core/dist/style.css';
@import '@vue-flow/core/dist/theme-default.css';
@import '@vue-flow/controls/dist/style.css';
@import '@vue-flow/minimap/dist/style.css';

.canvas-container {
  display: flex;
  flex-direction: column;
  height: 100%;
  position: relative;
  background: var(--bg-primary);
}

/* Toolbar */
.toolbar {
  display: flex;
  align-items: center;
  padding: 0 16px;
  background: var(--bg-secondary);
  border-bottom: 1px solid var(--border-color);
  z-index: 100;
}

.flow-canvas {
  flex: 1;
  height: 100%;
  overflow: hidden;
}

/* Vue Flow customizations */
.custom-node-flow :deep(.vue-flow__edges) {
  filter: invert(100%);
}

.custom-node-flow :deep(.vue-flow__minimap) {
  transform: scale(75%);
  transform-origin: bottom right;
}

/* Node list styles */
.nodes-wrapper {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.search-input {
  padding: 8px 12px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  font-size: 13px;
  background: var(--bg-secondary);
  color: var(--text-primary);
}

.search-input:focus {
  outline: none;
  border-color: var(--accent-color);
}

.category {
  border-radius: var(--radius-sm);
  overflow: hidden;
}

.category-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  cursor: pointer;
  user-select: none;
}

.category-header:hover {
  background: var(--border-color);
}

.category-title {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-primary);
}

.arrow {
  font-size: 10px;
  color: var(--text-secondary);
}

.category-nodes {
  background: var(--bg-secondary);
}

.node-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  cursor: grab;
  user-select: none;
  border-bottom: 1px solid var(--border-light);
  transition: background 0.15s;
}

.node-item:last-child {
  border-bottom: none;
}

.node-item:hover {
  background: var(--bg-hover);
}

.node-item:active {
  cursor: grabbing;
}

.node-icon-img {
  width: 24px;
  height: 24px;
  object-fit: contain;
}

.node-name {
  font-size: 13px;
  color: var(--text-primary);
}

/* Data preview styles */
.data-preview {
  display: flex;
  flex-direction: column;
  height: 100%;
  position: relative;
}

.preview-header {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 8px 0;
  font-size: 12px;
  color: var(--text-secondary);
  flex-shrink: 0;
}

.auto-size-btn {
  padding: 4px 8px;
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.15s;
  color: var(--text-primary);
  margin-left: auto;
}

.auto-size-btn:hover {
  background: var(--bg-hover);
  border-color: var(--accent-color);
}

.preview-grid-container {
  flex: 1;
  overflow: hidden;
  min-height: 0;
}

.preview-loading {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 40px;
  color: var(--text-secondary);
  font-size: 13px;
}

.error-message {
  color: var(--error-color);
  padding: 12px;
  background: rgba(244, 67, 54, 0.1);
  border-radius: var(--radius-sm);
  font-size: 13px;
}

.no-data {
  color: var(--text-secondary);
  text-align: center;
  padding: 40px 20px;
  font-size: 13px;
}

/* AG Grid Theme Overrides - ensure proper colors in both themes */
.ag-theme-balham {
  --ag-background-color: var(--bg-secondary);
  --ag-header-background-color: var(--bg-tertiary);
  --ag-odd-row-background-color: var(--bg-secondary);
  --ag-row-hover-color: var(--bg-hover);
  --ag-selected-row-background-color: var(--bg-selected);
  --ag-foreground-color: var(--text-primary);
  --ag-secondary-foreground-color: var(--text-secondary);
  --ag-header-foreground-color: var(--text-primary);
  --ag-border-color: var(--border-color);
  --ag-row-border-color: var(--border-light);
  --ag-input-focus-border-color: var(--accent-color);
  --ag-range-selection-border-color: var(--accent-color);
}

/* AG Grid pagination and filter popup styling */
.ag-theme-balham .ag-paging-panel {
  background: var(--bg-tertiary);
  border-top: 1px solid var(--border-color);
}

.ag-theme-balham .ag-popup {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
}
</style>
