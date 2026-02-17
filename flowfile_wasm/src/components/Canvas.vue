<template>
  <div class="canvas-container">
    <!-- Node List Sidebar - Using DraggablePanel -->
    <DraggablePanel
      title="Data Actions"
      panel-id="node-list-sidebar"
      initial-position="left"
      :initial-width="200"
      :initial-top="toolbarHeight"
      :default-z-index="100"
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
          v-if="effectiveToolbar.showRun"
          class="action-btn run-btn"
          @click="handleRunFlow"
          :disabled="isExecuting"
          title="Run Flow (Ctrl+E)"
        >
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
          <span class="btn-text">{{ isExecuting ? 'Running...' : 'Run' }}</span>
        </button>
        <div v-if="effectiveToolbar.showRun" class="toolbar-divider"></div>
        <button v-if="effectiveToolbar.showSaveLoad" class="action-btn" @click="handleSaveFlow" title="Save Flow">
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
          <span class="btn-text">Save</span>
        </button>
        <button v-if="effectiveToolbar.showSaveLoad" class="action-btn" @click="triggerLoadFlow" title="Load Flow">
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
          <span class="btn-text">Open</span>
        </button>
        <input
          ref="fileInputRef"
          type="file"
          accept=".json,.yaml,.yml"
          @change="handleLoadFlow"
          style="display: none"
        />
        <DemoButton v-if="effectiveToolbar.showDemo && hasSeenDemo" />
        <button
          v-if="effectiveToolbar.showCodeGen"
          class="action-btn"
          :class="{ active: showCodeGenerator }"
          title="Generate Python Code"
          @click="showCodeGenerator = true"
        >
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
          <span class="btn-text">Generate code</span>
        </button>
        <div v-if="effectiveToolbar.showClear" class="toolbar-divider"></div>
        <button v-if="effectiveToolbar.showClear" class="action-btn danger" @click="handleClearFlow" title="Clear Flow">
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
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
      panel-id="node-settings-panel"
      initial-position="right"
      :initial-width="450"
      :initial-top="toolbarHeight"
      :default-z-index="120"
      :on-close="() => flowStore.selectNode(null)"
    >
      <NodeTitle
        :title="getNodeDescription(selectedNode.type).title"
        :intro="getNodeDescription(selectedNode.type).intro"
      />
      <NodeSettingsWrapper
        :node-id="selectedNode.id"
        :settings="selectedNode.settings"
      >
        <component
          :is="getSettingsComponent(selectedNode.type)"
          :key="selectedNode.id"
          :node-id="selectedNode.id"
          :settings="selectedNode.settings"
          @update:settings="updateSettings"
        />
      </NodeSettingsWrapper>
    </DraggablePanel>

    <!-- Data Preview Panel (hidden for explore_data nodes which have their own preview) -->
    <DraggablePanel
      v-if="selectedNodeId !== null && selectedNode?.type !== 'explore_data'"
      title="Table Preview"
      panel-id="data-preview-panel"
      initial-position="bottom"
      :initial-height="280"
      :initial-left="200"
      :default-z-index="110"
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
              <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 3h6v6"/><path d="M9 21H3v-6"/><path d="M21 3l-7 7"/><path d="M3 21l7-7"/></svg>
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

    <!-- Layout Controls Button -->
    <LayoutControls @reset-layout="handleResetLayout" />

    <!-- Teleport target for context menus (inside CSS variable scope, outside VueFlow transforms) -->
    <div id="flowfile-context-menu-container"></div>

  </div>
</template>

<script setup lang="ts">
import { ref, computed, markRaw, onMounted, onUnmounted, nextTick, defineAsyncComponent } from 'vue'
import { VueFlow, useVueFlow, ConnectionMode } from '@vue-flow/core'
import type { Node, Edge, Connection, NodeChange, EdgeChange } from '@vue-flow/core'
import { MiniMap } from '@vue-flow/minimap'
import { Controls } from '@vue-flow/controls'
import { useFlowStore } from '../stores/flow-store'
import { storeToRefs } from 'pinia'
import type { NodeSettings, FlowEdge, ColumnSchema, NodeResult } from '../types'
import type { ToolbarConfig, NodeCategoryConfig } from '../lib/types'
import { iconUrls } from '../utils/iconUrls'

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
import ExternalDataSettings from './nodes/ExternalDataSettings.vue'
import FilterSettings from './nodes/FilterSettings.vue'
import SelectSettings from './nodes/SelectSettings.vue'
import GroupBySettings from './nodes/GroupBySettings.vue'
import JoinSettings from './nodes/JoinSettings.vue'
import SortSettings from './nodes/SortSettings.vue'
const PolarsCodeSettings = defineAsyncComponent(() => import('./nodes/PolarsCodeSettings.vue'))
import UniqueSettings from './nodes/UniqueSettings.vue'
import HeadSettings from './nodes/HeadSettings.vue'
import PreviewSettings from './nodes/PreviewSettings.vue'
const CodeGenerator = defineAsyncComponent(() => import('./CodeGenerator.vue'))
import PivotSettings from './nodes/PivotSettings.vue'
import UnpivotSettings from './nodes/UnpivotSettings.vue'
import OutputSettings from './nodes/OutputSettings.vue'
import ExternalOutputSettings from './nodes/ExternalOutputSettings.vue'
import NodeSettingsWrapper from './nodes/NodeSettingsWrapper.vue'
import { getNodeDescription } from '../config/nodeDescriptions'
import MissingFilesModal from './MissingFilesModal.vue'
import DemoButton from './DemoButton.vue'
import LayoutControls from './common/LayoutControls.vue'
import { useDemo } from '../composables/useDemo'

// Props for embeddable configuration
const props = withDefaults(defineProps<{
  toolbarConfig?: ToolbarConfig
  nodeCategoriesConfig?: NodeCategoryConfig[]
  readonly?: boolean
}>(), {
  readonly: false
})

const emit = defineEmits<{
  (e: 'execution-complete', results: Map<number, NodeResult>): void
  (e: 'output', data: { nodeId: number; content: string; fileName: string; mimeType: string }): void
}>()

// Merge toolbar config with defaults (all visible by default)
const effectiveToolbar = computed<Required<ToolbarConfig>>(() => ({
  showRun: props.toolbarConfig?.showRun !== false,
  showSaveLoad: props.toolbarConfig?.showSaveLoad !== false,
  showClear: props.toolbarConfig?.showClear !== false,
  showCodeGen: props.toolbarConfig?.showCodeGen !== false,
  showDemo: props.toolbarConfig?.showDemo ?? true
}))

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

// Get icon URL - uses explicit imports for library build compatibility
function getIconUrl(iconFile: string): string {
  return iconUrls[iconFile] || new URL(`../assets/icons/${iconFile}`, import.meta.url).href
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
      { type: 'read', name: 'Read CSV', icon: 'input_data.png', inputs: 0, outputs: 1 },
      { type: 'manual_input', name: 'Manual Input', icon: 'manual_input.png', inputs: 0, outputs: 1 },
      { type: 'external_data', name: 'External Data', icon: 'external_data.svg', inputs: 0, outputs: 1 }
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
      { type: 'explore_data', name: 'Preview', icon: 'view.png', inputs: 1, outputs: 0 },
      { type: 'output', name: 'Write Data', icon: 'output.png', inputs: 1, outputs: 0 },
      { type: 'external_output', name: 'External Output', icon: 'external_output.svg', inputs: 1, outputs: 0 }
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
    read: ReadCsvSettings,
    manual_input: ManualInputSettings,
    external_data: ExternalDataSettings,
    filter: FilterSettings,
    select: SelectSettings,
    group_by: GroupBySettings,
    join: JoinSettings,
    sort: SortSettings,
    polars_code: PolarsCodeSettings,
    unique: UniqueSettings,
    head: HeadSettings,
    explore_data: PreviewSettings,
    pivot: PivotSettings,
    unpivot: UnpivotSettings,
    output: OutputSettings,
    external_output: ExternalOutputSettings
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
// Use caching to prevent unnecessary re-renders that reset scroll position
let cachedDataRef: any[] | null = null
let cachedRowData: Record<string, any>[] = []

const rowData = computed(() => {
  const result = selectedNodeResult.value
  if (!result?.data?.data || !result?.data?.columns) {
    cachedDataRef = null
    cachedRowData = []
    return []
  }

  // Only recompute if the underlying data array reference changed
  if (result.data.data === cachedDataRef) {
    return cachedRowData
  }

  cachedDataRef = result.data.data
  const columns = result.data.columns
  cachedRowData = result.data.data.map((row: any[]) => {
    const obj: Record<string, any> = {}
    columns.forEach((colName: string, index: number) => {
      obj[colName] = row[index]
    })
    return obj
  })
  return cachedRowData
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
  emit('execution-complete', nodeResults.value)
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

// Handle layout reset from LayoutControls
function handleResetLayout() {
  // Dispatch custom event that DraggablePanel components listen to
  window.dispatchEvent(new CustomEvent('layout-reset'))
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

// Output callback for embeddable mode
function handleOutputCallback(data: { nodeId: number; content: string; fileName: string; mimeType: string }) {
  emit('output', data)
}

// Register keyboard shortcuts and output callbacks
onMounted(async () => {
  window.addEventListener('keydown', handleKeyDown)

  // Register output callback if the store supports it
  if (flowStore.onOutput) {
    flowStore.onOutput(handleOutputCallback)
  }

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
  // Unregister output callback
  if (flowStore.offOutput) {
    flowStore.offOutput(handleOutputCallback)
  }
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
  overflow: hidden;
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
