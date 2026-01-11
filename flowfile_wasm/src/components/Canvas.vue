<template>
  <div class="canvas-container">
    <!-- Node List Sidebar - Using DraggablePanel -->
    <DraggablePanel
      title="Data Actions"
      initial-position="left"
      :initial-width="200"
      :initial-top="50"
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
              <span class="node-icon">{{ node.icon }}</span>
              <span class="node-name">{{ node.name }}</span>
            </div>
          </div>
        </div>
      </div>
    </DraggablePanel>

    <!-- Vue Flow Canvas -->
    <div class="flow-canvas" @drop="onDrop" @dragover="onDragOver">
      <VueFlow
        ref="vueFlowRef"
        v-model:nodes="vueNodes"
        v-model:edges="vueEdges"
        :node-types="nodeTypes"
        :default-viewport="{ zoom: 1, x: 0, y: 0 }"
        :connection-mode="ConnectionMode.Strict"
        fit-view-on-init
        @connect="onConnect"
        @node-click="onNodeClick"
        @pane-click="onPaneClick"
        @edges-change="onEdgesChange"
        @nodes-change="onNodesChange"
      >
        <MiniMap />
        <Controls />
        <Background />
      </VueFlow>
    </div>

    <!-- Node Settings Panel -->
    <DraggablePanel
      v-if="selectedNode"
      :title="getNodeTitle(selectedNode.type)"
      initial-position="right"
      :initial-width="450"
      :initial-top="50"
      :on-close="() => flowStore.selectNode(null)"
    >
      <component
        :is="getSettingsComponent(selectedNode.type)"
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
      <div v-if="selectedNodeResult?.success && selectedNodeResult?.data" class="data-preview">
        <div class="preview-header">
          <span class="row-count">{{ selectedNodeResult.data.total_rows }} rows</span>
          <span class="col-count">{{ selectedNodeResult.data.columns?.length }} columns</span>
        </div>
        <div class="data-table-wrapper">
          <table class="data-table">
            <thead>
              <tr>
                <th v-for="col in selectedNodeResult.data.columns" :key="col">{{ col }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in selectedNodeResult.data.data" :key="idx">
                <td v-for="(cell, cidx) in row" :key="cidx">{{ formatCell(cell) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-else-if="selectedNodeResult?.error" class="error-message">
        {{ selectedNodeResult.error }}
      </div>
      <div v-else class="no-data">
        No data available. Run the flow to see results.
      </div>
    </DraggablePanel>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, markRaw } from 'vue'
import { VueFlow, useVueFlow, ConnectionMode } from '@vue-flow/core'
import type { Node, Edge, Connection, NodeChange, EdgeChange } from '@vue-flow/core'
import { MiniMap } from '@vue-flow/minimap'
import { Controls } from '@vue-flow/controls'
import { Background } from '@vue-flow/background'
import { useFlowStore } from '../stores/flow-store'
import { storeToRefs } from 'pinia'
import type { NodeSettings, FlowEdge } from '../types'

// Components
import DraggablePanel from './common/DraggablePanel.vue'
import FlowNode from './nodes/FlowNode.vue'
import ReadCsvSettings from './nodes/ReadCsvSettings.vue'
import FilterSettings from './nodes/FilterSettings.vue'
import SelectSettings from './nodes/SelectSettings.vue'
import GroupBySettings from './nodes/GroupBySettings.vue'
import JoinSettings from './nodes/JoinSettings.vue'
import SortSettings from './nodes/SortSettings.vue'
import WithColumnsSettings from './nodes/WithColumnsSettings.vue'
import UniqueSettings from './nodes/UniqueSettings.vue'
import HeadSettings from './nodes/HeadSettings.vue'
import PreviewSettings from './nodes/PreviewSettings.vue'

const flowStore = useFlowStore()
const { nodes: flowNodes, edges: flowEdges, selectedNodeId, nodeResults } = storeToRefs(flowStore)

const vueFlowRef = ref()
const { screenToFlowCoordinate } = useVueFlow()
const searchQuery = ref('')

// Node types for Vue Flow
const nodeTypes: Record<string, any> = {
  'flow-node': markRaw(FlowNode)
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
      { type: 'read_csv', name: 'Read CSV', icon: '📄', inputs: 0, outputs: 1 }
    ]
  },
  {
    name: 'Transformations',
    isOpen: true,
    nodes: [
      { type: 'filter', name: 'Filter', icon: '🔍', inputs: 1, outputs: 1 },
      { type: 'select', name: 'Select', icon: '📋', inputs: 1, outputs: 1 },
      { type: 'sort', name: 'Sort', icon: '↕️', inputs: 1, outputs: 1 },
      { type: 'with_columns', name: 'With Columns', icon: '➕', inputs: 1, outputs: 1 },
      { type: 'unique', name: 'Unique', icon: '🔷', inputs: 1, outputs: 1 },
      { type: 'head', name: 'Head/Limit', icon: '🔝', inputs: 1, outputs: 1 }
    ]
  },
  {
    name: 'Combine Operations',
    isOpen: true,
    nodes: [
      { type: 'join', name: 'Join', icon: '🔗', inputs: 2, outputs: 1 }
    ]
  },
  {
    name: 'Aggregations',
    isOpen: true,
    nodes: [
      { type: 'group_by', name: 'Group By', icon: '📊', inputs: 1, outputs: 1 }
    ]
  },
  {
    name: 'Output Operations',
    isOpen: true,
    nodes: [
      { type: 'preview', name: 'Preview', icon: '👁️', inputs: 1, outputs: 0 }
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
          icon: def?.icon || '📦',
          inputs: def?.inputs || 1,
          outputs: def?.outputs || 1,
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
      targetHandle: edge.targetHandle,
      animated: true
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
    }
  })
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
    filter: FilterSettings,
    select: SelectSettings,
    group_by: GroupBySettings,
    join: JoinSettings,
    sort: SortSettings,
    with_columns: WithColumnsSettings,
    unique: UniqueSettings,
    head: HeadSettings,
    preview: PreviewSettings
  }
  return components[type] || null
}

// Get node title
function getNodeTitle(type: string): string {
  const def = findNodeDef(type)
  return def?.name || type
}

// Format cell value for display
function formatCell(value: any): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
</script>

<style scoped>
.canvas-container {
  display: flex;
  height: 100%;
  position: relative;
  background: var(--bg-primary);
}

.flow-canvas {
  flex: 1;
  height: 100%;
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
  transition: background 0.15s;
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

.node-icon {
  font-size: 16px;
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
}

.preview-header {
  display: flex;
  gap: 16px;
  margin-bottom: 12px;
  font-size: 12px;
  color: var(--text-secondary);
}

.data-table-wrapper {
  flex: 1;
  overflow: auto;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.data-table th,
.data-table td {
  padding: 6px 10px;
  text-align: left;
  border: 1px solid var(--border-light);
  white-space: nowrap;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.data-table th {
  background: var(--bg-tertiary);
  font-weight: 500;
  position: sticky;
  top: 0;
  z-index: 1;
}

.data-table tr:hover td {
  background: var(--bg-hover);
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
</style>
