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
      :allow-full-screen="false"
    >
      <div class="nodes-wrapper">
        <input
          v-model="searchQuery"
          type="text"
          placeholder="Search nodes..."
          class="search-input"
        />

        <label class="availability-toggle">
          <input v-model="showUnavailable" type="checkbox" />
          <span>Show full-app nodes</span>
        </label>

        <div
          v-for="category in filteredCategories"
          :key="category.name"
          class="category"
        >
          <div class="category-header" @click="toggleCategory(category.name)">
            <span class="category-title">{{ category.name }}</span>
            <span class="arrow">{{ category.isOpen ? '▼' : '▶' }}</span>
          </div>
          <div v-if="category.isOpen" class="category-nodes">
            <div
              v-for="node in category.nodes"
              :key="node.type"
              class="node-item"
              :class="{ unavailable: node.available === false }"
              :draggable="node.available !== false"
              :title="
                node.available === false
                  ? `${node.name} runs in the full Flowfile app — not in this in-browser build`
                  : undefined
              "
              @dragstart="onDragStart($event, node)"
            >
              <img
                v-if="node.available !== false"
                :src="getIconUrl(node.icon)"
                :alt="node.name"
                class="node-icon-img"
              />
              <span v-else class="node-lock" aria-hidden="true">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
              </span>
              <span class="node-name">{{ node.name }}</span>
              <span v-if="node.available === false" class="node-badge">Full app</span>
            </div>
          </div>
        </div>
      </div>
    </DraggablePanel>

    <!-- Toolbar (hidden in app mode, where the header drives actions) -->
    <div v-if="showToolbar" ref="toolbarRef" class="toolbar">
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
        <button v-if="effectiveToolbar.showSaveLoad" class="action-btn" @click="handleSaveFlow" title="Save flow to the catalog">
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
          <span class="btn-text">{{ savedFlash ? 'Saved' : 'Save' }}</span>
        </button>
        <button v-if="effectiveToolbar.showSaveLoad" class="action-btn" @click="handleExportFlow" title="Export flow to file">
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
          <span class="btn-text">Export</span>
        </button>
        <button v-if="effectiveToolbar.showSaveLoad" class="action-btn" @click="triggerLoadFlow" title="Load Flow">
          <svg class="btn-icon" xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
          <span class="btn-text">Open</span>
        </button>
        <DemoButton v-if="effectiveToolbar.showDemo && hasSeenDemo" />
        <button
          v-if="effectiveToolbar.showCodeGen"
          class="action-btn"
          :class="{ active: uiStore.showCodeGenerator }"
          title="Generate Python Code"
          @click="uiStore.showCodeGenerator = true"
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
        :edge-types="edgeTypes"
        :default-viewport="{ zoom: 0.5 }"
        :connection-mode="ConnectionMode.Strict"
        class="custom-node-flow"
        fit-view-on-init
        @connect="onConnect"
        @node-click="onNodeClick"
        @node-double-click="onNodeDoubleClick"
        @pane-click="onPaneClick"
        @pane-context-menu="onPaneContextMenu"
        @edge-mouse-enter="onEdgeMouseEnter"
        @edge-mouse-leave="onEdgeMouseLeave"
        @edges-change="onEdgesChange"
        @nodes-change="onNodesChange"
      >
        <template #node-flow-node="nodeProps">
          <FlowNode
            :data="nodeProps.data"
            @delete="handleDeleteNode"
            @run="handleRunNode"
            @edit="handleEditNode"
            @view-data="handleViewData"
            @copy="handleCopyNode"
            @save-to-catalog="handleSaveToCatalog"
          />
        </template>
        <MiniMap />
        <Controls />
      </VueFlow>
    </div>

    <!-- Node Settings Panel -->
    <DraggablePanel
      v-if="selectedNode && showSettings"
      :title="getNodeDescription(selectedNode.type).title"
      panel-id="node-settings-panel"
      initial-position="right"
      :initial-width="450"
      :initial-top="toolbarHeight"
      :default-z-index="120"
      :on-close="closeSettings"
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

      <!-- Sticky Apply footer: runs the node and refreshes its preview, mirroring
           the main editor's NodeSettingsDrawer. Settings are already applied live,
           so this re-executes to surface the result. -->
      <template #footer>
        <button
          class="apply-btn"
          :class="{ applied: justApplied }"
          :disabled="isApplying"
          @click="applyNodeSettings"
        >
          {{ isApplying ? 'Applying…' : justApplied ? 'Applied ✓' : 'Apply' }}
        </button>
      </template>
    </DraggablePanel>

    <!-- Data Preview Panel (hidden for explore_data nodes which have their own preview) -->
    <DraggablePanel
      v-if="selectedNodeId !== null && showTablePreview && selectedNode?.type !== 'explore_data'"
      title="Table Preview"
      panel-id="data-preview-panel"
      initial-position="bottom"
      :initial-height="280"
      :initial-left="200"
      :default-z-index="110"
      group="bottomPanels"
      :sync-dimensions="true"
      :on-close="closeTable"
    >
      <div class="data-preview">
        <!-- Loading state -->
        <div v-if="isPreviewLoading" class="preview-loading">
          <div class="spinner small"></div>
          <span>Loading preview...</span>
        </div>

        <!-- Data grid -->
        <template v-else-if="selectedNodeResult?.success && selectedNodeResult?.data">
          <!-- Multi-output selector (only for nodes with >1 output; dormant today) -->
          <div v-if="hasMultipleOutputs" class="output-selector">
            <span class="output-selector__label">Output:</span>
            <button
              v-for="output in selectedNodeOutputs"
              :key="output.id"
              class="output-selector__button"
              :class="{ active: output.id === selectedOutputHandle }"
              @click="selectOutput(output.id)"
            >
              <span class="output-selector__letter">{{ output.label }}</span>
            </button>
          </div>
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
              :suppressFieldDotNotation="true"
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

        <!-- Schema preview: fields are known (lazy propagation) but no data fetched yet -->
        <div v-else-if="selectedNodeSchema.length" class="schema-preview">
          <div class="preview-header">
            <span class="col-count">{{ selectedNodeSchema.length }} columns</span>
            <span class="schema-preview-hint">Schema preview · fetch data to see rows</span>
          </div>
          <div class="schema-grid-container">
            <table class="schema-table">
              <thead>
                <tr>
                  <th v-for="col in selectedNodeSchema" :key="col.name">
                    <span class="schema-col-name">{{ col.name }}</span>
                    <span class="schema-col-type">{{ col.data_type }}</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr class="schema-empty-row">
                  <td v-for="col in selectedNodeSchema" :key="col.name">—</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="schema-preview-footer">
            <button
              class="fetch-data-button"
              :disabled="isFetching || isExecuting || !pyodideReady"
              @click="handleFetchData"
            >
              {{ isFetching ? 'Fetching…' : 'Fetch data' }}
            </button>
          </div>
        </div>

        <!-- Empty placeholder: opening the Table never runs the flow, so an
             un-executed node shows this instead of auto-running. Data appears
             only when the user explicitly runs it (this button, or Run flow). -->
        <div v-else class="no-data">
          <p class="no-data-text">This node hasn't run yet. Click Run (or Fetch data) to compute and preview its output.</p>
          <button
            class="fetch-data-button"
            :disabled="isFetching || isExecuting || !pyodideReady"
            @click="handleFetchData"
          >
            {{ isFetching ? 'Fetching…' : 'Fetch data' }}
          </button>
        </div>
      </div>
    </DraggablePanel>

    <!-- Hidden file input for Open (lives outside the toolbar so the header's
         Open action works even when the toolbar is hidden in app mode). -->
    <input
      ref="fileInputRef"
      type="file"
      accept=".json,.yaml,.yml"
      @change="handleLoadFlow"
      style="display: none"
    />

    <!-- Code Generator Modal -->
    <CodeGenerator
      :is-visible="uiStore.showCodeGenerator"
      @close="uiStore.showCodeGenerator = false"
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

    <!-- Canvas (pane) right-click menu -->
    <div
      v-if="paneMenuVisible"
      ref="paneMenuEl"
      class="context-menu pane-menu"
      :style="{ position: 'fixed', zIndex: 10000, top: `${paneMenu.y}px`, left: `${paneMenu.x}px` }"
    >
      <div class="context-menu-item" @click="paneFitView">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 3h6v6"/><path d="M9 21H3v-6"/><path d="M21 3l-7 7"/><path d="M3 21l7-7"/></svg>
        <span>Fit view</span>
      </div>
      <div class="context-menu-item" @click="paneZoomIn">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35M11 8v6M8 11h6"/></svg>
        <span>Zoom in</span>
      </div>
      <div class="context-menu-item" @click="paneZoomOut">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35M8 11h6"/></svg>
        <span>Zoom out</span>
      </div>
      <div v-if="canPaste" class="context-menu-divider"></div>
      <div v-if="canPaste" class="context-menu-item" @click="pasteHere">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/></svg>
        <span>Paste node</span>
      </div>
    </div>

  </div>
</template>

<script setup lang="ts">
import { ref, computed, markRaw, onMounted, onUnmounted, nextTick, defineAsyncComponent, provide, watch } from 'vue'
import { VueFlow, useVueFlow, ConnectionMode } from '@vue-flow/core'
import type { Node, Edge, Connection, NodeChange, EdgeChange } from '@vue-flow/core'
import { MiniMap } from '@vue-flow/minimap'
import { Controls } from '@vue-flow/controls'
import { useFlowStore } from '../stores/flow-store'
import { usePyodideStore } from '../stores/pyodide-store'
import { usePanelZIndexStore } from '../stores/panel-zindex-store'
import { useDesignerUiStore } from '../stores/designer-ui-store'
import { storeToRefs } from 'pinia'
import type { NodeSettings, FlowEdge, ColumnSchema, NodeResult } from '../types'
import type { ToolbarConfig, NodeCategoryConfig } from '../lib/types'
import { iconUrls } from '../utils/iconUrls'

import { AgGridVue } from '@ag-grid-community/vue3'
import { ClientSideRowModelModule } from '@ag-grid-community/client-side-row-model'
import { ModuleRegistry } from '@ag-grid-community/core'
import type { ColDef, GridReadyEvent, GridApi } from '@ag-grid-community/core'

ModuleRegistry.registerModules([ClientSideRowModelModule])

import DraggablePanel from './common/DraggablePanel.vue'
import FlowNode from './nodes/FlowNode.vue'
import DeletableEdge from './DeletableEdge.vue'
import NodeTitle from './nodes/NodeTitle.vue'
import ReadFileSettings from './nodes/ReadFileSettings.vue'
import ManualInputSettings from './nodes/ManualInputSettings.vue'
import ExternalDataSettings from './nodes/ExternalDataSettings.vue'
import ReadFromCatalogSettings from './nodes/ReadFromCatalogSettings.vue'
import FilterSettings from './nodes/FilterSettings.vue'
import SelectSettings from './nodes/SelectSettings.vue'
import GroupBySettings from './nodes/GroupBySettings.vue'
import JoinSettings from './nodes/JoinSettings.vue'
import SortSettings from './nodes/SortSettings.vue'
const PolarsCodeSettings = defineAsyncComponent(() => import('./nodes/PolarsCodeSettings.vue'))
import UniqueSettings from './nodes/UniqueSettings.vue'
import HeadSettings from './nodes/HeadSettings.vue'
// Lazy-load the explore_data panel so React + Graphic Walker only enter the
// bundle when a user actually opens an explore_data node.
const ExploreData = defineAsyncComponent(() => import('./nodes/exploreData/ExploreData.vue'))
const CodeGenerator = defineAsyncComponent(() => import('./CodeGenerator.vue'))
import PivotSettings from './nodes/PivotSettings.vue'
import UnpivotSettings from './nodes/UnpivotSettings.vue'
import OutputSettings from './nodes/OutputSettings.vue'
import ExternalOutputSettings from './nodes/ExternalOutputSettings.vue'
import WriteToCatalogSettings from './nodes/WriteToCatalogSettings.vue'
const FormulaSettings = defineAsyncComponent(() => import('./nodes/FormulaSettings.vue'))
import CrossJoinSettings from './nodes/CrossJoinSettings.vue'
import UnionSettings from './nodes/UnionSettings.vue'
import RecordIdSettings from './nodes/RecordIdSettings.vue'
import DynamicRenameSettings from './nodes/DynamicRenameSettings.vue'
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
  // App mode hides the in-canvas toolbar and drives actions from the header;
  // the embeddable library keeps the toolbar (default true).
  showToolbar?: boolean
}>(), {
  readonly: false,
  showToolbar: true
})

const emit = defineEmits<{
  (e: 'execution-complete', results: Map<number, NodeResult>): void
  (e: 'output', data: { nodeId: number; content: string; fileName: string; mimeType: string }): void
}>()

const effectiveToolbar = computed<Required<ToolbarConfig>>(() => ({
  showRun: props.toolbarConfig?.showRun !== false,
  showSaveLoad: props.toolbarConfig?.showSaveLoad !== false,
  showClear: props.toolbarConfig?.showClear !== false,
  showCodeGen: props.toolbarConfig?.showCodeGen !== false,
  showDemo: props.toolbarConfig?.showDemo ?? true
}))

const flowStore = useFlowStore()
const zIndexStore = usePanelZIndexStore()
const uiStore = useDesignerUiStore()
const { nodes: flowNodes, edges: flowEdges, selectedNodeId, showSettings, showTablePreview, nodeResults, isExecuting } = storeToRefs(flowStore)
const { isReady: pyodideReady } = storeToRefs(usePyodideStore())

const { hasSeenDemo } = useDemo()

const fileInputRef = ref<HTMLInputElement | null>(null)
const toolbarRef = ref<HTMLElement | null>(null)
const toolbarHeight = ref(52)
const { screenToFlowCoordinate, removeNodes, updateNode, fitView, zoomIn, zoomOut } = useVueFlow()

// Canvas (pane) right-click menu state.
const paneMenuVisible = ref(false)
const paneMenuEl = ref<HTMLElement | null>(null)
const paneMenu = ref({ x: 0, y: 0 })
let paneFlowPos = { x: 0, y: 0 }
const canPaste = computed(() => flowStore.hasClipboard())
const searchQuery = ref('')
// Full-app nodes that don't run in-browser are shown greyed-out by default so the
// breadth is visible; this toggle lets users hide them and see only what runs here.
const showUnavailable = ref(true)
const pendingNodeAdjustment = ref<number | null>(null)
const showMissingFilesModal = ref(false)
const missingFiles = ref<Array<{nodeId: number, fileName: string}>>([])
// Brief "Saved" confirmation on the toolbar Save button (lib/embed mode).
const savedFlash = ref(false)

// Apply-button state for the node settings panel footer.
const isApplying = ref(false)
const justApplied = ref(false)
let appliedTimer: ReturnType<typeof setTimeout> | null = null

// Data-preview state: fetch-to-run + (dormant) multi-output selector.
const isFetching = ref(false)
const selectedOutputHandle = ref('output-0')

const nodeTypes: Record<string, any> = {
  'flow-node': markRaw(FlowNode)
}

const edgeTypes: Record<string, any> = {
  default: markRaw(DeletableEdge)
}

// Get icon URL - uses explicit imports for library build compatibility
function getIconUrl(iconFile: string): string {
  return iconUrls[iconFile] || new URL(`../assets/icons/${iconFile}`, import.meta.url).href
}

interface NodeDefinition {
  type: string
  name: string
  icon: string
  inputs: number
  outputs: number
  // false → a full-app capability that can't run in this in-browser build; shown
  // greyed-out and locked (not draggable) so the breadth is still discoverable.
  available?: boolean
  // Extra search terms so the palette filter matches by concept, not just by name.
  keywords?: string[]
}

interface NodeCategory {
  name: string
  isOpen: boolean
  nodes: NodeDefinition[]
}

// Nodes flagged `available: false` run only in the full Flowfile app (they need a
// backend, network, or a heavier runtime than the in-browser Pyodide build). They
// render greyed-out and locked here so the full breadth stays discoverable.
const nodeCategories = ref<NodeCategory[]>([
  {
    name: 'Input Sources',
    isOpen: true,
    nodes: [
      { type: 'read', name: 'Read File', icon: 'input_data.png', inputs: 0, outputs: 1, keywords: ['csv', 'excel', 'parquet', 'json', 'file', 'import', 'load'] },
      { type: 'manual_input', name: 'Manual Input', icon: 'manual_input.png', inputs: 0, outputs: 1, keywords: ['paste', 'type', 'create', 'test data'] },
      { type: 'external_data', name: 'External Data', icon: 'external_data.svg', inputs: 0, outputs: 1, keywords: ['url', 'http', 'fetch', 'remote', 'web', 'api'] },
      { type: 'read_from_catalog', name: 'Read from Catalog', icon: 'catalog_reader.svg', inputs: 0, outputs: 1, keywords: ['catalog', 'table', 'dataset', 'saved'] },
      { type: 'database_reader', name: 'Read from Database', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['sql', 'postgres', 'postgresql', 'mysql', 'snowflake', 'oracle', 'redshift', 'bigquery', 'query', 'table', 'db'] },
      { type: 'cloud_storage_reader', name: 'Read from Cloud', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['s3', 'aws', 'azure', 'adls', 'gcs', 'blob', 'bucket', 'cloud', 'object storage'] },
      { type: 'rest_api_reader', name: 'REST API', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['rest', 'api', 'http', 'json', 'endpoint', 'pagination', 'auth'] },
      { type: 'kafka_source', name: 'Kafka Source', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['kafka', 'redpanda', 'stream', 'streaming', 'topic', 'events'] },
      { type: 'google_analytics_reader', name: 'Google Analytics', icon: '', inputs: 0, outputs: 1, available: false, keywords: ['google analytics', 'ga', 'ga4', 'analytics', 'web analytics'] }
    ]
  },
  {
    name: 'Transformations',
    isOpen: true,
    nodes: [
      { type: 'filter', name: 'Filter', icon: 'filter.png', inputs: 1, outputs: 1, keywords: ['where', 'subset', 'condition', 'rows'] },
      { type: 'select', name: 'Select', icon: 'select.png', inputs: 1, outputs: 1, keywords: ['columns', 'rename', 'reorder', 'keep', 'drop'] },
      { type: 'formula', name: 'Formula', icon: 'formula.png', inputs: 1, outputs: 1, keywords: ['expression', 'calculate', 'compute', 'sum', 'math', 'concat', 'new column'] },
      { type: 'sort', name: 'Sort', icon: 'sort.png', inputs: 1, outputs: 1, keywords: ['order', 'arrange', 'rank', 'ascending', 'descending'] },
      { type: 'polars_code', name: 'Polars Code', icon: 'polars_code.png', inputs: 1, outputs: 1, keywords: ['python', 'code', 'custom', 'script', 'dataframe'] },
      { type: 'unique', name: 'Unique', icon: 'unique.png', inputs: 1, outputs: 1, keywords: ['dedupe', 'distinct', 'drop duplicates', 'deduplicate'] },
      { type: 'dynamic_rename', name: 'Rename', icon: 'dynamic_rename.svg', inputs: 1, outputs: 1, keywords: ['rename', 'columns', 'prefix', 'suffix'] },
      { type: 'record_id', name: 'Record ID', icon: 'record_id.png', inputs: 1, outputs: 1, keywords: ['row number', 'index', 'id', 'sequence'] },
      { type: 'head', name: 'Take Sample', icon: 'sample.png', inputs: 1, outputs: 1, keywords: ['sample', 'limit', 'top', 'head', 'subset'] },
      { type: 'window_functions', name: 'Window Functions', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['window', 'rolling', 'cumulative', 'rank', 'partition', 'lag', 'lead', 'over'] },
      { type: 'sql_query', name: 'SQL Query', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['sql', 'query', 'select', 'where', 'duckdb'] },
      { type: 'python_script', name: 'Python Script', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['python', 'code', 'script', 'kernel', 'pandas'] }
    ]
  },
  {
    name: 'Combine Operations',
    isOpen: true,
    nodes: [
      { type: 'join', name: 'Join', icon: 'join.png', inputs: 2, outputs: 1, keywords: ['merge', 'lookup', 'vlookup', 'inner', 'left', 'right', 'outer'] },
      { type: 'cross_join', name: 'Cross Join', icon: 'cross_join.png', inputs: 2, outputs: 1, keywords: ['cartesian', 'cross', 'combinations'] },
      // inputs: 1 — single handle accepts multiple connections (like polars_code).
      { type: 'union', name: 'Union', icon: 'union.png', inputs: 1, outputs: 1, keywords: ['concat', 'append', 'stack', 'combine'] },
      { type: 'fuzzy_match', name: 'Fuzzy Match', icon: '', inputs: 2, outputs: 1, available: false, keywords: ['fuzzy', 'similarity', 'levenshtein', 'approximate', 'fuzzy join'] },
      { type: 'graph_solver', name: 'Graph Solver', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['graph', 'network', 'cluster', 'connected components'] }
    ]
  },
  {
    name: 'Aggregations',
    isOpen: true,
    nodes: [
      { type: 'group_by', name: 'Group By', icon: 'group_by.png', inputs: 1, outputs: 1, keywords: ['aggregate', 'sum', 'mean', 'average', 'count', 'min', 'max', 'median', 'summarize'] },
      { type: 'pivot', name: 'Pivot', icon: 'pivot.png', inputs: 1, outputs: 1, keywords: ['crosstab', 'wide', 'reshape', 'spread'] },
      { type: 'unpivot', name: 'Unpivot', icon: 'unpivot.png', inputs: 1, outputs: 1, keywords: ['melt', 'long', 'reshape', 'gather'] }
    ]
  },
  {
    name: 'Machine Learning',
    isOpen: true,
    nodes: [
      { type: 'train_model', name: 'Train Model', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['ml', 'machine learning', 'train', 'model', 'regression', 'classification', 'fit', 'sklearn'] },
      { type: 'apply_model', name: 'Apply Model', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['ml', 'machine learning', 'predict', 'score', 'inference', 'model'] },
      { type: 'evaluate_model', name: 'Evaluate Model', icon: '', inputs: 1, outputs: 1, available: false, keywords: ['ml', 'machine learning', 'evaluate', 'metrics', 'accuracy', 'model'] }
    ]
  },
  {
    name: 'Output Operations',
    isOpen: true,
    nodes: [
      { type: 'explore_data', name: 'Explore Data', icon: 'explore_data.png', inputs: 1, outputs: 0, keywords: ['profile', 'describe', 'preview', 'eda', 'visualize', 'chart'] },
      { type: 'output', name: 'Write Data', icon: 'output.png', inputs: 1, outputs: 0, keywords: ['csv', 'excel', 'parquet', 'write', 'save', 'export', 'file'] },
      { type: 'write_to_catalog', name: 'Write to Catalog', icon: 'catalog_writer.svg', inputs: 1, outputs: 0, keywords: ['catalog', 'table', 'save'] },
      { type: 'external_output', name: 'External Output', icon: 'external_output.svg', inputs: 1, outputs: 0, keywords: ['url', 'http', 'api', 'send', 'webhook'] },
      { type: 'database_writer', name: 'Write to Database', icon: '', inputs: 1, outputs: 0, available: false, keywords: ['sql', 'postgres', 'mysql', 'snowflake', 'redshift', 'bigquery', 'insert', 'table', 'db'] },
      { type: 'cloud_storage_writer', name: 'Write to Cloud', icon: '', inputs: 1, outputs: 0, available: false, keywords: ['s3', 'aws', 'azure', 'adls', 'gcs', 'blob', 'bucket', 'cloud'] }
    ]
  }
])

const filteredCategories = computed(() => {
  const query = searchQuery.value.trim().toLowerCase()
  return nodeCategories.value
    .map(cat => ({
      ...cat,
      nodes: cat.nodes.filter(n => {
        // Availability toggle: optionally hide the locked full-app nodes.
        if (!showUnavailable.value && n.available === false) return false
        if (!query) return true
        // Match the display name OR any of the node's search keywords.
        return (
          n.name.toLowerCase().includes(query) ||
          (n.keywords ?? []).some(k => k.toLowerCase().includes(query))
        )
      })
    }))
    .filter(cat => cat.nodes.length > 0)
})

// Toggle a category's open state on the source ref. filteredCategories returns
// shallow copies (it always filters nodes now), so we must flip the original.
function toggleCategory(name: string) {
  const cat = nodeCategories.value.find(c => c.name === name)
  if (cat) cat.isOpen = !cat.isOpen
}

function findNodeDef(type: string): NodeDefinition | undefined {
  for (const cat of nodeCategories.value) {
    const node = cat.nodes.find(n => n.type === type)
    if (node) return node
  }
  return undefined
}

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

const selectedNode = computed(() => {
  if (selectedNodeId.value === null) return null
  return flowNodes.value.get(selectedNodeId.value) || null
})

const selectedNodeResult = computed(() => {
  if (selectedNodeId.value === null) return null
  return nodeResults.value.get(selectedNodeId.value) || null
})

let draggedNodeDef: NodeDefinition | null = null

function onDragStart(event: DragEvent, node: NodeDefinition) {
  // Locked (full-app-only) nodes can't be added to the in-browser canvas.
  if (node.available === false) {
    event.preventDefault()
    return
  }
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
  showSettings.value = true

  pendingNodeAdjustment.value = nodeId
  draggedNodeDef = null
}

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

function onNodeClick(event: { node: Node }) {
  // Single click: open Settings only — never auto-opens the Table (mirrors the
  // main flowfile_frontend). The Table flag is left untouched, so if the user
  // already opened it, it stays open and retargets to this node.
  flowStore.selectNode(parseInt(event.node.id))
  showSettings.value = true
}

// Double click a node: open BOTH the Settings panel and the Table preview.
// Opening never executes — an already-run node shows its rows via selectNode's
// gated preview fetch; an un-run node shows the placeholder. Running the node is
// an explicit action (the Table's "Fetch data" button, or Run).
function onNodeDoubleClick(event: { node: Node }) {
  const nodeId = parseInt(event.node.id)
  showTablePreview.value = true
  flowStore.selectNode(nodeId)
  showSettings.value = true
  nextTick(() => {
    zIndexStore.bringToFront('node-settings-panel')
    zIndexStore.bringToFront('data-preview-panel')
  })
}

function onPaneClick() {
  flowStore.selectNode(null)
  showSettings.value = false
  showTablePreview.value = false
  closePaneMenu()
}

function onPaneContextMenu(event: MouseEvent) {
  event.preventDefault()
  paneFlowPos = screenToFlowCoordinate({ x: event.clientX, y: event.clientY })
  paneMenu.value = { x: event.clientX, y: event.clientY }
  paneMenuVisible.value = true
  setTimeout(() => window.addEventListener('click', handlePaneMenuClickOutside), 0)
  nextTick(() => {
    const el = paneMenuEl.value
    if (!el) return
    const rect = el.getBoundingClientRect()
    if (paneMenu.value.x + rect.width > window.innerWidth - 10) {
      paneMenu.value.x = window.innerWidth - rect.width - 10
    }
    if (paneMenu.value.y + rect.height > window.innerHeight - 10) {
      paneMenu.value.y = window.innerHeight - rect.height - 10
    }
  })
}

function handlePaneMenuClickOutside(event: MouseEvent) {
  // Note: `Node` is shadowed by VueFlow's Node type in this file; use HTMLElement.
  if (paneMenuEl.value && !paneMenuEl.value.contains(event.target as HTMLElement)) {
    closePaneMenu()
  }
}

function closePaneMenu() {
  paneMenuVisible.value = false
  window.removeEventListener('click', handlePaneMenuClickOutside)
}

function paneFitView() {
  fitView()
  closePaneMenu()
}

function paneZoomIn() {
  zoomIn()
  closePaneMenu()
}

function paneZoomOut() {
  zoomOut()
  closePaneMenu()
}

function pasteHere() {
  const newId = flowStore.pasteNode(paneFlowPos.x, paneFlowPos.y)
  if (newId !== null) {
    pendingNodeAdjustment.value = newId
    flowStore.selectNode(newId)
    showSettings.value = true
  }
  closePaneMenu()
}

function onEdgesChange(changes: EdgeChange[]) {
  changes.forEach(change => {
    if (change.type === 'remove') {
      flowStore.removeEdge(change.id)
    }
  })
}

// Reveal a delete button on the hovered edge; the leave-delay lets the cursor
// reach the button. Provided to DeletableEdge.vue.
const hoveredEdgeId = ref<string | null>(null)
let edgeLeaveTimer: ReturnType<typeof setTimeout> | null = null

function cancelEdgeLeave() {
  if (edgeLeaveTimer !== null) {
    clearTimeout(edgeLeaveTimer)
    edgeLeaveTimer = null
  }
}

function scheduleEdgeLeave(id: string) {
  cancelEdgeLeave()
  edgeLeaveTimer = setTimeout(() => {
    if (hoveredEdgeId.value === id) hoveredEdgeId.value = null
    edgeLeaveTimer = null
  }, 150)
}

function onEdgeMouseEnter({ edge }: { edge: Edge }) {
  cancelEdgeLeave()
  hoveredEdgeId.value = edge.id
}

function onEdgeMouseLeave({ edge }: { edge: Edge }) {
  scheduleEdgeLeave(edge.id)
}

provide('hoveredEdgeId', hoveredEdgeId)
provide('cancelEdgeLeave', cancelEdgeLeave)
provide('scheduleEdgeLeave', scheduleEdgeLeave)

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

function handleDeleteNode(nodeId: number) {
  // Deleting the selected node leaves no panel target — close both panels and
  // deselect so the Table doesn't linger pointing at a removed node.
  if (nodeId === selectedNodeId.value) {
    showSettings.value = false
    showTablePreview.value = false
    flowStore.selectNode(null)
  }
  removeNodes(String(nodeId))
  flowStore.removeNode(nodeId)
}

async function handleRunNode(nodeId: number) {
  await flowStore.executeNodeWithUpstream(nodeId)
}

// Edit (context menu): open the Settings panel only, and surface it.
function handleEditNode(nodeId: number) {
  flowStore.selectNode(nodeId)
  showSettings.value = true
  nextTick(() => zIndexStore.bringToFront('node-settings-panel'))
}

// View data (context menu): open the Table preview only (not Settings). Opening
// never executes — an already-run node shows its rows via selectNode's gated
// preview fetch; an un-run node shows the placeholder with a "Fetch data" button.
function handleViewData(nodeId: number) {
  showTablePreview.value = true
  flowStore.selectNode(nodeId)
  nextTick(() => zIndexStore.bringToFront('data-preview-panel'))
}

// Each panel closes independently. Closing one keeps the other (and the node
// selection) intact; closing the last open panel also clears the selection so
// the node stops being highlighted.
function closeSettings() {
  showSettings.value = false
  if (!showTablePreview.value) flowStore.selectNode(null)
}

function closeTable() {
  showTablePreview.value = false
  if (!showSettings.value) flowStore.selectNode(null)
}

function handleCopyNode(nodeId: number) {
  flowStore.copyNode(nodeId)
}

// Persist a source node's loaded CSV as a reusable catalog table. The catalog
// is the cross-flow store, so this bridges flow-bound data into it.
async function handleSaveToCatalog(nodeId: number) {
  if (flowStore.getFileContent(nodeId) && flowStore.getTextContent(nodeId) === undefined) {
    alert('The catalog stores CSV tables only — this node holds a binary file (Excel/Parquet).')
    return
  }
  const content = flowStore.getTextContent(nodeId)
  if (!content) {
    alert('This node has no loaded data to save. Run or load the node first.')
    return
  }
  const node = flowStore.getNode(nodeId)
  const s = (node?.settings ?? {}) as Record<string, any>
  const defaultName =
    (s.received_file?.name as string) ||
    (s.file_name as string) ||
    (s.dataset_name as string) ||
    node?.description ||
    `table_${nodeId}`
  const cleanName = String(defaultName).replace(/\.[^.]+$/, '').trim() || `table_${nodeId}`
  const name = window.prompt('Save as catalog table named:', cleanName)
  if (name === null) return
  const trimmed = name.trim()
  if (!trimmed) return
  if (
    flowStore.getCatalogDatasetNames().includes(trimmed) &&
    !window.confirm(`A catalog table named "${trimmed}" already exists. Replace it?`)
  ) {
    return
  }
  await flowStore.addCatalogDataset(trimmed, content)
}

function updateSettings(settings: NodeSettings) {
  if (selectedNodeId.value !== null) {
    flowStore.updateNodeSettings(selectedNodeId.value, settings)
  }
}

// Apply: settings already update live in the store, so this re-executes the
// selected node and refreshes its preview (the main editor's Apply does the
// equivalent push + run), with a brief "Applied ✓" confirmation.
async function applyNodeSettings() {
  const node = selectedNode.value
  if (!node || isApplying.value) return

  isApplying.value = true
  try {
    const result = await flowStore.executeNodeWithUpstream(node.id)
    if (result.success && node.type !== 'explore_data') {
      await flowStore.fetchNodePreview(node.id, { maxRows: 100 })
    }
    justApplied.value = true
    if (appliedTimer) clearTimeout(appliedTimer)
    appliedTimer = setTimeout(() => { justApplied.value = false }, 1500)
  } finally {
    isApplying.value = false
  }
}

// Reset the "Applied" confirmation and preview output selection when switching nodes.
watch(selectedNodeId, () => {
  justApplied.value = false
  selectedOutputHandle.value = 'output-0'
  if (appliedTimer) {
    clearTimeout(appliedTimer)
    appliedTimer = null
  }
})

function getSettingsComponent(type: string) {
  const components: Record<string, any> = {
    read: ReadFileSettings,
    manual_input: ManualInputSettings,
    external_data: ExternalDataSettings,
    read_from_catalog: ReadFromCatalogSettings,
    filter: FilterSettings,
    select: SelectSettings,
    group_by: GroupBySettings,
    join: JoinSettings,
    cross_join: CrossJoinSettings,
    union: UnionSettings,
    sort: SortSettings,
    polars_code: PolarsCodeSettings,
    formula: FormulaSettings,
    unique: UniqueSettings,
    dynamic_rename: DynamicRenameSettings,
    record_id: RecordIdSettings,
    head: HeadSettings,
    explore_data: ExploreData,
    pivot: PivotSettings,
    unpivot: UnpivotSettings,
    output: OutputSettings,
    external_output: ExternalOutputSettings,
    write_to_catalog: WriteToCatalogSettings
  }
  return components[type] || null
}

const gridApi = ref<GridApi | null>(null)
const isPreviewLoading = computed(() => {
  if (selectedNodeId.value === null) return false
  return flowStore.isPreviewLoading(selectedNodeId.value)
})

// Fetch-to-run: execute the selected node, then materialize its preview. No
// polling needed (everything runs in-browser), unlike the main editor.
async function handleFetchData() {
  if (selectedNodeId.value === null || isFetching.value) return
  const nodeId = selectedNodeId.value
  isFetching.value = true
  try {
    // Run the node together with its upstream chain so the preview can be
    // materialized even when the flow hasn't been run.
    const result = await flowStore.executeNodeWithUpstream(nodeId)
    if (result.success) {
      const node = flowNodes.value.get(nodeId)
      const maxRows = node?.type === 'explore_data' ? 1000 : 100
      await flowStore.fetchNodePreview(nodeId, { maxRows })
    }
    emit('execution-complete', nodeResults.value)
  } finally {
    isFetching.value = false
  }
}

// Multi-output selector (parity with the main editor). NOTE: dormant for now —
// no built-in WASM node has >1 output and the Pyodide engine keeps one frame per
// node_id, so this renders only if outputs>1 and selecting a handle does not yet
// re-query data. Real per-output preview needs engine changes (per-(node,handle)
// frames + fetchNodePreview(handle) + per-handle NodeResult).
const selectedNodeOutputs = computed(() => {
  if (!selectedNode.value) return []
  const def = findNodeDef(selectedNode.value.type)
  const count = def?.outputs ?? 1
  if (count <= 1) return []
  return Array.from({ length: count }, (_, i) => ({
    id: `output-${i}`,
    label: String.fromCharCode(65 + i)
  }))
})
const hasMultipleOutputs = computed(() => selectedNodeOutputs.value.length > 1)
function selectOutput(handle: string) {
  selectedOutputHandle.value = handle
}

const columnDefs = computed<ColDef[]>(() => {
  const result = selectedNodeResult.value
  if (!result?.data?.columns) return []

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
      valueFormatter: isNumeric ? (params: any) => {
        if (params.value === null || params.value === undefined) return 'null'
        if (typeof params.value === 'number') {
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

// Propagated schema for the selected node — drives the field preview shown
// before any data is fetched (lazy schema propagation makes this available
// without running the flow).
const selectedNodeSchema = computed<ColumnSchema[]>(() => {
  return selectedNodeResult.value?.schema || []
})

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

const defaultColDef: ColDef = {
  sortable: true,
  filter: true,
  resizable: true,
  minWidth: 80,
  flex: 1,
}

function onGridReady(params: GridReadyEvent) {
  gridApi.value = params.api
}

function autoSizeColumns() {
  if (gridApi.value) {
    gridApi.value.autoSizeAllColumns()
  }
}

async function handleRunFlow() {
  await flowStore.executeFlow()
  emit('execution-complete', nodeResults.value)
}

// Save the flow to the in-browser library (no file download). Prompts for a
// name only on first save of an untitled flow; re-saves update the same entry.
async function handleSaveFlow(): Promise<boolean> {
  const needsName = !flowStore.currentFlowId || flowStore.currentFlowName === 'Untitled Flow'
  let name = flowStore.currentFlowName
  if (needsName) {
    const entered = prompt('Save flow as:', name && name !== 'Untitled Flow' ? name : 'my_flow')
    if (!entered) return false
    name = entered
  }
  await flowStore.saveToLibrary(name)
  savedFlash.value = true
  setTimeout(() => (savedFlash.value = false), 1600)
  return true
}

// Export the flow to a downloaded file (separate from library save).
async function handleExportFlow() {
  let name = flowStore.currentFlowName
  if (!name || name === 'Untitled Flow') {
    name = prompt('Export flow as:', 'my_flow') || ''
    if (!name) return
  }
  await flowStore.downloadFlowfile(name)
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

function handleResetLayout() {
  window.dispatchEvent(new CustomEvent('layout-reset'))
}

function handleKeyDown(event: KeyboardEvent) {
  // Don't hijack shortcuts while typing in an input/textarea/editable field.
  const target = event.target as HTMLElement | null
  const isTyping =
    !!target &&
    (target.tagName === 'INPUT' ||
      target.tagName === 'TEXTAREA' ||
      target.isContentEditable)

  if ((event.ctrlKey || event.metaKey) && (event.key === 'e' || event.key === 'E')) {
    event.preventDefault()
    if (!isExecuting.value) {
      handleRunFlow()
    }
    return
  }

  if (isTyping) return

  // Copy the selected node.
  if ((event.ctrlKey || event.metaKey) && (event.key === 'c' || event.key === 'C')) {
    if (selectedNodeId.value !== null) {
      flowStore.copyNode(selectedNodeId.value)
    }
    return
  }

  // Paste the clipboard node near the centre of the current view.
  if ((event.ctrlKey || event.metaKey) && (event.key === 'v' || event.key === 'V')) {
    if (!flowStore.hasClipboard()) return
    const pos = screenToFlowCoordinate({ x: window.innerWidth / 2, y: window.innerHeight / 2 })
    const newId = flowStore.pasteNode(pos.x, pos.y)
    if (newId !== null) {
      pendingNodeAdjustment.value = newId
      flowStore.selectNode(newId)
      showSettings.value = true
    }
  }
}

function handleOutputCallback(data: { nodeId: number; content: string; fileName: string; mimeType: string }) {
  emit('output', data)
}

onMounted(async () => {
  window.addEventListener('keydown', handleKeyDown)

  if (flowStore.onOutput) {
    flowStore.onOutput(handleOutputCallback)
  }

  // Expose the flow actions so the app header can drive them (app mode).
  uiStore.registerActions({
    run: handleRunFlow,
    save: handleSaveFlow,
    exportFile: handleExportFlow,
    open: triggerLoadFlow,
    clear: handleClearFlow
  })

  await nextTick()
  if (props.showToolbar && toolbarRef.value) {
    const rect = toolbarRef.value.getBoundingClientRect()
    // Use the toolbar's own height (panels are positioned in container-local
    // coords, not viewport coords — rect.bottom would add the container offset).
    toolbarHeight.value = rect.height
  } else {
    // No toolbar: panels dock from the top of the canvas area.
    toolbarHeight.value = 0
  }
})

onUnmounted(() => {
  window.removeEventListener('keydown', handleKeyDown)
  window.removeEventListener('click', handlePaneMenuClickOutside)
  cancelEdgeLeave()
  uiStore.clearActions()
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

/* Canvas (pane) right-click menu — reuses global .context-menu classes */
.pane-menu .context-menu-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.pane-menu .context-menu-item svg {
  color: var(--text-secondary);
}

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
  height: 32px;
  padding: 8px 16px;
  background: var(--bg-muted);
  cursor: pointer;
  user-select: none;
}

.category-header:hover {
  background: var(--border-color);
}

.category-title {
  font-size: 12px;
  font-weight: 400;
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
  height: 32px;
  padding: 8px 16px;
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

/* Availability toggle: filters the locked full-app nodes in/out of the palette. */
.availability-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 2px 4px 4px;
  font-size: 12px;
  color: var(--text-secondary);
  cursor: pointer;
  user-select: none;
}

.availability-toggle input {
  cursor: pointer;
}

/* Locked nodes: greyed-out, not draggable, with a lock glyph and a "Full app" badge. */
.node-item.unavailable {
  cursor: not-allowed;
  opacity: 0.55;
}

.node-item.unavailable:hover {
  background: var(--bg-secondary);
}

.node-lock {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  flex-shrink: 0;
  color: var(--text-secondary);
}

.node-badge {
  margin-left: auto;
  padding: 1px 5px;
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  white-space: nowrap;
  color: var(--text-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
}

/* Node settings Apply button (mirrors the main editor's primary Apply) */
.apply-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 72px;
  height: 28px;
  padding: 0 14px;
  background: var(--color-accent);
  color: #fff;
  border: 1px solid var(--color-accent);
  border-radius: var(--radius-md);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.apply-btn:hover:not(:disabled) {
  background: var(--color-accent-hover);
  border-color: var(--color-accent-hover);
}

.apply-btn:active:not(:disabled) {
  transform: translateY(1px);
}

.apply-btn:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

.apply-btn.applied {
  background: var(--color-success);
  border-color: var(--color-success);
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
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  height: 100%;
  color: var(--text-secondary);
  text-align: center;
  padding: 40px 20px;
  font-size: 13px;
}

.no-data-text {
  margin: 0;
  max-width: 280px;
}

/* Schema preview — show propagated fields before any data is fetched */
.schema-preview {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
}

.schema-preview-hint {
  color: var(--text-secondary);
  font-size: 12px;
  font-style: italic;
}

.schema-grid-container {
  flex: 1;
  min-height: 0;
  overflow: auto;
}

.schema-table {
  border-collapse: collapse;
  width: 100%;
  font-size: 12px;
}

.schema-table th {
  position: sticky;
  top: 0;
  text-align: left;
  padding: 6px 12px;
  background: var(--bg-tertiary);
  border-right: 1px solid var(--border-color);
  border-bottom: 1px solid var(--border-color);
  white-space: nowrap;
  vertical-align: top;
}

.schema-col-name {
  display: block;
  color: var(--text-primary);
  font-weight: var(--font-weight-medium);
}

.schema-col-type {
  display: block;
  margin-top: 2px;
  color: var(--accent-color);
  font-size: 11px;
  font-family: var(--font-family-mono, monospace);
}

.schema-empty-row td {
  padding: 6px 12px;
  color: var(--text-secondary);
  border-right: 1px solid var(--border-light);
  border-bottom: 1px solid var(--border-light);
  text-align: center;
}

.schema-preview-footer {
  display: flex;
  justify-content: center;
  padding: 12px;
  border-top: 1px solid var(--border-color);
}

/* Fetch-to-run CTA (accent/primary, consistent with the settings Apply button) */
.fetch-data-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 110px;
  height: 32px;
  padding: 0 18px;
  background: var(--accent-color);
  color: #fff;
  border: 1px solid var(--accent-color);
  border-radius: var(--radius-md);
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.fetch-data-button:hover:not(:disabled) {
  background: var(--accent-hover);
  border-color: var(--accent-hover);
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.fetch-data-button:active:not(:disabled) {
  transform: translateY(0);
  box-shadow: none;
}

.fetch-data-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* Multi-output (A/B/C) selector — parity-shaped, dormant for current nodes */
.output-selector {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 0;
  flex-shrink: 0;
}

.output-selector__label {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-secondary);
}

.output-selector__button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 28px;
  height: 24px;
  padding: 0 8px;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: all var(--transition-fast);
}

.output-selector__button:hover {
  background: var(--bg-hover);
  border-color: var(--accent-color);
}

.output-selector__button.active {
  background: var(--accent-color);
  border-color: var(--accent-color);
  color: #fff;
}

.output-selector__letter {
  line-height: 1;
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
