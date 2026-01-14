/**
 * Flowfile WASM - Embeddable Vue Component Library
 *
 * This is the main entry point for embedding flowfile_wasm in Vue 3 applications.
 *
 * @example Basic usage:
 * ```typescript
 * import { createApp } from 'vue'
 * import { createPinia } from 'pinia'
 * import { FlowfileEditor, useFlowStore, usePyodideStore } from 'flowfile-wasm'
 * import 'flowfile-wasm/dist/flowfile-wasm.css'
 *
 * const app = createApp(App)
 * app.use(createPinia())
 * app.component('FlowfileEditor', FlowfileEditor)
 * app.mount('#app')
 * ```
 *
 * @example With initial data:
 * ```typescript
 * <FlowfileEditor
 *   :initial-data="csvContent"
 *   :config="{ autoExecute: true }"
 *   @flow-change="handleFlowChange"
 *   @execution-complete="handleExecutionComplete"
 * />
 * ```
 */

// =============================================================================
// COMPONENTS
// =============================================================================

// Main editor component - the primary embeddable component
export { default as FlowfileEditor } from './components/FlowfileEditor.vue'

// Canvas component (lower-level, if users want more control)
export { default as Canvas } from './components/Canvas.vue'

// Individual node components (for advanced customization)
// Note: Exported as FlowNodeComponent to avoid conflict with FlowNode type
export { default as FlowNodeComponent } from './components/nodes/FlowNode.vue'

// Settings panels (for building custom UIs)
// Note: Output settings component exported as OutputSettingsPanel to avoid conflict with type
export { default as ReadCsvSettings } from './components/nodes/ReadCsvSettings.vue'
export { default as ManualInputSettings } from './components/nodes/ManualInputSettings.vue'
export { default as FilterSettings } from './components/nodes/FilterSettings.vue'
export { default as SelectSettings } from './components/nodes/SelectSettings.vue'
export { default as GroupBySettings } from './components/nodes/GroupBySettings.vue'
export { default as JoinSettings } from './components/nodes/JoinSettings.vue'
export { default as SortSettings } from './components/nodes/SortSettings.vue'
export { default as PolarsCodeSettings } from './components/nodes/PolarsCodeSettings.vue'
export { default as UniqueSettings } from './components/nodes/UniqueSettings.vue'
export { default as HeadSettings } from './components/nodes/HeadSettings.vue'
export { default as PreviewSettings } from './components/nodes/PreviewSettings.vue'
export { default as PivotSettings } from './components/nodes/PivotSettings.vue'
export { default as UnpivotSettings } from './components/nodes/UnpivotSettings.vue'
export { default as OutputSettingsPanel } from './components/nodes/OutputSettings.vue'

// Common UI components
export { default as DraggablePanel } from './components/common/DraggablePanel.vue'
export { default as CodeGenerator } from './components/CodeGenerator.vue'

// =============================================================================
// STORES (Pinia)
// =============================================================================

export { useFlowStore } from './stores/flow-store'
export { usePyodideStore } from './stores/pyodide-store'
export { useThemeStore } from './stores/theme-store'
export { fileStorage, FileStorageManager } from './stores/file-storage'

// Panel state utilities (localStorage-based, not a Pinia store)
export {
  getPanelState,
  savePanelState,
  clearPanelState,
  clearAllPanelStates,
  type PanelState
} from './stores/panel-store'

// =============================================================================
// COMPOSABLES
// =============================================================================

export { useTheme } from './composables/useTheme'
export { useCodeGeneration } from './composables/useCodeGeneration'
export { useDemo } from './composables/useDemo'

// =============================================================================
// TYPES
// =============================================================================

export type {
  // Base types
  NodeBase,
  NodeSingleInput,
  NodeMultiInput,

  // Filter types
  FilterOperator,
  BasicFilter,
  FilterInput,

  // Select types
  SelectInput,

  // Sort types
  SortByInput,
  SortColumn,

  // Group by types
  AggType,
  AggCol,
  AggColumn,
  GroupByInput,

  // Join types
  JoinStrategy,
  JoinMap,
  JoinMapping,
  JoinInputs,
  JoinInput,
  JoinType,

  // Unique types
  UniqueInput,

  // Pivot/Unpivot types
  PivotInput,
  UnpivotInput,
  UnpivotDataTypeSelector,
  UnpivotSelectorMode,

  // Output types
  OutputFileType,
  OutputWriteMode,
  OutputCsvTable,
  OutputParquetTable,
  OutputTableSettings,
  OutputPolarsMethod,
  OutputSettings,

  // Formula types
  FieldInput,
  FunctionInput,

  // Sample types
  SampleInput,

  // Manual input types
  MinimalFieldInfo,
  RawData,

  // Input table types
  InputCsvTable,
  ReceivedTable,

  // Node settings types
  NodeReadSettings,
  NodeManualInputSettings,
  NodeFilterSettings,
  NodeSelectSettings,
  NodeSortSettings,
  NodeGroupBySettings,
  NodeJoinSettings,
  NodeUniqueSettings,
  NodeFormulaSettings,
  NodeSampleSettings,
  NodePreviewSettings,
  NodePivotSettings,
  NodeUnpivotSettings,
  NodeOutputSettings,
  NodeSettings,

  // Flowfile data types
  FlowfileSettings,
  FlowfileNode,
  NodeConnection,
  FlowfileData,

  // Runtime types
  FlowNode,
  FlowEdge,
  ColumnSchema,
  DataPreview,
  DownloadInfo,
  NodeResult,
  FlowState,

  // Node type
  NodeType,

  // Legacy types (for backwards compatibility)
  SelectColumn,
  WithColumnDef
} from './types'

export { NODE_TYPES, FILTER_OPERATOR_LABELS } from './types'

// =============================================================================
// UTILITIES
// =============================================================================

export { inferOutputSchema, isSourceNode, inferSchemaFromCsv, inferSchemaFromRawData } from './stores/schema-inference'
export { getNodeDescription } from './config/nodeDescriptions'

// =============================================================================
// CONFIGURATION TYPES
// =============================================================================

/**
 * Configuration options for the FlowfileEditor component
 */
export interface FlowfileEditorConfig {
  /**
   * Pyodide CDN URL (default: 'https://cdn.jsdelivr.net/pyodide/v0.27.7/full/')
   */
  pyodideUrl?: string

  /**
   * Additional Python packages to load
   */
  additionalPackages?: string[]

  /**
   * Whether to auto-execute the flow when data changes
   */
  autoExecute?: boolean

  /**
   * Whether to show the toolbar
   */
  showToolbar?: boolean

  /**
   * Whether to show the node sidebar
   */
  showSidebar?: boolean

  /**
   * Whether to show the data preview panel
   */
  showPreview?: boolean

  /**
   * Theme: 'light' or 'dark' (default: 'light')
   */
  theme?: 'light' | 'dark'

  /**
   * Initial flow data to load (FlowfileData format)
   */
  initialFlow?: import('./types').FlowfileData

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

/**
 * Events emitted by the FlowfileEditor component
 */
export interface FlowfileEditorEvents {
  /**
   * Emitted when the flow structure changes (nodes added/removed, edges changed)
   */
  'flow-change': (data: import('./types').FlowfileData) => void

  /**
   * Emitted when flow execution completes
   */
  'execution-complete': (results: Map<number, import('./types').NodeResult>) => void

  /**
   * Emitted when execution fails
   */
  'execution-error': (error: string) => void

  /**
   * Emitted when Pyodide is ready
   */
  'pyodide-ready': () => void

  /**
   * Emitted when a node is selected
   */
  'node-selected': (nodeId: number | null) => void

  /**
   * Emitted when data is loaded into a source node
   */
  'data-loaded': (nodeId: number, fileName: string) => void
}

// =============================================================================
// PLUGIN INSTALLATION
// =============================================================================

import type { App, Plugin } from 'vue'
import FlowfileEditor from './components/FlowfileEditor.vue'

/**
 * Vue plugin for installing Flowfile WASM components globally
 *
 * @example
 * ```typescript
 * import { createApp } from 'vue'
 * import { createPinia } from 'pinia'
 * import { FlowfileWasmPlugin } from 'flowfile-wasm'
 * import 'flowfile-wasm/dist/flowfile-wasm.css'
 *
 * const app = createApp(App)
 * app.use(createPinia())
 * app.use(FlowfileWasmPlugin)
 * app.mount('#app')
 * ```
 */
export const FlowfileWasmPlugin: Plugin = {
  install(app: App) {
    app.component('FlowfileEditor', FlowfileEditor)
  }
}

// Default export for convenience
export default FlowfileWasmPlugin
