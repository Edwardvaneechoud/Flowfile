/**
 * Public API types for the FlowfileEditor embeddable component
 */
import type { FlowfileData, NodeResult } from '../types'

/** Configuration for Pyodide initialization */
export interface PyodideConfig {
  /** Whether to auto-initialize Pyodide on mount (default: true) */
  autoInit?: boolean
  /** Custom Pyodide CDN URL (default: jsdelivr v0.27.7) */
  pyodideUrl?: string
}

/** Input data that can be passed programmatically to the editor */
export interface InputDataItem {
  /** The data content: CSV/JSON string, or binary bytes (Arrow IPC stream / Parquet file) */
  content: string | Uint8Array
  /** Data format (default: 'csv' for strings; binary defaults to a PAR1 sniff
   * — pass 'arrow-ipc' or 'parquet' explicitly when you know it) */
  format?: 'csv' | 'json' | 'arrow-ipc' | 'parquet'
  /** CSV delimiter (default: ',') */
  delimiter?: string
  /** Whether the CSV has headers (default: true) */
  hasHeaders?: boolean
}

export type InputDataMap = Record<string, InputDataItem | string>

/** Theme configuration */
export interface ThemeConfig {
  /** Initial theme mode */
  mode?: 'light' | 'dark' | 'system'
}

/** Toolbar configuration */
export interface ToolbarConfig {
  /** Show the Run button (default: true) */
  showRun?: boolean
  /** Show Save/Load buttons (default: true) */
  showSaveLoad?: boolean
  /** Show Clear button (default: true) */
  showClear?: boolean
  /** Show Code Generator button (default: true) */
  showCodeGen?: boolean
  /** Show Demo button (default: false for embedded) */
  showDemo?: boolean
}

/** Configuration for which node types are available in the editor */
export interface NodeCategoryConfig {
  name: string
  enabled?: boolean
  nodes?: string[]
}

/** Props for the FlowfileEditor component */
export interface FlowfileEditorProps {
  /** Initial flow state to load */
  initialFlow?: FlowfileData
  /** Input datasets available to the editor */
  inputData?: InputDataMap
  /** Pyodide initialization configuration */
  pyodide?: PyodideConfig
  /** Theme configuration */
  theme?: ThemeConfig
  /** Toolbar visibility configuration */
  toolbar?: ToolbarConfig
  /** Available node categories (null = all) */
  nodeCategories?: NodeCategoryConfig[]
  /** Whether the editor is read-only */
  readonly?: boolean
  /** Height of the editor (default: '100%') */
  height?: string
  /** Width of the editor (default: '100%') */
  width?: string
}

/** Output data emitted by output nodes */
export interface OutputData {
  nodeId: number
  content: string
  fileName: string
  mimeType: string
}

/** Error data emitted by the editor */
export interface EditorError {
  type: 'pyodide' | 'execution' | 'load'
  message: string
}

/** Return type for the exposed programmatic API (via template ref) */
export interface FlowfileEditorAPI {
  /** Whether Pyodide is initialized and ready */
  readonly isReady: boolean
  /** Whether a flow execution is in progress */
  readonly isExecuting: boolean
  /** Programmatically run the entire flow */
  executeFlow: () => Promise<void>
  /** Execute a single node */
  executeNode: (nodeId: number) => Promise<NodeResult | undefined>
  /** Export the current flow as FlowfileData */
  exportFlow: () => FlowfileData
  /** Import a flow from FlowfileData */
  importFlow: (data: FlowfileData) => boolean
  /** Set input data for a named dataset (matched by node_reference). Binary
   * content (Uint8Array) is treated as Arrow IPC / Parquet (pass format to be
   * explicit). */
  setInputData: (name: string, content: string | Uint8Array, format?: InputDataItem['format']) => void
  /** Get the result/preview for a specific node */
  getNodeResult: (nodeId: number) => NodeResult | undefined
  /** Get a node's full result frame as Arrow IPC stream bytes (null when the
   * node hasn't produced one). Zero CSV stringification — feed it straight to
   * arrow-js / duckdb-wasm. */
  getNodeResultArrow: (nodeId: number) => Promise<Uint8Array | null>
  /** Clear the entire flow */
  clearFlow: () => void
  /** Initialize Pyodide manually (when autoInit is false) */
  initializePyodide: () => Promise<void>
}
