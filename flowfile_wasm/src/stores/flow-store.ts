import { defineStore } from 'pinia'
import { ref, computed, shallowRef, watch } from 'vue'
import { usePyodideStore } from './pyodide-store'
import yaml from 'js-yaml'
import { inferOutputSchema, isSourceNode, inferSchemaFromCsv, inferSchemaFromRawData } from './schema-inference'
import { fileStorage, SIZE_THRESHOLD } from './file-storage'
import { asFileContent, contentByteSize, isBinary, type FileContent } from '../types/file-content'
import { ipcStreamToParquet, parquetToIpcStream } from '../utils/parquet-bridge'
import type {
  FlowNode,
  FlowEdge,
  NodeResult,
  NodeSettings,
  ColumnSchema,
  FlowfileData,
  FlowfileNode,
  NodeBase,
  NodeReadSettings,
  NodeManualInputSettings,
  NodeExternalDataSettings,
  NodeReadFromCatalogSettings,
  NodeExternalOutputSettings,
  NodeOutputSettings,
  NodeWriteToCatalogSettings,
  NodeFilterSettings,
  NodeSelectSettings,
  NodeGroupBySettings
} from '../types'

// Session storage keys
const STORAGE_KEY = 'flowfile_wasm_state'
const STORAGE_VERSION = '2'  // Increment when storage format changes

/**
 * A full, self-contained snapshot of a flow's live state — graph (FlowfileData),
 * in-memory CSV file contents (by node id), the node-id counter, and the flow
 * name. Used by the multi-flow tabs store to stash/restore tabs.
 */
export interface FlowStateSnapshot {
  name: string
  /** Stable library id, or null for a flow not yet saved to the library. */
  flowId: string | null
  snapshot: FlowfileData
  fileContents: Record<number, FileContent>
  nodeIdCounter: number
}

// Preview cache limits to prevent memory bloat
const PREVIEW_CACHE_MAX_SIZE = 20  // Max number of cached previews in TypeScript
const PREVIEW_CACHE_MAX_AGE_MS = 5 * 60 * 1000  // 5 minutes max age

// Fields to exclude from setting_input when exporting (matches flowfile_core)
const SETTING_INPUT_EXCLUDE = new Set([
  'flow_id',
  'node_id',
  'pos_x',
  'pos_y',
  'is_setup',
  'description',
  'node_reference',  // Stored at node level, not in setting_input
  'user_id',
  'is_flow_output',
  'is_user_defined',
  'depending_on_id',
  'depending_on_ids',
])

// Fields to exclude from nested objects (not in flowfile_core schema)
const NESTED_FIELDS_EXCLUDE = new Set([
  'is_available',  // Used internally for UI state, not part of flowfile_core
])

/**
 * Recursively clean an object/array by removing excluded fields
 */
function deepClean(value: any): any {
  if (value === null || value === undefined) return value
  if (Array.isArray(value)) {
    return value.map(item => deepClean(item))
  }
  if (typeof value === 'object') {
    const cleaned: Record<string, any> = {}
    for (const [key, val] of Object.entries(value)) {
      if (!NESTED_FIELDS_EXCLUDE.has(key)) {
        cleaned[key] = deepClean(val)
      }
    }
    return cleaned
  }
  return value
}

/**
 * Clean setting_input by removing fields that are excluded during export
 * This matches the behavior of flowfile_core's FlowfileNode serializer
 */
function cleanSettingInput(settings: NodeSettings): any {
  if (!settings) return null
  const cleaned: Record<string, any> = {}
  for (const [key, value] of Object.entries(settings)) {
    if (!SETTING_INPUT_EXCLUDE.has(key)) {
      cleaned[key] = deepClean(value)
    }
  }
  return cleaned
}

function toPythonJson(value: unknown): string {
  return JSON.stringify(JSON.stringify(value))
}

export const useFlowStore = defineStore('flow', () => {
  const pyodideStore = usePyodideStore()

  // State
  const nodes = ref<Map<number, FlowNode>>(new Map())
  const edges = ref<FlowEdge[]>([])
  const nodeResults = ref<Map<number, NodeResult>>(new Map())
  const selectedNodeId = ref<number | null>(null)
  const isExecuting = ref(false)
  const executionError = ref<string | null>(null)
  const nodeIdCounter = ref(0)
  // Name of the flow currently open (drives the library + Run History + header).
  const currentFlowName = ref<string>('Untitled Flow')
  // Stable library id of the flow currently open, or null until first saved.
  // Re-saving updates this entry; rename keeps it (non-lossy).
  const currentFlowId = ref<string | null>(null)

  function genFlowId(): string {
    return globalThis.crypto?.randomUUID?.() ?? `flow-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  }

  // File content storage for CSV nodes
  const fileContents = ref<Map<number, FileContent>>(new Map())

  // Output callbacks for embeddable mode
  type OutputCallback = (data: { nodeId: number; content: string; fileName: string; mimeType: string }) => void
  const outputCallbacks = new Set<OutputCallback>()

  // External datasets provided by the host application (for embedded mode)
  const externalDatasets = ref<Map<string, FileContent>>(new Map())
  // Catalog datasets: CSV tables the user uploads in the Catalog (persisted to
  // IndexedDB). Independent from host-injected externalDatasets. Read by the
  // dedicated "Read from Catalog" node.
  const catalogDatasets = ref<Map<string, string>>(new Map())

  // Before-export hooks: called right before the flow is exported to YAML.
  // Used by ExploreData.vue to flush the live Graphic Walker chart spec
  // into the node settings so it survives round-trip save/load.
  type BeforeExportHook = () => void | Promise<void>
  const beforeExportHooks = new Set<BeforeExportHook>()

  function registerBeforeExportHook(hook: BeforeExportHook): () => void {
    beforeExportHooks.add(hook)
    return () => beforeExportHooks.delete(hook)
  }

  async function runBeforeExportHooks(): Promise<void> {
    for (const hook of beforeExportHooks) {
      try {
        await hook()
      } catch (err) {
        console.warn('[flow-store] beforeExport hook failed:', err)
      }
    }
  }

  // Preview cache state (for lazy loading)
  const previewCache = ref<Map<number, {
    data: any;
    timestamp: number;
    loading: boolean;
  }>>(new Map())
  const previewLoadingNodes = ref<Set<number>>(new Set())

  // Track nodes that have been modified since last execution (dirty state)
  const dirtyNodes = ref<Set<number>>(new Set())

  async function loadFromStorage() {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY)
      if (saved) {
        const state = JSON.parse(saved)
        // Check for new FlowfileData format (version 2+)
        if (state.version === STORAGE_VERSION && state.flowfileData) {
          const data = state.flowfileData as FlowfileData

          for (const flowfileNode of data.nodes) {
            // Migrate old node types to new names (for backward compatibility)
            let nodeType = flowfileNode.type
            if (nodeType === 'read_csv') nodeType = 'read'
            if (nodeType === 'preview') nodeType = 'explore_data'

            // Migrate old settings field names
            let settings = flowfileNode.setting_input as NodeSettings
            if (settings && (settings as any).received_table && !(settings as any).received_file) {
              (settings as any).received_file = (settings as any).received_table
              delete (settings as any).received_table
            }

            // Get description and node_reference from FlowfileNode level (this is where flowfile_core stores them)
            const nodeDescription = flowfileNode.description || ''
            const nodeReference = flowfileNode.node_reference || undefined

            // Sync description and node_reference to settings for backward compatibility
            if (settings) {
              (settings as NodeBase).description = nodeDescription
              if (nodeReference) {
                (settings as NodeBase).node_reference = nodeReference
              }
            }

            const node: FlowNode = {
              id: flowfileNode.id,
              type: nodeType,
              x: flowfileNode.x_position,
              y: flowfileNode.y_position,
              settings,
              inputIds: flowfileNode.input_ids || [],
              leftInputId: flowfileNode.left_input_id,
              rightInputId: flowfileNode.right_input_id,
              description: nodeDescription,
              node_reference: nodeReference
            }
            nodes.value.set(flowfileNode.id, node)
          }

          // Import connections - support both explicit connections array and implicit derivation
          if (data.connections && data.connections.length > 0) {
            // Explicit connections array (WASM format)
            for (const conn of data.connections) {
              edges.value.push({
                id: `e${conn.from_node}-${conn.to_node}-${conn.from_handle}-${conn.to_handle}`,
                source: String(conn.from_node),
                target: String(conn.to_node),
                sourceHandle: conn.from_handle,
                targetHandle: conn.to_handle
              })
            }
          } else {
            // Derive edges from node relationships (flowfile_core format)
            deriveEdgesFromNodes(data.nodes)
          }

          // Restore file contents from sessionStorage (small text files)
          if (state.fileContents) {
            fileContents.value = new Map(
              (state.fileContents as Array<[number, string]>).map(([nid, content]) => [nid, asFileContent(content)])
            )
          }

          // Restore large/binary file contents from IndexedDB
          if (state.largeFileNodeIds && Array.isArray(state.largeFileNodeIds)) {
            await Promise.all(
              state.largeFileNodeIds.map(async (nodeId: number) => {
                try {
                  const content = await fileStorage.getFileContent(nodeId)
                  if (content) {
                    fileContents.value.set(nodeId, asFileContent(content))
                  }
                } catch (err) {
                  console.error(`Failed to load large file for node ${nodeId} from IndexedDB:`, err)
                }
              })
            )
          }

          // Restore node schemas for quick column access
          // Note: success is undefined since data wasn't actually executed in this session
          if (state.nodeSchemas) {
            for (const [id, schema] of state.nodeSchemas) {
              if (schema && schema.length > 0) {
                nodeResults.value.set(id, { schema })
              }
            }
          }

          const maxId = Math.max(0, ...data.nodes.map(n => n.id))
          nodeIdCounter.value = state.nodeIdCounter ?? maxId

          // Restore the active flow's library identity + name (safety net for a
          // page reload; the tabs store also carries these per tab).
          if (typeof state.currentFlowName === 'string') currentFlowName.value = state.currentFlowName
          if (typeof state.currentFlowId === 'string') currentFlowId.value = state.currentFlowId
        }
        // Fallback: legacy format (version 1)
        else if (state.nodes) {
          nodes.value = new Map(state.nodes)
          if (state.edges) edges.value = state.edges
          if (state.fileContents) {
            fileContents.value = new Map(
              (state.fileContents as Array<[number, string]>).map(([nid, content]) => [nid, asFileContent(content)])
            )
          }
          if (state.nodeIdCounter !== undefined) nodeIdCounter.value = state.nodeIdCounter
        }
      }
    } catch (err) {
      console.error('Failed to load state from session storage', err)
    }
  }

  /**
   * Derive edges from node relationships (flowfile_core format)
   * This handles imports from flowfile_core which doesn't have explicit connections
   */
  function deriveEdgesFromNodes(flowfileNodes: FlowfileNode[]) {
    const derivedEdges: FlowEdge[] = []

    for (const targetNode of flowfileNodes) {
      // Handle right_input_id (for join nodes - second input goes to input-1)
      if (targetNode.right_input_id !== undefined && targetNode.right_input_id !== null) {
        const sourceId = targetNode.right_input_id
        derivedEdges.push({
          id: `e${sourceId}-${targetNode.id}-output-0-input-1`,
          source: String(sourceId),
          target: String(targetNode.id),
          sourceHandle: 'output-0',
          targetHandle: 'input-1'
        })
      }

      // Handle input_ids - these go to input-0
      // In flowfile_core format: left_input_id is always null, inputs are in input_ids
      // For join nodes: input_ids contains the left input, right_input_id has the right input
      if (targetNode.input_ids && targetNode.input_ids.length > 0) {
        for (const sourceId of targetNode.input_ids) {
          derivedEdges.push({
            id: `e${sourceId}-${targetNode.id}-output-0-input-0`,
            source: String(sourceId),
            target: String(targetNode.id),
            sourceHandle: 'output-0',
            targetHandle: 'input-0'
          })
        }
      }

      // Handle legacy left_input_id (for backwards compatibility with old WASM format)
      if (targetNode.left_input_id !== undefined && targetNode.left_input_id !== null) {
        // Only add if not already in input_ids
        if (!targetNode.input_ids?.includes(targetNode.left_input_id)) {
          const sourceId = targetNode.left_input_id
          derivedEdges.push({
            id: `e${sourceId}-${targetNode.id}-output-0-input-0`,
            source: String(sourceId),
            target: String(targetNode.id),
            sourceHandle: 'output-0',
            targetHandle: 'input-0'
          })
        }
      }
    }

    edges.value.push(...derivedEdges)
    console.log('[deriveEdgesFromNodes] Created edges:', derivedEdges.map(e => `${e.source}->${e.target}`))
  }

  // Save state to session storage using FlowfileData format (flowfile_core compatible)
  async function saveToStorage() {
    // Build FlowfileData structure (without connections - flowfile_core format)
    const flowfileNodes: FlowfileNode[] = []

    nodes.value.forEach((node, id) => {
      const isStartNode = node.inputIds.length === 0 && !node.leftInputId && !node.rightInputId
      const outputs = edges.value
        .filter(e => e.source === String(id))
        .map(e => parseInt(e.target))

      // In flowfile_core format, left_input_id is always null - inputs are in input_ids
      // Only right_input_id is used (for join nodes' second input)
      // Read description and node_reference from node level (primary) with fallback to settings
      flowfileNodes.push({
        id: node.id,
        type: node.type,
        is_start_node: isStartNode,
        description: node.description || (node.settings as NodeBase).description || '',
        node_reference: node.node_reference || (node.settings as NodeBase).node_reference,
        x_position: Math.round(node.x),  // flowfile_core expects int
        y_position: Math.round(node.y),  // flowfile_core expects int
        right_input_id: node.rightInputId,
        input_ids: node.inputIds,
        outputs,
        setting_input: cleanSettingInput(node.settings)
      })
    })

    // No connections array - flowfile_core derives connections from node relationships
    const flowfileData: FlowfileData = {
      flowfile_version: '1.0.0',
      flowfile_id: Date.now(),
      flowfile_name: 'Session Flow',
      flowfile_settings: {
        description: '',
        execution_mode: 'Development',
        execution_location: 'local',
        auto_save: true,
        show_detailed_progress: false
      },
      nodes: flowfileNodes
    }

    // Separate small and large files for hybrid storage. sessionStorage keeps
    // the legacy [nodeId, string] shape; binary always lives in IndexedDB.
    const smallFiles: Array<[number, string]> = []
    const largeFileNodeIds: number[] = []

    for (const [nodeId, content] of fileContents.value.entries()) {
      if (isBinary(content) || fileStorage.shouldUseIndexedDB(content)) {
        largeFileNodeIds.push(nodeId)
        // Save asynchronously (don't await to avoid blocking)
        fileStorage.setFileContent(nodeId, content).catch(err => {
          console.error(`Failed to save large file for node ${nodeId} to IndexedDB:`, err)
        })
      } else {
        smallFiles.push([nodeId, content.data])
      }
    }

    try {
      const state = {
        version: STORAGE_VERSION,
        flowfileData,
        fileContents: smallFiles,
        largeFileNodeIds,
        nodeIdCounter: nodeIdCounter.value,
        currentFlowName: currentFlowName.value,
        currentFlowId: currentFlowId.value,
        // Save schemas separately for quick reload
        nodeSchemas: Array.from(nodeResults.value.entries()).map(([id, result]) => [id, result.schema])
      }

      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state))
    } catch (err) {
      console.error('Failed to save state to session storage', err)

      // If quota exceeded, try saving minimal state without schemas
      if (err instanceof DOMException && err.name === 'QuotaExceededError') {
        try {
          const minimalState = {
            version: STORAGE_VERSION,
            flowfileData: {
              ...flowfileData,
              nodes: flowfileData.nodes.map((n: FlowfileNode) => ({
                ...n,
                setting_input: {} // Clear settings to save space
              }))
            },
            fileContents: [], // No files in fallback
            largeFileNodeIds,
            nodeIdCounter: nodeIdCounter.value,
            nodeSchemas: [] // No schemas in fallback
          }
          sessionStorage.setItem(STORAGE_KEY, JSON.stringify(minimalState))
          console.warn('Saved minimal state due to quota limits. Large files are in IndexedDB.')
        } catch (fallbackErr) {
          console.error('Failed to save even minimal state:', fallbackErr)
          // Clear session storage if it's completely full
          try {
            sessionStorage.removeItem(STORAGE_KEY)
          } catch (clearErr) {
            console.error('Could not clear session storage:', clearErr)
          }
        }
      }
    }
  }

  // Watch for changes and save (debounced via microtask)
  let saveTimeout: number | null = null
  function scheduleSave() {
    if (saveTimeout) return
    saveTimeout = window.setTimeout(() => {
      saveToStorage()
      saveTimeout = null
    }, 100)
  }

  watch([nodes, edges, fileContents, nodeIdCounter], scheduleSave, { deep: true })

  watch(() => edges.value, () => {
    debouncedPropagateSchemas()
  }, { deep: true })

  // Re-run schema propagation once Pyodide finishes warming up, so column
  // pickers fill in automatically without requiring a manual run. Until then,
  // propagateSchemasTS() provides instant (less precise) source/inference paint.
  watch(() => pyodideStore.isReady, (ready) => {
    if (ready) debouncedPropagateSchemas()
  })

  // Watch for node settings changes to trigger schema propagation
  watch(
    () => {
      const settingsSnapshot: Record<number, string> = {}
      nodes.value.forEach((node, id) => {
        settingsSnapshot[id] = JSON.stringify(node.settings)
      })
      return settingsSnapshot
    },
    () => {
      debouncedPropagateSchemas()
    },
    { deep: true }
  )

  loadCatalogDatasets()
  loadFromStorage()
    .then(() => {
      // Ensure nodeResults are populated from storage before propagating
      return propagateSchemas()
    })
    .catch(err => console.error('Failed to initialize from storage:', err))

  // Getters
  const nodeList = computed(() => Array.from(nodes.value.values()))

  const getNode = (id: number) => nodes.value.get(id)

  const getNodeResult = (id: number) => nodeResults.value.get(id)

  const getDownloadContent = async (nodeId: number) => {
    return await fileStorage.getDownloadContent(nodeId)
  }

  const getNodeInputSchema = (nodeId: number): ColumnSchema[] => {
    const node = nodes.value.get(nodeId)
    if (!node) return []

    const inputIds = node.inputIds
    if (inputIds.length > 0) {
      const result = nodeResults.value.get(inputIds[0])
      return result?.schema || []
    }

    // Check left/right inputs for join nodes
    if (node.leftInputId) {
      const result = nodeResults.value.get(node.leftInputId)
      return result?.schema || []
    }

    return []
  }

  const getLeftInputSchema = (nodeId: number): ColumnSchema[] => {
    const node = nodes.value.get(nodeId)
    // Use leftInputId if set, otherwise fall back to inputIds[0] (main input)
    // In flowfile_core, join nodes use main input as the left table
    const leftId = node?.leftInputId ?? node?.inputIds[0]
    if (!leftId) return []
    const result = nodeResults.value.get(leftId)
    return result?.schema || []
  }

  const getRightInputSchema = (nodeId: number): ColumnSchema[] => {
    const node = nodes.value.get(nodeId)
    if (!node?.rightInputId) return []
    const result = nodeResults.value.get(node.rightInputId)
    return result?.schema || []
  }

  // Whether this node's input schema is fully known without a run. False when an
  // upstream output is data-dependent (e.g. pivot) and only known after running.
  const isInputSchemaResolved = (nodeId: number): boolean => {
    const node = nodes.value.get(nodeId)
    if (!node) return true
    const inputId = node.leftInputId || node.inputIds[0]
    if (!inputId) return true
    return nodeResults.value.get(inputId)?.schemaResolved !== false
  }

  // Actions
  function generateNodeId(): number {
    nodeIdCounter.value++
    return nodeIdCounter.value
  }

  function addNode(type: string, x: number, y: number): number {
    const id = generateNodeId()
    const defaultSettings = getDefaultSettings(type, id, x, y)

    const node: FlowNode = {
      id,
      type,
      x,
      y,
      settings: defaultSettings,
      inputIds: [],
      leftInputId: undefined,
      rightInputId: undefined,
      description: ''
    }

    nodes.value.set(id, node)
    return id
  }

  // Node clipboard persisted to localStorage so copy/paste survives reloads.
  // Loaded file content (keyed by node id) travels with the copy; content too
  // large for localStorage rides the in-memory clipboard for same-session paste.
  const CLIPBOARD_KEY = 'flowfile-wasm-node-clipboard'

  interface NodeClipboardPayload {
    type: string
    settings: Record<string, any>
    description?: string
    /** localStorage carries plain strings only; the in-memory clipboard may hold binary FileContent. */
    fileContent?: string | FileContent
    copiedAt?: number
  }

  const memoryClipboard = shallowRef<NodeClipboardPayload | null>(null)

  function copyNode(nodeId: number): boolean {
    const node = nodes.value.get(nodeId)
    if (!node) return false
    let payload: NodeClipboardPayload
    try {
      payload = {
        type: node.type,
        settings: JSON.parse(JSON.stringify(node.settings ?? {})),
        description: node.description || '',
        fileContent: fileContents.value.get(nodeId),
        copiedAt: Date.now()
      }
    } catch {
      return false
    }
    memoryClipboard.value = payload
    // Persist text-only: JSON.stringify corrupts Uint8Array (index-keyed object);
    // binary copies remain paste-able in-session via the in-memory clipboard.
    const fc = payload.fileContent as FileContent | undefined
    const persistedContent =
      fc !== undefined && !isBinary(fc) && !fileStorage.shouldUseIndexedDB(fc) ? fc.data : undefined
    try {
      localStorage.setItem(
        CLIPBOARD_KEY,
        JSON.stringify({ ...payload, fileContent: persistedContent })
      )
    } catch {
      // Quota exceeded — persist settings only; content stays in memory.
      try {
        localStorage.setItem(CLIPBOARD_KEY, JSON.stringify({ ...payload, fileContent: undefined }))
      } catch {
        /* localStorage unavailable — the in-memory clipboard still works */
      }
    }
    return true
  }

  function hasClipboard(): boolean {
    if (memoryClipboard.value) return true
    try {
      return !!localStorage.getItem(CLIPBOARD_KEY)
    } catch {
      return false
    }
  }

  function pasteNode(x: number, y: number): number | null {
    let stored: NodeClipboardPayload | null = null
    try {
      const raw = localStorage.getItem(CLIPBOARD_KEY)
      if (raw) stored = JSON.parse(raw)
    } catch {
      /* fall back to the in-memory clipboard */
    }
    // Prefer the in-memory copy (it can carry file content too large to
    // persist) unless another tab copied something newer into localStorage.
    const inMemory = memoryClipboard.value
    let payload = stored
    if (inMemory && (!stored || (stored.copiedAt ?? 0) <= (inMemory.copiedAt ?? 0))) {
      payload = inMemory
    }
    if (!payload?.type) return null

    const id = addNode(payload.type, x, y)
    const node = nodes.value.get(id)
    if (node) {
      // Merge copied settings but keep the new node's identity/position and drop
      // any input linkage from the source node.
      const merged: Record<string, any> = { ...(payload.settings || {}), node_id: id, pos_x: x, pos_y: y }
      delete merged.depending_on_id
      delete merged.depending_on_ids
      node.settings = merged as NodeSettings
      node.description = payload.description || ''
      nodes.value.set(id, { ...node })
      if (payload.fileContent !== undefined) {
        // Re-key the copied node's loaded data to the new id so the paste can
        // actually run (also seeds the inferred schema + preview invalidation).
        setFileContent(id, payload.fileContent)
      } else {
        invalidatePreviewCache(id)
      }
    }
    return id
  }

  function updateNode(id: number, updates: Partial<FlowNode>) {
    const node = nodes.value.get(id)
    if (node) {
      nodes.value.set(id, { ...node, ...updates })
    }
  }

  function updateNodeSettings(id: number, settings: NodeSettings) {
    const node = nodes.value.get(id)
    if (node) {
      node.settings = settings
      nodes.value.set(id, node)

      invalidatePreviewCache(id)

      // A stale download must not survive settings edits — the panel would
      // serve the OLD bytes under the NEW name/format until the next run.
      const result = nodeResults.value.get(id)
      if (result?.download) {
        nodeResults.value.set(id, { ...result, download: undefined })
        dirtyNodes.value.add(id)
      }
    }
  }

  /**
   * Update a subset of node settings without triggering cache invalidation
   * or re-execution. Used by explore_data to persist saved Graphic Walker
   * chart specs back to the node settings without re-running the flow.
   */
  function updateNodeSettingsSilent(id: number, partial: Record<string, any>) {
    const node = nodes.value.get(id)
    if (node) {
      node.settings = { ...(node.settings as any), ...partial } as NodeSettings
      nodes.value.set(id, { ...node })
    }
  }

  /**
   * Update the description for a node
   * This updates the node-level description (primary storage)
   * and syncs to settings.description for backward compatibility
   */
  function updateNodeDescription(id: number, description: string) {
    const node = nodes.value.get(id)
    if (node) {
      node.description = description
      // Also sync to settings for backward compatibility with flowfile_core
      if (node.settings) {
        (node.settings as NodeBase).description = description
      }
      nodes.value.set(id, node)
    }
  }

  /**
   * Update the node_reference for a node
   */
  function updateNodeReference(id: number, reference: string | undefined) {
    const node = nodes.value.get(id)
    if (node) {
      node.node_reference = reference || undefined
      if (node.settings) {
        (node.settings as NodeBase).node_reference = reference || undefined
      }
      nodes.value.set(id, node)
    }
  }

  /**
   * Validate a node_reference (lowercase, no spaces, unique)
   */
  function validateNodeReference(nodeId: number, reference: string): { valid: boolean; error: string | null } {
    if (!reference || reference === '') {
      return { valid: true, error: null }
    }

    if (reference !== reference.toLowerCase()) {
      return { valid: false, error: 'Reference must be lowercase' }
    }

    if (/\s/.test(reference)) {
      return { valid: false, error: 'Reference cannot contain spaces' }
    }

    if (!/^[a-z][a-z0-9_]*$/.test(reference)) {
      return { valid: false, error: 'Reference must start with a letter and contain only lowercase letters, numbers, and underscores' }
    }

    for (const [id, node] of nodes.value) {
      if (id !== nodeId) {
        const existingRef = node.node_reference || (node.settings as NodeBase)?.node_reference
        if (existingRef === reference) {
          return { valid: false, error: `Reference "${reference}" is already used by another node` }
        }
      }
    }

    return { valid: true, error: null }
  }

  function removeNode(id: number) {
    nodes.value.delete(id)
    nodeResults.value.delete(id)
    fileContents.value.delete(id)
    previewCache.value.delete(id)
    dirtyNodes.value.delete(id)

    fileStorage.deleteFileContent(id).catch(err => {
      // Silently ignore if file doesn't exist in IndexedDB
      if (err && err.name !== 'NotFoundError') {
        console.error(`Failed to delete file for node ${id} from IndexedDB:`, err)
      }
    })

    edges.value = edges.value.filter(
      e => e.source !== String(id) && e.target !== String(id)
    )

    nodes.value.forEach(node => {
      node.inputIds = node.inputIds.filter(inputId => inputId !== id)
      if (node.leftInputId === id) node.leftInputId = undefined
      if (node.rightInputId === id) node.rightInputId = undefined
    })
  }

  function addEdge(edge: FlowEdge) {
    const exists = edges.value.some(
      e => e.source === edge.source &&
           e.target === edge.target &&
           e.sourceHandle === edge.sourceHandle &&
           e.targetHandle === edge.targetHandle
    )

    if (!exists) {
      edges.value.push(edge)

      const targetId = parseInt(edge.target)
      const sourceId = parseInt(edge.source)
      const targetNode = nodes.value.get(targetId)

      if (targetNode) {
        if (edge.targetHandle === 'input-0' || !edge.targetHandle) {
          // input-0 is the default/left input
          if (!targetNode.inputIds.includes(sourceId)) {
            targetNode.inputIds.push(sourceId)
          }
          // For join nodes, also set leftInputId
          targetNode.leftInputId = sourceId
        } else if (edge.targetHandle === 'input-1') {
          // For join nodes, input-1 is the right input
          targetNode.rightInputId = sourceId
        }

        invalidatePreviewCache(targetId)
      }

      debouncedPropagateSchemas()
    }
  }

  function removeEdge(edgeId: string) {
    const edge = edges.value.find(e => e.id === edgeId)
    if (edge) {
      const targetId = parseInt(edge.target)
      const sourceId = parseInt(edge.source)
      const targetNode = nodes.value.get(targetId)

      if (targetNode) {
        targetNode.inputIds = targetNode.inputIds.filter(id => id !== sourceId)
        if (targetNode.leftInputId === sourceId) targetNode.leftInputId = undefined
        if (targetNode.rightInputId === sourceId) targetNode.rightInputId = undefined

        // Clear inferred schema for disconnected node (unless it has execution data)
        const existingResult = nodeResults.value.get(targetId)
        if (existingResult && !existingResult.data) {
          nodeResults.value.delete(targetId)
        }

        invalidatePreviewCache(targetId)
      }

      edges.value = edges.value.filter(e => e.id !== edgeId)

      debouncedPropagateSchemas()
    }
  }

  function setFileContent(nodeId: number, content: string | FileContent) {
    const fc = asFileContent(content)
    fileContents.value.set(nodeId, fc)

    // Binary always persists to IndexedDB; large text too (sessionStorage can't hold it)
    if (isBinary(fc) || fileStorage.shouldUseIndexedDB(fc)) {
      fileStorage.setFileContent(nodeId, fc).catch(err => {
        console.error(`Failed to save large file for node ${nodeId} to IndexedDB:`, err)
      })
    }

    const node = nodes.value.get(nodeId)
    if (fc.kind === 'text' && node && (node.type === 'read' || node.type === 'manual_input')) {
      let hasHeaders = true
      let delimiter = ','

      if (node.type === 'read') {
        const settings = node.settings as any
        hasHeaders = settings?.received_file?.table_settings?.has_headers ?? true
        delimiter = settings?.received_file?.table_settings?.delimiter ?? ','
      }

      const schema = inferSchemaFromCsv(fc.data, hasHeaders, delimiter)
      if (schema) {
        // Set schema for source node (success undefined = not yet executed, shows grey)
        nodeResults.value.set(nodeId, { schema })

        debouncedPropagateSchemas()
      }
    }

    invalidatePreviewCache(nodeId)
  }

  /** Whether a node currently has loaded file content in memory. */
  function hasFileContent(nodeId: number): boolean {
    return fileContents.value.has(nodeId)
  }

  /** Get a node's loaded content (in-memory), if any. */
  function getFileContent(nodeId: number): FileContent | undefined {
    return fileContents.value.get(nodeId)
  }

  /** Text content for text-only consumers (CSV bridge, inference); undefined for binary. */
  function getTextContent(nodeId: number): string | undefined {
    const fc = fileContents.value.get(nodeId)
    return fc?.kind === 'text' ? fc.data : undefined
  }

  // _temp_bytes is ONE shared Pyodide global with four writers (the sheet
  // picker plus the excel/parquet/external_data execution branches); a write
  // landing inside another caller's set→run→delete window swaps its bytes or
  // NameErrors it. Serialize the window through a promise chain.
  let tempBytesChain: Promise<unknown> = Promise.resolve()
  function withTempBytes<T>(bytes: Uint8Array, fn: () => Promise<T>): Promise<T> {
    const run = tempBytesChain.then(async () => {
      pyodideStore.setGlobal('_temp_bytes', bytes)
      try {
        return await fn()
      } finally {
        pyodideStore.deleteGlobal('_temp_bytes')
      }
    })
    tempBytesChain = run.catch(() => {})
    return run
  }

  /** A node's result frame as Arrow IPC stream bytes (host pull API);
   * null when the node has no executed frame. */
  async function getNodeResultArrow(nodeId: number): Promise<Uint8Array | null> {
    if (!pyodideStore.isReady) return null
    return pyodideStore.runPythonGetBytes(`get_node_arrow(${nodeId})`)
  }

  /** Worksheet names of a node's loaded xlsx (settings-panel sheet picker).
   * Throws with an actionable message when openpyxl can't be installed. */
  async function listExcelSheets(nodeId: number): Promise<string[]> {
    const fc = getFileContent(nodeId)
    if (!fc || !isBinary(fc) || fc.format !== 'excel' || !pyodideStore.isReady) return []
    await pyodideStore.ensurePyPackages(['openpyxl==3.1.5'])
    const res = await withTempBytes(fc.data, () =>
      pyodideStore.runPythonWithResult('list_excel_sheets(_temp_bytes.to_py())')
    )
    if (res?.success) return (res.sheets as string[]) ?? []
    throw new Error(res?.error || 'Could not list worksheets')
  }

  /**
   * Set external datasets available from the host application.
   * Called by FlowfileEditor when inputData prop changes. Hosts may pass text
   * (CSV) or binary FileContent (Arrow IPC / Parquet bytes).
   */
  function setExternalDatasets(datasets: Record<string, string | FileContent>) {
    externalDatasets.value.clear()
    for (const [name, content] of Object.entries(datasets)) {
      externalDatasets.value.set(name, asFileContent(content))
    }

    // Auto-load data into any external_data nodes that reference these datasets
    nodes.value.forEach((node, nodeId) => {
      if (node.type === 'external_data') {
        const settings = node.settings as NodeExternalDataSettings
        if (settings.dataset_name && externalDatasets.value.has(settings.dataset_name)) {
          setFileContent(nodeId, externalDatasets.value.get(settings.dataset_name)!)
        }
      }
    })
  }

  /**
   * Get available external dataset names
   */
  function getExternalDatasetNames(): string[] {
    return Array.from(externalDatasets.value.keys())
  }

  /**
   * Get content for an external dataset by name
   */
  function getExternalDatasetContent(name: string): FileContent | undefined {
    return externalDatasets.value.get(name)
  }

  // ── Catalog datasets (user-uploaded tables, persisted to IndexedDB) ─────────

  /** Load persisted catalog datasets into memory (called at store init). */
  async function loadCatalogDatasets() {
    try {
      const entries = await fileStorage.getAllCatalogDatasets()
      catalogDatasets.value.clear()
      for (const e of entries) catalogDatasets.value.set(e.name, e.content)
    } catch (err) {
      console.warn('[flow-store] failed to load catalog datasets:', err)
    }
  }

  /** Upload/replace a named catalog dataset (persisted). Auto-loads into any
   *  read_from_catalog nodes that reference it. */
  async function addCatalogDataset(name: string, content: string) {
    catalogDatasets.value.set(name, content)
    catalogDatasets.value = new Map(catalogDatasets.value) // trigger reactivity
    nodes.value.forEach((node, nodeId) => {
      if (node.type === 'read_from_catalog') {
        const settings = node.settings as { dataset_name?: string }
        if (settings.dataset_name === name) setFileContent(nodeId, content)
      }
    })
    try {
      await fileStorage.putCatalogDataset({ name, content })
    } catch (err) {
      // Keep the in-memory entry (UI stays responsive) but tell the caller it was
      // NOT persisted — otherwise the table looks saved yet vanishes on refresh.
      console.warn('[flow-store] failed to persist catalog dataset:', err)
      throw err instanceof Error ? err : new Error(String(err))
    }
  }

  function getCatalogDatasetNames(): string[] {
    return Array.from(catalogDatasets.value.keys())
  }

  function getCatalogDatasetContent(name: string): string | undefined {
    return catalogDatasets.value.get(name)
  }

  async function removeCatalogDataset(name: string) {
    catalogDatasets.value.delete(name)
    catalogDatasets.value = new Map(catalogDatasets.value)
    try {
      await fileStorage.deleteCatalogDataset(name)
    } catch (err) {
      console.warn('[flow-store] failed to delete catalog dataset:', err)
    }
  }

  /**
   * Materialise a catalog (or external) dataset into a Graphic Walker payload
   * for the Visuals feature: parse the CSV in Pyodide and return its fields +
   * up to GW_MAX_ROWS JSON-safe rows. Pyodide must already be ready (the caller
   * gates on it). Bridges the CSV via `_temp_content`, like the read node.
   */
  async function loadDatasetForVisual(name: string): Promise<{
    success: boolean
    fields?: Record<string, unknown>[]
    data?: Record<string, unknown>[]
    rowInfo?: Record<string, unknown>
    error?: string
  }> {
    const external = getExternalDatasetContent(name)
    const content = getCatalogDatasetContent(name) ?? (external?.kind === 'text' ? external.data : undefined)
    if (content === undefined) {
      if (isBinary(external)) {
        return { success: false, error: `Dataset "${name}" is binary (Arrow/Parquet) — visuals support CSV datasets only.` }
      }
      return { success: false, error: `Dataset "${name}" is not in the catalog.` }
    }
    const { runPythonWithResult, setGlobal, deleteGlobal } = pyodideStore
    setGlobal('_temp_content', content)
    try {
      const result = await runPythonWithResult(`
result = prepare_visual_data(_temp_content)
result
`)
      if (!result?.success) {
        return { success: false, error: result?.error ?? 'Failed to load dataset.' }
      }
      return {
        success: true,
        fields: result.fields ?? [],
        data: result.data ?? [],
        rowInfo: result.row_info,
      }
    } finally {
      deleteGlobal('_temp_content')
    }
  }

  /**
   * Set schema for a source node from raw data fields
   * Used by manual input nodes when data structure changes
   */
  function setSourceNodeSchema(nodeId: number, fields: { name: string; data_type: string }[]) {
    const schema = inferSchemaFromRawData(fields)
    if (schema) {
      // Set schema for source node (success undefined = not yet executed, shows grey)
      nodeResults.value.set(nodeId, { schema })

      debouncedPropagateSchemas()
    }
  }

  function selectNode(id: number | null) {
    const previousId = selectedNodeId.value

    // Clear preview data from previously selected node to free memory
    // (keeps schema and success status, just removes the large data array)
    if (previousId !== null && previousId !== id) {
      const prevResult = nodeResults.value.get(previousId)
      if (prevResult?.data) {
        nodeResults.value.set(previousId, {
          ...prevResult,
          data: undefined
        })
      }
      previewCache.value.delete(previousId)
    }

    selectedNodeId.value = id

    if (id !== null) {
      const result = nodeResults.value.get(id)
      if (result?.success && !hasPreviewCached(id)) {
        // Use more rows for explore_data nodes (Preview Settings)
        // Limited to 1000 to prevent UI lag with large datasets
        const node = nodes.value.get(id)
        const maxRows = node?.type === 'explore_data' ? 1000 : 100
        fetchNodePreview(id, { maxRows })
      }
    }
  }

  /**
   * Clean up orphaned Python dataframes for nodes that no longer exist in the flow.
   * This helps prevent memory leaks when nodes are deleted.
   */
  async function cleanupOrphanedData() {
    const currentNodeIds = new Set(nodes.value.keys())

    for (const [nodeId] of fileContents.value) {
      if (!currentNodeIds.has(nodeId)) {
        fileContents.value.delete(nodeId)
      }
    }

    for (const [nodeId] of nodeResults.value) {
      if (!currentNodeIds.has(nodeId)) {
        nodeResults.value.delete(nodeId)
      }
    }

    for (const [nodeId] of previewCache.value) {
      if (!currentNodeIds.has(nodeId)) {
        previewCache.value.delete(nodeId)
      }
    }

    for (const nodeId of dirtyNodes.value) {
      if (!currentNodeIds.has(nodeId)) {
        dirtyNodes.value.delete(nodeId)
      }
    }

    if (pyodideStore.isReady) {
      const nodeIdList = Array.from(currentNodeIds).join(',')
      await pyodideStore.runPython(`
# Clean up orphaned lazyframes
current_node_ids = {${nodeIdList}} if ${currentNodeIds.size} > 0 else set()
orphaned_ids = [nid for nid in list(_lazyframes.keys()) if nid not in current_node_ids]
for nid in orphaned_ids:
    clear_node(nid)
# Force garbage collection after cleanup
gc.collect()
`)
    }
  }

  /**
   * Detect cycles in the flow graph using DFS with a recursion stack.
   * Returns null if no cycle, or an array of node IDs forming the cycle path.
   */
  function detectCycle(): number[] | null {
    const visited = new Set<number>()
    const recursionStack = new Set<number>()
    const parent = new Map<number, number>()

    function dfs(id: number): number | null {
      visited.add(id)
      recursionStack.add(id)

      const node = nodes.value.get(id)
      if (!node) {
        recursionStack.delete(id)
        return null
      }

      const dependencies: number[] = [...node.inputIds]
      if (node.leftInputId) dependencies.push(node.leftInputId)
      if (node.rightInputId) dependencies.push(node.rightInputId)

      for (const depId of dependencies) {
        if (!visited.has(depId)) {
          parent.set(depId, id)
          const cycleStart = dfs(depId)
          if (cycleStart !== null) return cycleStart
        } else if (recursionStack.has(depId)) {
          parent.set(depId, id)
          return depId
        }
      }

      recursionStack.delete(id)
      return null
    }

    // Check all nodes (handles disconnected components)
    for (const [id] of nodes.value) {
      if (!visited.has(id)) {
        const cycleStart = dfs(id)
        if (cycleStart !== null) {
          const cyclePath: number[] = [cycleStart]
          let current = parent.get(cycleStart)
          while (current !== undefined && current !== cycleStart) {
            cyclePath.push(current)
            current = parent.get(current)
          }
          cyclePath.push(cycleStart)
          return cyclePath
        }
      }
    }

    return null
  }

  function getExecutionOrder(): number[] {
    const cycle = detectCycle()
    if (cycle) {
      const cycleDescription = cycle
        .map(id => {
          const node = nodes.value.get(id)
          return node ? `${node.type} (#${id})` : `#${id}`
        })
        .join(' -> ')
      throw new Error(`Circular dependency detected: ${cycleDescription}`)
    }

    const visited = new Set<number>()
    const order: number[] = []

    function visit(id: number) {
      if (visited.has(id)) return
      visited.add(id)

      const node = nodes.value.get(id)
      if (!node) return

      for (const inputId of node.inputIds) {
        visit(inputId)
      }
      if (node.leftInputId) visit(node.leftInputId)
      if (node.rightInputId) visit(node.rightInputId)

      order.push(id)
    }

    nodes.value.forEach((_, id) => visit(id))
    return order
  }

  /**
   * Sync a node's settings with its input schema
   * This updates column-based settings (like select_input) when upstream schema changes
   * Returns true if settings were modified (triggers re-set in nodes Map for reactivity)
   */
  function syncNodeSettingsWithSchema(node: FlowNode, inputSchema: ColumnSchema[], rightInputSchema?: ColumnSchema[] | null): boolean {
    const settings = node.settings as any
    let modified = false

    if (node.type === 'select') {
      const currentSelectInput = settings.select_input || []
      const existingColumns = new Map<string, any>(currentSelectInput.map((s: any) => [s.old_name, s]))
      const inputColumnNames = new Set(inputSchema.map(c => c.name))

      const newSelectInput: any[] = []

      inputSchema.forEach((col, index) => {
        const existing = existingColumns.get(col.name)
        if (existing) {
          newSelectInput.push({
            ...existing,
            data_type: col.data_type,
            is_available: true,
            position: (existing as any).position ?? index
          })
        } else {
          newSelectInput.push({
            old_name: col.name,
            new_name: col.name,
            data_type: col.data_type,
            keep: true,
            position: index,
            is_available: true
          })
        }
      })

      // Mark columns that no longer exist in input as unavailable
      currentSelectInput.forEach((s: any) => {
        if (!inputColumnNames.has(s.old_name)) {
          newSelectInput.push({
            ...s,
            is_available: false
          })
        }
      })

      newSelectInput.sort((a: any, b: any) => (a.position ?? 0) - (b.position ?? 0))

      settings.select_input = newSelectInput
      node.settings = settings
      modified = true
    }

    if (node.type === 'group_by') {
      const groupbyInput = settings.groupby_input || { agg_cols: [] }
      const currentAggCols = groupbyInput.agg_cols || []
      const inputColumnNames = new Set(inputSchema.map(c => c.name))

      const newAggCols = currentAggCols.map((col: any) => ({
        ...col,
        is_available: inputColumnNames.has(col.old_name)
      }))

      settings.groupby_input = { ...groupbyInput, agg_cols: newAggCols }
      node.settings = settings
      modified = true
    }

    if (node.type === 'join' && rightInputSchema) {
      // Store available columns for join configuration UI
      // The join settings UI can use getLeftInputSchema and getRightInputSchema
      // but we can also store column availability here if needed
    }

    if (node.type === 'filter') {
      const filterInput = settings.filter_input
      if (filterInput?.basic_filter?.field) {
        const fieldExists = inputSchema.some(c => c.name === filterInput.basic_filter.field)
        if (!fieldExists && filterInput.basic_filter.field !== '') {
          // Field no longer exists - we could clear it or mark it
          // For now, just leave it as is so user can see and fix
        }
      }
    }

    if (node.type === 'sort') {
      // Check if sorted columns still exist - sort_input is now a flat array
      const sortInput = settings.sort_input as any[]
      if (sortInput && Array.isArray(sortInput)) {
        const inputColumnNames = new Set(inputSchema.map(c => c.name))
        settings.sort_input = sortInput.map((col: any) => ({
          ...col,
          is_available: inputColumnNames.has(col.column)
        }))
        node.settings = settings
        modified = true
      }
    }

    return modified
  }

  /**
   * Check if a node type requires lazy execution to determine its output schema
   */
  function requiresLazyExecution(nodeType: string): boolean {
    return nodeType === 'polars_code' || nodeType === 'formula'
  }

  /**
   * Propagate schemas through the flow graph.
   * When Pyodide is ready, the Polars engine is the authority: an always-on,
   * data-free pass builds the real lazy chain on empty (0-row) frames and reads
   * collect_schema() for every node (mirrors flowfile_core's create_from_schema).
   * Before Pyodide is ready, falls back to instant TypeScript inference.
   */
  async function propagateSchemas() {
    if (!pyodideStore.isReady) {
      await propagateSchemasTS()
      return
    }

    const order = getExecutionOrder()

    // Tiny JSON payload — graph shape + source schemas only, never CSV content.
    const graphNodes: Record<number, {
      type: string
      input_ids: number[]
      left: number | null
      right: number | null
      settings: unknown
    }> = {}
    const sourceSchemas: Record<number, ColumnSchema[]> = {}

    for (const nodeId of order) {
      const node = nodes.value.get(nodeId)
      if (!node) continue
      graphNodes[nodeId] = {
        type: node.type,
        input_ids: node.inputIds,
        left: node.leftInputId ?? null,
        right: node.rightInputId ?? null,
        settings: node.settings
      }
      if (isSourceNode(node.type)) {
        const schema = getSourceSchemaForPropagation(node)
        if (schema && schema.length > 0) sourceSchemas[nodeId] = schema
      }
    }

    const graphJson = { order, nodes: graphNodes }

    let res: Record<string, { schema?: ColumnSchema[]; schema_resolved?: boolean; error?: string }> | undefined
    try {
      res = await pyodideStore.runPythonWithResult(`
import json
result = propagate_schemas(json.loads(${toPythonJson(graphJson)}), json.loads(${toPythonJson(sourceSchemas)}))
result
`)
    } catch (error) {
      console.warn('Lazy schema propagation failed, falling back to inference:', error)
      await propagateSchemasTS()
      return
    }

    if (!res) return
    applyPropagatedSchemas(order, res)
  }

  /**
   * Apply the Python schema-propagation results to nodeResults, keeping node
   * settings (select_input/agg_cols/…) in sync with each node's input schema.
   * Iterates in topological order so an upstream node's freshly-written schema
   * is visible when syncing a downstream node's settings.
   */
  function applyPropagatedSchemas(
    order: number[],
    res: Record<string, { schema?: ColumnSchema[]; schema_resolved?: boolean; error?: string }>
  ) {
    for (const nodeId of order) {
      const node = nodes.value.get(nodeId)
      if (!node) continue
      if (isSourceNode(node.type)) continue

      const primaryInputId = node.leftInputId || node.inputIds[0]
      const inputSchema = primaryInputId ? (nodeResults.value.get(primaryInputId)?.schema || null) : null
      const rightInputSchema = (node.type === 'join' && node.rightInputId)
        ? (nodeResults.value.get(node.rightInputId)?.schema || null)
        : null
      if (inputSchema && inputSchema.length > 0) {
        const modified = syncNodeSettingsWithSchema(node, inputSchema, rightInputSchema)
        if (modified) nodes.value.set(nodeId, { ...node })
      }

      const info = res[String(nodeId)]
      if (!info) continue

      const existing = nodeResults.value.get(nodeId)
      const wasExecuted = existing?.success !== undefined
      // Unresolved nodes (pivot / errored polars_code) keep their last-known schema.
      const outputSchema = (info.schema && info.schema.length > 0) ? info.schema : existing?.schema
      // A successfully-executed node has a real schema even if the lazy pass
      // couldn't resolve it (e.g. pivot), so don't flag it as unresolved.
      const resolved = info.schema_resolved === true
        || (existing?.success === true && !!(existing?.schema && existing.schema.length > 0))
      nodeResults.value.set(nodeId, {
        ...existing,
        success: wasExecuted ? existing!.success : undefined,
        schema: outputSchema,
        schemaResolved: resolved
      })
    }
  }

  /**
   * Resolve a source node's schema for the lazy pass: prefer an already-known
   * schema (from prior execution or load), else infer it once from content.
   */
  function getSourceSchemaForPropagation(node: FlowNode): ColumnSchema[] | null {
    const existing = nodeResults.value.get(node.id)?.schema
    if (existing && existing.length > 0) return existing

    const content = fileContents.value.get(node.id)
    if (!content) return null

    if (node.type === 'manual_input') {
      const rawData = (node.settings as any)?.raw_data_format
      if (rawData?.columns?.length) return inferSchemaFromRawData(rawData.columns)
    }

    // Binary sources (xlsx/parquet) can't be inferred in TS — schema resolves on execution
    if (content.kind !== 'text') return null

    const s = node.settings as any
    const hasHeaders = s?.received_file?.table_settings?.has_headers ?? s?.manual_input?.has_headers ?? true
    const delimiter = s?.received_file?.table_settings?.delimiter ?? s?.manual_input?.delimiter ?? ','
    return inferSchemaFromCsv(content.data, hasHeaders, delimiter)
  }

  /**
   * TypeScript fallback used before Pyodide is ready (instant, less precise).
   * Updates nodeResults with inferred schemas for all nodes that can be computed.
   */
  async function propagateSchemasTS() {
    const order = getExecutionOrder()

    for (const nodeId of order) {
      const node = nodes.value.get(nodeId)
      if (!node) continue

      // Skip source nodes - their schema comes from actual data
      if (isSourceNode(node.type)) {
        // Keep existing schema if present (from data load)
        continue
      }

      let inputSchema: ColumnSchema[] | null = null
      let rightInputSchema: ColumnSchema[] | null = null

      const primaryInputId = node.leftInputId || node.inputIds[0]
      let inputHasData = false
      if (primaryInputId) {
        const inputResult = nodeResults.value.get(primaryInputId)
        inputSchema = inputResult?.schema || null
        // Check if input was successfully executed (success=true means data is available)
        inputHasData = !!(inputResult?.success || inputResult?.data)
      }

      // For join nodes, also get right input schema
      if (node.type === 'join' && node.rightInputId) {
        const rightResult = nodeResults.value.get(node.rightInputId)
        rightInputSchema = rightResult?.schema || null
      }

      if (inputSchema && inputSchema.length > 0) {
        const modified = syncNodeSettingsWithSchema(node, inputSchema, rightInputSchema)
        // Trigger Vue reactivity by re-setting the node in the Map
        if (modified) {
          nodes.value.set(nodeId, { ...node })
        }
      }

      // For polars_code/formula nodes, try lazy execution if input data is available
      if (requiresLazyExecution(node.type) && inputHasData && pyodideStore.isReady) {
        try {
          const result = await executeNode(nodeId)
          if (result.success && result.schema) {
            // Schema is already set by executeNode, continue to next node
            continue
          }
        } catch (error) {
          console.warn(`Lazy execution failed for node ${nodeId}:`, error)
          // Fall through to keep existing result if any
        }
      }

      // Infer output schema (for non-lazy nodes or when lazy execution isn't possible)
      const inferredSchema = inferOutputSchema(
        node.type,
        inputSchema,
        node.settings,
        rightInputSchema
      )

        if (inferredSchema) {
          const existingResult = nodeResults.value.get(nodeId)
          // Preserve success if it was explicitly set (true OR false means it was executed)
          const wasExecuted = existingResult?.success !== undefined
          nodeResults.value.set(nodeId, {
            success: wasExecuted ? existingResult.success : undefined,
            schema: inferredSchema,
            data: existingResult?.data,
            error: existingResult?.error,
            execution_time: existingResult?.execution_time,
            download: existingResult?.download,
            // Preserve explore_data Graphic Walker payload across schema propagation
            graphic_walker_input: existingResult?.graphic_walker_input,
            row_info: existingResult?.row_info
          })
      } else {
        // inferOutputSchema returned null - this means:
        // 1. For polars_code/formula: schema can only be known after execution (and input wasn't available)
        // 2. For other nodes: input schema might be missing
        //
        // Keep any existing executed result (which has actual schema from Python)
        // Only clear never-executed placeholders (success === false is a real
        // execution failure that must stay visible on the node)
        const existingResult = nodeResults.value.get(nodeId)
        if (!inputSchema && !isSourceNode(node.type) && existingResult && !existingResult.data && existingResult.success === undefined) {
          nodeResults.value.delete(nodeId)
        }
        // If there's an existing result with actual data, keep it - the schema from
        // execution is authoritative for nodes we can't infer (like polars_code)
      }
    }
  }

  // Debounced schema propagation to avoid excessive updates
  let propagateTimeout: ReturnType<typeof setTimeout> | null = null
  function debouncedPropagateSchemas() {
    if (propagateTimeout) clearTimeout(propagateTimeout)
    propagateTimeout = setTimeout(() => {
      propagateSchemas().catch(err => console.error('Schema propagation error:', err))
      propagateTimeout = null
    }, 50)
  }

  // Preview Cache Management (Lazy Loading)

  /**
   * Evict old entries from previewCache to prevent memory bloat.
   * Uses LRU-style eviction: removes oldest entries first.
   */
  function evictPreviewCacheIfNeeded() {
    const now = Date.now()
    const entries = Array.from(previewCache.value.entries())

    for (const [nodeId, entry] of entries) {
      if (now - entry.timestamp > PREVIEW_CACHE_MAX_AGE_MS) {
        previewCache.value.delete(nodeId)
      }
    }

    if (previewCache.value.size > PREVIEW_CACHE_MAX_SIZE) {
      const sortedEntries = Array.from(previewCache.value.entries())
        .sort((a, b) => a[1].timestamp - b[1].timestamp)

      const toEvict = sortedEntries.slice(0, previewCache.value.size - PREVIEW_CACHE_MAX_SIZE)
      for (const [nodeId] of toEvict) {
        previewCache.value.delete(nodeId)
      }
    }
  }

  /**
   * Build downstream dependency graph
   */
  function buildDownstreamGraph(): Record<number, number[]> {
    const downstreamGraph: Record<number, number[]> = {}
    edges.value.forEach(edge => {
      const sourceId = parseInt(edge.source)
      const targetId = parseInt(edge.target)
      if (!downstreamGraph[sourceId]) {
        downstreamGraph[sourceId] = []
      }
      downstreamGraph[sourceId].push(targetId)
    })
    return downstreamGraph
  }

  /**
   * Invalidate preview cache for a node and its downstream dependents
   * Also marks these nodes as dirty (needing re-execution)
   */
  function invalidatePreviewCache(nodeId: number) {
    const downstreamGraph = buildDownstreamGraph()

    // FIRST: Synchronously clear TypeScript cache and mark nodes as dirty
    // This must happen immediately before any async operations
    const toInvalidate = [nodeId]
    const visited = new Set<number>()

    while (toInvalidate.length > 0) {
      const current = toInvalidate.shift()!
      if (visited.has(current)) continue
      visited.add(current)

      previewCache.value.delete(current)

      dirtyNodes.value.add(current)

      const downstream = downstreamGraph[current] || []
      toInvalidate.push(...downstream)
    }

    // THEN: Invalidate in Python asynchronously (fire and forget)
    if (pyodideStore.isReady) {
      pyodideStore.runPython(`
import json
node_graph = json.loads('${JSON.stringify(downstreamGraph)}')
node_graph = {int(k): v for k, v in node_graph.items()}
invalidate_downstream_previews(${nodeId}, node_graph)
`).catch(err => {
        console.error('Failed to invalidate Python preview cache:', err)
      })
    }
  }

  /**
   * Check if a node's preview is currently loading
   */
  function isPreviewLoading(nodeId: number): boolean {
    return previewLoadingNodes.value.has(nodeId)
  }

  /**
   * Check if a node has cached preview data
   */
  function hasPreviewCached(nodeId: number): boolean {
    const cached = previewCache.value.get(nodeId)
    if (cached && !cached.loading && cached.data !== null) {
      return true
    }
    const result = nodeResults.value.get(nodeId)
    return !!(result?.data)
  }

  /**
   * Check if a node is dirty (has changes since last execution)
   */
  function isNodeDirty(nodeId: number): boolean {
    return dirtyNodes.value.has(nodeId)
  }

  /**
   * Check if a node has ever been executed successfully
   */
  function hasNodeExecuted(nodeId: number): boolean {
    const result = nodeResults.value.get(nodeId)
    return result?.success === true || result?.success === false
  }

  /**
   * Fetch preview data for a node (on-demand, cached)
   * This is called when user clicks on a node to view its data
   */
  async function fetchNodePreview(
    nodeId: number,
    options: { maxRows?: number; forceRefresh?: boolean } = {}
  ): Promise<{ success: boolean; data?: any; error?: string; fromCache?: boolean }> {
    const { maxRows = 100, forceRefresh = false } = options

    if (!forceRefresh) {
      const cached = previewCache.value.get(nodeId)
      if (cached && !cached.loading && cached.data) {
        const existingResult = nodeResults.value.get(nodeId)
        if (existingResult) {
          nodeResults.value.set(nodeId, {
            ...existingResult,
            data: cached.data
          })
        }
        return { success: true, data: cached.data, fromCache: true }
      }

      const result = nodeResults.value.get(nodeId)
      if (result?.data) {
        return { success: true, data: result.data, fromCache: true }
      }
    }

    if (!pyodideStore.isReady) {
      return { success: false, error: 'Pyodide is not ready' }
    }

    previewLoadingNodes.value.add(nodeId)
    previewCache.value.set(nodeId, {
      data: null,
      timestamp: Date.now(),
      loading: true
    })

    try {
      const result = await pyodideStore.runPythonWithResult(`
result = fetch_preview(${nodeId}, max_rows=${maxRows}, force_refresh=${forceRefresh ? 'True' : 'False'})
result
`)

      if (result.success) {
        previewCache.value.set(nodeId, {
          data: result.data,
          timestamp: Date.now(),
          loading: false
        })

        // Evict old entries to prevent memory bloat
        evictPreviewCacheIfNeeded()

        const existingResult = nodeResults.value.get(nodeId)
        if (existingResult) {
          nodeResults.value.set(nodeId, {
            ...existingResult,
            data: result.data
          })
        }

        return {
          success: true,
          data: result.data,
          fromCache: result.from_cache
        }
      } else {
          const existingResult = nodeResults.value.get(nodeId)
          if (existingResult) {
            nodeResults.value.set(nodeId, {
              ...existingResult,
              success: false,
              error: result.error,
              data: undefined
            })
          }
          return { success: false, error: result.error }
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      return { success: false, error: errorMessage }
    } finally {
      previewLoadingNodes.value.delete(nodeId)
      const cached = previewCache.value.get(nodeId)
      if (cached) {
        cached.loading = false
      }
    }
  }

  // Node Execution

  /**
   * Topologically-ordered chain of a node's transitive inputs, ending with the
   * node itself. Used to materialize data on demand ("Fetch data") without a
   * full flow run — executing the chain rebuilds the upstream LazyFrames the
   * node needs (cheap: nothing collects until the final preview).
   */
  function getAncestorChain(nodeId: number): number[] {
    const needed = new Set<number>()
    const stack = [nodeId]
    while (stack.length) {
      const id = stack.pop()!
      if (needed.has(id)) continue
      needed.add(id)
      const node = nodes.value.get(id)
      if (!node) continue
      const inputs = [...node.inputIds]
      if (node.leftInputId) inputs.push(node.leftInputId)
      if (node.rightInputId) inputs.push(node.rightInputId)
      for (const inp of inputs) stack.push(inp)
    }
    return getExecutionOrder().filter((id) => needed.has(id))
  }

  /**
   * Ensure a node's LazyFrame pointer is wired and current, then return it ready
   * to collect — without re-running upstream steps that are already built and
   * unchanged. Each node is just a pointer to a LazyFrame (`_lazyframes[id]`);
   * once wired it persists, so we only (re)build pointers that are MISSING (never
   * built this session) or DIRTY (their settings/edges/inputs changed). Built &
   * clean upstream is reused as-is — its source frame is already in memory.
   * Stops and surfaces the first failing node's error.
   */
  async function executeNodeWithUpstream(nodeId: number): Promise<NodeResult> {
    const chain = getAncestorChain(nodeId)

    // Ground truth for "pointer already wired in this runtime" is the Python data
    // store itself — NOT nodeResults.success, which survives a page refresh while
    // _lazyframes does not (refresh => empty => everything correctly rebuilds).
    let built = new Set<number>()
    if (pyodideStore.isReady) {
      try {
        const ids = await pyodideStore.runPythonWithResult('list(_lazyframes.keys())')
        if (Array.isArray(ids)) built = new Set(ids.map(Number))
      } catch {
        /* leave empty → rebuild the chain */
      }
    }

    let last: NodeResult = { success: false, error: 'Node not found' }
    for (const id of chain) {
      if (built.has(id) && !isNodeDirty(id)) {
        // Pointer already wired and current — reuse it, don't re-run the step.
        last = { success: true }
        continue
      }
      last = await executeNode(id)
      if (!last.success) return last
    }
    return last
  }

  /**
   * Record a pre-execution failure (missing file/input/config) on the node so
   * it surfaces in the UI (red status + preview error) instead of leaving the
   * node looking never-executed after a run.
   */
  function failNode(nodeId: number, error: string): NodeResult {
    console.warn(`[flowfile] node ${nodeId} (${nodes.value.get(nodeId)?.type}) not executed: ${error}`)
    const existing = nodeResults.value.get(nodeId)
    const result: NodeResult = { ...existing, success: false, error, data: undefined }
    nodeResults.value.set(nodeId, result)
    return result
  }

  async function executeNode(nodeId: number): Promise<NodeResult> {
    const node = nodes.value.get(nodeId)
    if (!node) {
      console.warn(`[flowfile] executeNode skipped: node ${nodeId} not found`)
      return { success: false, error: 'Node not found' }
    }

    console.debug(`[flowfile] executeNode ${nodeId} (${node.type})`)
    const { runPythonWithResult, setGlobal, deleteGlobal } = pyodideStore

    try {
      let result: NodeResult

      switch (node.type) {
        case 'read': {
          const fileType = (node.settings as NodeReadSettings).received_file?.file_type ?? 'csv'
          const fc = getFileContent(nodeId)
          if (!fc) {
            return failNode(nodeId, 'No file loaded')
          }

          if (fileType === 'excel') {
            if (!isBinary(fc)) {
              return failNode(nodeId, 'Node is configured for Excel but holds text content — re-pick the file')
            }
            try {
              await pyodideStore.ensurePyPackages(['openpyxl==3.1.5'])
            } catch (err) {
              return failNode(nodeId, err instanceof Error ? err.message : String(err))
            }
            result = await withTempBytes(fc.data, () => runPythonWithResult(`
import json
result = execute_read_excel(${nodeId}, _temp_bytes.to_py(), json.loads(${toPythonJson(node.settings)}))
result
`))
            break
          }

          if (fileType === 'json') {
            return failNode(nodeId, 'JSON read is not supported in the browser — use the desktop app or convert to CSV')
          }

          if (fileType === 'parquet') {
            if (!isBinary(fc)) {
              return failNode(nodeId, 'Node is configured for Parquet but holds text content — re-pick the file')
            }
            let ipc: Uint8Array
            try {
              // Decode in JS (parquet-wasm, CDN-loaded on first use); the engine
              // only sees Arrow IPC stream bytes. Original parquet bytes stay in
              // fileContents (compressed); the IPC copy is transient.
              ipc = await parquetToIpcStream(fc.data)
            } catch (err) {
              return failNode(nodeId, err instanceof Error ? err.message : String(err))
            }
            result = await withTempBytes(ipc, () => runPythonWithResult(`
import json
result = execute_read_ipc(${nodeId}, _temp_bytes.to_py(), json.loads(${toPythonJson(node.settings)}))
result
`))
            break
          }

          const content = getTextContent(nodeId)
          if (content === undefined) {
            return failNode(nodeId, 'File content is binary but the node is configured as CSV — re-pick the file')
          }
          if (!content) {
            return failNode(nodeId, 'The loaded file is empty')
          }
          setGlobal('_temp_content', content)
          try {
            result = await runPythonWithResult(`
import json
result = execute_read_csv(${nodeId}, _temp_content, json.loads(${toPythonJson(node.settings)}))
result
`)
          } finally {
            deleteGlobal('_temp_content')
          }
          break
        }

        case 'manual_input': {
          const content = getTextContent(nodeId)
          if (!content) {
            return failNode(nodeId, 'No data entered')
          }
          setGlobal('_temp_content', content)
          try {
            result = await runPythonWithResult(`
import json
result = execute_manual_input(${nodeId}, _temp_content, json.loads(${toPythonJson(node.settings)}))
result
`)
          } finally {
            deleteGlobal('_temp_content')
          }
          break
        }

        case 'external_data': {
          const fc = getFileContent(nodeId)
          if (!fc) {
            const settings = node.settings as NodeExternalDataSettings
            const dsName = settings.dataset_name
            return failNode(nodeId, dsName ? `No data loaded for dataset "${dsName}". Ensure the host provides this dataset.` : 'No dataset selected')
          }

          if (isBinary(fc)) {
            // Host-provided binary: Arrow IPC executes directly; Parquet decodes
            // to IPC in JS first. (Excel bytes are not part of the host contract.)
            let ipc = fc.data
            if (fc.format === 'parquet') {
              try {
                ipc = await parquetToIpcStream(fc.data)
              } catch (err) {
                return failNode(nodeId, err instanceof Error ? err.message : String(err))
              }
            } else if (fc.format !== 'arrow-ipc') {
              return failNode(nodeId, `External datasets support CSV, Arrow IPC or Parquet — got ${fc.format}`)
            }
            result = await withTempBytes(ipc, () => runPythonWithResult(`
import json
result = execute_read_ipc(${nodeId}, _temp_bytes.to_py(), json.loads(${toPythonJson(node.settings)}))
result
`))
            break
          }

          // Text reuses execute_manual_input because the data format is identical
          // (CSV string + settings).
          setGlobal('_temp_content', fc.data)
          try {
            result = await runPythonWithResult(`
import json
result = execute_manual_input(${nodeId}, _temp_content, json.loads(${toPythonJson(node.settings)}))
result
`)
          } finally {
            deleteGlobal('_temp_content')
          }
          break
        }

        case 'read_from_catalog': {
          // Reads a CSV table uploaded to the Catalog. The settings panel loads
          // the chosen dataset's content into fileContents; execution reuses
          // execute_manual_input (CSV string in → frame). Independent from
          // external_data so the two can diverge later.
          const content = getTextContent(nodeId)
          if (!content) {
            const settings = node.settings as { dataset_name?: string }
            const dsName = settings.dataset_name
            return failNode(nodeId, dsName ? `Catalog table "${dsName}" not found. Upload it in the Catalog.` : 'No catalog table selected')
          }
          setGlobal('_temp_content', content)
          try {
            result = await runPythonWithResult(`
import json
result = execute_manual_input(${nodeId}, _temp_content, json.loads(${toPythonJson(node.settings)}))
result
`)
          } finally {
            deleteGlobal('_temp_content')
          }
          break
        }

        case 'filter': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_filter(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'select': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_select(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'group_by': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_group_by(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'join': {
          const leftId = node.leftInputId || node.inputIds[0]
          const rightId = node.rightInputId
          if (!leftId || !rightId) {
            return failNode(nodeId, 'Both left and right inputs required for join')
          }
          result = await runPythonWithResult(`
import json
result = execute_join(${nodeId}, ${leftId}, ${rightId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'sort': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_sort(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'polars_code': {
          result = await runPythonWithResult(`
import json
result = execute_polars_code(${nodeId}, [${node.inputIds.join(', ')}], json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'unique': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_unique(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'head': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_head(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'explore_data': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_explore_data(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'pivot': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_pivot(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'unpivot': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          result = await runPythonWithResult(`
import json
result = execute_unpivot(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'output': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          const outputFileType = (node.settings as NodeOutputSettings).output_settings?.file_type
          if (outputFileType === 'excel') {
            try {
              await pyodideStore.ensurePyPackages(['XlsxWriter==3.2.0'])
            } catch (err) {
              return failNode(nodeId, err instanceof Error ? err.message : String(err))
            }
          }
          const outputResult = await runPythonWithResult(`
import json
result = execute_output(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          // Store download content separately in IndexedDB (not in nodeResults)
          if (outputResult?.success && outputResult?.download) {
            const { file_name, file_type, mime_type, row_count, content_kind, transport } = outputResult.download
            let content: string | Uint8Array = outputResult.download.content
            if (content_kind === 'binary') {
              // Bytes don't survive the toJs() bridge — one-shot pull from the registry.
              // Failures here happen AFTER the engine stored the node's LazyFrame, so
              // mark the node dirty or the next run reuses the "built" pointer and
              // never regenerates the download.
              const bytes = await pyodideStore.runPythonGetBytes(`take_output_binary(${nodeId})`)
              if (!bytes) {
                dirtyNodes.value.add(nodeId)
                return failNode(nodeId, 'Output file bytes were not produced — re-run the node')
              }
              if (transport === 'arrow-ipc') {
                // The engine staged Arrow IPC; encode the final .parquet in JS
                try {
                  content = await ipcStreamToParquet(bytes)
                } catch (err) {
                  dirtyNodes.value.add(nodeId)
                  return failNode(nodeId, err instanceof Error ? err.message : String(err))
                }
              } else {
                content = bytes
              }
            }
            await fileStorage.setDownloadContent(
              nodeId,
              content,
              file_name,
              file_type,
              mime_type,
              row_count
            )
            // Notify output callbacks (for embeddable mode) — text outputs only;
            // binary results are pulled by hosts via the Arrow API.
            if (typeof content === 'string') {
              outputCallbacks.forEach(cb => cb({
                nodeId,
                content: content as string,
                fileName: file_name,
                mimeType: mime_type
              }))
            }
            // Create result without content - just metadata
            result = {
              success: outputResult.success,
              schema: outputResult.schema,
              download: {
                file_name,
                file_type,
                mime_type,
                row_count,
                // content is NOT included - it's in IndexedDB
                content: ''
              }
            }
          } else {
            result = outputResult
          }
          break
        }

        case 'external_output': {
          // External output: execute like output but always emit CSV to callbacks
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          const settings = node.settings as NodeExternalOutputSettings
          const outputName = settings.output_name || 'result'
          const outputSettings = {
            output_settings: {
              name: `${outputName}.csv`,
              directory: '.',
              file_type: 'csv',
              write_mode: 'overwrite',
              table_settings: {
                file_type: 'csv',
                delimiter: ',',
                encoding: 'utf-8'
              },
              polars_method: 'sink_csv'
            }
          }
          const extResult = await runPythonWithResult(`
import json
result = execute_output(${nodeId}, ${inputId}, json.loads(${toPythonJson(outputSettings)}))
result
`)
          if (extResult?.success && extResult?.download) {
            const { content, file_name, mime_type } = extResult.download
            // Notify output callbacks (primary purpose of this node in embedded mode)
            outputCallbacks.forEach(cb => cb({
              nodeId,
              content,
              fileName: file_name,
              mimeType: mime_type
            }))
            result = {
              success: extResult.success,
              schema: extResult.schema,
              data: extResult.data
            }
          } else {
            result = extResult
          }
          break
        }

        case 'write_to_catalog': {
          // Write the input result to the persistent Catalog as a reusable table.
          // Materialize to CSV via execute_output (same as external_output), then
          // persist the CSV through addCatalogDataset (IndexedDB-backed).
          const inputId = node.inputIds[0]
          if (!inputId) {
            return failNode(nodeId, 'No input connected')
          }
          const settings = node.settings as NodeWriteToCatalogSettings
          const tableName = (settings.dataset_name || '').trim()
          if (!tableName) {
            return failNode(nodeId, 'No catalog table name set. Open the node settings and name the table.')
          }
          const outputSettings = {
            output_settings: {
              name: `${tableName}.csv`,
              directory: '.',
              file_type: 'csv',
              write_mode: 'overwrite',
              table_settings: {
                file_type: 'csv',
                delimiter: ',',
                encoding: 'utf-8'
              },
              polars_method: 'sink_csv'
            }
          }
          const writeResult = await runPythonWithResult(`
import json
result = execute_output(${nodeId}, ${inputId}, json.loads(${toPythonJson(outputSettings)}))
result
`)
          if (writeResult?.success && writeResult?.download) {
            // Persist + make it appear in the Catalog (reactive, survives flow switches).
            await addCatalogDataset(tableName, writeResult.download.content)
            result = {
              success: true,
              schema: writeResult.schema,
              data: writeResult.data
            }
          } else {
            result = writeResult
          }
          break
        }

        default:
          return failNode(nodeId, `Unknown node type: ${node.type}`)
      }

      // Store result - success=true indicates data is available in Python
      // Preserve existing data if schema unchanged (data might be stale otherwise)
      const existingResult = nodeResults.value.get(nodeId)
      const schemaUnchanged = existingResult?.schema &&
                              result.schema &&
                              JSON.stringify(existingResult.schema) === JSON.stringify(result.schema)

      const nodeResult: NodeResult = {
        success: result.success,
        schema: result.schema,
        error: result.error,
        download: result.download,
        graphic_walker_input: result.graphic_walker_input,
        row_info: result.row_info,
        // Preserve data if schema unchanged (prevents data loss during schema propagation)
        data: schemaUnchanged ? existingResult?.data : undefined
      }

      nodeResults.value.set(nodeId, nodeResult)

      if (result.success) {
        dirtyNodes.value.delete(nodeId)
      }

      // Clear preview cache only if schema changed (prevents cache thrashing during schema propagation)
      if (!schemaUnchanged) {
        previewCache.value.delete(nodeId)
      }

      return nodeResult
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      console.error(`[flowfile] executeNode ${nodeId} (${node.type}) threw:`, error)
      const errorResult: NodeResult = {
        success: false,
        error: errorMessage
      }
      nodeResults.value.set(nodeId, errorResult)
      return errorResult
    }
  }

  async function executeFlow() {
    if (!pyodideStore.isReady) {
      console.warn('[flowfile] executeFlow skipped: Pyodide is not ready yet')
      return
    }

    isExecuting.value = true
    executionError.value = null
    nodeResults.value.clear()
    previewCache.value.clear()
    dirtyNodes.value.clear()    // Clear all dirty flags (will be re-set if execution fails)

    const runStartedAt = Date.now()
    try {
      await cleanupOrphanedData()

      await pyodideStore.runPython('clear_all()')

      // This builds the lazy query plans - should be fast!
      const order = getExecutionOrder()
      console.debug(`[flowfile] executeFlow: ${order.length} nodes in order [${order.join(', ')}]`)

      for (const nodeId of order) {
        await executeNode(nodeId)
      }

      // After execution, propagate schemas to update downstream node settings
      // This syncs select_input, agg_cols, etc. with actual executed schemas
      await propagateSchemas()

      // Optional: Auto-fetch preview for selected node
      // explore_data nodes use Graphic Walker (payload already on nodeResult),
      // so the AG Grid preview is not needed for them.
      if (selectedNodeId.value !== null) {
        const result = nodeResults.value.get(selectedNodeId.value)
        if (result?.success) {
          const node = nodes.value.get(selectedNodeId.value)
          if (node?.type !== 'explore_data') {
            await fetchNodePreview(selectedNodeId.value, { maxRows: 100 })
          }
        }
      }

      // Force garbage collection after flow execution
      await pyodideStore.runPython('gc.collect()')
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      executionError.value = errorMessage
      console.error('Flow execution error:', error)
    } finally {
      isExecuting.value = false

      // Record a per-run summary for the Catalog Run History (client-side only).
      try {
        const nodesCompleted = Array.from(nodeResults.value.values()).filter(r => r.success === true).length
        console.debug(
          `[flowfile] executeFlow done: ${nodesCompleted}/${nodes.value.size} nodes ok in ${Date.now() - runStartedAt}ms` +
          (executionError.value ? ` (error: ${executionError.value})` : '')
        )
        await fileStorage.putRun({
          id: (globalThis.crypto?.randomUUID?.() ?? `run-${runStartedAt}`),
          flowId: currentFlowId.value ?? undefined,
          flowName: currentFlowName.value,
          startedAt: runStartedAt,
          durationMs: Date.now() - runStartedAt,
          nodesTotal: nodes.value.size,
          nodesCompleted,
          success: !executionError.value,
          error: executionError.value
        })
        await fileStorage.pruneRuns(50)
      } catch (e) {
        console.warn('[flow-store] failed to record run history:', e)
      }
    }
  }

  function getDefaultSettings(type: string, nodeId: number, x: number = 0, y: number = 0): NodeSettings {
    // Base settings matching flowfile_core/schemas/input_schema.py NodeBase
    const base: NodeBase = {
      node_id: nodeId,
      is_setup: false,
      cache_results: true,
      pos_x: x,
      pos_y: y,
      description: ''
    }

    switch (type) {
      case 'read':
        return {
          ...base,
          received_file: {
            name: '',
            path: '',  // Required by flowfile_core
            file_type: 'csv',
            table_settings: {
              file_type: 'csv',
              delimiter: ',',
              has_headers: true,
              encoding: 'utf-8',
              starting_from_line: 0,
              infer_schema_length: 100,
              truncate_ragged_lines: false,
              ignore_errors: false
            }
          },
          file_name: ''
        } as NodeReadSettings

      case 'manual_input':
        return {
          ...base,
          raw_data_format: {
            columns: [],
            data: []
          }
        } as NodeManualInputSettings

      case 'external_data':
        return {
          ...base,
          dataset_name: '',
          schema_snapshot: []
        } as NodeExternalDataSettings

      case 'read_from_catalog':
        return {
          ...base,
          dataset_name: '',
          schema_snapshot: []
        } as NodeReadFromCatalogSettings

      case 'filter':
        return {
          ...base,
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: '',
              operator: 'equals',
              value: '',
              value2: ''
            },
            advanced_filter: ''
          }
        } as NodeFilterSettings

      case 'select':
        return {
          ...base,
          select_input: [],
          keep_missing: false
        } as NodeSelectSettings

      case 'group_by':
        return {
          ...base,
          groupby_input: {
            agg_cols: []
          }
        } as NodeGroupBySettings

      case 'join':
        return {
          ...base,
          depending_on_ids: [],
          join_input: {
            join_type: 'inner',
            how: 'inner',
            join_mapping: [],
            left_suffix: '_left',
            right_suffix: '_right'
          }
        } as any

      case 'sort':
        return {
          ...base,
          // sort_input is a flat array matching flowfile_core's NodeSort schema
          sort_input: []
        } as any

      case 'formula':
      case 'polars_code':
        return {
          ...base,
          polars_code_input: {
            polars_code: 'input_df'
          }
        } as any

      case 'unique':
        return {
          ...base,
          unique_input: {
            columns: [],
            subset: [],
            strategy: 'first',
            keep: 'first',
            maintain_order: true
          }
        } as any

      case 'sample':
      case 'head':
        return {
          ...base,
          sample_size: 10,
          head_input: {
            n: 10
          }
        } as any

      case 'pivot':
        return {
          ...base,
          pivot_input: {
            index_columns: [],
            pivot_column: '',
            value_col: '',
            aggregations: ['sum']
          }
        } as any

      case 'unpivot':
        return {
          ...base,
          unpivot_input: {
            index_columns: [],
            value_columns: [],
            data_type_selector: undefined,
            data_type_selector_mode: 'column'
          }
        } as any

      case 'explore_data':
        return {
          ...base,
          graphic_walker_input: {
            is_initial: true,
            dataModel: { fields: [], data: [] },
            specList: []
          }
        } as any

      case 'output':
        return {
          ...base,
          output_settings: {
            name: 'output.csv',
            directory: '.',
            file_type: 'csv',
            write_mode: 'overwrite',
            table_settings: {
              file_type: 'csv',
              delimiter: ',',
              encoding: 'utf-8'
            }
          }
        } as any

      case 'external_output':
        return {
          ...base,
          output_name: 'result'
        } as NodeExternalOutputSettings

      case 'write_to_catalog':
        return {
          ...base,
          dataset_name: ''
        } as NodeWriteToCatalogSettings

      default:
        return base as NodeSettings
    }
  }

  /**
   * Export the current flow state to FlowfileData format
   * This produces a JSON structure compatible with flowfile_core
   *
   * Key compatibility features:
   * - No connections array (flowfile_core derives connections from node relationships)
   * - Cleaned setting_input (excludes flow_id, node_id, pos_x, pos_y, etc.)
   * - Description at node level
   * - Uses version '1.0.0' for cross-system compatibility
   */
  function exportToFlowfile(name: string = 'Untitled Flow'): FlowfileData {
    console.log('[exportToFlowfile] Current edges:', edges.value.map(e => `${e.source}->${e.target}`))
    const flowfileNodes: FlowfileNode[] = []

    nodes.value.forEach((node, id) => {
      const isStartNode = node.inputIds.length === 0 && !node.leftInputId && !node.rightInputId

      const outputs = edges.value
        .filter(e => e.source === String(id))
        .map(e => parseInt(e.target))

      // In flowfile_core format, left_input_id is always null - inputs are in input_ids
      // Only right_input_id is used (for join nodes' second input)
      // Read description and node_reference from node level (primary) with fallback to settings
      let settingInput = cleanSettingInput(node.settings)

      // For external_data nodes, save schema snapshot (not the actual data)
      if (node.type === 'external_data') {
        const result = nodeResults.value.get(id)
        if (result?.schema) {
          settingInput = {
            ...settingInput,
            schema_snapshot: result.schema.map(col => ({ name: col.name, data_type: col.data_type }))
          }
        }
      }

      const flowfileNode: FlowfileNode = {
        id: node.id,
        type: node.type,
        is_start_node: isStartNode,
        description: node.description || (node.settings as NodeBase).description || '',
        node_reference: node.node_reference || (node.settings as NodeBase).node_reference,
        x_position: Math.round(node.x),  // flowfile_core expects int
        y_position: Math.round(node.y),  // flowfile_core expects int
        right_input_id: node.rightInputId,
        input_ids: node.inputIds,
        outputs,
        setting_input: settingInput
      }

      flowfileNodes.push(flowfileNode)
    })

    // No connections array - flowfile_core derives connections from node relationships
    const flowfileData: FlowfileData = {
      flowfile_version: '1.0.0',
      flowfile_id: Date.now(),
      flowfile_name: name,
      flowfile_settings: {
        description: '',
        execution_mode: 'Development',
        execution_location: 'local',
        auto_save: true,
        show_detailed_progress: false
      },
      nodes: flowfileNodes
    }

    return flowfileData
  }

  /**
   * Import a FlowfileData structure into the current state
   * This loads a flow file that was created in flowfile_core or this WASM app
   *
   * Supports two formats:
   * 1. WASM format with explicit connections array
   * 2. flowfile_core format with implicit connections (derived from node relationships)
   */
  function importFromFlowfile(data: FlowfileData): boolean {

    try {
      nodes.value.clear()
      edges.value = []
      nodeResults.value.clear()
      fileContents.value.clear()
      previewCache.value.clear()
      dirtyNodes.value.clear()
      selectedNodeId.value = null
      currentFlowName.value = (data as any)?.flowfile_name || 'Untitled Flow'
      // Raw FlowfileData (file/template/snapshot) carries no library identity;
      // callers that restore a saved flow set currentFlowId afterwards.
      currentFlowId.value = null

      fileStorage.clearAll().catch(err => {
        console.error('Failed to clear IndexedDB:', err)
      })

      // Reset the Pyodide engine so LazyFrames/schemas keyed by node_id from a
      // previously loaded flow can't leak into this one (node ids are reused).
      if (pyodideStore.isReady) {
        pyodideStore.runPython('clear_all()').catch(err => console.error('clear_all failed on import:', err))
      }

      let maxId = 0

      for (const flowfileNode of data.nodes) {
        if (flowfileNode.id > maxId) maxId = flowfileNode.id

        // Migrate old node types to new names (for backward compatibility)
        let nodeType = flowfileNode.type
        if (nodeType === 'read_csv') nodeType = 'read'
        if (nodeType === 'preview') nodeType = 'explore_data'

        // Migrate old settings field names
        let settings = flowfileNode.setting_input as NodeSettings
        if (settings && (settings as any).received_table && !(settings as any).received_file) {
          (settings as any).received_file = (settings as any).received_table
          delete (settings as any).received_table
        }

        // Get description and node_reference from FlowfileNode level (this is where flowfile_core stores them)
        const nodeDescription = flowfileNode.description || ''
        const nodeReference = flowfileNode.node_reference || undefined

        // Sync description and node_reference to settings for backward compatibility
        if (settings) {
          (settings as NodeBase).description = nodeDescription
          if (nodeReference) {
            (settings as NodeBase).node_reference = nodeReference
          }
        }

        const node: FlowNode = {
          id: flowfileNode.id,
          type: nodeType,
          x: flowfileNode.x_position ?? 0,
          y: flowfileNode.y_position ?? 0,
          settings,
          inputIds: flowfileNode.input_ids || [],
          leftInputId: flowfileNode.left_input_id,
          rightInputId: flowfileNode.right_input_id,
          description: nodeDescription,
          node_reference: nodeReference
        }

        nodes.value.set(flowfileNode.id, node)
      }

      nodeIdCounter.value = maxId

      // Import connections - support both explicit connections array and implicit derivation
      if (data.connections && data.connections.length > 0) {
        // Explicit connections array (WASM format)
        for (const conn of data.connections) {
          const edge: FlowEdge = {
            id: `e${conn.from_node}-${conn.to_node}-${conn.from_handle}-${conn.to_handle}`,
            source: String(conn.from_node),
            target: String(conn.to_node),
            sourceHandle: conn.from_handle,
            targetHandle: conn.to_handle
          }
          edges.value.push(edge)
        }
      } else {
        // Derive edges from node relationships (flowfile_core format)
        deriveEdgesFromNodes(data.nodes)
      }

      // Note: Source nodes will need data loaded before schemas propagate
      setTimeout(() => {
        propagateSchemas().catch(err => console.error('Import schema propagation error:', err))
      }, 0)

      return true
    } catch (error) {
      console.error('Failed to import flow', error)
      return false
    }
  }

  /** Bundle the in-memory input CSVs that are small enough to persist inline.
   * Text-only: binary content is restored via re-pick, never inlined into
   * library entries (and the wrapper would defeat a Blob size check anyway). */
  function collectSmallFileContents(): Record<number, string> {
    const smallFiles: Record<number, string> = {}
    for (const [nid, content] of fileContents.value) {
      if (content.kind === 'text' && contentByteSize(content) < SIZE_THRESHOLD) {
        smallFiles[nid] = content.data
      }
    }
    return smallFiles
  }

  /**
   * Persist the active flow to the in-browser library (IndexedDB), the WASM
   * analogue of the full app's catalog registration. Upserts by currentFlowId
   * (minting one on first save) so re-saving updates the same entry and rename
   * is non-lossy. Does NOT download a file — see exportFlowfile for that.
   */
  async function saveToLibrary(name?: string): Promise<{ id: string; name: string }> {
    await runBeforeExportHooks()
    if (!currentFlowId.value) currentFlowId.value = genFlowId()
    if (name) currentFlowName.value = name
    const flowName = currentFlowName.value || 'Untitled Flow'
    // Deep-clone to a plain object: exportToFlowfile carries reactive (Proxy)
    // arrays/objects that IndexedDB's structured clone rejects (DataCloneError).
    const data = JSON.parse(JSON.stringify(exportToFlowfile(flowName))) as FlowfileData

    const existing = await fileStorage.getSavedFlow(currentFlowId.value)
    const now = Date.now()
    const smallFiles = collectSmallFileContents()
    await fileStorage.putSavedFlow({
      id: currentFlowId.value,
      name: flowName,
      description: existing?.description ?? '',
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
      nodeCount: nodes.value.size,
      snapshot: data,
      fileContents: Object.keys(smallFiles).length ? smallFiles : undefined
    })
    return { id: currentFlowId.value, name: flowName }
  }

  /**
   * Export the current flow as a downloaded file (yaml/json). File output only —
   * persisting to the library is saveToLibrary's job.
   * @param name - Optional name for the flow
   * @param format - 'yaml' or 'json' (default: 'yaml' for flowfile_core compatibility)
   */
  async function exportFlowfile(name?: string, format: 'yaml' | 'json' = 'yaml') {
    // Flush any pending state from open explore_data panels so saved chart
    // specs end up in the exported node settings.
    await runBeforeExportHooks()

    const flowName = name || currentFlowName.value || `flow_${new Date().toISOString().slice(0, 10)}`
    const data = exportToFlowfile(flowName)
    currentFlowName.value = flowName

    let content: string
    let mimeType: string
    let extension: string

    if (format === 'yaml') {
      content = yaml.dump(data, {
        indent: 2,
        lineWidth: -1,  // No line wrapping
        noRefs: true,   // Don't use YAML references
        sortKeys: false // Preserve key order
      })
      mimeType = 'application/x-yaml'
      extension = 'yaml'
    } else {
      content = JSON.stringify(data, null, 2)
      mimeType = 'application/json'
      extension = 'json'
    }

    const blob = new Blob([content], { type: mimeType })
    const url = URL.createObjectURL(blob)

    const a = document.createElement('a')
    a.href = url
    a.download = `${flowName}.${extension}`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  // Back-compat alias (older callers / the embeddable Canvas toolbar).
  const downloadFlowfile = exportFlowfile

  /**
   * Validate flowfile data using Pydantic schemas (via Pyodide)
   * Returns validation result with any errors
   */
  async function validateFlowfileData(data: FlowfileData): Promise<{ success: boolean; error?: string }> {
    if (!pyodideStore.isReady) {
      // If Pyodide isn't ready, skip validation
      return { success: true }
    }

    try {
      const dataJson = JSON.stringify(data)
      const result = await pyodideStore.runPythonWithResult(`
import json
data = json.loads('''${dataJson.replace(/'/g, "\\'")}''')
result = validate_flowfile_data(data)
result
`)
      return {
        success: result.success,
        error: result.error
      }
    } catch (error) {
      console.error('Validation error:', error)
      // If validation fails to run, don't block - just warn
      return { success: true }
    }
  }

  /**
   * Load a flowfile from a File object
   * Supports both JSON and YAML formats (auto-detected by extension or content)
   */
  async function loadFlowfile(file: File): Promise<{
    success: boolean
    missingFiles?: Array<{nodeId: number, fileName: string}>
  }> {
    try {
      const text = await file.text()
      const fileName = file.name.toLowerCase()
  
      let data: FlowfileData
  
      if (fileName.endsWith('.yaml') || fileName.endsWith('.yml')) {
        data = yaml.load(text) as FlowfileData
      } else if (fileName.endsWith('.json') || fileName.endsWith('.flowfile')) {
        data = JSON.parse(text) as FlowfileData
      } else {
        // Try to auto-detect: if it starts with '{', it's probably JSON
        const trimmed = text.trim()
        if (trimmed.startsWith('{')) {
          data = JSON.parse(text) as FlowfileData
        } else {
          // Assume YAML
          data = yaml.load(text) as FlowfileData
        }
      }
  
      if (!data.flowfile_version || !data.nodes) {
        throw new Error('Invalid flowfile format')
      }
  
      const validation = await validateFlowfileData(data)
      if (!validation.success) {
        console.warn('Flowfile validation warning:', validation.error)
        // Continue anyway - validation is advisory
      }

      fileStorage.clearAll().catch(err => {
        console.error('Failed to clear IndexedDB:', err)
      })
  
      const imported = importFromFlowfile(data)
      if (imported) {
        const missingFiles = getMissingFileNodes()
        return { success: true, missingFiles }
      }
      return { success: false }
    } catch (error) {
      console.error('Failed to load flowfile', error)
      return { success: false }
    }
  }

  function getMissingFileNodes(): Array<{nodeId: number, fileName: string}> {
    const missing: Array<{nodeId: number, fileName: string}> = []
    
    for (const [id, node] of nodes.value) {
      if (node.type === 'read') {
        const settings = node.settings as NodeReadSettings
        const fileName = settings.file_name || settings.received_file?.name
        
        if (fileName && !fileContents.value.has(id)) {
          missing.push({ nodeId: id, fileName })
        }
      }
    }
    return missing
  }


  function clearFlow() {
    nodes.value.clear()
    edges.value = []
    nodeResults.value.clear()
    fileContents.value.clear()
    previewCache.value.clear()
    dirtyNodes.value.clear()
    selectedNodeId.value = null
    nodeIdCounter.value = 0
    currentFlowName.value = 'Untitled Flow'
    currentFlowId.value = null
    sessionStorage.removeItem(STORAGE_KEY)

    fileStorage.clearAll().catch(err => {
      console.error('Failed to clear IndexedDB:', err)
    })

    // Reset the Pyodide engine so the next flow's (reused) node ids start clean.
    if (pyodideStore.isReady) {
      pyodideStore.runPython('clear_all()').catch(err => console.error('clear_all failed on clear:', err))
    }
  }

  /**
   * Snapshot the full live flow state (graph + in-memory file contents + id
   * counter + name). Used by the multi-flow tabs store to stash a tab before
   * switching away. Lossless for in-session round-trips.
   */
  function captureSnapshot(): FlowStateSnapshot {
    const fc: Record<number, FileContent> = {}
    for (const [nid, content] of fileContents.value) fc[nid] = content
    return {
      name: currentFlowName.value,
      flowId: currentFlowId.value,
      snapshot: exportToFlowfile(currentFlowName.value),
      fileContents: fc,
      nodeIdCounter: nodeIdCounter.value
    }
  }

  /**
   * Restore a previously captured snapshot into the live flow. Replaces the
   * current flow entirely (importFromFlowfile clears state + resets the Pyodide
   * engine), then re-applies the snapshot's file contents and id counter. Does
   * NOT re-execute — the graph + inputs are restored, results are cleared.
   */
  function loadFromSnapshot(snap: FlowStateSnapshot): boolean {
    const ok = importFromFlowfile(snap.snapshot)
    if (!ok) return false
    for (const [nid, content] of Object.entries(snap.fileContents)) {
      setFileContent(Number(nid), content)
    }
    nodeIdCounter.value = snap.nodeIdCounter
    currentFlowName.value = snap.name
    // importFromFlowfile cleared the id; restore the snapshot's library identity.
    currentFlowId.value = snap.flowId ?? null
    return true
  }

  function updateNodeFile(nodeId: number, fileName: string, content: string | FileContent) {
    // Route through setFileContent so normalization, IndexedDB routing, and
    // schema inference all apply (a raw Map.set bypassed them all).
    setFileContent(nodeId, content)

    const node = nodes.value.get(nodeId)
    if (node && node.type === 'read') {
      const settings = node.settings as NodeReadSettings
      settings.file_name = fileName
      if (settings.received_file) {
        settings.received_file.name = fileName
        settings.received_file.path = fileName  // Also set path as required by flowfile_core
      }
    }
    
    // Mark node as dirty so it re-executes
    dirtyNodes.value.add(nodeId)
    saveToStorage()
  }

  return {
    // State
    nodes,
    edges,
    nodeResults,
    selectedNodeId,
    isExecuting,
    executionError,
    currentFlowName,
    currentFlowId,
    fileContents,

    // Getters
    nodeList,
    getNode,
    getNodeResult,
    getDownloadContent,
    getNodeInputSchema,
    getLeftInputSchema,
    getRightInputSchema,
    isInputSchemaResolved,
    getMissingFileNodes,

    // Actions
    generateNodeId,
    addNode,
    copyNode,
    pasteNode,
    hasClipboard,
    updateNode,
    updateNodeSettings,
    updateNodeSettingsSilent,
    registerBeforeExportHook,
    updateNodeDescription,
    updateNodeReference,
    validateNodeReference,
    removeNode,
    addEdge,
    removeEdge,
    setFileContent,
    hasFileContent,
    getFileContent,
    getTextContent,
    listExcelSheets,
    getNodeResultArrow,
    externalDatasets,
    setExternalDatasets,
    getExternalDatasetNames,
    getExternalDatasetContent,
    catalogDatasets,
    addCatalogDataset,
    getCatalogDatasetNames,
    getCatalogDatasetContent,
    removeCatalogDataset,
    loadDatasetForVisual,
    selectNode,
    executeNode,
    executeFlow,
    clearFlow,
    cleanupOrphanedData,
    propagateSchemas,
    executeNodeWithUpstream,
    setSourceNodeSchema,
    updateNodeFile, 

    // Preview management (lazy loading)
    fetchNodePreview,
    isPreviewLoading,
    hasPreviewCached,
    invalidatePreviewCache,

    // Dirty state tracking
    isNodeDirty,
    hasNodeExecuted,
    dirtyNodes,

    // Import/Export (FlowfileData format)
    exportToFlowfile,
    importFromFlowfile,
    captureSnapshot,
    loadFromSnapshot,
    saveToLibrary,
    exportFlowfile,
    downloadFlowfile,
    loadFlowfile,
    validateFlowfileData,

    // Output callbacks (for embeddable mode)
    onOutput: (cb: OutputCallback) => { outputCallbacks.add(cb) },
    offOutput: (cb: OutputCallback) => { outputCallbacks.delete(cb) },
    /** Remove all output callbacks (useful for cleanup) */
    clearOutputCallbacks: () => { outputCallbacks.clear() }
  }
})