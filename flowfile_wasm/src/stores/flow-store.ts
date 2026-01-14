import { defineStore } from 'pinia'
import { ref, computed, watch } from 'vue'
import { usePyodideStore } from './pyodide-store'
import yaml from 'js-yaml'
import { inferOutputSchema, isSourceNode, inferSchemaFromCsv, inferSchemaFromRawData } from './schema-inference'
import { fileStorage } from './file-storage'
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
  NodeFilterSettings,
  NodeSelectSettings,
  NodeGroupBySettings
} from '../types'

// Session storage keys
const STORAGE_KEY = 'flowfile_wasm_state'
const STORAGE_VERSION = '2'  // Increment when storage format changes

// Fields to exclude from setting_input when exporting (matches flowfile_core)
const SETTING_INPUT_EXCLUDE = new Set([
  'flow_id',
  'node_id',
  'pos_x',
  'pos_y',
  'is_setup',
  'description',
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

  // File content storage for CSV nodes
  const fileContents = ref<Map<number, string>>(new Map())

  // Preview cache state (for lazy loading)
  const previewCache = ref<Map<number, {
    data: any;
    timestamp: number;
    loading: boolean;
  }>>(new Map())
  const previewLoadingNodes = ref<Set<number>>(new Set())

  // Track nodes that have been modified since last execution (dirty state)
  const dirtyNodes = ref<Set<number>>(new Set())

  // Load state from session storage on init
  async function loadFromStorage() {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY)
      if (saved) {
        const state = JSON.parse(saved)
        // Check for new FlowfileData format (version 2+)
        if (state.version === STORAGE_VERSION && state.flowfileData) {
          const data = state.flowfileData as FlowfileData

          // Import from FlowfileData
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

            const node: FlowNode = {
              id: flowfileNode.id,
              type: nodeType,
              x: flowfileNode.x_position,
              y: flowfileNode.y_position,
              settings,
              inputIds: flowfileNode.input_ids || [],
              leftInputId: flowfileNode.left_input_id,
              rightInputId: flowfileNode.right_input_id,
              description: flowfileNode.description
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

          // Restore file contents from sessionStorage (small files)
          if (state.fileContents) {
            fileContents.value = new Map(state.fileContents)
          }

          // Restore large file contents from IndexedDB
          if (state.largeFileNodeIds && Array.isArray(state.largeFileNodeIds)) {
            // Load large files asynchronously from IndexedDB
            await Promise.all(
              state.largeFileNodeIds.map(async (nodeId: number) => {
                try {
                  const content = await fileStorage.getFileContent(nodeId)
                  if (content) {
                    fileContents.value.set(nodeId, content)
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

          // Restore counter
          const maxId = Math.max(0, ...data.nodes.map(n => n.id))
          nodeIdCounter.value = state.nodeIdCounter ?? maxId
        }
        // Fallback: legacy format (version 1)
        else if (state.nodes) {
          nodes.value = new Map(state.nodes)
          if (state.edges) edges.value = state.edges
          if (state.fileContents) fileContents.value = new Map(state.fileContents)
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
    // Build a map of node id -> node for quick lookups
    const nodeMap = new Map<number, FlowfileNode>()
    for (const node of flowfileNodes) {
      nodeMap.set(node.id, node)
    }

    // For each node, look at incoming connections based on input_ids, left_input_id, right_input_id
    for (const targetNode of flowfileNodes) {
      // Handle left_input_id (for join nodes - first input)
      if (targetNode.left_input_id !== undefined && targetNode.left_input_id !== null) {
        const sourceId = targetNode.left_input_id
        edges.value.push({
          id: `e${sourceId}-${targetNode.id}-output-0-input-0`,
          source: String(sourceId),
          target: String(targetNode.id),
          sourceHandle: 'output-0',
          targetHandle: 'input-0'
        })
      }

      // Handle right_input_id (for join nodes - second input)
      if (targetNode.right_input_id !== undefined && targetNode.right_input_id !== null) {
        const sourceId = targetNode.right_input_id
        edges.value.push({
          id: `e${sourceId}-${targetNode.id}-output-0-input-1`,
          source: String(sourceId),
          target: String(targetNode.id),
          sourceHandle: 'output-0',
          targetHandle: 'input-1'
        })
      }

      // Handle input_ids for non-join nodes (nodes without left/right inputs)
      if (targetNode.input_ids && targetNode.input_ids.length > 0) {
        // Skip if this node has left/right inputs (join node) - those are handled above
        if (!targetNode.left_input_id && !targetNode.right_input_id) {
          for (const sourceId of targetNode.input_ids) {
            edges.value.push({
              id: `e${sourceId}-${targetNode.id}-output-0-input-0`,
              source: String(sourceId),
              target: String(targetNode.id),
              sourceHandle: 'output-0',
              targetHandle: 'input-0'
            })
          }
        }
      }
    }
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

      // For join nodes (nodes with rightInputId), left_input_id should be null
      // because the left input is already represented in input_ids (flowfile_core format)
      const leftInputId = node.rightInputId ? undefined : node.leftInputId

      flowfileNodes.push({
        id: node.id,
        type: node.type,
        is_start_node: isStartNode,
        description: (node.settings as NodeBase).description || '',
        x_position: Math.round(node.x),  // flowfile_core expects int
        y_position: Math.round(node.y),  // flowfile_core expects int
        left_input_id: leftInputId,
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

    // Separate small and large files for hybrid storage
    const smallFiles: Array<[number, string]> = []
    const largeFileNodeIds: number[] = []

    for (const [nodeId, content] of fileContents.value.entries()) {
      if (fileStorage.shouldUseIndexedDB(content)) {
        // Large file: save to IndexedDB
        largeFileNodeIds.push(nodeId)
        // Save asynchronously (don't await to avoid blocking)
        fileStorage.setFileContent(nodeId, content).catch(err => {
          console.error(`Failed to save large file for node ${nodeId} to IndexedDB:`, err)
        })
      } else {
        // Small file: save to sessionStorage
        smallFiles.push([nodeId, content])
      }
    }

    try {
      const state = {
        version: STORAGE_VERSION,
        flowfileData,
        fileContents: smallFiles,
        largeFileNodeIds,
        nodeIdCounter: nodeIdCounter.value,
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

  // Watch for edge changes to trigger schema propagation
  watch(() => edges.value, () => {
    debouncedPropagateSchemas()
  }, { deep: true })

  // Watch for node settings changes to trigger schema propagation
  // We need to watch the settings of each node for changes
  watch(
    () => {
      // Create a dependency on all node settings
      const settingsSnapshot: Record<number, string> = {}
      nodes.value.forEach((node, id) => {
        // Stringify settings to detect deep changes
        settingsSnapshot[id] = JSON.stringify(node.settings)
      })
      return settingsSnapshot
    },
    () => {
      debouncedPropagateSchemas()
    },
    { deep: true }
  )

  // Load on init (async)
  loadFromStorage()
    .then(() => {
      // Initial schema propagation after loading
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

    // Find input node(s) and get their schema
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
    if (!node?.leftInputId) return []
    const result = nodeResults.value.get(node.leftInputId)
    return result?.schema || []
  }

  const getRightInputSchema = (nodeId: number): ColumnSchema[] => {
    const node = nodes.value.get(nodeId)
    if (!node?.rightInputId) return []
    const result = nodeResults.value.get(node.rightInputId)
    return result?.schema || []
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
      rightInputId: undefined
    }

    nodes.value.set(id, node)
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

      // Invalidate preview cache for this node and downstream
      invalidatePreviewCache(id)
    }
  }

  function removeNode(id: number) {
    nodes.value.delete(id)
    nodeResults.value.delete(id)
    fileContents.value.delete(id)
    previewCache.value.delete(id)
    dirtyNodes.value.delete(id)

    // Delete file from IndexedDB if it exists there
    fileStorage.deleteFileContent(id).catch(err => {
      // Silently ignore if file doesn't exist in IndexedDB
      if (err && err.name !== 'NotFoundError') {
        console.error(`Failed to delete file for node ${id} from IndexedDB:`, err)
      }
    })

    // Remove related edges
    edges.value = edges.value.filter(
      e => e.source !== String(id) && e.target !== String(id)
    )

    // Remove from other nodes' inputs
    nodes.value.forEach(node => {
      node.inputIds = node.inputIds.filter(inputId => inputId !== id)
      if (node.leftInputId === id) node.leftInputId = undefined
      if (node.rightInputId === id) node.rightInputId = undefined
    })
  }

  function addEdge(edge: FlowEdge) {
    // Check if edge already exists
    const exists = edges.value.some(
      e => e.source === edge.source &&
           e.target === edge.target &&
           e.sourceHandle === edge.sourceHandle &&
           e.targetHandle === edge.targetHandle
    )

    if (!exists) {
      edges.value.push(edge)

      // Update node inputs
      const targetId = parseInt(edge.target)
      const sourceId = parseInt(edge.source)
      const targetNode = nodes.value.get(targetId)

      if (targetNode) {
        // Determine which input based on handle
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

        // Invalidate preview cache for target node
        invalidatePreviewCache(targetId)
      }

      // Trigger immediate schema propagation for new connection
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

        // Invalidate preview cache for target node
        invalidatePreviewCache(targetId)
      }

      edges.value = edges.value.filter(e => e.id !== edgeId)

      // Trigger schema propagation to update downstream nodes
      debouncedPropagateSchemas()
    }
  }

  function setFileContent(nodeId: number, content: string) {
    fileContents.value.set(nodeId, content)

    // If file is large, immediately save to IndexedDB for performance
    if (fileStorage.shouldUseIndexedDB(content)) {
      fileStorage.setFileContent(nodeId, content).catch(err => {
        console.error(`Failed to save large file for node ${nodeId} to IndexedDB:`, err)
      })
    }

    // Get node to check settings for CSV parsing options
    const node = nodes.value.get(nodeId)
    if (node && (node.type === 'read' || node.type === 'manual_input')) {
      let hasHeaders = true
      let delimiter = ','

      if (node.type === 'read') {
        const settings = node.settings as any
        hasHeaders = settings?.received_file?.table_settings?.has_headers ?? true
        delimiter = settings?.received_file?.table_settings?.delimiter ?? ','
      }

      // Infer schema from CSV content
      const schema = inferSchemaFromCsv(content, hasHeaders, delimiter)
      if (schema) {
        // Set schema for source node (success undefined = not yet executed, shows grey)
        nodeResults.value.set(nodeId, { schema })

        // Trigger schema propagation to update downstream nodes
        debouncedPropagateSchemas()
      }
    }

    // Invalidate preview cache
    invalidatePreviewCache(nodeId)
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

      // Trigger schema propagation
      debouncedPropagateSchemas()
    }
  }

  function selectNode(id: number | null) {
    selectedNodeId.value = id

    // Auto-fetch preview when selecting a node that has data
    if (id !== null) {
      const result = nodeResults.value.get(id)
      if (result?.success && !hasPreviewCached(id)) {
        fetchNodePreview(id)
      }
    }
  }

  /**
   * Clean up orphaned Python dataframes for nodes that no longer exist in the flow.
   * This helps prevent memory leaks when nodes are deleted.
   */
  async function cleanupOrphanedData() {
    // Get current node IDs
    const currentNodeIds = new Set(nodes.value.keys())

    // Clean up file contents for removed nodes
    for (const [nodeId] of fileContents.value) {
      if (!currentNodeIds.has(nodeId)) {
        fileContents.value.delete(nodeId)
      }
    }

    // Clean up node results for removed nodes
    for (const [nodeId] of nodeResults.value) {
      if (!currentNodeIds.has(nodeId)) {
        nodeResults.value.delete(nodeId)
      }
    }

    // Clean up preview cache for removed nodes
    for (const [nodeId] of previewCache.value) {
      if (!currentNodeIds.has(nodeId)) {
        previewCache.value.delete(nodeId)
      }
    }

    // Clean up dirty flags for removed nodes
    for (const nodeId of dirtyNodes.value) {
      if (!currentNodeIds.has(nodeId)) {
        dirtyNodes.value.delete(nodeId)
      }
    }

    // Clean up Python lazyframes for removed nodes
    if (pyodideStore.isReady) {
      const nodeIdList = Array.from(currentNodeIds).join(',')
      await pyodideStore.runPython(`
# Clean up orphaned lazyframes
current_node_ids = {${nodeIdList}} if ${currentNodeIds.size} > 0 else set()
orphaned_ids = [nid for nid in list(_lazyframes.keys()) if nid not in current_node_ids]
for nid in orphaned_ids:
    clear_node(nid)
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

      // Get all dependencies (nodes this node depends on)
      const dependencies: number[] = [...node.inputIds]
      if (node.leftInputId) dependencies.push(node.leftInputId)
      if (node.rightInputId) dependencies.push(node.rightInputId)

      for (const depId of dependencies) {
        if (!visited.has(depId)) {
          parent.set(depId, id)
          const cycleStart = dfs(depId)
          if (cycleStart !== null) return cycleStart
        } else if (recursionStack.has(depId)) {
          // Found a cycle - return the start of the cycle
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
          // Reconstruct cycle path
          const cyclePath: number[] = [cycleStart]
          let current = parent.get(cycleStart)
          while (current !== undefined && current !== cycleStart) {
            cyclePath.push(current)
            current = parent.get(current)
          }
          cyclePath.push(cycleStart) // Complete the cycle
          return cyclePath
        }
      }
    }

    return null
  }

  function getExecutionOrder(): number[] {
    // Check for cycles first
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

    // Topological sort for execution order
    const visited = new Set<number>()
    const order: number[] = []

    function visit(id: number) {
      if (visited.has(id)) return
      visited.add(id)

      const node = nodes.value.get(id)
      if (!node) return

      // Visit dependencies first
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
      // Sync select_input with available columns
      const currentSelectInput = settings.select_input || []
      const existingColumns = new Map<string, any>(currentSelectInput.map((s: any) => [s.old_name, s]))
      const inputColumnNames = new Set(inputSchema.map(c => c.name))

      // Build new select_input array
      const newSelectInput: any[] = []

      // First, add all columns from input schema
      inputSchema.forEach((col, index) => {
        const existing = existingColumns.get(col.name)
        if (existing) {
          // Keep existing settings but update position and mark as available
          newSelectInput.push({
            ...existing,
            data_type: col.data_type,
            is_available: true,
            position: (existing as any).position ?? index
          })
        } else {
          // New column - add with defaults
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

      // Sort by position
      newSelectInput.sort((a: any, b: any) => (a.position ?? 0) - (b.position ?? 0))

      // Update settings
      settings.select_input = newSelectInput
      node.settings = settings
      modified = true
    }

    if (node.type === 'group_by') {
      // Sync groupby agg_cols with available columns
      const groupbyInput = settings.groupby_input || { agg_cols: [] }
      const currentAggCols = groupbyInput.agg_cols || []
      const inputColumnNames = new Set(inputSchema.map(c => c.name))

      // Mark unavailable columns
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
      // Check if the filtered field still exists
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
   * Propagate schemas through the flow graph
   * This updates nodeResults with inferred schemas for all nodes that can be computed
   * For polars_code/formula nodes, lazy execution is used when input data is available
   */
  async function propagateSchemas() {
    const order = getExecutionOrder()

    for (const nodeId of order) {
      const node = nodes.value.get(nodeId)
      if (!node) continue

      // Skip source nodes - their schema comes from actual data
      if (isSourceNode(node.type)) {
        // Keep existing schema if present (from data load)
        continue
      }

      // Get input schema from upstream node
      let inputSchema: ColumnSchema[] | null = null
      let rightInputSchema: ColumnSchema[] | null = null

      // Get primary input schema and check if input has been executed
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

      // Sync node settings with input schema (updates select_input, agg_cols, etc.)
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
          // Execute the node to get its actual output schema
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
            error: existingResult?.error,  // ADD THIS - preserve error!
            execution_time: existingResult?.execution_time,
            download: existingResult?.download
          })
      } else {
        // inferOutputSchema returned null - this means:
        // 1. For polars_code/formula: schema can only be known after execution (and input wasn't available)
        // 2. For other nodes: input schema might be missing
        //
        // Keep any existing executed result (which has actual schema from Python)
        // Only clear if there's no input AND no existing data
        const existingResult = nodeResults.value.get(nodeId)
        if (!inputSchema && !isSourceNode(node.type) && existingResult && !existingResult.data && !existingResult.success) {
          nodeResults.value.delete(nodeId)
        }
        // If there's an existing result with actual data, keep it - the schema from
        // execution is authoritative for nodes we can't infer (like polars_code)
      }
    }
  }

  // Debounced schema propagation to avoid excessive updates
  // Using a wrapper to handle async
  let propagateTimeout: ReturnType<typeof setTimeout> | null = null
  function debouncedPropagateSchemas() {
    if (propagateTimeout) clearTimeout(propagateTimeout)
    propagateTimeout = setTimeout(() => {
      propagateSchemas().catch(err => console.error('Schema propagation error:', err))
      propagateTimeout = null
    }, 50)
  }

  // =============================================================================
  // Preview Cache Management (Lazy Loading)
  // =============================================================================

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

      // Mark node as dirty (has changes since last run)
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
    // Also check nodeResults for data
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

    // Check local cache first (unless force refresh)
    if (!forceRefresh) {
      const cached = previewCache.value.get(nodeId)
      if (cached && !cached.loading && cached.data) {
        // Also update nodeResults for display
        const existingResult = nodeResults.value.get(nodeId)
        if (existingResult) {
          nodeResults.value.set(nodeId, {
            ...existingResult,
            data: cached.data
          })
        }
        return { success: true, data: cached.data, fromCache: true }
      }

      // Check nodeResults too
      const result = nodeResults.value.get(nodeId)
      if (result?.data) {
        return { success: true, data: result.data, fromCache: true }
      }
    }

    if (!pyodideStore.isReady) {
      return { success: false, error: 'Pyodide is not ready' }
    }

    // Mark as loading
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

        // Also update nodeResults with the preview data for display
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

  // =============================================================================
  // Node Execution
  // =============================================================================

  async function executeNode(nodeId: number): Promise<NodeResult> {
    const node = nodes.value.get(nodeId)
    if (!node) {
      return { success: false, error: 'Node not found' }
    }

    const { runPythonWithResult, setGlobal, deleteGlobal } = pyodideStore

    try {
      let result: NodeResult

      switch (node.type) {
        case 'read': {
          const content = fileContents.value.get(nodeId)
          if (!content) {
            return { success: false, error: 'No file loaded' }
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
          const content = fileContents.value.get(nodeId)
          if (!content) {
            return { success: false, error: 'No data entered' }
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
            return { success: false, error: 'No input connected' }
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
            return { success: false, error: 'No input connected' }
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
            return { success: false, error: 'No input connected' }
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
            return { success: false, error: 'Both left and right inputs required for join' }
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
            return { success: false, error: 'No input connected' }
          }
          result = await runPythonWithResult(`
import json
result = execute_sort(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'polars_code': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          result = await runPythonWithResult(`
import json
result = execute_polars_code(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          break
        }

        case 'unique': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
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
            return { success: false, error: 'No input connected' }
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
            return { success: false, error: 'No input connected' }
          }
          result = await runPythonWithResult(`
result = execute_preview(${nodeId}, ${inputId})
result
`)
          break
        }

        case 'pivot': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
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
            return { success: false, error: 'No input connected' }
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
            return { success: false, error: 'No input connected' }
          }
          const outputResult = await runPythonWithResult(`
import json
result = execute_output(${nodeId}, ${inputId}, json.loads(${toPythonJson(node.settings)}))
result
`)
          // Store download content separately in IndexedDB (not in nodeResults)
          if (outputResult?.success && outputResult?.download) {
            const { content, file_name, file_type, mime_type, row_count } = outputResult.download
            await fileStorage.setDownloadContent(
              nodeId,
              content,
              file_name,
              file_type,
              mime_type,
              row_count
            )
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

        default:
          return { success: false, error: `Unknown node type: ${node.type}` }
      }

      // Store result - success=true indicates data is available in Python
      const nodeResult: NodeResult = {
        success: result.success,
        schema: result.schema,
        error: result.error,
        download: result.download
      }

      nodeResults.value.set(nodeId, nodeResult)

      // Clear dirty flag since we just executed
      if (result.success) {
        dirtyNodes.value.delete(nodeId)
      }

      // Clear preview cache since we just executed
      previewCache.value.delete(nodeId)

      return nodeResult
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
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
      return
    }

    isExecuting.value = true
    executionError.value = null
    nodeResults.value.clear()
    previewCache.value.clear()  // Clear all preview cache
    dirtyNodes.value.clear()    // Clear all dirty flags (will be re-set if execution fails)

    try {
      // Clean up orphaned data before execution
      await cleanupOrphanedData()

      // Clear Python state
      await pyodideStore.runPython('clear_all()')

      // Get execution order and execute nodes
      // This builds the lazy query plans - should be fast!
      const order = getExecutionOrder()

      for (const nodeId of order) {
        await executeNode(nodeId)
      }

      // After execution, propagate schemas to update downstream node settings
      // This syncs select_input, agg_cols, etc. with actual executed schemas
      await propagateSchemas()

      // Optional: Auto-fetch preview for selected node
      if (selectedNodeId.value !== null) {
        const result = nodeResults.value.get(selectedNodeId.value)
        if (result?.success) {
          await fetchNodePreview(selectedNodeId.value)
        }
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      executionError.value = errorMessage
      console.error('Flow execution error:', error)
    } finally {
      isExecuting.value = false
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
          ...base
        } as NodeSettings

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
    const flowfileNodes: FlowfileNode[] = []

    // Convert nodes
    nodes.value.forEach((node, id) => {
      // Determine if this is a start node (no inputs)
      const isStartNode = node.inputIds.length === 0 && !node.leftInputId && !node.rightInputId

      // Get output nodes from edges
      const outputs = edges.value
        .filter(e => e.source === String(id))
        .map(e => parseInt(e.target))

      // For join nodes (nodes with rightInputId), left_input_id should be null
      // because the left input is already represented in input_ids (flowfile_core format)
      const leftInputId = node.rightInputId ? undefined : node.leftInputId

      const flowfileNode: FlowfileNode = {
        id: node.id,
        type: node.type,
        is_start_node: isStartNode,
        description: (node.settings as NodeBase).description || '',
        x_position: Math.round(node.x),  // flowfile_core expects int
        y_position: Math.round(node.y),  // flowfile_core expects int
        left_input_id: leftInputId,
        right_input_id: node.rightInputId,
        input_ids: node.inputIds,
        outputs,
        setting_input: cleanSettingInput(node.settings)
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
      // Clear existing state
      nodes.value.clear()
      edges.value = []
      nodeResults.value.clear()
      fileContents.value.clear()
      previewCache.value.clear()
      dirtyNodes.value.clear()
      selectedNodeId.value = null

      // Clear IndexedDB file storage
      fileStorage.clearAll().catch(err => {
        console.error('Failed to clear IndexedDB:', err)
      })

      // Find max node id for counter
      let maxId = 0

      // Import nodes
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

        const node: FlowNode = {
          id: flowfileNode.id,
          type: nodeType,
          x: flowfileNode.x_position ?? 0,
          y: flowfileNode.y_position ?? 0,
          settings,
          inputIds: flowfileNode.input_ids || [],
          leftInputId: flowfileNode.left_input_id,
          rightInputId: flowfileNode.right_input_id,
          description: flowfileNode.description
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

      // Trigger schema propagation after import
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

  /**
   * Download the current flow as a file
   * @param name - Optional name for the flow
   * @param format - 'yaml' or 'json' (default: 'yaml' for flowfile_core compatibility)
   */
  function downloadFlowfile(name?: string, format: 'yaml' | 'json' = 'yaml') {
    const flowName = name || `flow_${new Date().toISOString().slice(0, 10)}`
    const data = exportToFlowfile(flowName)

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
  
      // Detect format by extension or content
      if (fileName.endsWith('.yaml') || fileName.endsWith('.yml')) {
        // Parse as YAML
        data = yaml.load(text) as FlowfileData
      } else if (fileName.endsWith('.json') || fileName.endsWith('.flowfile')) {
        // Parse as JSON
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
  
      // Optional: Validate using Pydantic schemas
      const validation = await validateFlowfileData(data)
      if (!validation.success) {
        console.warn('Flowfile validation warning:', validation.error)
        // Continue anyway - validation is advisory
      }
  
      // Clear IndexedDB file storage
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
    sessionStorage.removeItem(STORAGE_KEY)

    // Clear IndexedDB file storage
    fileStorage.clearAll().catch(err => {
      console.error('Failed to clear IndexedDB:', err)
    })
  }

  function updateNodeFile(nodeId: number, fileName: string, content: string) {
    fileContents.value.set(nodeId, content)
    
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
    fileContents,

    // Getters
    nodeList,
    getNode,
    getNodeResult,
    getDownloadContent,
    getNodeInputSchema,
    getLeftInputSchema,
    getRightInputSchema,
    getMissingFileNodes,

    // Actions
    generateNodeId,
    addNode,
    updateNode,
    updateNodeSettings,
    removeNode,
    addEdge,
    removeEdge,
    setFileContent,
    selectNode,
    executeNode,
    executeFlow,
    clearFlow,
    cleanupOrphanedData,
    propagateSchemas,
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
    downloadFlowfile,
    loadFlowfile,
    validateFlowfileData
  }
})