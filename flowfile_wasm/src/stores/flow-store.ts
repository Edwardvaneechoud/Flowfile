import { defineStore } from 'pinia'
import { ref, computed, watch } from 'vue'
import { usePyodideStore } from './pyodide-store'
import { inferOutputSchema, isSourceNode, inferSchemaFromCsv, inferSchemaFromRawData } from './schema-inference'
import type {
  FlowNode,
  FlowEdge,
  NodeResult,
  NodeSettings,
  ColumnSchema,
  FlowfileData,
  FlowfileNode,
  NodeConnection,
  NodeBase,
  NodeReadSettings,
  NodeManualInputSettings,
  NodeFilterSettings,
  NodeSelectSettings,
  NodeGroupBySettings
} from '../types'

// Simple debounce utility
function debounce<T extends (...args: any[]) => any>(fn: T, delay: number): T {
  let timeoutId: ReturnType<typeof setTimeout> | null = null
  return ((...args: Parameters<T>) => {
    if (timeoutId) clearTimeout(timeoutId)
    timeoutId = setTimeout(() => {
      fn(...args)
      timeoutId = null
    }, delay)
  }) as T
}

// Session storage keys
const STORAGE_KEY = 'flowfile_wasm_state'
const STORAGE_VERSION = '2'  // Increment when storage format changes

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

  // Load state from session storage on init
  function loadFromStorage() {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY)
      if (saved) {
        const state = JSON.parse(saved)
        // Check for new FlowfileData format (version 2+)
        if (state.version === STORAGE_VERSION && state.flowfileData) {
          const data = state.flowfileData as FlowfileData

          // Import from FlowfileData
          for (const flowfileNode of data.nodes) {
            const node: FlowNode = {
              id: flowfileNode.id,
              type: flowfileNode.type,
              x: flowfileNode.x_position,
              y: flowfileNode.y_position,
              settings: flowfileNode.setting_input as NodeSettings,
              inputIds: flowfileNode.input_ids || [],
              leftInputId: flowfileNode.left_input_id,
              rightInputId: flowfileNode.right_input_id,
              description: flowfileNode.description
            }
            nodes.value.set(flowfileNode.id, node)
          }

          for (const conn of data.connections) {
            edges.value.push({
              id: `e${conn.from_node}-${conn.to_node}-${conn.from_handle}-${conn.to_handle}`,
              source: String(conn.from_node),
              target: String(conn.to_node),
              sourceHandle: conn.from_handle,
              targetHandle: conn.to_handle
            })
          }

          // Restore file contents separately (not part of FlowfileData)
          if (state.fileContents) {
            fileContents.value = new Map(state.fileContents)
          }

          // Restore node schemas for quick column access
          if (state.nodeSchemas) {
            for (const [id, schema] of state.nodeSchemas) {
              if (schema && schema.length > 0) {
                nodeResults.value.set(id, { success: true, schema })
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

  // Save state to session storage using FlowfileData format
  function saveToStorage() {
    try {
      // Build FlowfileData structure
      const flowfileNodes: FlowfileNode[] = []
      const flowfileConnections: NodeConnection[] = []

      nodes.value.forEach((node, id) => {
        const isStartNode = node.inputIds.length === 0 && !node.leftInputId && !node.rightInputId
        const outputs = edges.value
          .filter(e => e.source === String(id))
          .map(e => parseInt(e.target))

        flowfileNodes.push({
          id: node.id,
          type: node.type,
          is_start_node: isStartNode,
          description: (node.settings as NodeBase).description || '',
          x_position: node.x,
          y_position: node.y,
          left_input_id: node.leftInputId,
          right_input_id: node.rightInputId,
          input_ids: node.inputIds,
          outputs,
          setting_input: node.settings
        })
      })

      edges.value.forEach(edge => {
        flowfileConnections.push({
          from_node: parseInt(edge.source),
          to_node: parseInt(edge.target),
          from_handle: edge.sourceHandle,
          to_handle: edge.targetHandle
        })
      })

      const flowfileData: FlowfileData = {
        flowfile_version: '1.0.0-wasm',
        flowfile_id: Date.now(),
        flowfile_name: 'Session Flow',
        flowfile_settings: {
          description: '',
          execution_mode: 'Development',
          execution_location: 'local',
          auto_save: true,
          show_detailed_progress: false
        },
        nodes: flowfileNodes,
        connections: flowfileConnections
      }

      const state = {
        version: STORAGE_VERSION,
        flowfileData,
        fileContents: Array.from(fileContents.value.entries()),
        nodeIdCounter: nodeIdCounter.value,
        // Save schemas separately for quick reload
        nodeSchemas: Array.from(nodeResults.value.entries()).map(([id, result]) => [id, result.schema])
      }

      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state))
    } catch (err) {
      console.error('Failed to save state to session storage', err)
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

  // Load on init
  loadFromStorage()

  // Initial schema propagation after loading
  // Use setTimeout to ensure nodeResults are populated from storage
  setTimeout(() => {
    propagateSchemas()
  }, 0)

  // Getters
  const nodeList = computed(() => Array.from(nodes.value.values()))

  const getNode = (id: number) => nodes.value.get(id)

  const getNodeResult = (id: number) => nodeResults.value.get(id)

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
    }
  }

  function removeNode(id: number) {
    nodes.value.delete(id)
    nodeResults.value.delete(id)
    fileContents.value.delete(id)

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
      }

      edges.value = edges.value.filter(e => e.id !== edgeId)

      // Trigger schema propagation to update downstream nodes
      debouncedPropagateSchemas()
    }
  }

  function setFileContent(nodeId: number, content: string) {
    fileContents.value.set(nodeId, content)

    // Get node to check settings for CSV parsing options
    const node = nodes.value.get(nodeId)
    if (node && (node.type === 'read_csv' || node.type === 'manual_input')) {
      let hasHeaders = true
      let delimiter = ','

      if (node.type === 'read_csv') {
        const settings = node.settings as any
        hasHeaders = settings?.received_table?.table_settings?.has_headers ?? true
        delimiter = settings?.received_table?.table_settings?.delimiter ?? ','
      }

      // Infer schema from CSV content
      const schema = inferSchemaFromCsv(content, hasHeaders, delimiter)
      if (schema) {
        // Set schema for source node
        nodeResults.value.set(nodeId, {
          success: true,
          schema
        })

        // Trigger schema propagation to update downstream nodes
        debouncedPropagateSchemas()
      }
    }
  }

  /**
   * Set schema for a source node from raw data fields
   * Used by manual input nodes when data structure changes
   */
  function setSourceNodeSchema(nodeId: number, fields: { name: string; data_type: string }[]) {
    const schema = inferSchemaFromRawData(fields)
    if (schema) {
      nodeResults.value.set(nodeId, {
        success: true,
        schema
      })

      // Trigger schema propagation
      debouncedPropagateSchemas()
    }
  }

  function selectNode(id: number | null) {
    selectedNodeId.value = id
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

    // Clean up Python dataframes for removed nodes
    if (pyodideStore.isReady) {
      const nodeIdList = Array.from(currentNodeIds).join(',')
      await pyodideStore.runPython(`
# Clean up orphaned dataframes
current_node_ids = {${nodeIdList}} if ${currentNodeIds.size} > 0 else set()
orphaned_ids = [nid for nid in list(_dataframes.keys()) if nid not in current_node_ids]
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
   */
  function syncNodeSettingsWithSchema(node: FlowNode, inputSchema: ColumnSchema[], rightInputSchema?: ColumnSchema[] | null) {
    const settings = node.settings as any

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
      // Check if sorted columns still exist
      const sortInput = settings.sort_input
      if (sortInput?.sort_cols) {
        const inputColumnNames = new Set(inputSchema.map(c => c.name))
        sortInput.sort_cols = sortInput.sort_cols.map((col: any) => ({
          ...col,
          is_available: inputColumnNames.has(col.column)
        }))
        settings.sort_input = sortInput
        node.settings = settings
      }
    }
  }

  /**
   * Propagate schemas through the flow graph
   * This updates nodeResults with inferred schemas for all nodes that can be computed
   * without actually executing the flow
   */
  function propagateSchemas() {
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

      // Get primary input schema
      const primaryInputId = node.leftInputId || node.inputIds[0]
      if (primaryInputId) {
        const inputResult = nodeResults.value.get(primaryInputId)
        inputSchema = inputResult?.schema || null
      }

      // For join nodes, also get right input schema
      if (node.type === 'join' && node.rightInputId) {
        const rightResult = nodeResults.value.get(node.rightInputId)
        rightInputSchema = rightResult?.schema || null
      }

      // Sync node settings with input schema (updates select_input, agg_cols, etc.)
      if (inputSchema && inputSchema.length > 0) {
        syncNodeSettingsWithSchema(node, inputSchema, rightInputSchema)
      }

      // Infer output schema
      const inferredSchema = inferOutputSchema(
        node.type,
        inputSchema,
        node.settings,
        rightInputSchema
      )

      // Update nodeResults with inferred schema
      if (inferredSchema) {
        const existingResult = nodeResults.value.get(nodeId)
        // Only update if we don't have execution data or if schema changed
        if (!existingResult?.data) {
          nodeResults.value.set(nodeId, {
            success: true,
            schema: inferredSchema,
            // Preserve existing data if any
            data: existingResult?.data,
            execution_time: existingResult?.execution_time
          })
        }
      } else if (!inputSchema && !isSourceNode(node.type)) {
        // No input schema available and not a source node - clear any inferred schema
        // but keep results with actual data
        const existingResult = nodeResults.value.get(nodeId)
        if (existingResult && !existingResult.data) {
          nodeResults.value.delete(nodeId)
        }
      }
    }
  }

  // Debounced schema propagation to avoid excessive updates
  const debouncedPropagateSchemas = debounce(propagateSchemas, 50)

  async function executeNode(nodeId: number): Promise<NodeResult> {
    const node = nodes.value.get(nodeId)
    if (!node) {
      return { success: false, error: 'Node not found' }
    }

    const { runPythonWithResult, setGlobal, deleteGlobal } = pyodideStore

    try {
      let result: NodeResult

      switch (node.type) {
        case 'read_csv': {
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

        case 'preview': {
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

        default:
          return { success: false, error: `Unknown node type: ${node.type}` }
      }

      nodeResults.value.set(nodeId, result)
      return result
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

    try {
      // Clean up orphaned data before execution
      await cleanupOrphanedData()

      // Clear Python state
      await pyodideStore.runPython('clear_all()')

      // Get execution order and execute nodes
      const order = getExecutionOrder()

      for (const nodeId of order) {
        await executeNode(nodeId)
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
      case 'read_csv':
        return {
          ...base,
          received_table: {
            name: '',
            file_type: 'csv',
            table_settings: {
              file_type: 'csv',
              delimiter: ',',
              has_headers: true,
              starting_from_line: 0,
              encoding: 'utf-8',
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
          raw_data: {
            fields: [],
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
          sort_input: {
            sort_cols: []
          }
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

      case 'preview':
        return {
          ...base
        } as NodeSettings

      default:
        return base as NodeSettings
    }
  }

  /**
   * Export the current flow state to FlowfileData format
   * This produces a JSON structure compatible with flowfile_core
   */
  function exportToFlowfile(name: string = 'Untitled Flow'): FlowfileData {

    const flowfileNodes: FlowfileNode[] = []
    const flowfileConnections: NodeConnection[] = []

    // Convert nodes
    nodes.value.forEach((node, id) => {
      // Determine if this is a start node (no inputs)
      const isStartNode = node.inputIds.length === 0 && !node.leftInputId && !node.rightInputId

      // Get output nodes
      const outputs = edges.value
        .filter(e => e.source === String(id))
        .map(e => parseInt(e.target))

      const flowfileNode: FlowfileNode = {
        id: node.id,
        type: node.type,
        is_start_node: isStartNode,
        description: (node.settings as NodeBase).description || '',
        x_position: node.x,
        y_position: node.y,
        left_input_id: node.leftInputId,
        right_input_id: node.rightInputId,
        input_ids: node.inputIds,
        outputs,
        setting_input: node.settings
      }

      flowfileNodes.push(flowfileNode)
    })

    // Convert edges to connections
    edges.value.forEach(edge => {
      const connection: NodeConnection = {
        from_node: parseInt(edge.source),
        to_node: parseInt(edge.target),
        from_handle: edge.sourceHandle,
        to_handle: edge.targetHandle
      }
      flowfileConnections.push(connection)
    })

    const flowfileData: FlowfileData = {
      flowfile_version: '1.0.0-wasm',
      flowfile_id: Date.now(),
      flowfile_name: name,
      flowfile_settings: {
        description: '',
        execution_mode: 'Development',
        execution_location: 'local',
        auto_save: true,
        show_detailed_progress: false
      },
      nodes: flowfileNodes,
      connections: flowfileConnections
    }

    return flowfileData
  }

  /**
   * Import a FlowfileData structure into the current state
   * This loads a flow file that was created in flowfile_core or this WASM app
   */
  function importFromFlowfile(data: FlowfileData): boolean {

    try {
      // Clear existing state
      nodes.value.clear()
      edges.value = []
      nodeResults.value.clear()
      fileContents.value.clear()
      selectedNodeId.value = null

      // Find max node id for counter
      let maxId = 0

      // Import nodes
      for (const flowfileNode of data.nodes) {
        if (flowfileNode.id > maxId) maxId = flowfileNode.id

        const node: FlowNode = {
          id: flowfileNode.id,
          type: flowfileNode.type,
          x: flowfileNode.x_position,
          y: flowfileNode.y_position,
          settings: flowfileNode.setting_input as NodeSettings,
          inputIds: flowfileNode.input_ids || [],
          leftInputId: flowfileNode.left_input_id,
          rightInputId: flowfileNode.right_input_id,
          description: flowfileNode.description
        }

        nodes.value.set(flowfileNode.id, node)
      }

      nodeIdCounter.value = maxId

      // Import connections as edges
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

      // Trigger schema propagation after import
      // Note: Source nodes will need data loaded before schemas propagate
      setTimeout(() => propagateSchemas(), 0)

      return true
    } catch (error) {
      console.error('Failed to import flow', error)
      return false
    }
  }

  /**
   * Download the current flow as a .flowfile JSON file
   */
  function downloadFlowfile(name?: string) {
    const flowName = name || `flow_${new Date().toISOString().slice(0, 10)}`
    const data = exportToFlowfile(flowName)
    const json = JSON.stringify(data, null, 2)
    const blob = new Blob([json], { type: 'application/json' })
    const url = URL.createObjectURL(blob)

    const a = document.createElement('a')
    a.href = url
    a.download = `${flowName}.flowfile`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  /**
   * Load a .flowfile from a File object
   */
  async function loadFlowfile(file: File): Promise<boolean> {
    try {
      const text = await file.text()
      const data = JSON.parse(text) as FlowfileData

      if (!data.flowfile_version || !data.nodes) {
        throw new Error('Invalid flowfile format')
      }

      return importFromFlowfile(data)
    } catch (error) {
      console.error('Failed to load flowfile', error)
      return false
    }
  }

  function clearFlow() {
    nodes.value.clear()
    edges.value = []
    nodeResults.value.clear()
    fileContents.value.clear()
    selectedNodeId.value = null
    nodeIdCounter.value = 0
    sessionStorage.removeItem(STORAGE_KEY)
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
    getNodeInputSchema,
    getLeftInputSchema,
    getRightInputSchema,

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

    // Import/Export (FlowfileData format)
    exportToFlowfile,
    importFromFlowfile,
    downloadFlowfile,
    loadFlowfile
  }
})
