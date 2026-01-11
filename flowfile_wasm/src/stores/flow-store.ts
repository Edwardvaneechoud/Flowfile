import { defineStore } from 'pinia'
import { ref, computed, watch } from 'vue'
import { usePyodideStore } from './pyodide-store'
import yaml from 'js-yaml'
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

/**
 * Clean setting_input by removing fields that are excluded during export
 * This matches the behavior of flowfile_core's FlowfileNode serializer
 */
function cleanSettingInput(settings: NodeSettings): any {
  if (!settings) return null
  const cleaned: Record<string, any> = {}
  for (const [key, value] of Object.entries(settings)) {
    if (!SETTING_INPUT_EXCLUDE.has(key)) {
      cleaned[key] = value
    }
  }
  return cleaned
}

export const useFlowStore = defineStore('flow', () => {
  const pyodideStore = usePyodideStore()

  // State
  const nodes = ref<Map<number, FlowNode>>(new Map())
  const edges = ref<FlowEdge[]>([])
  const nodeResults = ref<Map<number, NodeResult>>(new Map())
  const selectedNodeId = ref<number | null>(null)
  const isExecuting = ref(false)
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
      // Handle left_input_id (for join nodes)
      if (targetNode.left_input_id !== undefined && targetNode.left_input_id !== null) {
        const sourceId = targetNode.left_input_id
        edges.value.push({
          id: `e${sourceId}-${targetNode.id}-output-input-0`,
          source: String(sourceId),
          target: String(targetNode.id),
          sourceHandle: 'output',
          targetHandle: 'input-0'
        })
      }

      // Handle right_input_id (for join nodes)
      if (targetNode.right_input_id !== undefined && targetNode.right_input_id !== null) {
        const sourceId = targetNode.right_input_id
        edges.value.push({
          id: `e${sourceId}-${targetNode.id}-output-input-1`,
          source: String(sourceId),
          target: String(targetNode.id),
          sourceHandle: 'output',
          targetHandle: 'input-1'
        })
      }

      // Handle input_ids for non-join nodes (nodes without left/right inputs)
      if (targetNode.input_ids && targetNode.input_ids.length > 0) {
        // Skip if this node has left/right inputs (join node) - those are handled above
        if (!targetNode.left_input_id && !targetNode.right_input_id) {
          for (const sourceId of targetNode.input_ids) {
            edges.value.push({
              id: `e${sourceId}-${targetNode.id}-output-input-0`,
              source: String(sourceId),
              target: String(targetNode.id),
              sourceHandle: 'output',
              targetHandle: 'input-0'
            })
          }
        }
      }
    }
  }

  // Save state to session storage using FlowfileData format (flowfile_core compatible)
  function saveToStorage() {
    try {
      // Build FlowfileData structure (without connections - flowfile_core format)
      const flowfileNodes: FlowfileNode[] = []

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

  // Load on init
  loadFromStorage()

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
      }

      edges.value = edges.value.filter(e => e.id !== edgeId)
    }
  }

  function setFileContent(nodeId: number, content: string) {
    fileContents.value.set(nodeId, content)
  }

  function selectNode(id: number | null) {
    selectedNodeId.value = id
  }

  function getExecutionOrder(): number[] {
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

  async function executeNode(nodeId: number): Promise<NodeResult> {
    const node = nodes.value.get(nodeId)
    if (!node) {
      return { success: false, error: 'Node not found' }
    }

    const { runPythonWithResult } = pyodideStore

    try {
      let result: NodeResult

      switch (node.type) {
        case 'read_csv': {
          const content = fileContents.value.get(nodeId)
          if (!content) {
            return { success: false, error: 'No file loaded' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedContent = content.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_read_csv(${nodeId}, '''${escapedContent}''', json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'manual_input': {
          const content = fileContents.value.get(nodeId)
          if (!content) {
            return { success: false, error: 'No data entered' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedContent = content.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_manual_input(${nodeId}, '''${escapedContent}''', json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'filter': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_filter(${nodeId}, ${inputId}, json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'select': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_select(${nodeId}, ${inputId}, json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'group_by': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_group_by(${nodeId}, ${inputId}, json.loads('${escapedSettings}'))
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
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_join(${nodeId}, ${leftId}, ${rightId}, json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'sort': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_sort(${nodeId}, ${inputId}, json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'polars_code': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_polars_code(${nodeId}, ${inputId}, json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'unique': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_unique(${nodeId}, ${inputId}, json.loads('${escapedSettings}'))
result
`)
          break
        }

        case 'head': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          const escapedSettings = settings.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r')
          result = await runPythonWithResult(`
import json
result = execute_head(${nodeId}, ${inputId}, json.loads('${escapedSettings}'))
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
    nodeResults.value.clear()

    try {
      // Clear Python state
      await pyodideStore.runPython('clear_all()')

      // Get execution order and execute nodes
      const order = getExecutionOrder()
      for (const nodeId of order) {
        await executeNode(nodeId)
      }
    } catch (error) {
      console.error('Flow execution error', error)
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
      selectedNodeId.value = null

      // Find max node id for counter
      let maxId = 0

      // Import nodes
      for (const flowfileNode of data.nodes) {
        if (flowfileNode.id > maxId) maxId = flowfileNode.id

        const node: FlowNode = {
          id: flowfileNode.id,
          type: flowfileNode.type,
          x: flowfileNode.x_position ?? 0,
          y: flowfileNode.y_position ?? 0,
          settings: flowfileNode.setting_input as NodeSettings,
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
   * Load a flowfile from a File object
   * Supports both JSON and YAML formats (auto-detected by extension or content)
   */
  async function loadFlowfile(file: File): Promise<boolean> {
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

    // Import/Export (FlowfileData format)
    exportToFlowfile,
    importFromFlowfile,
    downloadFlowfile,
    loadFlowfile
  }
})
