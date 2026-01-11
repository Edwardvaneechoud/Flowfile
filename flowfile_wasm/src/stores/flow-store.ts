import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { usePyodideStore } from './pyodide-store'
import type { FlowNode, FlowEdge, NodeResult, NodeSettings, ColumnSchema } from '../types'

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
    const defaultSettings = getDefaultSettings(type, id)

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
        if (edge.targetHandle === 'input-0') {
          if (!targetNode.inputIds.includes(sourceId)) {
            targetNode.inputIds.push(sourceId)
          }
        } else if (edge.targetHandle === 'input-1') {
          // For join nodes, input-1 is the right input
          targetNode.rightInputId = sourceId
          // Also set left input if not set
          if (targetNode.inputIds.length > 0) {
            targetNode.leftInputId = targetNode.inputIds[0]
          }
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
          result = await runPythonWithResult(`
import json
result = execute_read_csv(${nodeId}, '''${escapedContent}''', json.loads('${settings}'))
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
          result = await runPythonWithResult(`
import json
result = execute_filter(${nodeId}, ${inputId}, json.loads('${settings}'))
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
          result = await runPythonWithResult(`
import json
result = execute_select(${nodeId}, ${inputId}, json.loads('${settings}'))
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
          result = await runPythonWithResult(`
import json
result = execute_group_by(${nodeId}, ${inputId}, json.loads('${settings}'))
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
          result = await runPythonWithResult(`
import json
result = execute_join(${nodeId}, ${leftId}, ${rightId}, json.loads('${settings}'))
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
          result = await runPythonWithResult(`
import json
result = execute_sort(${nodeId}, ${inputId}, json.loads('${settings}'))
result
`)
          break
        }

        case 'with_columns': {
          const inputId = node.inputIds[0]
          if (!inputId) {
            return { success: false, error: 'No input connected' }
          }
          const settings = JSON.stringify(node.settings)
          result = await runPythonWithResult(`
import json
result = execute_with_columns(${nodeId}, ${inputId}, json.loads('${settings}'))
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
          result = await runPythonWithResult(`
import json
result = execute_unique(${nodeId}, ${inputId}, json.loads('${settings}'))
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
          result = await runPythonWithResult(`
import json
result = execute_head(${nodeId}, ${inputId}, json.loads('${settings}'))
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
      const errorResult: NodeResult = {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      }
      nodeResults.value.set(nodeId, errorResult)
      return errorResult
    }
  }

  async function executeFlow() {
    if (!pyodideStore.isReady) {
      console.error('Pyodide not ready')
      return
    }

    isExecuting.value = true
    nodeResults.value.clear()

    try {
      // Clear Python state
      await pyodideStore.runPython('clear_all()')

      // Get execution order
      const order = getExecutionOrder()

      // Execute nodes in order
      for (const nodeId of order) {
        const result = await executeNode(nodeId)
        if (!result.success) {
          console.error(`Node ${nodeId} failed:`, result.error)
          // Continue execution even if a node fails
        }
      }
    } catch (error) {
      console.error('Flow execution error:', error)
    } finally {
      isExecuting.value = false
    }
  }

  function getDefaultSettings(type: string, nodeId: number): NodeSettings {
    const base = {
      node_id: nodeId,
      is_setup: false,
      cache_results: true
    }

    switch (type) {
      case 'read_csv':
        return {
          ...base,
          file_name: '',
          has_headers: true,
          delimiter: ',',
          skip_rows: 0
        }

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
        }

      case 'select':
        return {
          ...base,
          select_input: [],
          keep_missing: false
        }

      case 'group_by':
        return {
          ...base,
          groupby_input: {
            agg_cols: []
          }
        }

      case 'join':
        return {
          ...base,
          join_input: {
            join_type: 'inner',
            join_mapping: [],
            left_suffix: '_left',
            right_suffix: '_right'
          }
        }

      case 'sort':
        return {
          ...base,
          sort_input: {
            sort_cols: []
          }
        }

      case 'with_columns':
        return {
          ...base,
          with_columns_input: {
            columns: []
          }
        }

      case 'unique':
        return {
          ...base,
          unique_input: {
            subset: [],
            keep: 'first',
            maintain_order: true
          }
        }

      case 'head':
        return {
          ...base,
          head_input: {
            n: 10
          }
        }

      default:
        return base as NodeSettings
    }
  }

  function clearFlow() {
    nodes.value.clear()
    edges.value = []
    nodeResults.value.clear()
    fileContents.value.clear()
    selectedNodeId.value = null
    nodeIdCounter.value = 0
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
    clearFlow
  }
})
