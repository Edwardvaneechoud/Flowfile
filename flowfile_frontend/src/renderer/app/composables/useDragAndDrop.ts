// composables/useDragAndDrop.ts
// Drag and drop composable for flow canvas
import { useVueFlow, Node, Position } from "@vue-flow/core"
import { ref, watch, markRaw, nextTick } from "vue"
import type { NodeTemplate, NodeInput, VueFlowInput, NodeCopyInput, NodePromise } from "../types"
import { FlowApi } from "../api"

// Dynamic component imports using import.meta.glob for Vite compatibility
// This creates a map of all node components that can be dynamically loaded
const nodeModules = import.meta.glob('../features/designer/nodes/elements/**/*.vue')

let id = 0

function getId(): number {
  return ++id
}

const state = {
  draggedType: ref<string | null>(null),
  isDragOver: ref(false),
  isDragging: ref(false),
}

// Utility function to convert snake_case to TitleCase
function toTitleCase(str: string): string {
  return str
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join('')
}

// Utility function to convert snake_case to camelCase
function toCamelCase(str: string): string {
  const parts = str.split('_')
  return parts[0].toLowerCase() + parts.slice(1)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join('')
}

// Validate that a string only contains safe characters for module paths
function isValidModuleName(name: string): boolean {
  return /^[a-zA-Z][a-zA-Z0-9]*$/.test(name)
}

// Cache for node templates - persists for the entire session
let nodeTemplatesCache: NodeTemplate[] | null = null
let cachePromise: Promise<NodeTemplate[]> | null = null

// Component cache to avoid re-importing
const componentCache: Map<string, Promise<any>> = new Map()

/**
 * Fetches node templates with caching to minimize API calls
 */
async function fetchNodeTemplates(): Promise<NodeTemplate[]> {
  if (nodeTemplatesCache !== null) {
    return nodeTemplatesCache
  }

  if (cachePromise !== null) {
    return cachePromise
  }

  const { default: axios } = await import('axios')

  cachePromise = axios.get('/node_list')
    .then(response => {
      const allNodes = response.data as NodeTemplate[]
      nodeTemplatesCache = allNodes
      return nodeTemplatesCache as NodeTemplate[]
    })
    .catch(error => {
      console.error("Failed to fetch node templates:", error)
      cachePromise = null
      throw error
    })

  return cachePromise
}

/**
 * Gets a specific node template by item name
 */
export async function getNodeTemplateByItem(item: string): Promise<NodeTemplate | undefined> {
  try {
    const { default: axios } = await import('axios')
    const response = await axios.get('/node_list')
    const allNodes = response.data as NodeTemplate[]
    return allNodes.find(node => node.item === item)
  } catch (error) {
    console.error("Failed to get node template for item:", item, error)
    return undefined
  }
}

/**
 * Gets a Vue component for a node
 */
async function getComponent(node: NodeTemplate | string): Promise<any> {
  const nodeItem = typeof node === 'string' ? node : node.item

  if (componentCache.has(nodeItem)) {
    return componentCache.get(nodeItem)!
  }

  const nodeTemplate = typeof node === 'string'
    ? await getNodeTemplateByItem(node)
    : node

  if (!nodeTemplate) {
    throw new Error(`Node template not found for item: ${nodeItem}`)
  }

  const formattedItemName = toTitleCase(nodeTemplate.item)
  const dirName = toCamelCase(nodeTemplate.item)

  // Use CustomNode for nodes marked as custom_node, otherwise use specific component
  const modulePath = nodeTemplate.custom_node
    ? '../features/designer/nodes/elements/customNode/CustomNode.vue'
    : `../features/designer/nodes/elements/${dirName}/${formattedItemName}.vue`

  console.log("Loading component:", formattedItemName, "custom_node:", nodeTemplate.custom_node)

  // Validate module names to prevent path traversal (only needed for non-custom nodes)
  if (!nodeTemplate.custom_node && (!isValidModuleName(formattedItemName) || !isValidModuleName(dirName))) {
    throw new Error(`Invalid module name: ${formattedItemName}`)
  }

  const moduleLoader = nodeModules[modulePath]

  if (!moduleLoader || typeof moduleLoader !== 'function') {
    const error = new Error(`Component not found: ${formattedItemName} at ${modulePath}`)
    console.error("Failed to load component:", formattedItemName, error)
    console.log('Available modules:', Object.keys(nodeModules))
    throw error
  }

  const validatedLoader = moduleLoader
  const componentPromise = validatedLoader()
    .then((module: any) => {
      const component = markRaw(module.default)
      return component
    })
    .catch(error => {
      console.error("Failed to load component:", formattedItemName, error)
      componentCache.delete(nodeItem)
      throw error
    })

  componentCache.set(nodeItem, componentPromise)
  return componentPromise
}

async function getComponentRaw(item: string): Promise<any> {
  // Fetch NodeTemplate to check custom_node property
  const nodeTemplate = await getNodeTemplateByItem(item)

  if (!nodeTemplate) {
    throw new Error(`Node template not found for item: ${item}`)
  }

  const formattedItemName = toTitleCase(nodeTemplate.item)
  const dirName = toCamelCase(nodeTemplate.item)

  // Use CustomNode for nodes marked as custom_node, otherwise use specific component
  const modulePath = nodeTemplate.custom_node
    ? '../features/designer/nodes/elements/customNode/CustomNode.vue'
    : `../features/designer/nodes/elements/${dirName}/${formattedItemName}.vue`

  console.log("Loading component:", formattedItemName, "custom_node:", nodeTemplate.custom_node)

  // Validate module names to prevent path traversal (only needed for non-custom nodes)
  if (!nodeTemplate.custom_node && (!isValidModuleName(formattedItemName) || !isValidModuleName(dirName))) {
    throw new Error(`Invalid module name: ${formattedItemName}`)
  }

  const moduleLoader = nodeModules[modulePath]

  if (!moduleLoader || typeof moduleLoader !== 'function') {
    const error = new Error(`Component not found: ${formattedItemName} at ${modulePath}`)
    console.error("Failed to load component:", formattedItemName, error)
    console.log('Available modules:', Object.keys(nodeModules))
    throw error
  }

  const validatedLoader = moduleLoader
  return validatedLoader()
    .then((module: any) => markRaw(module.default))
    .catch(error => {
      console.error("Failed to load component:", formattedItemName, error)
      throw error
    })
}

export default function useDragAndDrop() {
  const { draggedType, isDragOver, isDragging } = state

  const { addNodes, screenToFlowCoordinate, onNodesInitialized, updateNode, addEdges, fromObject } = useVueFlow()

  watch(isDragging, (dragging) => {
    document.body.style.userSelect = dragging ? "none" : ""
  })

  function onDragStart(event: DragEvent, nodeTemplate: NodeTemplate) {
    if (event.dataTransfer) {
      event.dataTransfer.setData(
        "application/vueflow",
        JSON.stringify(nodeTemplate),
      )
      event.dataTransfer.effectAllowed = "move"
    }

    draggedType.value = nodeTemplate.item
    isDragging.value = true

    document.addEventListener("drop", onDragEnd)
  }

  function onDragOver(event: DragEvent) {
    event.preventDefault()

    if (draggedType.value) {
      isDragOver.value = true

      if (event.dataTransfer) {
        event.dataTransfer.dropEffect = "move"
      }
    }
  }

  function onDragLeave() {
    isDragOver.value = false
  }

  function onDragEnd() {
    isDragging.value = false
    isDragOver.value = false
    draggedType.value = null
    document.removeEventListener("drop", onDragEnd)
  }

  function createCopyNode(node: NodeCopyInput) {
    getComponentRaw(node.type).then((component) => {
      let nodeId: number = getId()
      const newNode: Node = {
        id: String(nodeId),
        type: "custom-node",
        position: {
          x: node.posX,
          y: node.posY,
        },
        data: {
          id: nodeId,
          label: node.label,
          component: markRaw(component),
          inputs: Array.from({ length: node.numberOfInputs }, (_, i) => ({
            id: `input-${i}`,
            position: Position.Left,
          })),
          outputs: Array.from({ length: node.numberOfOutputs }, (_, i) => ({
            id: `output-${i}`,
            position: Position.Right,
          })),
          nodeTemplate: node.nodeTemplate,
        },
      }
      const nodePromise: NodePromise = {
        node_id: nodeId,
        flow_id: node.flowId,
        node_type: node.typeSnakeCase,
        pos_x: node.posX,
        pos_y: node.posY,
        cache_results: true,
      }
      FlowApi.copyNode(node.nodeIdToCopyFrom, node.flowIdToCopyFrom, nodePromise)

      addNodes(newNode)
    })
  }

  const getMaxDataId = (nodes: NodeInput[]): number => {
    return nodes.reduce((maxId, node) => {
      return node.id > maxId ? node.id : maxId
    }, 0)
  }

  async function getNodeToAdd(node: NodeInput): Promise<Node> {
    const numberOfInputs: number = (node.multi) ? 1 : node.input

    const nodeTemplate = await getNodeTemplateByItem(node.item)
    const component = await getComponent(nodeTemplate || node.item)

    const newNode: Node = {
      id: String(node.id),
      type: "custom-node",
      position: {
        x: node.pos_x,
        y: node.pos_y,
      },
      data: {
        id: node.id,
        label: node.name,
        component: markRaw(component),
        inputs: Array.from({ length: numberOfInputs }, (_, i) => ({
          id: `input-${i}`,
          position: Position.Left,
        })),
        outputs: Array.from({ length: node.output }, (_, i) => ({
          id: `output-${i}`,
          position: Position.Right,
        })),
        nodeTemplate: nodeTemplate,
      },
    }
    return newNode
  }

  async function createEmptyFlow() {
    const emptyFlow = { "nodes": [], "edges": [], "position": [0, 0] as [number, number], "zoom": 1, "viewport": { "x": 0, "y": 0, "zoom": 1 } }
    await fromObject(emptyFlow)
    await nextTick()
  }

  async function importFlow(flowData: VueFlowInput) {
    await createEmptyFlow()
    const allNodes = await Promise.all(
      flowData.node_inputs.map((node) => getNodeToAdd(node))
    )

    addNodes(allNodes)
    id = getMaxDataId(flowData.node_inputs)
    addEdges(flowData.node_edges)
  }

  function onDrop(event: DragEvent, flowId: number): Node | undefined {
    const position = screenToFlowCoordinate({
      x: event.clientX,
      y: event.clientY,
    })
    if (!event.dataTransfer) return

    const nodeData: NodeTemplate = JSON.parse(
      event.dataTransfer.getData("application/vueflow"),
    )
    const nodeId = getId()

    getComponent(nodeData)
      .then((component) => {
        const numberOfInputs: number = (nodeData.multi) ? 1 : nodeData.input

        const newNode: Node = {
          id: String(nodeId),
          type: "custom-node",
          position,
          data: {
            id: nodeId,
            label: nodeData.name,
            component: markRaw(component),
            inputs: Array.from({ length: numberOfInputs }, (_, i) => ({
              id: `input-${i}`,
              position: Position.Left,
            })),
            outputs: Array.from({ length: nodeData.output }, (_, i) => ({
              id: `output-${i}`,
              position: Position.Right,
            })),
            nodeTemplate: nodeData,
          },
        }


        const { off } = onNodesInitialized(() => {
          updateNode(String(nodeId), (node) => ({
            position: {
              x: node.position.x - (node.dimensions?.width || 0) / 55,
              y: node.position.y - (node.dimensions?.height || 0) / 55,
            },
          }))

          off()
        })

        FlowApi.insertNode(flowId, nodeId, nodeData.item)
        addNodes(newNode)
      })
      .catch((error) => {
        console.error("Error importing component for:", nodeData.item, error)
      })
  }

  return {
    draggedType,
    isDragOver,
    isDragging,
    onDragStart,
    onDragLeave,
    onDragOver,
    onDrop,
    createCopyNode,
    importFlow,
  }
}
