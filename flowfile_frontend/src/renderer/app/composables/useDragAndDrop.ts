// composables/useDragAndDrop.ts
// Drag and drop composable for flow canvas
import { useVueFlow, Node, Position } from "@vue-flow/core";
import { ref, watch, markRaw, nextTick } from "vue";
import type {
  NodeTemplate,
  NodeInput,
  VueFlowInput,
  NodeCopyInput,
  NodePromise,
  MultiNodeCopyValue,
  EdgeCopyValue,
  NodeConnection,
  OperationResponse,
} from "../types";
import { FlowApi } from "../api";

// Dynamic component imports using import.meta.glob for Vite compatibility
// This creates a map of all node components that can be dynamically loaded
const nodeModules = import.meta.glob("../components/nodes/node-types/elements/**/*.vue");

// Validate that parsed JSON data is a valid NodeTemplate
// This prevents unvalidated dynamic method calls from untrusted data
function isValidNodeTemplate(data: unknown): data is NodeTemplate {
  if (typeof data !== "object" || data === null) return false;
  const obj = data as Record<string, unknown>;
  return (
    typeof obj.name === "string" &&
    typeof obj.item === "string" &&
    typeof obj.input === "number" &&
    typeof obj.output === "number" &&
    typeof obj.custom_node === "boolean"
  );
}

let id = 0;

function getId(): number {
  return ++id;
}

const state = {
  draggedType: ref<string | null>(null),
  isDragOver: ref(false),
  isDragging: ref(false),
};

// Utility function to convert snake_case to TitleCase
function toTitleCase(str: string): string {
  return str
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join("");
}

// Utility function to convert snake_case to camelCase
function toCamelCase(str: string): string {
  const parts = str.split("_");
  return (
    parts[0].toLowerCase() +
    parts
      .slice(1)
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join("")
  );
}

// Validate that a string only contains safe characters for module paths
function isValidModuleName(name: string): boolean {
  return /^[a-zA-Z][a-zA-Z0-9]*$/.test(name);
}

// Component cache to avoid re-importing
const componentCache: Map<string, Promise<any>> = new Map();

/**
 * Gets a specific node template by item name
 */
export async function getNodeTemplateByItem(item: string): Promise<NodeTemplate | undefined> {
  try {
    const { default: axios } = await import("axios");
    const response = await axios.get("/node_list");
    const allNodes = response.data as NodeTemplate[];
    return allNodes.find((node) => node.item === item);
  } catch (error) {
    console.error("Failed to get node template for item:", item, error);
    return undefined;
  }
}

/**
 * Gets a Vue component for a node
 */
async function getComponent(node: NodeTemplate | string): Promise<any> {
  const nodeItem = typeof node === "string" ? node : node.item;

  if (componentCache.has(nodeItem)) {
    return componentCache.get(nodeItem)!;
  }

  const nodeTemplate = typeof node === "string" ? await getNodeTemplateByItem(node) : node;

  if (!nodeTemplate) {
    throw new Error(`Node template not found for item: ${nodeItem}`);
  }

  const formattedItemName = toTitleCase(nodeTemplate.item);
  const dirName = toCamelCase(nodeTemplate.item);

  // Use CustomNode for nodes marked as custom_node, otherwise use specific component
  const modulePath = nodeTemplate.custom_node
    ? "../components/nodes/node-types/elements/customNode/CustomNode.vue"
    : `../components/nodes/node-types/elements/${dirName}/${formattedItemName}.vue`;

  // Validate module names to prevent path traversal (only needed for non-custom nodes)
  if (
    !nodeTemplate.custom_node &&
    (!isValidModuleName(formattedItemName) || !isValidModuleName(dirName))
  ) {
    throw new Error(`Invalid module name: ${formattedItemName}`);
  }

  // Use Object.hasOwn to safely check module exists (prevents prototype pollution)
  if (!Object.hasOwn(nodeModules, modulePath)) {
    const error = new Error(`Component not found: ${formattedItemName} at ${modulePath}`);
    console.error("Failed to load component:", formattedItemName, error);
    throw error;
  }

  // Safe to access after hasOwn check
  const moduleLoader = nodeModules[modulePath];

  if (typeof moduleLoader !== "function") {
    const error = new Error(`Invalid module loader for: ${formattedItemName}`);
    console.error("Failed to load component:", formattedItemName, error);
    throw error;
  }

  const componentPromise = moduleLoader()
    .then((module: any) => {
      const component = markRaw(module.default);
      return component;
    })
    .catch((error) => {
      console.error("Failed to load component:", formattedItemName, error);
      componentCache.delete(nodeItem);
      throw error;
    });

  componentCache.set(nodeItem, componentPromise);
  return componentPromise;
}

async function getComponentRaw(item: string): Promise<any> {
  // Fetch NodeTemplate to check custom_node property
  const nodeTemplate = await getNodeTemplateByItem(item);

  if (!nodeTemplate) {
    throw new Error(`Node template not found for item: ${item}`);
  }

  const formattedItemName = toTitleCase(nodeTemplate.item);
  const dirName = toCamelCase(nodeTemplate.item);

  // Use CustomNode for nodes marked as custom_node, otherwise use specific component
  const modulePath = nodeTemplate.custom_node
    ? "../components/nodes/node-types/elements/customNode/CustomNode.vue"
    : `../components/nodes/node-types/elements/${dirName}/${formattedItemName}.vue`;

  // Validate module names to prevent path traversal (only needed for non-custom nodes)
  if (
    !nodeTemplate.custom_node &&
    (!isValidModuleName(formattedItemName) || !isValidModuleName(dirName))
  ) {
    throw new Error(`Invalid module name: ${formattedItemName}`);
  }

  // Use Object.hasOwn to safely check module exists (prevents prototype pollution)
  if (!Object.hasOwn(nodeModules, modulePath)) {
    const error = new Error(`Component not found: ${formattedItemName} at ${modulePath}`);
    console.error("Failed to load component:", formattedItemName, error);
    throw error;
  }

  // Safe to access after hasOwn check
  const moduleLoader = nodeModules[modulePath];

  if (typeof moduleLoader !== "function") {
    const error = new Error(`Invalid module loader for: ${formattedItemName}`);
    console.error("Failed to load component:", formattedItemName, error);
    throw error;
  }

  return moduleLoader()
    .then((module: any) => markRaw(module.default))
    .catch((error) => {
      console.error("Failed to load component:", formattedItemName, error);
      throw error;
    });
}

export default function useDragAndDrop() {
  const { draggedType, isDragOver, isDragging } = state;

  const { addNodes, screenToFlowCoordinate, onNodesInitialized, updateNode, addEdges, fromObject } =
    useVueFlow();

  watch(isDragging, (dragging) => {
    document.body.style.userSelect = dragging ? "none" : "";
  });

  function onDragStart(event: DragEvent, nodeTemplate: NodeTemplate) {
    if (event.dataTransfer) {
      event.dataTransfer.setData("application/vueflow", JSON.stringify(nodeTemplate));
      event.dataTransfer.effectAllowed = "move";
    }

    draggedType.value = nodeTemplate.item;
    isDragging.value = true;

    document.addEventListener("drop", onDragEnd);
  }

  function onDragOver(event: DragEvent) {
    event.preventDefault();

    if (draggedType.value) {
      isDragOver.value = true;

      if (event.dataTransfer) {
        event.dataTransfer.dropEffect = "move";
      }
    }
  }

  function onDragLeave() {
    isDragOver.value = false;
  }

  function onDragEnd() {
    isDragging.value = false;
    isDragOver.value = false;
    draggedType.value = null;
    document.removeEventListener("drop", onDragEnd);
  }

  async function createCopyNode(node: NodeCopyInput): Promise<OperationResponse | undefined> {
    try {
      const component = await getComponentRaw(node.type);
      const nodeId: number = getId();
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
      };
      const nodePromise: NodePromise = {
        node_id: nodeId,
        flow_id: node.flowId,
        node_type: node.typeSnakeCase,
        pos_x: node.posX,
        pos_y: node.posY,
        cache_results: true,
      };
      const response = await FlowApi.copyNode(
        node.nodeIdToCopyFrom,
        node.flowIdToCopyFrom,
        nodePromise,
      );

      addNodes(newNode);
      return response;
    } catch (error) {
      console.error("Error creating copy node:", error);
      return undefined;
    }
  }

  const getMaxDataId = (nodes: NodeInput[]): number => {
    return nodes.reduce((maxId, node) => {
      return node.id > maxId ? node.id : maxId;
    }, 0);
  };

  async function getNodeToAdd(node: NodeInput): Promise<Node> {
    const numberOfInputs: number = node.multi ? 1 : node.input;

    const nodeTemplate = await getNodeTemplateByItem(node.item);
    const component = await getComponent(nodeTemplate || node.item);

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
    };
    return newNode;
  }

  async function createEmptyFlow() {
    const emptyFlow = {
      nodes: [],
      edges: [],
      position: [0, 0] as [number, number],
      zoom: 1,
      viewport: { x: 0, y: 0, zoom: 1 },
    };
    await fromObject(emptyFlow);
    await nextTick();
  }

  async function importFlow(flowData: VueFlowInput) {
    await createEmptyFlow();
    const allNodes = await Promise.all(flowData.node_inputs.map((node) => getNodeToAdd(node)));

    addNodes(allNodes);
    id = getMaxDataId(flowData.node_inputs);
    addEdges(flowData.node_edges);
  }

  async function onDrop(event: DragEvent, flowId: number): Promise<OperationResponse | undefined> {
    const position = screenToFlowCoordinate({
      x: event.clientX,
      y: event.clientY,
    });
    if (!event.dataTransfer) return undefined;

    // Parse and validate the drag data to prevent unvalidated dynamic method calls
    const rawData = event.dataTransfer.getData("application/vueflow");
    if (!rawData) return undefined;

    let parsedData: unknown;
    try {
      parsedData = JSON.parse(rawData);
    } catch {
      console.error("Invalid JSON in drag data");
      return undefined;
    }

    if (!isValidNodeTemplate(parsedData)) {
      console.error("Invalid node template data in drag event");
      return undefined;
    }

    const nodeData: NodeTemplate = parsedData;
    const nodeId = getId();

    try {
      const component = await getComponent(nodeData);
      const numberOfInputs: number = nodeData.multi ? 1 : nodeData.input;

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
      };

      const { off } = onNodesInitialized(() => {
        updateNode(String(nodeId), (node) => ({
          position: {
            x: node.position.x - (node.dimensions?.width || 0) / 55,
            y: node.position.y - (node.dimensions?.height || 0) / 55,
          },
        }));

        off();
      });

      const response = await FlowApi.insertNode(
        flowId,
        nodeId,
        nodeData.item,
        position.x,
        position.y,
      );
      addNodes(newNode);
      return response;
    } catch (error) {
      console.error("Error importing component for:", nodeData.item, error);
      return undefined;
    }
  }

  /**
   * Creates multiple copied nodes with their connections preserved
   * Returns the last OperationResponse for history state updates
   */
  async function createMultiCopyNodes(
    multiCopyValue: MultiNodeCopyValue,
    baseX: number,
    baseY: number,
    flowId: number,
  ): Promise<OperationResponse | undefined> {
    // Map old node IDs to new node IDs
    const nodeIdMapping: Map<number, number> = new Map();

    // Pre-assign all node IDs and calculate positions using relative positions
    const nodeInfos: Array<{
      node: (typeof multiCopyValue.nodes)[0];
      newNodeId: number;
      offsetX: number;
      offsetY: number;
    }> = [];

    for (let i = 0; i < multiCopyValue.nodes.length; i++) {
      const node = multiCopyValue.nodes[i];
      const newNodeId = getId();
      nodeIdMapping.set(node.nodeIdToCopyFrom, newNodeId);

      // Use relative positions if available, otherwise fall back to staggered layout
      const offsetX = baseX + (node.relativeX ?? (i % 3) * 200);
      const offsetY = baseY + (node.relativeY ?? Math.floor(i / 3) * 150);

      nodeInfos.push({ node, newNodeId, offsetX, offsetY });
    }

    // First pass: Create all UI nodes and wait for components to load
    const uiNodePromises = nodeInfos.map(async ({ node, newNodeId, offsetX, offsetY }) => {
      const component = await getComponentRaw(node.type);
      const newNode: Node = {
        id: String(newNodeId),
        type: "custom-node",
        position: {
          x: offsetX,
          y: offsetY,
        },
        data: {
          id: newNodeId,
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
      };
      addNodes(newNode);
      return { node, newNodeId, offsetX, offsetY };
    });

    // Wait for all UI nodes to be created
    const createdNodes = await Promise.all(uiNodePromises);

    // Second pass: Copy all nodes in the backend and wait for completion
    let lastResponse: OperationResponse | undefined;
    const backendCopyPromises = createdNodes.map(async ({ node, newNodeId, offsetX, offsetY }) => {
      const nodePromise: NodePromise = {
        node_id: newNodeId,
        flow_id: flowId,
        node_type: node.typeSnakeCase,
        pos_x: offsetX,
        pos_y: offsetY,
        cache_results: true,
      };
      return FlowApi.copyNode(node.nodeIdToCopyFrom, multiCopyValue.flowIdToCopyFrom, nodePromise);
    });

    // Wait for ALL backend copy operations to complete
    const copyResponses = await Promise.all(backendCopyPromises);
    if (copyResponses.length > 0) {
      lastResponse = copyResponses[copyResponses.length - 1];
    }

    // Wait for Vue to update
    await nextTick();

    // Third pass: Create connections between the new nodes (after all nodes exist)
    const connectionPromises = multiCopyValue.edges.map(async (edge) => {
      const newSourceId = nodeIdMapping.get(edge.sourceNodeId);
      const newTargetId = nodeIdMapping.get(edge.targetNodeId);

      if (newSourceId !== undefined && newTargetId !== undefined) {
        // Create the edge in the UI
        const newEdge = {
          id: `e${newSourceId}-${newTargetId}-${edge.sourceHandle}-${edge.targetHandle}`,
          source: String(newSourceId),
          target: String(newTargetId),
          sourceHandle: edge.sourceHandle,
          targetHandle: edge.targetHandle,
        };
        addEdges([newEdge]);

        // Create the connection in the backend
        const nodeConnection: NodeConnection = {
          input_connection: {
            node_id: newTargetId,
            connection_class: edge.targetHandle as any,
          },
          output_connection: {
            node_id: newSourceId,
            connection_class: edge.sourceHandle as any,
          },
        };
        return FlowApi.connectNode(flowId, nodeConnection);
      }
      return undefined;
    });

    // Wait for all connections to be created
    const connectionResponses = await Promise.all(connectionPromises);
    // Return the last successful connection response, or the last copy response
    const validConnectionResponses = connectionResponses.filter(
      (r): r is OperationResponse => r !== undefined,
    );
    if (validConnectionResponses.length > 0) {
      lastResponse = validConnectionResponses[validConnectionResponses.length - 1];
    }

    return lastResponse;
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
    createMultiCopyNodes,
    importFlow,
  };
}
