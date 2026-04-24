// composables/useDragAndDrop.ts
// Drag and drop composable for flow canvas
import { useVueFlow, Node, Position, Edge } from "@vue-flow/core";
import { ref, watch, markRaw, nextTick } from "vue";
import { ElMessage } from "element-plus";
import type {
  NodeTemplate,
  NodeInput,
  NodeHandle,
  VueFlowInput,
  NodeCopyInput,
  NodePromise,
  MultiNodeCopyValue,
  NodeConnection,
  OperationResponse,
} from "../types";
import { FlowApi, NodeApi } from "../api";
import { useEditorStore } from "../stores/editor-store";
import { parseTabularText, inferColumnDataType } from "../utils/clipboardUtils";
import { DEFAULT_OUTPUT_HANDLE, outputHandle, outputLabel } from "../utils/outputHandle";

const EDGE_DROP_CLASS = "edge-drop-target";
let hoveredEdgeId: string | null = null;

// Edge ids whose UI removal should NOT trigger a backend deleteConnection call.
// Used during drag-to-insert: we delete the backend connection ourselves first,
// then call `removeEdges` for the UI update — without this set, Canvas.vue's
// `@edges-change` handler would issue a second, failing deleteConnection.
export const suppressedEdgeRemovals = new Set<string>();

function markHoveredEdge(nextId: string | null) {
  if (hoveredEdgeId === nextId) return;
  if (hoveredEdgeId) {
    document
      .querySelector(`.vue-flow__edge[data-id="${CSS.escape(hoveredEdgeId)}"]`)
      ?.classList.remove(EDGE_DROP_CLASS);
  }
  if (nextId) {
    document
      .querySelector(`.vue-flow__edge[data-id="${CSS.escape(nextId)}"]`)
      ?.classList.add(EDGE_DROP_CLASS);
  }
  hoveredEdgeId = nextId;
}

function detectEdgeUnderPointer(clientX: number, clientY: number): string | null {
  // elementsFromPoint (plural) returns the full z-stack, so we can find an
  // edge even when a dragged node is painted on top and obscures it.
  const stack = document.elementsFromPoint(clientX, clientY);
  for (const el of stack) {
    const edgeEl = (el as Element).closest(".vue-flow__edge");
    if (edgeEl) return edgeEl.getAttribute("data-id");
  }
  return null;
}

function buildConnection(
  sourceId: number,
  sourceHandle: string,
  targetId: number,
  targetHandle: string,
): NodeConnection {
  return {
    input_connection: {
      node_id: targetId,
      connection_class: targetHandle as NodeConnection["input_connection"]["connection_class"],
    },
    output_connection: {
      node_id: sourceId,
      connection_class: sourceHandle as NodeConnection["output_connection"]["connection_class"],
    },
  };
}

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

// Build the outputs array for a custom-node. When a node declares more than one
// output, each handle gets a compact letter id (A, B, …) for the canvas and the
// user-defined name (when available) as a hover tooltip via the `title` attr.
// For nodes whose output count is user-configurable (e.g. random_split), the
// effective count is whichever is larger: the template's static count or the
// number of saved output names.
export function buildOutputHandles(outputCount: number, names?: string[]): NodeHandle[] {
  const count = Math.max(outputCount, names?.length ?? 0);
  const multi = count > 1;
  return Array.from({ length: count }, (_, i) => ({
    id: outputHandle(i),
    position: Position.Right,
    label: multi ? outputLabel(i) : undefined,
    title: multi ? names?.[i] : undefined,
  }));
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
    console.log("Available modules:", Object.keys(nodeModules));
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

  console.log("Loading component:", formattedItemName, "custom_node:", nodeTemplate.custom_node);

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
    console.log("Available modules:", Object.keys(nodeModules));
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

  const {
    addNodes,
    screenToFlowCoordinate,
    onNodesInitialized,
    updateNode,
    addEdges,
    removeEdges,
    findEdge,
    fromObject,
  } = useVueFlow();

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
      markHoveredEdge(detectEdgeUnderPointer(event.clientX, event.clientY));
    }
  }

  function onDragLeave() {
    isDragOver.value = false;
    markHoveredEdge(null);
  }

  function onDragEnd() {
    isDragging.value = false;
    isDragOver.value = false;
    draggedType.value = null;
    markHoveredEdge(null);
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
          outputs: buildOutputHandles(
            node.numberOfOutputs,
            node.nodeTemplate?.output_names ?? undefined,
          ),
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
        nodeReference: node.node_reference,
        inputs: Array.from({ length: numberOfInputs }, (_, i) => ({
          id: `input-${i}`,
          position: Position.Left,
        })),
        outputs: buildOutputHandles(node.output, node.output_names ?? undefined),
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

    // Add labels to edges from source node output handles, node_reference, or df_{nodeId} default
    const editorStore = useEditorStore();
    const edgesWithLabels = flowData.node_edges.map((edge) => {
      if (!editorStore.showEdgeLabels) return edge;
      const sourceNode = allNodes.find((n) => n.id === edge.source);
      if (sourceNode?.data?.outputs) {
        const output = (sourceNode.data.outputs as NodeHandle[]).find(
          (o) => o.id === edge.sourceHandle,
        );
        if (output?.label) {
          return { ...edge, label: output.label };
        }
      }
      if (sourceNode?.data?.nodeReference) {
        return { ...edge, label: sourceNode.data.nodeReference };
      }
      return { ...edge, label: `df_${sourceNode?.data?.id ?? edge.source}` };
    });

    addEdges(edgesWithLabels);
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

    // Snapshot which edge (if any) the user was hovering at drop time, then clear
    // the hover cue — we'll consume the snapshot below.
    const droppedOnEdgeId = hoveredEdgeId;
    markHoveredEdge(null);

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
          outputs: buildOutputHandles(nodeData.output, nodeData.output_names ?? undefined),
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

      // `multi` nodes render a single input handle that accepts many sources,
      // so for splice purposes they behave like a 1-input node regardless of
      // the backend's `input` count (e.g. polars_code/python_script have input=10).
      const effectiveInputCount = nodeData.multi ? 1 : nodeData.input;
      if (droppedOnEdgeId && effectiveInputCount === 1 && nodeData.output >= 1) {
        const insertResponse = await insertNodeOnEdge(flowId, nodeId, nodeData, droppedOnEdgeId);
        if (insertResponse) return insertResponse;
      }
      return response;
    } catch (error) {
      console.error("Error importing component for:", nodeData.item, error);
      return undefined;
    }
  }

  /**
   * Splice a freshly-created node into an existing edge: A -> B becomes
   * A -> new -> B. The new node has already been created on both UI and
   * backend by onDrop; this only reshuffles the edges.
   *
   * Returns the last OperationResponse on success, or undefined on failure
   * (after a best-effort rollback — the original edge is re-added if
   * possible so the user isn't stranded with a disconnected graph).
   */
  async function insertNodeOnEdge(
    flowId: number,
    newNodeId: number,
    nodeData: NodeTemplate,
    edgeId: string,
  ): Promise<OperationResponse | undefined> {
    const edge = findEdge(edgeId);
    if (!edge) return undefined;

    const sourceId = parseInt(edge.source, 10);
    const targetId = parseInt(edge.target, 10);
    const sourceHandle = edge.sourceHandle ?? DEFAULT_OUTPUT_HANDLE;
    const targetHandle = edge.targetHandle ?? "input-0";
    const newOutputHandle = outputHandle(0);
    const newInputHandle = "input-0";

    const oldConnection = buildConnection(sourceId, sourceHandle, targetId, targetHandle);
    const upstream = buildConnection(sourceId, sourceHandle, newNodeId, newInputHandle);
    const downstream = buildConnection(newNodeId, newOutputHandle, targetId, targetHandle);

    // Keep a snapshot of the original edge so we can rehydrate the UI on rollback.
    const originalEdge: Edge = {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourceHandle,
      targetHandle: edge.targetHandle,
      ...(edge.label ? { label: edge.label } : {}),
    };

    let stage: "none" | "deleted-old" | "added-upstream" = "none";
    try {
      await FlowApi.deleteConnection(flowId, oldConnection);
      stage = "deleted-old";
      // Backend already knows the edge is gone; tell handleEdgeChange to skip it.
      suppressedEdgeRemovals.add(edge.id);
      removeEdges([edge.id]);

      await FlowApi.connectNode(flowId, upstream);
      stage = "added-upstream";

      const lastResponse = await FlowApi.connectNode(flowId, downstream);

      addEdges([
        {
          id: `e${sourceId}-${newNodeId}-${sourceHandle}-${newInputHandle}`,
          source: String(sourceId),
          target: String(newNodeId),
          sourceHandle,
          targetHandle: newInputHandle,
          ...(edge.label ? { label: edge.label } : {}),
        },
        {
          id: `e${newNodeId}-${targetId}-${newOutputHandle}-${targetHandle}`,
          source: String(newNodeId),
          target: String(targetId),
          sourceHandle: newOutputHandle,
          targetHandle,
        },
      ]);

      return lastResponse;
    } catch (error) {
      console.error("Insert-on-edge failed, attempting rollback:", error);
      if (stage === "added-upstream") {
        await FlowApi.deleteConnection(flowId, upstream).catch(() => undefined);
      }
      if (stage !== "none") {
        await FlowApi.connectNode(flowId, oldConnection).catch(() => undefined);
        addEdges([originalEdge]);
      }
      ElMessage.error(`Could not insert ${nodeData.name} onto the edge`);
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
          outputs: buildOutputHandles(
            node.numberOfOutputs,
            node.nodeTemplate?.output_names ?? undefined,
          ),
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
        // Look up the source node's output label for the edge
        const sourceNodeInfo = multiCopyValue.nodes.find(
          (n) => n.nodeIdToCopyFrom === edge.sourceNodeId,
        );
        const outputIndex = parseInt(edge.sourceHandle.replace("output-", ""), 10);
        const outputLabel =
          sourceNodeInfo?.nodeTemplate?.output_names &&
          sourceNodeInfo.nodeTemplate.output_names.length > 1
            ? sourceNodeInfo.nodeTemplate.output_names[outputIndex]
            : undefined;

        // Create the edge in the UI
        const newEdge = {
          id: `e${newSourceId}-${newTargetId}-${edge.sourceHandle}-${edge.targetHandle}`,
          source: String(newSourceId),
          target: String(newTargetId),
          sourceHandle: edge.sourceHandle,
          targetHandle: edge.targetHandle,
          ...(outputLabel ? { label: outputLabel } : {}),
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

  /**
   * Creates a manual_input node from clipboard tabular data pasted on the canvas.
   * Returns the OperationResponse if successful, undefined otherwise.
   */
  async function createManualInputFromClipboard(
    flowId: number,
    x: number,
    y: number,
  ): Promise<OperationResponse | undefined> {
    let clipboardText: string;
    try {
      clipboardText = await navigator.clipboard.readText();
    } catch {
      return undefined;
    }

    const parsed = parseTabularText(clipboardText);
    if (!parsed || parsed.length < 2) return undefined;

    // First row is headers, rest is data
    const headers = parsed[0];
    const dataRows = parsed.slice(1);

    // Build column-major RawDataFormat with inferred types
    const data: unknown[][] = headers.map((_, colIdx) => dataRows.map((row) => row[colIdx] ?? ""));
    const columns = headers.map((name, colIdx) => ({
      name: name || `Column ${colIdx + 1}`,
      data_type: inferColumnDataType(data[colIdx]),
    }));

    const nodeId = getId();

    try {
      const [component, nodeTemplate] = await Promise.all([
        getComponent("manual_input"),
        getNodeTemplateByItem("manual_input"),
      ]);

      const response = await FlowApi.insertNode(flowId, nodeId, "manual_input", x, y);

      const newNode: Node = {
        id: String(nodeId),
        type: "custom-node",
        position: { x, y },
        data: {
          id: nodeId,
          label: "Manual Input",
          component: markRaw(component),
          inputs: [],
          outputs: [{ id: DEFAULT_OUTPUT_HANDLE, position: Position.Right }],
          nodeTemplate,
        },
      };
      addNodes(newNode);

      // Set the data on the newly created node
      await NodeApi.updateSettingsDirectly("manual_input", {
        flow_id: flowId,
        node_id: nodeId,
        pos_x: x,
        pos_y: y,
        cache_results: false,
        is_setup: true,
        raw_data_format: { columns, data },
      });

      return response;
    } catch (error) {
      console.error("Error creating manual input from clipboard:", error);
      return undefined;
    }
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
    createManualInputFromClipboard,
    importFlow,
    insertNodeOnEdge,
  };
}

export { markHoveredEdge, detectEdgeUnderPointer };
