<script setup lang="ts">
import {
  ref,
  markRaw,
  onMounted,
  onUnmounted,
  defineExpose,
  nextTick,
  defineEmits,
  provide,
  watch,
} from "vue";
import {
  VueFlow,
  NodeTypesObject,
  NodeComponent,
  EdgeComponent,
  Node,
  useVueFlow,
  ConnectionMode,
} from "@vue-flow/core";
import { MiniMap } from "@vue-flow/minimap";

import CustomNode from "../../components/nodes/NodeWrapper.vue";
import DeletableEdge from "./DeletableEdge.vue";
import useDragAndDrop from "./useDnD";
import {
  suppressedEdgeRemovals,
  markHoveredEdge,
  detectEdgeUnderPointer,
} from "../../composables/useDragAndDrop";
import CodeGenerator from "./CodeGenerator/CodeGenerator.vue";
import NodeList from "./NodeList.vue";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import { useFlowStore } from "../../stores/flow-store";
import NodeSettingsDrawer from "./NodeSettingsDrawer.vue";
import {
  getFlowData,
  deleteConnection,
  deleteNode,
  connectNode,
  NodeConnection,
} from "./backendInterface";
import { FlowApi } from "../../api";
import { DEFAULT_OUTPUT_HANDLE } from "../../utils/outputHandle";
import { snapshotClipboard } from "../../utils/clipboardUtils";
import DraggableItem from "../../components/common/DraggableItem/DraggableItem.vue";
import FlowParametersPanel from "../../components/layout/FlowParametersPanel/FlowParametersPanel.vue";
import layoutControls from "../../components/common/DraggableItem/layoutControls.vue";
import { useItemStore } from "../../components/common/DraggableItem/stateStore";
import DataPreview from "../../features/designer/dataPreview.vue";
import FlowResults from "../../features/designer/editor/results.vue";
import LogViewer from "./LogViewer/LogViewer.vue";
import ContextMenu from "./ContextMenu.vue";
import UndoRedoControls from "./UndoRedoControls.vue";
import {
  NodeCopyInput,
  NodeCopyValue,
  MultiNodeCopyValue,
  EdgeCopyValue,
  ContextMenuAction,
  CursorPosition,
} from "./types";
import type { NodeHandle, NodeTemplate } from "../../types/flow.types";
import type { Connection } from "@vue-flow/core";
import { applyStandardLayout } from "./editorLayoutInterface";
import { ElMessage } from "element-plus";

/** Typed subset of VueFlow node data used for edge label computation. */
interface FlowNodeData {
  id?: number;
  nodeReference?: string;
  outputs?: NodeHandle[];
}

const itemStore = useItemStore();
const availableHeight = ref(0);
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const flowStore = useFlowStore();
const rawCustomNode = markRaw(CustomNode);
const rawDeletableEdge = markRaw(DeletableEdge);
const { updateEdge, addEdges, fitView, screenToFlowCoordinate, addSelectedNodes } = useVueFlow();
const vueFlow = ref<InstanceType<typeof VueFlow>>();
const nodeTypes: NodeTypesObject = {
  "custom-node": rawCustomNode as NodeComponent,
};
const edgeTypes = {
  default: rawDeletableEdge as EdgeComponent,
};
const hoveredEdgeId = ref<string | null>(null);
provide("hoveredEdgeId", hoveredEdgeId);

function onEdgeMouseEnter({ edge }: { edge: { id: string } }) {
  hoveredEdgeId.value = edge.id;
}

function onEdgeMouseLeave() {
  hoveredEdgeId.value = null;
}

/**
 * Dragging an existing canvas node onto an edge should splice it in the
 * same way a palette-dropped node does. We reuse the edge hit-test helper
 * from the composable and, on drop, call insertNodeOnEdge — the node is
 * already on both the UI and backend, so only the edges need reshuffling.
 */
let nodeDragInsertCandidate: string | null = null;

function onNodeDrag({ event, node }: { event: MouseEvent | TouchEvent; node: Node }) {
  const template = (node.data as { nodeTemplate?: NodeTemplate } | undefined)?.nodeTemplate;
  // `multi` nodes render one input handle that accepts many sources, so they
  // qualify as 1-input for splice purposes even though template.input is high.
  const effectiveInputCount = template?.multi ? 1 : (template?.input ?? 0);
  if (!template || effectiveInputCount !== 1 || template.output < 1) {
    markHoveredEdge(null);
    nodeDragInsertCandidate = null;
    return;
  }
  // Splicing via node-drag only applies to unconnected nodes — otherwise the
  // new edges could clash with existing ones or close a cycle.
  const nodeAlreadyConnected = instance.getEdges.value.some(
    (e) => e.source === node.id || e.target === node.id,
  );
  if (nodeAlreadyConnected) {
    markHoveredEdge(null);
    nodeDragInsertCandidate = null;
    return;
  }
  const evt = event as MouseEvent;
  const edgeId = detectEdgeUnderPointer(evt.clientX, evt.clientY);
  markHoveredEdge(edgeId);
  nodeDragInsertCandidate = edgeId;
}

async function onNodeDragStop({ node }: { node: Node }) {
  const edgeId = nodeDragInsertCandidate;
  nodeDragInsertCandidate = null;
  markHoveredEdge(null);
  if (!edgeId) return;
  const template = (node.data as { nodeTemplate?: NodeTemplate } | undefined)?.nodeTemplate;
  if (!template) return;
  const response = await insertNodeOnEdge(flowStore.flowId, Number(node.id), template, edgeId);
  if (response?.history) {
    flowStore.updateHistoryState(response.history);
  }
}
const nodes = ref<Node[]>([]);
const edges = ref([]);
const instance = useVueFlow();
const showTablePreview = ref(false);
const mainContainerRef = ref<HTMLElement | null>(null);
const {
  onDrop,
  onDragOver,
  onDragStart,
  importFlow,
  createCopyNode,
  createMultiCopyNodes,
  createManualInputFromClipboard,
  insertNodeOnEdge,
} = useDragAndDrop();
const dataPreview = ref<InstanceType<typeof DataPreview>>();
const tablePreviewHeight = ref(0);
const nodeSettingsHeight = ref(0);
const selectedNodeIdInTable = ref(0);
const showContextMenu = ref(false);
const clickedPosition = ref<CursorPosition>({ x: 0, y: 0 });
const contextMenuTarget = ref({ type: "pane", id: "" });
const emit = defineEmits<{
  (e: "save", flowId: number): void;
  (e: "run", flowId: number): void;
  (e: "new"): void;
}>();

interface NodeChange {
  id: string;
  type: "remove" | "add" | "update";
}

interface EdgeChange {
  id: string;
  source: string;
  target: string;
  sourceHandle:
    | "output-0"
    | "output-1"
    | "output-2"
    | "output-3"
    | "output-4"
    | "output-5"
    | "output-6"
    | "output-7"
    | "output-8"
    | "output-9";
  targetHandle:
    | "input-0"
    | "input-1"
    | "input-2"
    | "input-3"
    | "input-4"
    | "input-5"
    | "input-6"
    | "input-7"
    | "input-8"
    | "input-9";
  type: "remove" | "add" | "update";
}

const handleCanvasClick = (event: any | PointerEvent) => {
  showTablePreview.value = false;
  nodeStore.nodeId = -1;
  editorStore.activeDrawerComponent = null;
  nodeStore.hideLogViewer();
  clickedPosition.value = {
    x: event.x,
    y: event.y,
  };
  // Clear any browser text selection when clicking on canvas
  window.getSelection()?.removeAllRanges();
};

const handleNodeSettingsClose = (event: any | PointerEvent) => {
  nodeStore.nodeId = -1;
  editorStore.activeDrawerComponent = null;
  clickedPosition.value = {
    x: event.x,
    y: event.y,
  };
};

const toggleShowTablePreview = () => {
  showTablePreview.value = !showTablePreview.value;
};

function onEdgeUpdate({ edge, connection }: { edge: any; connection: any }) {
  updateEdge(edge, connection);
}

const loadFlow = async () => {
  const vueFlowInput = await getFlowData(flowStore.flowId);
  await nextTick();
  await importFlow(vueFlowInput);
  await nextTick();
  restoreViewport();
  // Fetch history state and artifact data after loading flow
  try {
    const historyState = await FlowApi.getHistoryStatus(flowStore.flowId);
    flowStore.updateHistoryState(historyState);
  } catch (error) {
    console.error("Failed to fetch history state:", error);
  }
  flowStore.fetchArtifacts();
};

const selectNodeExternally = (nodeId: number) => {
  showTablePreview.value = true;

  setNodeTableView(nodeId);
  nextTick().then(() => {
    setNodeTableView(nodeId);
  });
  fitView({ nodes: [nodeId.toString()] });
};

/**
 * Compute the label for an edge based on its source node.
 * Priority: output handle label > nodeReference > df_{nodeId} default.
 */
function computeEdgeLabel(
  sourceNode: ReturnType<typeof instance.findNode>,
  sourceHandle?: string,
): string {
  const data = sourceNode?.data as FlowNodeData | undefined;
  if (data?.outputs && sourceHandle) {
    const output = data.outputs.find((o) => o.id === sourceHandle);
    if (output?.label) {
      return output.label;
    }
  }
  if (data?.nodeReference) {
    return data.nodeReference;
  }
  return `df_${data?.id ?? sourceNode?.id ?? ""}`;
}

/**
 * Refresh labels on all edges (respects showEdgeLabels toggle).
 */
function refreshAllEdgeLabels() {
  const allEdges = instance.getEdges.value;
  for (const edge of allEdges) {
    if (editorStore.showEdgeLabels) {
      const sourceNode = instance.findNode(edge.source);
      edge.label = computeEdgeLabel(sourceNode, edge.sourceHandle ?? undefined);
    } else {
      edge.label = undefined;
    }
  }
}

/**
 * Update edge labels for all outgoing edges of a specific node.
 */
function updateEdgeLabelsForNode(nodeId: string) {
  if (!editorStore.showEdgeLabels) return;
  const allEdges = instance.getEdges.value;
  const sourceNode = instance.findNode(nodeId);
  for (const edge of allEdges) {
    if (edge.source === nodeId) {
      edge.label = computeEdgeLabel(sourceNode, edge.sourceHandle ?? undefined);
    }
  }
}

/**
 * Reject a would-be connection that violates a frontend invariant.
 *
 * Two rules:
 *   1. A target handle on a non-multi node accepts at most one connection.
 *   2. The edge must not close a cycle (source reachable from target today).
 *
 * Returning false blocks the drop. If any rejection happened during the drag,
 * onConnectEnd surfaces it as a toast. State is reset per drag via
 * onConnectStart so retries after the first rejection still fire.
 */
let rejectionDuringDrag: string | null = null;

function rejectConnection(reason: string): false {
  rejectionDuringDrag = reason;
  return false;
}

function isValidConnection(connection: Connection): boolean {
  const source = connection.source;
  const target = connection.target;
  if (!source || !target) return false;
  if (source === target) return rejectConnection("A node can't connect to itself");

  const currentEdges = instance.getEdges.value;

  const targetNode = instance.findNode(target);
  const targetTemplate = (targetNode?.data as { nodeTemplate?: NodeTemplate } | undefined)
    ?.nodeTemplate;
  if (targetTemplate && !targetTemplate.multi && connection.targetHandle) {
    const handleOccupied = currentEdges.some(
      (e) => e.target === target && e.targetHandle === connection.targetHandle,
    );
    if (handleOccupied) {
      return rejectConnection(
        `Input on "${targetTemplate.name}" already has a connection — remove it first`,
      );
    }
  }

  const adjacency = new Map<string, string[]>();
  for (const e of currentEdges) {
    const list = adjacency.get(e.source);
    if (list) list.push(e.target);
    else adjacency.set(e.source, [e.target]);
  }
  const visited = new Set<string>([target]);
  const stack = [target];
  while (stack.length) {
    const node = stack.pop() as string;
    const outgoing = adjacency.get(node);
    if (!outgoing) continue;
    for (const next of outgoing) {
      if (next === source) {
        return rejectConnection("This connection would create a loop in the flow");
      }
      if (!visited.has(next)) {
        visited.add(next);
        stack.push(next);
      }
    }
  }
  // Moving onto a valid target supersedes any earlier rejection in this drag.
  rejectionDuringDrag = null;
  return true;
}

function onConnectStart() {
  rejectionDuringDrag = null;
}

function onConnectEnd() {
  if (rejectionDuringDrag) {
    ElMessage.warning(rejectionDuringDrag);
    rejectionDuringDrag = null;
  }
}

async function onConnect(params: Connection & { label?: string }) {
  if (!params.target || !params.source) return;
  if (!isValidConnection(params)) {
    // Belt-and-suspenders: if VueFlow ever lets an invalid drop through, bail quietly.
    return;
  }
  const nodeConnection: NodeConnection = {
    input_connection: {
      node_id: parseInt(params.target, 10),
      connection_class:
        params.targetHandle as NodeConnection["input_connection"]["connection_class"],
    },
    output_connection: {
      node_id: parseInt(params.source, 10),
      connection_class:
        params.sourceHandle as NodeConnection["output_connection"]["connection_class"],
    },
  };
  let response: Awaited<ReturnType<typeof connectNode>> | undefined;
  try {
    response = await connectNode(flowStore.flowId, nodeConnection);
  } catch (err) {
    const detail =
      (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
      "Failed to create connection";
    ElMessage.error(detail);
    return;
  }

  if (editorStore.showEdgeLabels) {
    const sourceNode = instance.findNode(params.source);
    params.label = computeEdgeLabel(sourceNode, params.sourceHandle ?? undefined);
  }

  addEdges([params]);
  if (response?.history) {
    flowStore.updateHistoryState(response.history);
  }
}

const NodeIsSelected = (nodeId: string) => {
  return selectedNodeIdInTable.value === +nodeId;
};

const nodeClick = (mouseEvent: any) => {
  showTablePreview.value = true;

  nextTick().then(() => {
    nodeStore.nodeId = parseInt(mouseEvent.node.id);
    itemStore.bringToFront("tablePreview");
    itemStore.bringToFront("nodeSettings");

    if (
      (mouseEvent.node.id && !NodeIsSelected(mouseEvent.node.id)) ||
      (dataPreview.value &&
        dataPreview.value.dataLength == 0 &&
        dataPreview.value.columnLength == 0)
    ) {
      setNodeTableView(mouseEvent.node.id);
    }
    selectedNodeIdInTable.value = +mouseEvent.node.id;
  });
};

const setNodeTableView = (nodeId: number) => {
  if (dataPreview.value) {
    dataPreview.value.downloadData(nodeId);
  }
};

const handleNodeChange = async (nodeChangesEvent: any) => {
  const nodeChanges = nodeChangesEvent as NodeChange[];
  let lastResponse: Awaited<ReturnType<typeof deleteNode>> | undefined;
  for (const nodeChange of nodeChanges) {
    if (nodeChange.type === "remove") {
      const nodeChangeId = Number(nodeChange.id);
      lastResponse = await deleteNode(flowStore.flowId, nodeChangeId);
    }
  }
  // Update history state from the last response
  if (lastResponse?.history) {
    flowStore.updateHistoryState(lastResponse.history);
  }
};

const convertEdgeChangeToNodeConnection = (edgeChange: EdgeChange): NodeConnection => {
  return {
    input_connection: {
      node_id: Number(edgeChange.target),
      connection_class: edgeChange.targetHandle,
    },
    output_connection: {
      node_id: Number(edgeChange.source),
      connection_class: edgeChange.sourceHandle,
    },
  };
};

const handleEdgeChange = async (edgeChangesEvent: any) => {
  const edgeChanges = edgeChangesEvent as EdgeChange[];
  if (edgeChanges.length >= 2) {
    return;
  }
  let lastResponse: Awaited<ReturnType<typeof deleteConnection>> | undefined;
  for (const edgeChange of edgeChanges) {
    if (edgeChange.type === "remove") {
      if (suppressedEdgeRemovals.delete(edgeChange.id)) {
        // Edge was already deleted on the backend by an in-flight operation
        // (e.g. drag-to-insert) — skip the redundant API call.
        continue;
      }
      const nodeConnection = convertEdgeChangeToNodeConnection(edgeChange);
      lastResponse = await deleteConnection(flowStore.flowId, nodeConnection);
    }
  }
  // Update history state from the last response
  if (lastResponse?.history) {
    flowStore.updateHistoryState(lastResponse.history);
  }
};

const handleDrop = async (event: DragEvent) => {
  if (!nodeStore.isRunning) {
    const response = await onDrop(event, flowStore.flowId);
    // Update history state from response
    if (response?.history) {
      flowStore.updateHistoryState(response.history);
    }
  }
};

const toSnakeCase = (str: string): string => {
  return str
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .replace(/[\s-]+/g, "_")
    .toLowerCase();
};

// Copy selected nodes (single or multiple) to localStorage
const copySelectedNodes = () => {
  const selectedNodes = instance.getSelectedNodes.value;
  const allEdges = instance.getEdges.value;

  if (selectedNodes.length === 0) {
    return;
  }

  // Snapshot the current system clipboard so we can detect external copies on paste
  snapshotClipboard();

  if (selectedNodes.length === 1) {
    // Single node copy - use existing format for backward compatibility
    const node = selectedNodes[0];
    const nodeCopyValue: NodeCopyValue = {
      nodeIdToCopyFrom: node.data.id,
      type: node.data.nodeTemplate?.item || node.data.component?.__name || "unknown",
      label: node.data.label,
      description: "",
      numberOfInputs: node.data.inputs.length,
      numberOfOutputs: node.data.outputs.length,
      typeSnakeCase:
        node.data.nodeTemplate?.item || toSnakeCase(node.data.component?.__name || "unknown"),
      flowIdToCopyFrom: flowStore.flowId,
      multi: node.data.nodeTemplate?.multi,
      nodeTemplate: node.data.nodeTemplate,
    };
    localStorage.setItem("copiedNode", JSON.stringify(nodeCopyValue));
    localStorage.removeItem("copiedMultiNodes");
  } else {
    // Multiple nodes copy - calculate bounding box and store relative positions
    const selectedNodeIds = new Set(selectedNodes.map((n) => n.data.id));

    // Find bounding box of selection
    let minX = Infinity,
      minY = Infinity;
    for (const node of selectedNodes) {
      minX = Math.min(minX, node.position.x);
      minY = Math.min(minY, node.position.y);
    }

    // Store nodes with their relative positions from the top-left of the bounding box
    const nodes: NodeCopyValue[] = selectedNodes.map((node) => ({
      nodeIdToCopyFrom: node.data.id,
      type: node.data.nodeTemplate?.item || node.data.component?.__name || "unknown",
      label: node.data.label,
      description: "",
      numberOfInputs: node.data.inputs.length,
      numberOfOutputs: node.data.outputs.length,
      typeSnakeCase:
        node.data.nodeTemplate?.item || toSnakeCase(node.data.component?.__name || "unknown"),
      flowIdToCopyFrom: flowStore.flowId,
      multi: node.data.nodeTemplate?.multi,
      nodeTemplate: node.data.nodeTemplate,
      relativeX: node.position.x - minX,
      relativeY: node.position.y - minY,
    }));

    // Find edges that connect nodes within the selection
    const edges: EdgeCopyValue[] = allEdges
      .filter((edge) => {
        const sourceId = parseInt(edge.source);
        const targetId = parseInt(edge.target);
        return selectedNodeIds.has(sourceId) && selectedNodeIds.has(targetId);
      })
      .map((edge) => ({
        sourceNodeId: parseInt(edge.source),
        targetNodeId: parseInt(edge.target),
        sourceHandle: edge.sourceHandle || DEFAULT_OUTPUT_HANDLE,
        targetHandle: edge.targetHandle || "input-0",
      }));

    const multiNodeCopyValue: MultiNodeCopyValue = {
      nodes,
      edges,
      flowIdToCopyFrom: flowStore.flowId,
    };

    localStorage.setItem("copiedMultiNodes", JSON.stringify(multiNodeCopyValue));
    localStorage.removeItem("copiedNode");
  }
};

const copyValue = async (x: number, y: number) => {
  const flowPosition = screenToFlowCoordinate({
    x: x,
    y: y,
  });

  // Check for multi-node copy first
  const copiedMultiNodesStr = localStorage.getItem("copiedMultiNodes");
  if (copiedMultiNodesStr) {
    const multiNodeCopyValue: MultiNodeCopyValue = JSON.parse(copiedMultiNodesStr);
    const response = await createMultiCopyNodes(
      multiNodeCopyValue,
      flowPosition.x,
      flowPosition.y,
      flowStore.flowId,
    );
    // Update history state from response
    if (response?.history) {
      flowStore.updateHistoryState(response.history);
    }
    return;
  }

  // Fall back to single node copy
  const copiedNodeStr = localStorage.getItem("copiedNode");
  if (!copiedNodeStr) return;

  const nodeCopyValue: NodeCopyValue = JSON.parse(copiedNodeStr);

  const nodeCopyInput: NodeCopyInput = {
    ...nodeCopyValue,
    posX: flowPosition.x,
    posY: flowPosition.y,
    flowId: flowStore.flowId,
  };
  const response = await createCopyNode(nodeCopyInput);
  // Update history state from response
  if (response?.history) {
    flowStore.updateHistoryState(response.history);
  }
};

const handleCanvasPaste = async (x: number, y: number) => {
  const hasCopiedNode =
    localStorage.getItem("copiedMultiNodes") || localStorage.getItem("copiedNode");

  if (hasCopiedNode) {
    // A node was previously copied. Check if the system clipboard has changed
    // since then — if so, the user copied something new externally (e.g. from
    // Excel) and we should try tabular paste instead.
    let clipboardChanged = false;
    try {
      const currentClipboard = await navigator.clipboard.readText();
      const snapshot = localStorage.getItem("clipboardAtNodeCopy") ?? "";
      clipboardChanged = currentClipboard !== snapshot;
    } catch {
      // Can't read clipboard — fall back to node paste
    }

    if (!clipboardChanged) {
      copyValue(x, y);
      return;
    }
  }

  // Try clipboard tabular data
  const flowPosition = screenToFlowCoordinate({ x, y });
  const response = await createManualInputFromClipboard(
    flowStore.flowId,
    flowPosition.x,
    flowPosition.y,
  );
  if (response) {
    if (response.history) {
      flowStore.updateHistoryState(response.history);
    }
    return;
  }

  // Clipboard wasn't tabular — fall back to node paste if available
  if (hasCopiedNode) {
    copyValue(x, y);
  }
};

const handleContextMenuAction = async (actionData: ContextMenuAction) => {
  const { actionId, position } = actionData;
  if (actionId === "fit-view") {
    fitView();
  } else if (actionId === "zoom-in") {
    instance.zoomIn();
  } else if (actionId === "zoom-out") {
    instance.zoomOut();
  } else if (actionId === "paste-node") {
    handleCanvasPaste(position.x, position.y);
  }
};

const handleResetLayoutGraph = async () => {
  await applyStandardLayout(flowStore.flowId);
  sessionStorage.removeItem(getViewportStorageKey(flowStore.flowId));
  await loadFlow();
  // loadFlow already fetches history state
  fitView({ padding: 0.2 });
  saveViewportToSession();
};

const hideLogViewer = () => {
  editorStore.hideLogViewerForThisRun = true;
  nodeStore.hideLogViewer();
};

const handleKeyDown = (event: KeyboardEvent) => {
  let eventKeyClicked = event.ctrlKey || event.metaKey;
  // Normalize key to lowercase to handle Caps Lock being on
  const key = event.key.toLowerCase();

  // Skip if typing in an input field or code editor
  const target = event.target as HTMLElement;
  const isInputElement =
    target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable;
  // Check if inside a CodeMirror editor
  const isInCodeMirror = target.closest(".cm-editor") !== null;

  // Check if text is selected - if so, let browser handle copy/paste natively
  const selection = window.getSelection();
  const hasTextSelected = selection && selection.toString().trim().length > 0;

  if (eventKeyClicked && key === "a" && !isInputElement && !isInCodeMirror) {
    // Select all nodes on canvas (prevent browser from selecting all page text)
    event.preventDefault();
    const allNodes = instance.getNodes.value;
    addSelectedNodes(allNodes);
  } else if (eventKeyClicked && key === "c" && !isInputElement && !hasTextSelected) {
    // Copy selected nodes only if no text is selected
    copySelectedNodes();
    event.preventDefault();
  } else if (eventKeyClicked && key === "v" && !isInputElement && !hasTextSelected) {
    // Paste: try clipboard tabular data first, then fall back to node paste
    event.preventDefault();
    handleCanvasPaste(clickedPosition.value.x, clickedPosition.value.y);
  } else if (eventKeyClicked && key === "n") {
    // Create new flow
    event.preventDefault();
    emit("new");
  } else if (eventKeyClicked && key === "s") {
    if (flowStore.flowId) {
      event.preventDefault();
      emit("save", flowStore.flowId);
    }
  } else if (eventKeyClicked && key === "e") {
    if (flowStore.flowId) {
      event.preventDefault();
      emit("run", flowStore.flowId);
    }
  } else if (eventKeyClicked && key === "g") {
    if (flowStore.flowId) {
      event.preventDefault();
      nodeStore.toggleCodeGenerator();
    }
  }
};

const handleContextMenu = (event: Event) => {
  event.preventDefault();
  let pointerEvent = event as PointerEvent;

  clickedPosition.value = {
    x: pointerEvent.x,
    y: pointerEvent.y,
  };
  showContextMenu.value = true;
};

const closeContextMenu = () => {
  showContextMenu.value = false;
  nodeStore.setCodeGeneratorVisibility(false);
};

// Prevent text selection during shift+drag selection on canvas
const handleSelectionStart = () => {
  document.body.style.userSelect = "none";
};

const handleSelectionEnd = () => {
  document.body.style.userSelect = "";
};

// Viewport persistence helpers
const getViewportStorageKey = (flowId: number) => `flowfile_viewport_${flowId}`;

const saveViewportToSession = () => {
  const viewport = instance.getViewport();
  const key = getViewportStorageKey(flowStore.flowId);
  sessionStorage.setItem(key, JSON.stringify(viewport));
};

const restoreViewport = () => {
  const key = getViewportStorageKey(flowStore.flowId);
  const saved = sessionStorage.getItem(key);
  if (saved) {
    try {
      instance.setViewport(JSON.parse(saved));
    } catch (e) {
      console.error("Failed to restore viewport from session:", e);
    }
  }
};

const handleMoveEnd = () => {
  saveViewportToSession();
};

onMounted(async () => {
  availableHeight.value = window.innerHeight - 50;
  tablePreviewHeight.value = availableHeight.value * 0.25; // 30% of the available height
  nodeSettingsHeight.value = availableHeight.value * 0.75; // 70% of the available height
  window.addEventListener("keydown", handleKeyDown);

  nodeStore.setVueFlowInstance(instance);
  loadFlow();

  // Refresh artifact data when flow execution completes
  watch(
    () => editorStore.isRunning,
    (running, wasRunning) => {
      if (!running && wasRunning) {
        flowStore.fetchArtifacts();
      }
    },
  );

  // Refresh edge labels when toggle changes
  watch(
    () => editorStore.showEdgeLabels,
    () => {
      refreshAllEdgeLabels();
    },
  );
});

onUnmounted(() => {
  window.removeEventListener("keydown", handleKeyDown);
});

defineExpose({
  loadFlow,
  updateEdgeLabelsForNode,
  refreshAllEdgeLabels,
});
</script>

<template>
  <div class="container">
    <main ref="mainContainerRef" @drop="handleDrop" @dragover="onDragOver">
      <VueFlow
        ref="vueFlow"
        :nodes="nodes"
        :edges="edges"
        :node-types="nodeTypes"
        :edge-types="edgeTypes"
        class="custom-node-flow"
        :connection-mode="ConnectionMode.Strict"
        :connection-radius="60"
        :edge-updater-radius="15"
        :default-viewport="{ zoom: 1 }"
        :is-valid-connection="isValidConnection"
        @edge-update="onEdgeUpdate"
        @edge-mouse-enter="onEdgeMouseEnter"
        @edge-mouse-leave="onEdgeMouseLeave"
        @connect="onConnect"
        @connect-start="onConnectStart"
        @connect-end="onConnectEnd"
        @node-drag="onNodeDrag"
        @node-drag-stop="onNodeDragStop"
        @pane-click="handleCanvasClick"
        @node-click="nodeClick"
        @nodes-change="handleNodeChange"
        @edges-change="handleEdgeChange"
        @pane-context-menu="handleContextMenu"
        @click="closeContextMenu"
        @selection-start="handleSelectionStart"
        @selection-end="handleSelectionEnd"
        @move-end="handleMoveEnd"
      >
        <MiniMap />
      </VueFlow>
      <context-menu
        v-if="showContextMenu"
        :x="clickedPosition.x"
        :y="clickedPosition.y"
        :target-type="contextMenuTarget.type"
        :target-id="contextMenuTarget.id"
        :on-close="closeContextMenu"
        @action="handleContextMenuAction"
      />
      <UndoRedoControls @refresh-flow="loadFlow" />
    </main>
    <draggable-item
      id="dataActions"
      :show-left="true"
      :initial-width="230"
      initial-position="left"
      title="Data actions"
      :allow-free-move="true"
      :prevent-overlap="false"
    >
      <NodeList @dragstart="onDragStart" />
    </draggable-item>
    <draggable-item
      v-if="nodeStore.isShowingLogViewer"
      id="logViewer"
      :show-bottom="true"
      title="Log overview"
      :allow-full-screen="true"
      initial-position="bottom"
      :initial-left="180"
      :on-minize="hideLogViewer"
      group="bottomPanels"
      :sync-dimensions="true"
      :prevent-overlap="false"
    >
      <LogViewer />
    </draggable-item>
    <draggable-item
      v-if="nodeStore.showFlowResult"
      id="flowresults"
      :show-right="true"
      title="flow results"
      initial-position="right"
      :initial-width="400"
      group="rightPanels"
      :prevent-overlap="false"
    >
      <FlowResults :on-click="selectNodeExternally" />
    </draggable-item>
    <draggable-item
      v-if="showTablePreview"
      id="tablePreview"
      :show-bottom="true"
      :allow-full-screen="true"
      title="Table Preview"
      initial-position="bottom"
      :on-minize="toggleShowTablePreview"
      :initial-height="tablePreviewHeight"
      :initial-left="180"
      group="bottomPanels"
      :sync-dimensions="true"
      :prevent-overlap="false"
    >
      <data-preview ref="dataPreview"> text </data-preview>
    </draggable-item>
    <draggable-item
      v-if="nodeStore.isDrawerOpen"
      id="nodeSettings"
      :show-right="true"
      initial-position="right"
      :initial-width="600"
      :initial-height="nodeSettingsHeight"
      title="Node Settings"
      :on-minize="handleNodeSettingsClose"
      :allow-full-screen="true"
      :prevent-overlap="false"
    >
      <NodeSettingsDrawer />
    </draggable-item>
    <draggable-item
      v-if="nodeStore.showCodeGenerator"
      id="generatedCode"
      :show-left="true"
      :initial-width="800"
      initial-position="right"
      :allow-free-move="true"
      :allow-full-screen="true"
      :on-minize="() => nodeStore.setCodeGeneratorVisibility(false)"
      :prevent-overlap="false"
    >
      <CodeGenerator />
    </draggable-item>
    <draggable-item
      v-if="editorStore.showParametersPanel"
      id="flowParameters"
      :show-right="true"
      initial-position="right"
      :initial-width="520"
      title="Flow Parameters"
      :on-minize="() => editorStore.setParametersPanelVisibility(false)"
      :allow-full-screen="true"
      :prevent-overlap="false"
      group="rightPanels"
    >
      <FlowParametersPanel />
    </draggable-item>
    <layoutControls @reset-layout-graph="handleResetLayoutGraph" />
  </div>
</template>

<style>
@import "@vue-flow/core/dist/style.css";
@import "@vue-flow/core/dist/theme-default.css";
@import "@vue-flow/controls/dist/style.css";
@import "@vue-flow/minimap/dist/style.css";
@import "@vue-flow/node-resizer/dist/style.css";

html,
body,
#app {
  margin: 0;
  height: 100%;
}

#app {
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  font-style: default;
}

.vue-flow__minimap {
  transform: scale(75%);
  transform-origin: bottom right;
}

/* Custom cursors for Windows: system grab/crosshair cursors can be white
   and invisible on light backgrounds. These SVG cursors guarantee contrast. */
.custom-node-flow .vue-flow__pane.draggable,
.custom-node-flow .vue-flow__node.draggable,
.custom-node-flow .vue-flow__nodesselection-rect {
  cursor:
    url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='20' height='20' fill='none'%3E%3Cpath d='M6.5 12V7.5a1.25 1.25 0 0 1 2.5 0V11m0-4a1.25 1.25 0 0 1 2.5 0V11m0-3a1.25 1.25 0 0 1 2.5 0V11m0-1a1.25 1.25 0 0 1 2.5 0v3c0 3-2 5-5 5S6 16 6 13' stroke='white' stroke-width='2.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3Cpath d='M6.5 12V7.5a1.25 1.25 0 0 1 2.5 0V11m0-4a1.25 1.25 0 0 1 2.5 0V11m0-3a1.25 1.25 0 0 1 2.5 0V11m0-1a1.25 1.25 0 0 1 2.5 0v3c0 3-2 5-5 5S6 16 6 13' stroke='%23333' stroke-width='1.2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E")
      10 5,
    grab;
}

.custom-node-flow .vue-flow__pane.dragging,
.custom-node-flow .vue-flow__node.dragging,
.custom-node-flow .vue-flow__nodesselection-rect.dragging {
  cursor:
    url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='20' height='20' fill='none'%3E%3Cpath d='M6.5 13v-2c0-1 .8-1.5 2-1.5h4c1.2 0 2 .5 2 1.5v2c0 3-2 5-4 5s-4-2-4-5' stroke='white' stroke-width='2.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3Cpath d='M6.5 13v-2c0-1 .8-1.5 2-1.5h4c1.2 0 2 .5 2 1.5v2c0 3-2 5-4 5s-4-2-4-5' stroke='%23333' stroke-width='1.2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E")
      10 10,
    grabbing;
}

.custom-node-flow .vue-flow__handle {
  width: 8px;
  height: 8px;
}

.custom-node-flow .vue-flow__handle.connectable {
  cursor:
    url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='20' height='20'%3E%3Cpath d='M10 3v14M3 10h14' stroke='white' stroke-width='3' stroke-linecap='round'/%3E%3Cpath d='M10 3v14M3 10h14' stroke='%23333' stroke-width='1.5' stroke-linecap='round'/%3E%3C/svg%3E")
      10 10,
    crosshair;
}

.custom-node-flow .vue-flow__edges {
  filter: invert(100%);
}

.custom-node-flow .vue-flow__edge-textwrapper {
  filter: invert(100%);
}

.custom-node-flow .vue-flow__edge-text {
  font-size: 11px;
  font-weight: 500;
  fill: #555;
}

.custom-node-flow .vue-flow__edge-textbg {
  fill: #fff;
  rx: 4;
  ry: 4;
  opacity: 0.9;
}

/* Visual cue while a dragged node hovers an edge — the drop will splice
   the node into this edge (A -> new -> B). Thick + dashed reads through
   VueFlow's color-inverted edge layer. */
.custom-node-flow .vue-flow__edge.edge-drop-target .vue-flow__edge-path {
  stroke-width: 4;
  stroke-dasharray: 6 4;
}

.animated-bg-gradient {
  background: linear-gradient(
    122deg,
    var(--color-gradient-canvas-1),
    var(--color-gradient-canvas-2),
    var(--color-gradient-canvas-3),
    var(--color-gradient-canvas-4)
  );
  background-size: 800% 800%;
  -webkit-animation: gradient 4s ease infinite;
  -moz-animation: gradient 4s ease infinite;
  animation: gradient 4s ease infinite;
}

@-webkit-keyframes gradient {
  0% {
    background-position: 0% 22%;
  }
  50% {
    background-position: 100% 79%;
  }
  to {
    background-position: 0% 22%;
  }
}

@-moz-keyframes gradient {
  0% {
    background-position: 0% 22%;
  }
  50% {
    background-position: 100% 79%;
  }
  to {
    background-position: 0% 22%;
  }
}

@keyframes gradient {
  0% {
    background-position: 0% 22%;
  }
  50% {
    background-position: 100% 79%;
  }
  to {
    background-position: 0% 22%;
  }
}

.container {
  display: flex;
  height: 100vh;
}

main {
  flex-grow: 1;
  position: relative;
}
</style>
