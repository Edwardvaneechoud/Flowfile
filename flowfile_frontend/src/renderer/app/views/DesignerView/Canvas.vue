<script setup lang="ts">
// TODO(refactor): ~1170 LOC; bundles 7+ concerns. Plan to extract:
//   - 6 draggable panel wrappers (~lines 927-1021) → individual *Panel components
//   - clipboard/copy-paste logic (~lines 533-700) → useFlowClipboard composable
//   - context menu handling (~lines 702-794) → useContextMenu composable
//   - keyboard shortcuts (~lines 729-778) → useFlowHotkeys composable
import {
  ref,
  computed,
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
  NodeMouseEvent,
  useVueFlow,
  ConnectionMode,
} from "@vue-flow/core";
import { MiniMap } from "@vue-flow/minimap";

import CustomNode from "../../components/nodes/NodeWrapper.vue";
import GroupNode from "../../components/nodes/GroupNode.vue";
import {
  useNodeGroups,
  isGroupNodeId,
  GROUP_PROXY_EDGE_PREFIX,
  GROUP_PROXY_EDGE_TYPE,
} from "../../composables/useNodeGroups";
import DeletableEdge from "./DeletableEdge.vue";
import GroupProxyEdge from "./GroupProxyEdge.vue";
import useDragAndDrop from "./useDnD";
import {
  suppressedEdgeRemovals,
  markHoveredEdge,
  detectEdgeUnderPointer,
} from "../../composables/useDragAndDrop";
import CodeGenerator from "./CodeGenerator/CodeGenerator.vue";
import NodeList from "./NodeList.vue";
import { useAiStore } from "../../stores/ai-store";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import { useFlowStore, FLOW_ID_STORAGE_KEY } from "../../stores/flow-store";
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
import layoutControls from "../../components/common/DraggableItem/layoutControls.vue";
import { useItemStore } from "../../components/common/DraggableItem/stateStore";
import DataPreview from "../../features/designer/dataPreview.vue";
import FlowResults from "../../features/designer/editor/results.vue";
import LogViewer from "./LogViewer/LogViewer.vue";
import ContextMenu from "./ContextMenu.vue";
import AiAssistant from "../../features/ai/AiAssistant.vue";
import AiCommandPalette from "../../features/ai/AiCommandPalette.vue";
import AiGhostNode from "../../features/ai/AiGhostNode.vue";
import { useGhostNodeSuggestions } from "../../features/ai/useGhostNodeSuggestions";
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
import { ElMessage, ElMessageBox } from "element-plus";
import axios from "axios";

/** Typed subset of VueFlow node data used for edge label computation. */
interface FlowNodeData {
  id?: number;
  nodeReference?: string;
  outputs?: NodeHandle[];
}

const itemStore = useItemStore();
// Tracks the live height of the canvas <main> element. Driven by ResizeObserver
// in onMounted so derived heights (table preview, node settings) stay in sync
// with the actual canvas region instead of a stale window.innerHeight snapshot.
const availableHeight = ref(window.innerHeight - 50);
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const flowStore = useFlowStore();
const aiStore = useAiStore();
const rawCustomNode = markRaw(CustomNode);
const rawGroupNode = markRaw(GroupNode);
const rawDeletableEdge = markRaw(DeletableEdge);
const rawGroupProxyEdge = markRaw(GroupProxyEdge);
const { updateEdge, addEdges, fitView, screenToFlowCoordinate, addSelectedNodes, onPaneReady } =
  useVueFlow();

let resolvePaneReady: () => void;
const paneReadyPromise = new Promise<void>((resolve) => {
  resolvePaneReady = resolve;
});
onPaneReady(() => resolvePaneReady());

// Closure-scoped (non-reactive) counter used to discard stale loadFlow runs
// when a newer one starts. Each call captures its own myToken; if loadToken
// has advanced past it at any await boundary, that run bails out.
let loadToken = 0;
// Reactive flag the parent (DesignerView) reads to drive its switch-indicator.
// Stays true for the entire async load so the spinner doesn't flicker off
// before the canvas actually finishes populating.
const isLoadingFlow = ref(false);
const vueFlow = ref<InstanceType<typeof VueFlow>>();
const nodeTypes: NodeTypesObject = {
  "custom-node": rawCustomNode as NodeComponent,
  group: rawGroupNode as NodeComponent,
};
const edgeTypes = {
  default: rawDeletableEdge as EdgeComponent,
  [GROUP_PROXY_EDGE_TYPE]: rawGroupProxyEdge as EdgeComponent,
};
const hoveredEdgeId = ref<string | null>(null);
// Short delay before clearing on edge-leave so the cursor can cross the SVG→HTML
// boundary onto the delete button (rendered via EdgeLabelRenderer/Teleport)
// without the button hiding from under it. Also lets a same-frame enter on a
// neighbouring edge cancel the clear, killing a race where mouseleave on the
// previous edge would wipe state set by mouseenter on the next one.
let leaveTimeout: ReturnType<typeof setTimeout> | null = null;

const cancelEdgeLeave = () => {
  if (leaveTimeout) {
    clearTimeout(leaveTimeout);
    leaveTimeout = null;
  }
};

const scheduleEdgeLeave = (edgeId: string) => {
  cancelEdgeLeave();
  leaveTimeout = setTimeout(() => {
    if (hoveredEdgeId.value === edgeId) {
      hoveredEdgeId.value = null;
    }
    leaveTimeout = null;
  }, 150);
};

provide("hoveredEdgeId", hoveredEdgeId);
provide("cancelEdgeLeave", cancelEdgeLeave);
provide("scheduleEdgeLeave", scheduleEdgeLeave);

// — schema-grounded next-node suggestions on edge hover. The composable
// owns its own debounce + AbortController so a hover-flick doesn't fire N
// requests; clear is wired into handleCanvasClick below so a click anywhere
// off the popover dismisses it.
const ghostNode = useGhostNodeSuggestions();

function onEdgeMouseEnter(payload: {
  edge: { id: string; source: string; target: string };
  event: unknown;
}) {
  cancelEdgeLeave();
  hoveredEdgeId.value = payload.edge.id;
  // VueFlow's GraphEdge carries sourceX/Y/targetX/Y at runtime even though
  // the public ``EdgeMouseEvent`` declares the narrower ``Edge`` shape; the
  // composable defaults missing coords to 0 so this cast is safe.
  ghostNode.onEdgeMouseEnter(
    payload as Parameters<typeof ghostNode.onEdgeMouseEnter>[0],
    flowStore.flowId,
  );
}

function onEdgeMouseLeave({ edge }: { edge: { id: string } }) {
  if (hoveredEdgeId.value !== edge.id) return;
  scheduleEdgeLeave(edge.id);
  ghostNode.onEdgeMouseLeave();
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
  if (edgeId) {
    const template = (node.data as { nodeTemplate?: NodeTemplate } | undefined)?.nodeTemplate;
    if (!template) return;
    const response = await insertNodeOnEdge(flowStore.flowId, Number(node.id), template, edgeId);
    if (response?.history) {
      flowStore.updateHistoryState(response.history);
    }
    return;
  }
  // No edge-splice: persist the new position(s). Also closes the long-standing gap
  // where dragged node positions were never sent to the backend.
  const graphNode = instance.findNode(node.id);
  if (graphNode) {
    await persistDrag(graphNode);
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
  createEmptyFlow,
  createCopyNode,
  createMultiCopyNodes,
  createManualInputFromClipboard,
  insertNodeOnEdge,
} = useDragAndDrop();
const { groupSelectedNodes, removeSelectedFromGroup, persistDrag } = useNodeGroups();
const dataPreview = ref<InstanceType<typeof DataPreview>>();
// 25 / 75 split of the canvas height between the bottom table preview and the
// right-side node settings drawer. Initial-only — DraggableItem reads these
// once on mount and the user manages further sizing via resize handles.
const tablePreviewHeight = computed(() => Math.max(120, Math.floor(availableHeight.value * 0.25)));
const nodeSettingsHeight = computed(() => Math.max(200, Math.floor(availableHeight.value * 0.75)));
const selectedNodeIdInTable = ref(0);
const showContextMenu = ref(false);
const clickedPosition = ref<CursorPosition>({ x: 0, y: 0 });
const contextMenuTarget = ref({ type: "pane", id: "" });
// Whether the right-clicked node is currently inside a group (drives Group vs
// Remove-from-group in the node context menu).
const contextMenuTargetInGroup = ref(false);
const emit = defineEmits<{
  (e: "save", flowId: number): void;
  (e: "run", flowId: number): void;
  (e: "new"): void;
  (e: "openSettings"): void;
  (e: "open"): void;
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
  ghostNode.onViewportClick();
  clickedPosition.value = {
    x: event.x,
    y: event.y,
  };
  // Clear any browser text selection when clicking on canvas
  window.getSelection()?.removeAllRanges();
};

// VueFlow only emits @pane-click — there's no @pane-dblclick. We listen for
// native dblclick on <main> instead, then ignore double-clicks that landed on
// a node, edge, handle, or any floating panel. What's left is the empty pane.
const handleMainDblClick = (event: MouseEvent) => {
  const target = event.target as HTMLElement | null;
  if (!target) return;
  if (
    target.closest(".overlay") ||
    target.closest(".vue-flow__node") ||
    target.closest(".vue-flow__edge") ||
    target.closest(".vue-flow__handle") ||
    target.closest(".vue-flow__minimap") ||
    target.closest(".layout-widget-wrapper")
  ) {
    return;
  }
  // Hide every floating overlay (right-side + bottom). Left palette stays.
  editorStore.hideAllPanels();
  showTablePreview.value = false;
  nodeStore.nodeId = -1;
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
  const myToken = ++loadToken;
  isLoadingFlow.value = true;
  try {
    // Wait for VueFlow to finish its first internal mount before populating it.
    // Already-resolved on every call after the first.
    await paneReadyPromise;
    if (myToken !== loadToken) return;

    const flowIdAtStart = flowStore.flowId;
    const vueFlowInput = await getFlowData(flowIdAtStart);
    if (myToken !== loadToken) return;

    await importFlow(vueFlowInput);
    // Stale check after importFlow: createEmptyFlow inside importFlow already
    // cleared the canvas, so bailing here is safe — the newer in-flight run
    // (which bumped loadToken) will repopulate.
    if (myToken !== loadToken) return;

    await nextTick();
    restoreViewport(flowIdAtStart);

    try {
      const historyState = await FlowApi.getHistoryStatus(flowIdAtStart);
      if (myToken !== loadToken) return;
      flowStore.updateHistoryState(historyState);
    } catch (error) {
      console.error("Failed to fetch history state:", error);
    }
    // Fire-and-forget; fetchArtifacts re-checks flowId before writing.
    flowStore.fetchArtifacts(flowIdAtStart);
  } finally {
    // Only clear if we're still the most recent run — otherwise the newer
    // run's spinner would be turned off prematurely.
    if (myToken === loadToken) isLoadingFlow.value = false;
  }
};

const reloadCurrentFlow = () => loadFlow();

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
  // A collapsed group's pill edges are added programmatically but still validated through
  // here, so they must be accepted or they won't render. Users can't draw them by hand
  // (group nodes are connectable:false); this only greenlights those UI-only proxy edges.
  if (isGroupNodeId(source) || isGroupNodeId(target)) return true;
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
  if (isGroupNodeId(mouseEvent.node.id)) return; // groups have no node data to preview
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
      // Group boxes are not real nodes — their removal is handled by the ungroup
      // action (which calls deleteGroup). Skip them so we don't deleteNode(NaN).
      if (isGroupNodeId(nodeChange.id)) continue;
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
      // UI-only proxy edges must not trigger a backend deleteConnection.
      if (edgeChange.id.startsWith(GROUP_PROXY_EDGE_PREFIX)) {
        continue;
      }
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

const promptLineageQuestion = async (focusLabel: string): Promise<string | null> => {
  // — use Element Plus's imperative prompt (already in use elsewhere
  // in the codebase, e.g. CatalogView) instead of a new dialog component.
  // Returns the trimmed question on confirm, or ``null`` on cancel.
  try {
    const result = await ElMessageBox.prompt(
      `Ask the AI a lineage question about ${focusLabel}. The AI will read recent run history alongside the current flow graph to answer.`,
      "Ask about lineage",
      {
        confirmButtonText: "Ask",
        cancelButtonText: "Cancel",
        inputType: "textarea",
        inputPlaceholder: "e.g. Why is column customer_id null since Tuesday?",
        inputValidator: (value: string) =>
          (value && value.trim().length > 0) || "Please type a question.",
      },
    );
    const value = (result as { value?: string }).value;
    return value?.trim() || null;
  } catch {
    // ElMessageBox throws on cancel/close.
    return null;
  }
};

const handleContextMenuAction = async (actionData: ContextMenuAction) => {
  const { actionId, position, targetId } = actionData;
  if (actionId === "fit-view") {
    fitView();
  } else if (actionId === "zoom-in") {
    instance.zoomIn();
  } else if (actionId === "zoom-out") {
    instance.zoomOut();
  } else if (actionId === "paste-node") {
    handleCanvasPaste(position.x, position.y);
  } else if (actionId === "generate-documentation") {
    // — pull the canonical flow name server-side so the doc title
    // matches what the user sees in the title bar. Falsy → undefined so
    // the store falls back to ``flow ${flowId}``.
    if (flowStore.flowId === null) return;
    const settings = await FlowApi.getFlowSettings(flowStore.flowId);
    const name = settings?.name?.trim() || undefined;
    await aiStore.generateDocumentation(flowStore.flowId, name);
  } else if (actionId === "add-descriptions-all") {
    // Bulk variant of the per-node ✨ "Add description" action. Confirms
    // first since each node is a separate LLM call (cost + time scale
    // linearly with N), then streams them sequentially through the quiet
    // ai-store path that writes straight to node.setting_input.description
    // without flooding the chat drawer.
    if (flowStore.flowId === null) return;
    const allNodes = instance.getNodes.value;
    if (allNodes.length === 0) {
      ElMessage.info("No nodes on the canvas.");
      return;
    }
    try {
      await ElMessageBox.confirm(
        `Generate AI descriptions for all ${allNodes.length} node${allNodes.length === 1 ? "" : "s"}? Existing descriptions will be replaced.`,
        "Add description to all nodes",
        {
          confirmButtonText: "Generate",
          cancelButtonText: "Cancel",
          type: "warning",
        },
      );
    } catch {
      return;
    }
    const nodeIds = allNodes.map((n) => Number(n.id)).filter((id) => Number.isFinite(id));
    ElMessage.info(
      `Generating descriptions for ${nodeIds.length} node${nodeIds.length === 1 ? "" : "s"}…`,
    );
    const { succeeded, failed, aborted } = await aiStore.runBulkAddDescriptions(
      flowStore.flowId,
      nodeIds,
    );
    if (aborted) {
      ElMessage.warning(`Aborted. Updated ${succeeded} of ${nodeIds.length}.`);
    } else if (failed === 0) {
      ElMessage.success(`Updated ${succeeded} description${succeeded === 1 ? "" : "s"}.`);
    } else {
      ElMessage.warning(`Updated ${succeeded} of ${nodeIds.length} (${failed} failed).`);
    }
  } else if (actionId === "ask-lineage") {
    // — whole-flow lineage Q&A.
    if (flowStore.flowId === null) return;
    const question = await promptLineageQuestion("this flow");
    if (!question) return;
    await aiStore.askLineageQuestion(flowStore.flowId, question);
  } else if (actionId === "ask-lineage-node") {
    // — focused lineage Q&A on a single node id.
    if (flowStore.flowId === null) return;
    const focusNodeId = Number(targetId);
    if (!Number.isFinite(focusNodeId)) return;
    const question = await promptLineageQuestion(`node ${focusNodeId}`);
    if (!question) return;
    await aiStore.askLineageQuestion(flowStore.flowId, question, focusNodeId);
  } else if (actionId === "group-selection") {
    await groupSelectedNodes();
  } else if (actionId === "remove-from-group") {
    await removeSelectedFromGroup();
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
  } else if (eventKeyClicked && key === ",") {
    if (flowStore.flowId) {
      event.preventDefault();
      emit("openSettings");
    }
  } else if (eventKeyClicked && key === "o" && !isInputElement && !isInCodeMirror) {
    // Open file picker — guarded against input/CodeMirror so users typing
    // "o" with a stuck modifier (or rapid macro) don't open the dialog.
    event.preventDefault();
    emit("open");
  } else if (eventKeyClicked && key === "k" && !isInputElement && !isInCodeMirror) {
    // Cmd+K / Ctrl+K toggles the AI assistant drawer. Originally
    // wired to the AI command palette; rewired to the drawer because
    // the palette UX confused users. Palette component, store, and
    // route are kept intact — reversible by restoring
    // `commandPalette.toggle()` here. Skipped when typing in any
    // input or CodeMirror so plain k presses pass through.
    if (flowStore.flowId && flowStore.flowId > 0) {
      event.preventDefault();
      editorStore.toggleAiDrawer();
    }
  }
};

const handleContextMenu = (event: Event) => {
  event.preventDefault();
  let pointerEvent = event as PointerEvent;

  contextMenuTarget.value = { type: "pane", id: "" };
  contextMenuTargetInGroup.value = false;
  clickedPosition.value = {
    x: pointerEvent.x,
    y: pointerEvent.y,
  };
  showContextMenu.value = true;
};

// Right-click on a node opens the context menu with node-target actions (incl.
// grouping). The container "group" node has its own affordances, so skip it here.
const handleNodeContextMenu = ({ event, node }: NodeMouseEvent) => {
  event.preventDefault();
  if (node.type === "group") return;
  // Ensure the right-clicked node participates in the selection-based group action.
  if (!node.selected) {
    addSelectedNodes([node]);
  }
  const mouseEvent = event as MouseEvent;
  contextMenuTarget.value = { type: "node", id: node.id };
  contextMenuTargetInGroup.value = Boolean(node.parentNode);
  clickedPosition.value = { x: mouseEvent.clientX, y: mouseEvent.clientY };
  showContextMenu.value = true;
};

// Right-click on the multi-selection rectangle (the highlighted box around several
// selected nodes) — VueFlow routes this here rather than to pane/node menus.
const handleSelectionContextMenu = ({ event }: { event: MouseEvent }) => {
  event.preventDefault();
  contextMenuTarget.value = { type: "selection", id: "" };
  contextMenuTargetInGroup.value = false;
  clickedPosition.value = { x: event.clientX, y: event.clientY };
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

const restoreViewport = (flowId: number) => {
  const key = getViewportStorageKey(flowId);
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

let mainResizeObserver: ResizeObserver | null = null;

onMounted(async () => {
  if (mainContainerRef.value) {
    availableHeight.value = mainContainerRef.value.clientHeight;
    mainResizeObserver = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        availableHeight.value = entry.contentRect.height;
      }
    });
    mainResizeObserver.observe(mainContainerRef.value);
  }
  window.addEventListener("keydown", handleKeyDown);

  nodeStore.setVueFlowInstance(instance);

  watch(
    () => flowStore.flowId,
    async (id) => {
      if (id && id > 0) {
        try {
          await loadFlow();
        } catch (e: unknown) {
          console.error("loadFlow failed:", e);
          // A stale flowId in sessionStorage causes a permanent boot loop
          // (every refresh 404s). Clear storage and reset the in-memory id
          // so the current session recovers without a hard refresh.
          if (axios.isAxiosError(e) && e.response?.status === 404) {
            sessionStorage.removeItem(FLOW_ID_STORAGE_KEY);
            flowStore.flowId = -1;
          }
          ElMessage.error("Failed to load flow");
        }
      } else {
        // No active flow — visually clear the canvas (previously handled by
        // the v-if unmount in DesignerView, which we no longer use). Goes
        // through the same loadToken so a concurrent loadFlow can't lose to
        // a slow createEmptyFlow.
        const myToken = ++loadToken;
        isLoadingFlow.value = true;
        try {
          await createEmptyFlow();
          if (myToken !== loadToken) return;
        } finally {
          if (myToken === loadToken) isLoadingFlow.value = false;
        }
      }
    },
    { immediate: true },
  );

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

  // Bring the AI assistant DraggableItem to the front when the drawer
  // opens. Replaces the previous `#aiAssistant.overlay { z-index: 245
  // !important }` CSS hack — that rule clobbered stateStore's
  // bring-to-front semantics for every other panel. nextTick gives
  // DraggableItem.onMounted time to register itself with itemStore
  // before we ask it to bump its zIndex.
  watch(
    () => editorStore.isAiOpen,
    (open) => {
      if (open) {
        nextTick().then(() => itemStore.bringToFront("aiAssistant"));
      }
    },
  );

  // External-mutation signal — the backend mutated the live flow without
  // going through the in-canvas mutation paths. Triggered today by
  // `useAiDiffStore.accept()` after the apply_diff lands; future
  // workstreams that mutate the server graph (e.g.'s
  // `update_node_settings` end-to-end) call `flowStore.requestReload()`
  // and Canvas reloads. The closure-scoped `loadToken` in `loadFlow`
  // already cancels stale runs if multiple bumps land in quick succession.
  watch(
    () => flowStore.pendingReloadCounter,
    (count, prev) => {
      if (count > (prev ?? 0)) {
        void loadFlow();
      }
    },
  );

  // — Layout-reset signal. Bumped by
  // ``flowStore.requestLayoutReset()`` from the post-agent_live
  // banner's [Reorganize] button. Re-runs the same code path the
  // manual "Reset layout graph" toolbar button triggers.
  watch(
    () => flowStore.pendingLayoutResetCounter,
    (count, prev) => {
      if (count > (prev ?? 0)) {
        void handleResetLayoutGraph();
      }
    },
  );
});

onUnmounted(() => {
  window.removeEventListener("keydown", handleKeyDown);
  mainResizeObserver?.disconnect();
  mainResizeObserver = null;
  cancelEdgeLeave();
});

defineExpose({
  reloadCurrentFlow,
  isLoadingFlow,
  updateEdgeLabelsForNode,
  refreshAllEdgeLabels,
});
</script>

<template>
  <div class="container">
    <main
      ref="mainContainerRef"
      @drop="handleDrop"
      @dragover="onDragOver"
      @dblclick="handleMainDblClick"
    >
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
        :zoom-on-double-click="false"
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
        @node-context-menu="handleNodeContextMenu"
        @selection-context-menu="handleSelectionContextMenu"
        @click="closeContextMenu"
        @selection-start="handleSelectionStart"
        @selection-end="handleSelectionEnd"
        @move-end="handleMoveEnd"
      >
        <MiniMap />
        <AiGhostNode :composable="ghostNode" />
      </VueFlow>
      <context-menu
        v-if="showContextMenu"
        :x="clickedPosition.x"
        :y="clickedPosition.y"
        :target-type="contextMenuTarget.type"
        :target-id="contextMenuTarget.id"
        :target-in-group="contextMenuTargetInGroup"
        :on-close="closeContextMenu"
        @action="handleContextMenuAction"
      />
      <draggable-item
        id="dataActions"
        :show-left="true"
        :initial-width="230"
        initial-position="left"
        title="Data actions"
        :allow-free-move="true"
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
        :allow-full-screen="true"
        group="rightPanels"
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
      >
        <CodeGenerator />
      </draggable-item>
      <draggable-item
        v-if="editorStore.isAiOpen"
        id="aiAssistant"
        :show-right="true"
        initial-position="right"
        :initial-width="600"
        title="AI Assistant"
        :on-minize="editorStore.closeAiDrawer"
        :allow-full-screen="true"
      >
        <AiAssistant />
      </draggable-item>
      <AiCommandPalette />
      <layoutControls @reset-layout-graph="handleResetLayoutGraph" />
    </main>
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
  height: 100%;
  position: relative;
}

main {
  flex-grow: 1;
  position: relative;
  overflow: hidden;
}
</style>
