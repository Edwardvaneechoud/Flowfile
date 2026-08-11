<script setup lang="ts">
// TODO(refactor): bundles several concerns. Still to extract:
//   - 6 draggable panel wrappers → individual *Panel components
//   - context menu handling → useContextMenu composable
// (clipboard/copy-paste now lives in composables/useFlowClipboard, keyboard
//  shortcuts in composables/useFlowHotkeys.)
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
import { useStaleModifierGuard } from "../../composables/useStaleModifierGuard";
import NodeList from "./NodeList.vue";
import { useAiStore } from "../../stores/ai-store";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import { useFlowStore, FLOW_ID_STORAGE_KEY } from "../../stores/flow-store";
import { useDrawerStore } from "../../stores/drawer-store";
import { useTutorialStore } from "../../stores/tutorial-store";
import {
  getFlowData,
  deleteConnection,
  deleteNode,
  connectNode,
  NodeConnection,
} from "./backendInterface";
import { FlowApi } from "../../api";
import {
  copyNodesToBuffer,
  isCanvasClipboardTarget,
  isNodeBufferArmed,
  readNodeClipboardBuffer,
  resolvePasteIntent,
  writeSentinelToOsClipboard,
} from "../../composables/useFlowClipboard";
import type { PasteIntent } from "../../composables/useFlowClipboard";
import { useFlowHotkeys } from "../../composables/useFlowHotkeys";
import { desktop, isDesktop } from "../../../lib/desktop";
import DraggableItem from "../../components/common/DraggableItem/DraggableItem.vue";
import layoutControls from "../../components/common/DraggableItem/layoutControls.vue";
import { useItemStore } from "../../components/common/DraggableItem/stateStore";
import TabbedDrawer from "./TabbedDrawer.vue";
import { drawers } from "./drawerRegistry";
import type { DrawerDef } from "../../types/drawer.types";
import ContextMenu from "./ContextMenu.vue";
import FileDropOverlay from "./FileDropOverlay.vue";
import FileDropUploadDialog from "./FileDropUploadDialog.vue";
import { useFileDropImport } from "../../composables/useFileDropImport";
import AiCommandPalette from "../../features/ai/AiCommandPalette.vue";
import AiGhostNode from "../../features/ai/AiGhostNode.vue";
import { useGhostNodeSuggestions } from "../../features/ai/useGhostNodeSuggestions";
import { NodeCopyInput, ContextMenuAction, CursorPosition } from "./types";
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
// Live height of the canvas <main> element, from the store's shared container
// measurement (registered in onMounted). Falls back to the window while the
// container is unmeasured (first frame).
const availableHeight = computed(() => itemStore.containerBounds.height || window.innerHeight - 50);
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const flowStore = useFlowStore();
const drawerStore = useDrawerStore();
const aiStore = useAiStore();
const rawCustomNode = markRaw(CustomNode);
const rawGroupNode = markRaw(GroupNode);
const rawDeletableEdge = markRaw(DeletableEdge);
const rawGroupProxyEdge = markRaw(GroupProxyEdge);
const { updateEdge, addEdges, fitView, screenToFlowCoordinate, addSelectedNodes, onPaneReady } =
  useVueFlow();

useStaleModifierGuard();

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

// Schema-grounded "suggest next node" — a deliberate action (right-click a
// node → "Suggest next node…") that opens an inline intent box and, on submit,
// previews AI-proposed downstream nodes to confirm. The popover + request
// state live in the composable/store; dismiss is wired into handleCanvasClick.
const ghostNode = useGhostNodeSuggestions();

function onEdgeMouseEnter(payload: {
  edge: { id: string; source: string; target: string };
  event: unknown;
}) {
  cancelEdgeLeave();
  hoveredEdgeId.value = payload.edge.id;
}

function onEdgeMouseLeave({ edge }: { edge: { id: string } }) {
  if (hoveredEdgeId.value !== edge.id) return;
  scheduleEdgeLeave(edge.id);
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
  // Dynamic-input nodes never splice: their input-0 is the parameter handle.
  const effectiveInputCount = template?.multi ? 1 : (template?.input ?? 0);
  if (!template || template.dynamic_inputs || effectiveInputCount !== 1 || template.output < 1) {
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
const fileDrop = useFileDropImport();
const { groupSelectedNodes, removeSelectedFromGroup, persistDrag } = useNodeGroups();
// Default drawer sizing. The bottom dock takes ~25% of the canvas height; the
// right-side drawers span from the canvas top down to the dock, so the two
// tile the right column without overlap. These are reactive DEFAULTS — they
// apply until the user commits an explicit size (then intent wins).
const tablePreviewHeight = computed(() => Math.max(120, Math.floor(availableHeight.value * 0.25)));
const drawerHeightOverride = (d: DrawerDef): number | undefined => {
  if (d.id === "bottomDock") return tablePreviewHeight.value;
  if (d.side === "right") return Math.max(200, availableHeight.value - tablePreviewHeight.value);
  return undefined;
};
const DATA_ACTIONS_WIDTH = 230;
// The bottom dock starts right next to the docked palette, tracking its live
// width (only while the user hasn't chosen an explicit dock offset).
const dataActionsWidth = computed(() => {
  const intent = itemStore.items["dataActions"];
  if (!intent || intent.dock !== "left") return 0;
  return intent.h.size ?? DATA_ACTIONS_WIDTH;
});
const drawerLeftOverride = (d: DrawerDef): number | undefined =>
  d.id === "bottomDock" ? dataActionsWidth.value : undefined;
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

useFlowHotkeys({
  flowId: () => flowStore.flowId,
  selectAll: () => addSelectedNodes(instance.getNodes.value),
  newFlow: () => emit("new"),
  save: () => emit("save", flowStore.flowId),
  run: () => emit("run", flowStore.flowId),
  toggleCodeGenerator: () => nodeStore.toggleCodeGenerator(),
  openFlowSettings: () => emit("openSettings"),
  openFile: () => emit("open"),
  toggleAiDrawer: () => editorStore.toggleAiDrawer(),
});

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
  drawerStore.clearPreview();
  nodeStore.nodeId = -1;
  editorStore.activeDrawerComponent = null;
  nodeStore.hideLogViewer();
  ghostNode.onViewportClick();
  clickedPosition.value = {
    x: event.x,
    y: event.y,
  };
  window.getSelection()?.removeAllRanges();
};

// VueFlow only emits @pane-click — there's no @pane-dblclick. We listen for
// native dblclick on <main> instead, then ignore double-clicks that landed on
// a node, edge, handle, or any floating panel. What's left is the empty pane.
const handleMainDblClick = (event: MouseEvent) => {
  const target = event.target as HTMLElement | null;
  if (!target) return;

  // Double-click on a node body: open Settings AND the Data preview.
  const nodeEl = target.closest(".vue-flow__node") as HTMLElement | null;
  if (nodeEl) {
    // Leave the description editor (node header) alone — it owns dblclick there.
    if (target.closest(".custom-node-header")) return;
    const dataId = nodeEl.getAttribute("data-id");
    if (dataId && !isGroupNodeId(dataId)) {
      const id = parseInt(dataId);
      openNodeSettings(id);
      openNodeData(id);
    }
    return;
  }

  if (
    target.closest(".overlay") ||
    target.closest(".vue-flow__edge") ||
    target.closest(".vue-flow__handle") ||
    target.closest(".vue-flow__minimap") ||
    target.closest(".layout-widget-wrapper")
  ) {
    return;
  }
  // Hide every floating overlay (right-side + bottom). Left palette stays.
  editorStore.hideAllPanels();
  nodeStore.nodeId = -1;
  window.getSelection()?.removeAllRanges();
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
    flowStore.fetchSettingsValidation(flowIdAtStart);
  } finally {
    // Only clear if we're still the most recent run — otherwise the newer
    // run's spinner would be turned off prematurely.
    if (myToken === loadToken) isLoadingFlow.value = false;
  }
};

const reloadCurrentFlow = () => loadFlow();

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
  // Every handle of a dynamic_inputs node (run_flow) is single-edge, including
  // the parameter handle input-0.
  if (
    targetTemplate &&
    (targetTemplate.dynamic_inputs || !targetTemplate.multi) &&
    connection.targetHandle
  ) {
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
  const sourceId = parseInt(params.source, 10);
  const targetId = parseInt(params.target, 10);
  if (Number.isFinite(sourceId) && Number.isFinite(targetId)) {
    useTutorialStore().notify({ type: "edge-connected", sourceId, targetId });
  }
  if (response?.history) {
    flowStore.updateHistoryState(response.history);
  }
  flowStore.fetchSettingsValidation();
}

// Open + front a node's Settings drawer. Settings only ever opens by setting
// nodeStore.nodeId, which the per-node nodeButton watcher turns into openDrawer.
const openNodeSettings = async (nodeId: number) => {
  if (isGroupNodeId(String(nodeId))) return;
  if (nodeStore.nodeId === nodeId && editorStore.drawerOpen) {
    // Already the selected node and the drawer is open — just front it. (Don't
    // re-trigger the watcher; that would reload settings, e.g. the second click
    // of a double-click.)
    itemStore.bringToFront("rightDrawer");
    drawerStore.setActiveTab("rightDrawer", "settings");
    useTutorialStore().notify({ type: "node-settings-opened", nodeId });
    return;
  }
  if (nodeStore.nodeId === nodeId) {
    // Same node but the drawer is closed/minimized: assigning the same value is
    // a no-op and won't reopen it. Bounce through -1 across a tick so the
    // watcher observes old(-1)->new and re-runs openDrawer.
    nodeStore.nodeId = -1;
    await nextTick();
  }
  nodeStore.nodeId = nodeId;
  await nextTick(); // settings tab mounts before fronting
  itemStore.bringToFront("rightDrawer");
  drawerStore.setActiveTab("rightDrawer", "settings");
  useTutorialStore().notify({ type: "node-settings-opened", nodeId });
};

// Open + front the bottom Data preview for a node. The dock's Data tab reacts to
// drawerStore.previewNodeId; re-selecting the same node bumps a refresh token.
const openNodeData = (nodeId: number) => {
  if (isGroupNodeId(String(nodeId))) return;
  drawerStore.setPreviewNode(nodeId);
  drawerStore.setActiveTab("bottomDock", "data");
  nextTick().then(() => itemStore.bringToFront("bottomDock"));
};

const nodeClick = (mouseEvent: any) => {
  // Single click opens Settings; if the dock is already open (data or logs), show this node's Data.
  const rawId = String(mouseEvent.node.id);
  openNodeSettings(parseInt(rawId));
  const dockOpen = drawerStore.previewNodeId !== null || editorStore.isShowingLogViewer;
  if (!isGroupNodeId(rawId) && dockOpen) {
    drawerStore.setPreviewNode(parseInt(rawId));
    drawerStore.setActiveTab("bottomDock", "data");
  }
};

// The per-node right-click menu (NodeWrapper) can't reach Canvas-local refs, so
// it asks via these editor-store counters. Watch the token, dispatch by node id.
watch(
  () => editorStore.nodeSettingsOpenRequest.token,
  () => openNodeSettings(editorStore.nodeSettingsOpenRequest.nodeId),
);
watch(
  () => editorStore.nodeDataOpenRequest.token,
  () => openNodeData(editorStore.nodeDataOpenRequest.nodeId),
);

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
      useTutorialStore().notify({ type: "node-removed", nodeId: nodeChangeId });
    }
  }
  if (lastResponse) {
    flowStore.fetchSettingsValidation();
  }
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
  if (lastResponse) {
    flowStore.fetchSettingsValidation();
  }
  if (lastResponse?.history) {
    flowStore.updateHistoryState(lastResponse.history);
  }
};

// OS file drops are consumed synchronously by fileDrop; anything else is a palette drag.
const handleDrop = async (event: DragEvent) => {
  if (fileDrop.handleDrop(event)) return;
  if (!nodeStore.isRunning) {
    const response = await onDrop(event, flowStore.flowId);
    if (response?.history) {
      flowStore.updateHistoryState(response.history);
    }
  }
};

const handleDragOver = (event: DragEvent) => {
  if (fileDrop.handleDragOver(event)) return;
  onDragOver(event);
};

// Paste the node buffer at screen coordinates. The buffer only exists when a
// copy armed it; failures (including cross-flow copies whose source flow is
// gone) surface as a toast instead of silence.
const pasteNodeFromBuffer = async (x: number, y: number) => {
  const buffer = readNodeClipboardBuffer();
  if (!buffer) return;
  const flowPosition = screenToFlowCoordinate({ x, y });

  if (buffer.multi) {
    const response = await createMultiCopyNodes(
      buffer.multi,
      flowPosition.x,
      flowPosition.y,
      flowStore.flowId,
    );
    if (response?.history) {
      flowStore.updateHistoryState(response.history);
    } else if (!response) {
      ElMessage.error("Failed to paste nodes");
    }
    return;
  }

  if (!buffer.single) return;
  const nodeCopyInput: NodeCopyInput = {
    ...buffer.single,
    posX: flowPosition.x,
    posY: flowPosition.y,
    flowId: flowStore.flowId,
  };
  const response = await createCopyNode(nodeCopyInput);
  if (response?.history) {
    flowStore.updateHistoryState(response.history);
  } else if (!response) {
    ElMessage.error("Failed to paste node");
  }
};

// Guards two fast Cmd+Vs from creating two nodes at identical coordinates
// while the first request is still in flight.
let pasteInFlight = false;

const handleCanvasPaste = async (x: number, y: number, intent: PasteIntent) => {
  // The document-level ClipboardEvent listener bypasses the empty-state
  // overlay — never paste against a dead flow id.
  if (flowStore.flowId <= 0) return;
  if (intent.kind === "none" || pasteInFlight) return;
  pasteInFlight = true;
  try {
    if (intent.kind === "node") {
      await pasteNodeFromBuffer(x, y);
      return;
    }
    const flowPosition = screenToFlowCoordinate({ x, y });
    const response = await createManualInputFromClipboard(
      flowStore.flowId,
      flowPosition.x,
      flowPosition.y,
      intent.text,
    );
    if (response?.history) {
      flowStore.updateHistoryState(response.history);
    }
  } finally {
    pasteInFlight = false;
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
    // Explicit intent: the user picked "Paste Node" from the menu, so the
    // buffer pastes without any clipboard read or sentinel check (a read is
    // impossible outside a paste event on web Firefox / insecure contexts).
    handleCanvasPaste(position.x, position.y, { kind: "node" });
  } else if (actionId === "generate-documentation") {
    // — pull the canonical flow name server-side so the doc title
    // matches what the user sees in the title bar. Falsy → undefined so
    // the store falls back to ``flow ${flowId}``.
    if (flowStore.flowId <= 0) return;
    const settings = await FlowApi.getFlowSettings(flowStore.flowId);
    const name = settings?.display_name?.trim() || settings?.name?.trim() || undefined;
    await aiStore.generateDocumentation(flowStore.flowId, name);
  } else if (actionId === "add-descriptions-all") {
    // Bulk variant of the per-node ✨ "Add description" action. Confirms
    // first since each node is a separate LLM call (cost + time scale
    // linearly with N), then streams them sequentially through the quiet
    // ai-store path that writes straight to node.setting_input.description
    // without flooding the chat drawer.
    if (flowStore.flowId <= 0) return;
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
    if (flowStore.flowId <= 0) return;
    const question = await promptLineageQuestion("this flow");
    if (!question) return;
    await aiStore.askLineageQuestion(flowStore.flowId, question);
  } else if (actionId === "ask-lineage-node") {
    // — focused lineage Q&A on a single node id.
    if (flowStore.flowId <= 0) return;
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

// Clipboard events aimed at <body> carry no location information, so they are
// classified by the last pointerdown target (see isCanvasClipboardTarget).
let lastPointerDownEl: Element | null = null;
const trackPointerDown = (event: PointerEvent) => {
  if (event.target instanceof Element) {
    lastPointerDownEl = event.target;
  }
};

// Live pointer position over the canvas: a keyboard paste drops the node under
// the cursor (falling back to the canvas centre before any pointer movement).
let lastPointerPosition: CursorPosition | null = null;
const trackPointerMove = (event: PointerEvent) => {
  lastPointerPosition = { x: event.clientX, y: event.clientY };
};
const keyboardPastePosition = (): CursorPosition => {
  if (lastPointerPosition) return lastPointerPosition;
  const bounds = mainContainerRef.value?.getBoundingClientRect();
  return bounds
    ? { x: bounds.left + bounds.width / 2, y: bounds.top + bounds.height / 2 }
    : { x: 0, y: 0 };
};

// Native `copy`/`paste` ClipboardEvents drive canvas node copy/paste. They fire
// for both browser shortcuts (web mode) and the Tauri Edit-menu copy:/paste:
// roles (desktop). Keydown can't be used on macOS desktop because the menu
// accelerator consumes Cmd+C / Cmd+V before it reaches the keydown handler.
// Both handlers decide intent first and only then cancel the native action.
const handleCopyEvent = (event: ClipboardEvent) => {
  if (event.defaultPrevented) return; // someone (e.g. the data grid) already handled it
  if (!isCanvasClipboardTarget(event.target, lastPointerDownEl)) return;
  const sentinel = copyNodesToBuffer(
    instance.getSelectedNodes.value,
    instance.getEdges.value,
    flowStore.flowId,
  );
  if (!sentinel) return; // nothing selected — leave the native copy alone
  if (event.clipboardData) {
    event.clipboardData.setData("text/plain", sentinel);
    event.preventDefault(); // required in the same handler, or setData is discarded
  } else {
    void writeSentinelToOsClipboard(sentinel);
  }
};

const handlePasteEvent = (event: ClipboardEvent) => {
  if (event.defaultPrevented) return; // CodeMirror preventDefaults pastes it consumes
  if (!isCanvasClipboardTarget(event.target, lastPointerDownEl)) return;
  if (flowStore.flowId <= 0) return;
  const text = event.clipboardData?.getData("text/plain") ?? null;
  const intent = resolvePasteIntent({
    buffer: readNodeClipboardBuffer(),
    clipboardText: text,
    isCanvasTarget: true,
  });
  if (intent.kind === "none") return; // not ours — never swallow a paste we don't handle
  event.preventDefault();
  const position = keyboardPastePosition();
  void handleCanvasPaste(position.x, position.y, intent);
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
  if (flowStore.flowId <= 0) return;
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

let unregisterContainer: (() => void) | null = null;
let unlistenViewZoom: (() => void) | null = null;

onMounted(async () => {
  if (mainContainerRef.value) {
    // Single shared container measurement for every overlay panel (and the
    // derived drawer heights above).
    unregisterContainer = itemStore.registerContainer(mainContainerRef.value);
  }
  document.addEventListener("copy", handleCopyEvent);
  document.addEventListener("paste", handlePasteEvent);
  // Capture phase so panels that stopPropagation on pointerdown still register.
  document.addEventListener("pointerdown", trackPointerDown, true);

  // Drive canvas zoom from the native View menu / Cmd+`+`/`-`/`0` (desktop only).
  if (isDesktop) {
    unlistenViewZoom = await desktop.onViewZoom((direction) => {
      if (direction === "in") {
        instance.zoomIn();
      } else if (direction === "out") {
        instance.zoomOut();
      } else {
        instance.zoomTo(1);
      }
    });
  }

  nodeStore.setVueFlowInstance(instance);

  watch(
    () => flowStore.flowId,
    async (id) => {
      // Switching flows: clear selection + overlays so a drawer/preview from the
      // previous flow can't leak (node ids collide across flows).
      editorStore.hideAllPanels();
      nodeStore.nodeId = -1;
      nodeStore.nodeData = null;
      nodeStore.nodeDataFlowId = -1;
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
        flowStore.fetchSettingsValidation();
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
  document.removeEventListener("copy", handleCopyEvent);
  document.removeEventListener("paste", handlePasteEvent);
  document.removeEventListener("pointerdown", trackPointerDown, true);
  unlistenViewZoom?.();
  unlistenViewZoom = null;
  unregisterContainer?.();
  unregisterContainer = null;
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
      @dragover="handleDragOver"
      @dragenter="fileDrop.handleDragEnter"
      @dragleave="fileDrop.handleDragLeave"
      @dragend="fileDrop.handleDragEnd"
      @dblclick="handleMainDblClick"
      @pointermove="trackPointerMove"
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
      </VueFlow>
      <AiGhostNode :composable="ghostNode" @accepted="reloadCurrentFlow" />
      <FileDropOverlay :controller="fileDrop" />
      <FileDropUploadDialog :controller="fileDrop" />
      <context-menu
        v-if="showContextMenu"
        :x="clickedPosition.x"
        :y="clickedPosition.y"
        :target-type="contextMenuTarget.type"
        :target-id="contextMenuTarget.id"
        :target-in-group="contextMenuTargetInGroup"
        :can-paste-node="isNodeBufferArmed()"
        :on-close="closeContextMenu"
        @action="handleContextMenuAction"
      />
      <!-- Floating panels sit above the DesignerView empty-state overlay
           (z-index 100000) — hide them while no flow is open. -->
      <draggable-item
        v-if="flowStore.flowId > 0"
        id="dataActions"
        :show-left="true"
        :initial-width="DATA_ACTIONS_WIDTH"
        initial-position="left"
        height-behaviour="scale"
        title="Data actions"
        :allow-free-move="true"
      >
        <NodeList @dragstart="onDragStart" />
      </draggable-item>
      <TabbedDrawer
        v-for="d in drawers"
        :key="d.id"
        :def="d"
        :height-override="drawerHeightOverride(d)"
        :left-override="drawerLeftOverride(d)"
      />
      <AiCommandPalette />
      <layoutControls v-if="flowStore.flowId > 0" @reset-layout-graph="handleResetLayoutGraph" />
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
