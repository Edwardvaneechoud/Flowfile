<script setup lang="ts">
import { ref, markRaw, onMounted, onUnmounted, defineExpose, nextTick, defineEmits } from "vue";
import {
  VueFlow,
  NodeTypesObject,
  NodeComponent,
  Node,
  useVueFlow,
  ConnectionMode,
} from "@vue-flow/core";
import { MiniMap } from "@vue-flow/minimap";

import CustomNode from "../../components/nodes/NodeWrapper.vue";
import useDragAndDrop from "./useDnD";
import CodeGenerator from "./CodeGenerator/CodeGenerator.vue";
import NodeList from "./NodeList.vue";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import NodeSettingsDrawer from "./NodeSettingsDrawer.vue";
import {
  getFlowData,
  deleteConnection,
  deleteNode,
  connectNode,
  NodeConnection,
} from "./backendInterface";
import DraggableItem from "../../components/common/DraggableItem/DraggableItem.vue";
import layoutControls from "../../components/common/DraggableItem/layoutControls.vue";
import { useItemStore } from "../../components/common/DraggableItem/stateStore";
import DataPreview from "../../features/designer/dataPreview.vue";
import FlowResults from "../../features/designer/editor/results.vue";
import LogViewer from "./LogViewer/LogViewer.vue";
import ContextMenu from "./ContextMenu.vue";
import { NodeCopyInput, NodeCopyValue, MultiNodeCopyValue, EdgeCopyValue, ContextMenuAction, CursorPosition } from "./types";
import { applyStandardLayout } from "./editorLayoutInterface";

const itemStore = useItemStore();
const availableHeight = ref(0);
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const rawCustomNode = markRaw(CustomNode);
const { updateEdge, addEdges, fitView, screenToFlowCoordinate } = useVueFlow();
const vueFlow = ref<InstanceType<typeof VueFlow>>();
const nodeTypes: NodeTypesObject = {
  "custom-node": rawCustomNode as NodeComponent,
};
const nodes = ref<Node[]>([]);
const edges = ref([]);
const instance = useVueFlow();
const showTablePreview = ref(false);
const mainContainerRef = ref<HTMLElement | null>(null);
const { onDrop, onDragOver, onDragStart, importFlow, createCopyNode, createMultiCopyNodes } = useDragAndDrop();
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
  const vueFlowInput = await getFlowData(nodeStore.flow_id);
  await nextTick();
  await importFlow(vueFlowInput);
};

const selectNodeExternally = (nodeId: number) => {
  showTablePreview.value = true;

  setNodeTableView(nodeId);
  nextTick().then(() => {
    setNodeTableView(nodeId);
  });
  fitView({ nodes: [nodeId.toString()] });
};

async function onConnect(params: any) {
  if (params.target && params.source) {
    const nodeConnection: NodeConnection = {
      input_connection: {
        node_id: parseInt(params.target),
        connection_class: params.targetHandle,
      },
      output_connection: {
        node_id: parseInt(params.source),
        connection_class: params.sourceHandle,
      },
    };
    await connectNode(nodeStore.flow_id, nodeConnection);
    addEdges([params]);
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

const handleNodeChange = (nodeChangesEvent: any) => {
  const nodeChanges = nodeChangesEvent as NodeChange[];
  const nodeChange = nodeChanges[0];
  const nodeChangeId = Number(nodeChange.id);
  if (nodeChange.type === "remove") {
    deleteNode(nodeStore.flow_id, nodeChangeId);
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

const handleEdgeChange = (edgeChangesEvent: any) => {
  const edgeChanges = edgeChangesEvent as EdgeChange[];
  if (edgeChanges.length >= 2) {
    console.log("Edge changes length is 2 so coming from a node change event");
    return;
  }
  for (const edgeChange of edgeChanges) {
    if (edgeChange.type === "remove") {
      const nodeConnection = convertEdgeChangeToNodeConnection(edgeChange);
      deleteConnection(nodeStore.flow_id, nodeConnection);
    }
  }
};

const handleDrop = (event: DragEvent) => {
  if (!nodeStore.isRunning) {
    onDrop(event, nodeStore.flow_id);
  }
};

// Helper to convert snake_case to snake_case (identity) and handle node type
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
      flowIdToCopyFrom: nodeStore.flow_id,
      multi: node.data.nodeTemplate?.multi,
      nodeTemplate: node.data.nodeTemplate,
    };
    localStorage.setItem("copiedNode", JSON.stringify(nodeCopyValue));
    localStorage.removeItem("copiedMultiNodes");
  } else {
    // Multiple nodes copy - use new multi-node format
    const selectedNodeIds = new Set(selectedNodes.map((n) => n.data.id));

    const nodes: NodeCopyValue[] = selectedNodes.map((node) => ({
      nodeIdToCopyFrom: node.data.id,
      type: node.data.nodeTemplate?.item || node.data.component?.__name || "unknown",
      label: node.data.label,
      description: "",
      numberOfInputs: node.data.inputs.length,
      numberOfOutputs: node.data.outputs.length,
      typeSnakeCase:
        node.data.nodeTemplate?.item || toSnakeCase(node.data.component?.__name || "unknown"),
      flowIdToCopyFrom: nodeStore.flow_id,
      multi: node.data.nodeTemplate?.multi,
      nodeTemplate: node.data.nodeTemplate,
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
        sourceHandle: edge.sourceHandle || "output-0",
        targetHandle: edge.targetHandle || "input-0",
      }));

    const multiNodeCopyValue: MultiNodeCopyValue = {
      nodes,
      edges,
      flowIdToCopyFrom: nodeStore.flow_id,
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
    await createMultiCopyNodes(multiNodeCopyValue, flowPosition.x, flowPosition.y, nodeStore.flow_id);
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
    flowId: nodeStore.flow_id,
  };
  createCopyNode(nodeCopyInput);
};

// Group selected nodes into a visual group
const groupSelectedNodes = () => {
  const selectedNodes = instance.getSelectedNodes.value;

  if (selectedNodes.length < 2) {
    return;
  }

  // Calculate the bounding box of selected nodes
  let minX = Infinity, minY = Infinity;
  let maxX = -Infinity, maxY = -Infinity;

  for (const node of selectedNodes) {
    const nodeWidth = 200; // Approximate node width
    const nodeHeight = 100; // Approximate node height

    minX = Math.min(minX, node.position.x);
    minY = Math.min(minY, node.position.y);
    maxX = Math.max(maxX, node.position.x + nodeWidth);
    maxY = Math.max(maxY, node.position.y + nodeHeight);
  }

  // Add padding around the group
  const padding = 40;
  const groupX = minX - padding;
  const groupY = minY - padding - 30; // Extra space for label
  const groupWidth = maxX - minX + padding * 2;
  const groupHeight = maxY - minY + padding * 2 + 30;

  // Create a unique group ID
  const groupId = `group-${Date.now()}`;

  // Create the group node
  const groupNode = {
    id: groupId,
    type: "group",
    position: { x: groupX, y: groupY },
    style: {
      width: `${groupWidth}px`,
      height: `${groupHeight}px`,
      backgroundColor: "rgba(59, 130, 246, 0.08)",
      border: "2px dashed rgba(59, 130, 246, 0.4)",
      borderRadius: "12px",
    },
    data: {
      label: "Node Group",
    },
    selectable: true,
    draggable: true,
  };

  // Add the group node first
  instance.addNodes([groupNode]);

  // Update selected nodes to be children of the group
  const nodeUpdates = selectedNodes.map((node: any) => ({
    id: node.id,
    parentNode: groupId,
    extent: "parent" as const,
    position: {
      x: node.position.x - groupX,
      y: node.position.y - groupY,
    },
  }));

  // Update each node to be a child of the group
  for (const update of nodeUpdates) {
    instance.updateNode(update.id, {
      parentNode: update.parentNode,
      extent: update.extent,
      position: update.position,
    });
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
    copyValue(position.x, position.y);
  } else if (actionId === "group-nodes") {
    groupSelectedNodes();
  }
};

const handleResetLayoutGraph = async () => {
  await applyStandardLayout(nodeStore.flow_id);
  loadFlow();
};

const hideLogViewer = () => {
  editorStore.hideLogViewerForThisRun = true;
  nodeStore.hideLogViewer();
};

const handleKeyDown = (event: KeyboardEvent) => {
  let eventKeyClicked = event.ctrlKey || event.metaKey;
  // Normalize key to lowercase to handle Caps Lock being on
  const key = event.key.toLowerCase();

  // Skip if typing in an input field
  const target = event.target as HTMLElement;
  const isInputElement =
    target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable;

  if (eventKeyClicked && key === "c" && !isInputElement) {
    // Copy selected nodes
    copySelectedNodes();
    event.preventDefault();
  } else if (eventKeyClicked && key === "v" && !isInputElement) {
    // Paste nodes
    copyValue(clickedPosition.value.x, clickedPosition.value.y);
    event.preventDefault();
  } else if (eventKeyClicked && key === "s") {
    if (nodeStore.flow_id) {
      event.preventDefault();
      emit("save", nodeStore.flow_id);
    }
  } else if (eventKeyClicked && key === "e") {
    if (nodeStore.flow_id) {
      event.preventDefault();
      emit("run", nodeStore.flow_id);
    }
  } else if (eventKeyClicked && event.shiftKey && key === "g" && !isInputElement) {
    // Cmd+Shift+G: Group selected nodes
    event.preventDefault();
    groupSelectedNodes();
  } else if (eventKeyClicked && key === "g") {
    if (nodeStore.flow_id) {
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

onMounted(async () => {
  availableHeight.value = window.innerHeight - 50;
  tablePreviewHeight.value = availableHeight.value * 0.25; // 30% of the available height
  nodeSettingsHeight.value = availableHeight.value * 0.75; // 70% of the available height
  window.addEventListener("keydown", handleKeyDown);

  nodeStore.setVueFlowInstance(instance);
  loadFlow();
});

onUnmounted(() => {
  window.removeEventListener("keydown", handleKeyDown);
});

defineExpose({
  loadFlow,
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
        class="custom-node-flow"
        :connection-mode="ConnectionMode.Strict"
        :default-viewport="{ zoom: 1 }"
        @edge-update="onEdgeUpdate"
        @connect="onConnect"
        @pane-click="handleCanvasClick"
        @node-click="nodeClick"
        @nodes-change="handleNodeChange"
        @edges-change="handleEdgeChange"
        @pane-context-menu="handleContextMenu"
        @click="closeContextMenu"
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
        :selected-nodes-count="instance.getSelectedNodes.value.length"
        @action="handleContextMenuAction"
      />
    </main>
    <draggable-item
      id="dataActions"
      :show-left="true"
      :initial-width="180"
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

.custom-node-flow .vue-flow__edges {
  filter: invert(100%);
}

.animated-bg-gradient {
  background: linear-gradient(122deg, #6f3381, #81c7d4, #fedfe1, #fffffb);
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
