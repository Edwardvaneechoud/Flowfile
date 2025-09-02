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

import CustomNode from "./CustomNode.vue";
import useDragAndDrop from "./useDnD";
import CodeGenerator from "./codeGenerator/CodeGenerator.vue";
import NodeList from "./NodeList.vue";
import { useNodeStore } from "../../../../stores/column-store";
import {
  getFlowData,
  deleteConnection,
  deleteNode,
  connectNode,
  NodeConnection,
} from "./backendInterface";
import DraggableItem from "./DraggableItem/DraggableItem.vue";
import layoutControls from "./DraggableItem/layoutControls.vue";
import DataPreview from "../../dataPreview.vue";
import FlowResults from "../../editor/results.vue";
import LogViewer from "./canvasFlow/LogViewer.vue";
import ContextMenu from "./ContextMenu.vue";
import { NodeCopyInput, NodeCopyValue, ContextMenuAction, CursorPosition } from "./types";

const availableHeight = ref(0);
const nodeStore = useNodeStore();
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
const { onDrop, onDragOver, onDragStart, importFlow, createCopyNode } = useDragAndDrop();
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
  console.log("Closing drawer")
  nodeStore.closeDrawer();
  showTablePreview.value = false;
  nodeStore.hideLogViewer();
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
  // nodeStore.node_id = nodeId;
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
  console.log(mouseEvent)
  showTablePreview.value = true;
  nextTick().then(() => {
    console.log(mouseEvent.node.id)
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
  console.log(dataPreview.value);
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
  console.log("Edge changes", edgeChanges);
  for (const edgeChange of edgeChanges) {
    if (edgeChange.type === "add") {
      console.log("This edge change does not work");
    } else if (edgeChange.type === "remove") {
      const nodeConnection = convertEdgeChangeToNodeConnection(edgeChange);
      console.log("Removing connection", nodeConnection);
      deleteConnection(nodeStore.flow_id, nodeConnection);
    }
  }
};

const handleOpenDrawer = async (nodeId: number) => {
  console.log("Opening drawer for node:", nodeId);
  
  // Check if drawer is already open for this node
  if (nodeStore.node_id === nodeId && nodeStore.isDrawerOpen) {
    console.log("Drawer already open for this node");
    return;
  }
  nodeStore.closeDrawer();
  nodeStore.node_id = nodeId;
  nodeStore.isDrawerOpen = true;
  await nextTick();
  window.dispatchEvent(new CustomEvent('open-node-drawer', { 
    detail: { nodeId } 
  }));
};

const handleDrop = (event: DragEvent) => {
  if (!nodeStore.isRunning) {
    onDrop(event, nodeStore.flow_id);
  }
};

const copyValue = async (x: number, y: number) => {
  const flowPosition = screenToFlowCoordinate({
    x: x,
    y: y,
  });
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

const handleContextMenuAction = async (actionData: ContextMenuAction) => {
  const { actionId, targetType, targetId, position } = actionData;
  if (actionId === "fit-view") {
    fitView();
  } else if (actionId === "zoom-in") {
    instance.zoomIn();
  } else if (actionId === "zoom-out") {
    instance.zoomOut();
  } else if (actionId === "paste-node") {
    copyValue(position.x, position.y);
  }
};

const handleKeyDown = (event: KeyboardEvent) => {
  let eventKeyClicked = event.ctrlKey || event.metaKey;
  if (eventKeyClicked && event.key === "v" && event.target) {
    const target = event.target as HTMLElement;
    const isInputElement =
      target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable;
    if (isInputElement) {
      return;
    }
    copyValue(clickedPosition.value.x, clickedPosition.value.y);
    event.preventDefault();
  } else if (eventKeyClicked && event.key == "s") {
    if (nodeStore.flow_id) {
      event.preventDefault();
      emit("save", nodeStore.flow_id);
    }
  } else if (eventKeyClicked && event.key == "e") {
    if (nodeStore.flow_id) {
      event.preventDefault();
      emit("run", nodeStore.flow_id);
    }
  } else if (eventKeyClicked && event.key == "g") {
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
        @action="handleContextMenuAction"
      />
    </main>
    {{ nodeStore.node_id }}
    <draggable-item
      id="dataActions"
      :show-left="true"
      :initial-width="200"
      initial-position="left"
      title="Data actions"
      :allow-free-move="true"
      :prevent-overlap="false" >
      <NodeList @dragstart="onDragStart" />
    </draggable-item>
    <draggable-item
      v-if="nodeStore.isShowingLogViewer"
      id="logViewer"
      :show-bottom="true"
      title="Log overview"
      :allow-full-screen="true"
      initial-position="bottom"
      :initial-left="200"
      :on-minize="nodeStore.toggleLogViewer"
      group="bottomPanels" :sync-dimensions="true" :prevent-overlap="false" >
      <LogViewer :flow-id="1" />
    </draggable-item>
    <draggable-item
      v-if="nodeStore.showFlowResult"
      id="flowresults"
      :show-right="true"
      title="flow results"
      initial-position="right"
      :initial-width="400"
      :initial-top="-50"
      group="rightPanels" :prevent-overlap="false" >
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
      :initial-left="200"
      group="bottomPanels" :sync-dimensions="true" :prevent-overlap="false" >
      <data-preview ref="dataPreview"> text </data-preview>
    </draggable-item>
    <draggable-item
      v-if="nodeStore.isDrawerOpen"
      id="nodeSettings"
      :show-right="true"
      initial-position="right"
      :initial-top="0"
      :initial-width="800"
      :initial-height="nodeSettingsHeight"
      title="Node Settings"
      :on-minize="nodeStore.closeDrawer"
      :allow-full-screen="true"
      group="rightPanels" :prevent-overlap="false" >
      <div id="nodesettings" class="content"></div>
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
    <layoutControls/>
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
