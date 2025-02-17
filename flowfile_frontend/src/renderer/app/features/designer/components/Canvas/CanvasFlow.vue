<script setup lang="ts">
import { ref, markRaw, onMounted, defineExpose, nextTick } from "vue";
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
import DataPreview from "../../dataPreview.vue";
import FlowResults from "../../editor/results.vue";
import LogViewer from "./canvasFlow/LogViewer.vue";

const availableHeight = ref(0);
const nodeStore = useNodeStore();
const rawCustomNode = markRaw(CustomNode);
const { updateEdge, addEdges, fitView } = useVueFlow();
const vueFlow = ref<InstanceType<typeof VueFlow>>();
const nodeTypes: NodeTypesObject = {
  "custom-node": rawCustomNode as NodeComponent,
};
const nodes = ref<Node[]>([]);
const edges = ref([]);
const instance = useVueFlow();
const showTablePreview = ref(false);
const mainContainerRef = ref<HTMLElement | null>(null);
const { onDrop, onDragOver, onDragStart, importFlow } = useDragAndDrop();
const dataPreview = ref<InstanceType<typeof DataPreview>>();
const tablePreviewHeight = ref(0);
const nodeSettingsHeight = ref(0);
const selectedNodeIdInTable = ref(0);
const logViewer = ref<InstanceType<typeof LogViewer>>();

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

const closeDrawer = () => {
  nodeStore.closeDrawer();
  showTablePreview.value = false;
  nodeStore.hideLogViewer();
};

const toggleShowTablePreview = () => {
  showTablePreview.value = !showTablePreview.value;
};

function onEdgeUpdate({ edge, connection }: { edge: any; connection: any }) {
  updateEdge(edge, connection);
}

const loadFlow = async () => {
  const vueFlowInput = await getFlowData();
  console.log("vueFlowInput", vueFlowInput);
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
  nodeStore.node_id = nodeId;
};

async function onConnect(params: any) {
  console.log("params", params);
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
    await connectNode(1, nodeConnection);
    addEdges([params]);
  }
}

onMounted(async () => {
  availableHeight.value = window.innerHeight - 50;
  tablePreviewHeight.value = availableHeight.value * 0.25; // 30% of the available height
  nodeSettingsHeight.value = availableHeight.value * 0.75; // 70% of the available height

  nodeStore.setVueFlowInstance(instance);
  loadFlow();
  nodeStore.setFlowId(1);
});

const NodeIsSelected = (nodeId: string) => {
  return selectedNodeIdInTable.value === +nodeId;
};

const nodeClick = (mouseEvent: any) => {
  showTablePreview.value = true;
  nextTick().then(() => {
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

defineExpose({
  loadFlow,
});

const handleNodeChange = (nodeChangesEvent: any) => {
  const nodeChanges = nodeChangesEvent as NodeChange[];
  const nodeChange = nodeChanges[0];
  const nodeChangeId = Number(nodeChange.id);
  if (nodeChange.type === "remove") {
    deleteNode(1, nodeChangeId);
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
      // const nodeConnection = convertEdgeChangeToNodeConnection(edgeChange)
      // connectNode(1, nodeConnection)
    } else if (edgeChange.type === "remove") {
      const nodeConnection = convertEdgeChangeToNodeConnection(edgeChange);
      console.log("Removing connection", nodeConnection);
      deleteConnection(1, nodeConnection);
    }
  }
};

const toggleShowRunResult = () => {
  nodeStore.showFlowResult = !nodeStore.showFlowResult;
};
</script>

<template>
  <div class="container">
    <main ref="mainContainerRef" @drop="onDrop" @dragover="onDragOver">
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
        @pane-click="closeDrawer"
        @node-click="nodeClick"
        @nodes-change="handleNodeChange"
        @edges-change="handleEdgeChange"
      >
        <MiniMap />
      </VueFlow>
    </main>
    <draggable-item
      id="dataActions"
      :show-left="true"
      :initial-width="200"
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
      :on-minize="nodeStore.toggleLogViewer"
    >
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
    >
      <data-preview ref="dataPreview" :flow-id="1"> text </data-preview>
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
    >
      <div id="nodesettings" class="content"></div>
    </draggable-item>
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
