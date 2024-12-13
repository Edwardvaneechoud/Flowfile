<script lang="ts" setup>
import { nextTick, ref, toRaw } from "vue";
import { CodeLoader } from "vue-content-loader";
import {
  GraphicWalkerInput,
  NodeGraphicWalker,
  IRow,
  IMutField,
  IChart,
} from "./vueGraphicWalker/interfaces";
import VueGraphicWalker from "./vueGraphicWalker/VueGraphicWalker.vue";
import {
  useGraphicWalkerStore,
  resetGraphicWalkerStore,
} from "./vueGraphicWalker/GraphicWalkerStore"; // Adjust the path to where your global store is defined
import { fetchGraphicWalkerData } from "./vueGraphicWalker/utils";
import { useNodeStore } from "../../../../../stores/column-store";
const nodeGraphicWalker = ref<null | NodeGraphicWalker>(null);
const graphicWalkerInput = ref<null | GraphicWalkerInput>(null);
const dataLoaded = ref(false);
const chartList = ref<IChart[]>([]);
const data = ref<IRow[]>([]);
const fields = ref<IMutField[]>([]);
const nodeStore = useNodeStore();
const graphicWalkerStore = useGraphicWalkerStore();
const globalNodeId = ref(-1);
const testMode = ref(false);

interface GraphicWalkerRef {
  waitForMethodToBeAvailable: (timeout?: number) => Promise<void>;
}

const graphicWalkerRef = ref<GraphicWalkerRef | null>(null);

const loadData = (graphicWalkerData: GraphicWalkerInput) => {
  fields.value = graphicWalkerData.dataModel.fields;
  data.value = graphicWalkerData.dataModel.data;
  chartList.value = graphicWalkerData.specList;
};

const loadNodeData = async (nodeId: number) => {
  globalNodeId.value = nodeId;
  nodeGraphicWalker.value = await fetchGraphicWalkerData(1, nodeId);
  graphicWalkerInput.value = nodeGraphicWalker.value.graphic_walker_input;
  loadData(graphicWalkerInput.value);
  dataLoaded.value = true;
  await nextTick();
  await graphicWalkerRef.value?.waitForMethodToBeAvailable();
  console.log("method available");
  await nextTick();
  if (graphicWalkerInput.value.specList && graphicWalkerStore.current) {
    graphicWalkerStore.current.importCode(JSON.parse(JSON.stringify(chartList.value)));
    console.log("specList found and imported");
  } else {
    console.log("No specList found");
  }
};

const action = () => {
  if (graphicWalkerStore.current) {
    console.log(graphicWalkerStore.current.exportCode());
  }
};

const pushNodeData = async () => {
  console.log("pushNodeData");
  if (nodeGraphicWalker.value && graphicWalkerStore.current) {
    const d = toRaw(graphicWalkerStore.current.exportCode()[0]);
    nodeGraphicWalker.value.graphic_walker_input.specList = JSON.parse(JSON.stringify([d]));
    nodeGraphicWalker.value.graphic_walker_input.dataModel.data = [];
    nodeGraphicWalker.value.graphic_walker_input.dataModel.fields = [];
  }
  nodeStore.node_id = globalNodeId.value;
  await nodeStore.updateSettings(nodeGraphicWalker);
  nextTick();
  resetGraphicWalkerStore();
};
defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<template>
  <button v-if="testMode" @click="action">Export</button>
  <div v-if="dataLoaded">
    <VueGraphicWalker
      ref="graphicWalkerRef"
      appearance="light"
      :store-ref="graphicWalkerStore"
      :data="data"
      :fields="fields"
    />
  </div>
  <CodeLoader v-else />
</template>
