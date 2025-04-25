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
const flowHasRun = ref(false);


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
  dataLoaded.value = false;
  flowHasRun.value = true;
  globalNodeId.value = nodeId;
    try {
      nodeGraphicWalker.value = await fetchGraphicWalkerData(nodeStore.flow_id, nodeId);
    } catch (error: any) {
      console.error("Error loading GraphicWalker data:", error);

      if (error.response && error.response.status === 422) {
        flowHasRun.value = false;
        return
    }
  }
  if (!nodeGraphicWalker.value) {
  return
  }
  console.log(nodeGraphicWalker.value.graphic_walker_input);
  flowHasRun.value = true;
  graphicWalkerInput.value = nodeGraphicWalker.value.graphic_walker_input;
  loadData(graphicWalkerInput.value);
  dataLoaded.value = true;
  await nextTick();
  await graphicWalkerRef.value?.waitForMethodToBeAvailable();
  
  console.log("method available");
  await nextTick();

  if (graphicWalkerInput.value.specList && graphicWalkerStore.current) {
    console.log("charlist.value", chartList.value)
    const rawChartList = toRaw(chartList.value);
    // const specToImport = JSON.parse(JSON.stringify(rawChartList));
    // graphicWalkerStore.current.importCode(specToImport);
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
    console.log("pushing data", d);
    nodeGraphicWalker.value.graphic_walker_input.specList = JSON.parse(JSON.stringify([d]));
    nodeGraphicWalker.value.graphic_walker_input.dataModel.data = [];
    nodeGraphicWalker.value.graphic_walker_input.dataModel.fields = [];
  }
  nodeStore.node_id = globalNodeId.value;
  console.log("nodeStore.node_id", nodeStore.node_id);
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
  
    <!-- Data loaded scenario -->
    <div v-if="dataLoaded">
    <VueGraphicWalker
      ref="graphicWalkerRef"
      appearance="light"
      :store-ref="graphicWalkerStore"
      :data="data"
      :fields="fields"
    />
  </div>

  <!-- No flow run scenario -->
  <div v-else-if="!flowHasRun" class="flow-not-run">
    <div class="warning-container">
      <svg class="warning-icon" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
        <line x1="12" y1="9" x2="12" y2="13"></line>
        <line x1="12" y1="17" x2="12.01" y2="17"></line>
      </svg>
      <div class="warning-message">
        <h3>Flow has not been run</h3>
        <p>This node needs to be run before visualization can be displayed.</p>
      </div>
    </div>
  </div>
  

  
  <!-- Loading scenario -->
  <CodeLoader v-else />
</template>

<style scoped>
.flow-not-run {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  padding: 2rem;
}

.warning-container {
  display: flex;
  align-items: flex-start;
  background-color: #FFF9E6;
  border: 1px solid #FFE58F;
  border-radius: 4px;
  padding: 1.5rem;
  max-width: 500px;
}

.warning-icon {
  color: #FAAD14;
  margin-right: 1rem;
  flex-shrink: 0;
}

.warning-message {
  color: #5F5F5F;
}

.warning-message h3 {
  margin-top: 0;
  margin-bottom: 0.5rem;
  color: #262626;
}

</style>