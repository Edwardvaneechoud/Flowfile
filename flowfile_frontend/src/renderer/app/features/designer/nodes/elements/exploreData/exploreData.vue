// exploreData.vue

<script lang="ts" setup>
import { ref, computed } from "vue";
import { CodeLoader } from "vue-content-loader";
import type { IRow, IMutField, IChart } from "@kanaries/graphic-walker/dist/interfaces";
// Ensure this path points to the VueGraphicWalker component
// that internally manages the storeRef
import VueGraphicWalker from "./vueGraphicWalker/VueGraphicWalker.vue";
import type { NodeGraphicWalker, GraphicWalkerInput } from "./vueGraphicWalker/interfaces";
import { fetchGraphicWalkerData } from "./vueGraphicWalker/utils";
import { useNodeStore } from "../../../../../stores/column-store";
import { Console } from "console";

// --- Component State Refs ---
const isLoading = ref(false);
const nodeData = ref<NodeGraphicWalker | null>(null);
const chartList = ref<IChart[]>([]); // Holds initial spec list
const data = ref<IRow[]>([]);
const fields = ref<IMutField[]>([]);
const errorMessage = ref<string | null>(null);
const nodeStore = useNodeStore();
const globalNodeId = ref(-1);
const testMode = ref(true); // Button will be visible

const vueGraphicWalkerRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const canDisplayVisualization = computed(() => {
  return !isLoading.value && !errorMessage.value;
});

// --- Data Loading Logic ---
const loadNodeData = async (nodeId: number) => {
  isLoading.value = true;
  errorMessage.value = null;
  globalNodeId.value = nodeId;
  nodeData.value = null;
  data.value = [];
  fields.value = [];
  chartList.value = [];

  try {
    if (!nodeStore.flow_id) {
      throw new Error("Flow ID is missing in nodeStore.");
    }
    const fetchedNodeData = await fetchGraphicWalkerData(nodeStore.flow_id, nodeId);
    if (!fetchedNodeData?.graphic_walker_input) {
      throw new Error("Received invalid data structure from backend.");
    }
    nodeData.value = fetchedNodeData;
    const inputData = fetchedNodeData.graphic_walker_input;
    fields.value = inputData.dataModel?.fields || [];
    data.value = inputData.dataModel?.data || [];
    chartList.value = inputData.specList || [];
  } catch (error: any) {
    console.error("Error loading GraphicWalker data:", error);
    if (error.response && error.response.status === 422) {
      errorMessage.value = "The analysis flow has not been run yet. Please run the flow.";
    } else if (error instanceof Error) {
       errorMessage.value = `Failed to load data: ${error.message}`;
    } else {
       errorMessage.value = "An unknown error occurred while loading data.";
    }
  } finally {
    isLoading.value = false;
  }
};

// --- Step 1: Get and Parse Current Spec ---
const getCurrentSpec = async (): Promise<IChart[] | null> => {
  if (!vueGraphicWalkerRef.value) {
    console.error("Cannot get spec: GraphicWalker component reference is missing.");
    errorMessage.value = "Cannot get spec: Component reference missing.";
    return null;
  }

  try {
    // Call the exportCode method exposed by the child VueGraphicWalker component
    const exportResult = vueGraphicWalkerRef.value.exportCode();
    // Log the raw result for debugging
    console.log('exportResult from child component:', exportResult);

    if (exportResult === null) {
      console.error("Failed to export chart specification (method returned null).");
      errorMessage.value = "Failed to retrieve current chart configuration.";
      return null;
    }

    // Process the result (could be async generator or array)
    let exportedCharts: IChart[] = [];
    if (exportResult && typeof exportResult[Symbol.asyncIterator] === 'function') {
      console.log("Handling async generator from exportCode...");
      for await (const chart of exportResult) {
        if (chart) exportedCharts.push(chart);
      }
    } else if (Array.isArray(exportResult)) {
      console.log("Handling direct array from exportCode...");
      exportedCharts = exportResult;
    } else {
      // Handle potential case where it's neither (though null check above should catch most failures)
      console.error("Unexpected return type from exportCode:", exportResult);
      throw new Error(`Unexpected return type from exportCode: ${typeof exportResult}`);
    }

    if (exportedCharts.length === 0) {
      console.log("No charts were exported from Graphic Walker.");
      return []; // Return empty array if nothing was exported
    }

    // Deep clone and return the parsed specs
    const parsedSpecs = JSON.parse(JSON.stringify(exportedCharts));
    console.log("Successfully retrieved and parsed spec:", parsedSpecs); // Log the final result
    return parsedSpecs;

  } catch (error: any) {
    console.error("Error getting or parsing GraphicWalker spec:", error);
    errorMessage.value = `Failed to process configuration: ${error.message || 'Unknown error'}`;
    return null;
  }
};

// --- Step 2: Save Spec to Node Store ---
const saveSpecToNodeStore = async (specsToSave: IChart[]) => {
  if (!nodeData.value) {
    console.error("Cannot save: Original node data is missing.");
    errorMessage.value = "Cannot save: Missing original node data.";
    return false; // Indicate failure
  }

  try {
    const saveData: NodeGraphicWalker = {
        ...nodeData.value,
        graphic_walker_input: {
            ...nodeData.value.graphic_walker_input,
            specList: specsToSave, // Use the passed specs
            dataModel: { data: [], fields: [] } // Clear data/fields
        }
    };
    console.log('saveData', saveData)
    nodeStore.node_id = globalNodeId.value;
    await nodeStore.updateSettingsDirectly(saveData);
    console.log("Node settings updated successfully.");
    return true; // Indicate success

  } catch (error: any) {
    console.error("Error saving spec to node store:", error);
    errorMessage.value = `Failed to save configuration: ${error.message || 'Unknown error'}`;
    return false; // Indicate failure
  }
};

// --- Orchestrator Function (Not directly called by button in this version) ---
const pushNodeData = async () => {
    errorMessage.value = null;
    const currentSpec = await getCurrentSpec(); // Still useful internally or if button changes back
    console.log(currentSpec)
    if (currentSpec !== null) {
        // if (currentSpec.length > 0) {
            const saveSuccess = await saveSpecToNodeStore(currentSpec);
            if (saveSuccess) console.log("Save process completed successfully.");
            else console.log("Save process failed.");
        // } else { console.log("No spec content exported, skipping save."); }
    } else {
         console.log("Spec retrieval failed, skipping save.");
    }
};


// --- Expose methods for external use ---
defineExpose({
  loadNodeData,
  pushNodeData,
  getCurrentSpec,
  saveSpecToNodeStore
});

</script>

<template>
  <div class="explore-data-container">
    <button v-if="testMode" @click="getCurrentSpec" :disabled="isLoading">Test Get Current Spec</button>

    <CodeLoader v-if="isLoading" />

    <div v-else-if="errorMessage" class="error-display">
       <p>⚠️ Error: {{ errorMessage }}</p>
    </div>

    <div v-else-if="canDisplayVisualization" class="graphic-walker-wrapper">
       <VueGraphicWalker
          v-if="data.length > 0 && fields.length > 0"
          ref="vueGraphicWalkerRef"
          appearance="light"
          :data="data"
          :fields="fields"
          :specList="chartList"
          />
       <div v-else class="empty-data-message">
           Data loaded successfully, but the dataset appears to be empty or lacks fields.
       </div>
    </div>

     <div v-else class="fallback-message">
       Please load data for the node.
     </div>
  </div>
</template>

<style scoped>
.explore-data-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.graphic-walker-wrapper {
    flex-grow: 1;
    min-height: 300px;
}

.error-display {
  padding: 1rem;
  color: red;
  border: 1px solid red;
  background-color: #ffebeb;
  margin: 1rem;
  border-radius: 4px;
}

.empty-data-message, .fallback-message {
    padding: 1rem;
    text-align: center;
    color: grey;
}
</style>