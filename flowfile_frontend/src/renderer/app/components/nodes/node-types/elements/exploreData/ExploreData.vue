<script lang="ts" setup>
import { ref, computed } from "vue";
import { CodeLoader } from "vue-content-loader";
import type { IRow, IMutField, IChart } from "@kanaries/graphic-walker/dist/interfaces";
import VueGraphicWalker from "./vueGraphicWalker/VueGraphicWalker.vue";
import type { NodeGraphicWalker } from "./vueGraphicWalker/interfaces";
import { fetchGraphicWalkerData } from "./vueGraphicWalker/utils";
import { useNodeStore } from "../../../../../stores/column-store";
import { useItemStore } from "../../../../common/DraggableItem/stateStore";

const isLoading = ref(false);
const nodeData = ref<NodeGraphicWalker | null>(null);
const chartList = ref<IChart[]>([]);
const data = ref<IRow[]>([]);
const fields = ref<IMutField[]>([]);
const errorMessage = ref<string | null>(null);
const nodeStore = useNodeStore();
const globalNodeId = ref(-1);
const windowStore = useItemStore();
const vueGraphicWalkerRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const canDisplayVisualization = computed(() => !isLoading.value && !errorMessage.value);

const loadNodeData = async (nodeId: number) => {
  isLoading.value = true;
  errorMessage.value = null;
  globalNodeId.value = nodeId;
  nodeData.value = null;
  data.value = [];
  fields.value = [];
  chartList.value = [];
  windowStore.setFullScreen("nodeSettings", true);

  try {
    const fetchedNodeData = await fetchGraphicWalkerData(nodeStore.flow_id, nodeId);
    if (!fetchedNodeData?.graphic_walker_input)
      throw new Error("Received invalid data structure from backend.");

    nodeData.value = fetchedNodeData;
    const inputData = fetchedNodeData.graphic_walker_input;
    fields.value = inputData.dataModel?.fields || [];
    data.value = inputData.dataModel?.data || [];
    chartList.value = inputData.specList || [];
  } catch (error: any) {
    console.error("Error loading GraphicWalker data:", error);
    // Set user-friendly error message
    if (error.response && error.response.status === 422) {
      errorMessage.value = "The analysis flow has not been run yet.";
    } else if (error instanceof Error) {
      errorMessage.value = `Failed to load data: ${error.message}`;
    } else {
      errorMessage.value = "An unknown error occurred while loading data.";
    }
  } finally {
    isLoading.value = false;
  }
};

const getCurrentSpec = async (): Promise<IChart[] | null> => {
  if (!vueGraphicWalkerRef.value) {
    console.error("Cannot get spec: GraphicWalker component reference is missing.");
    errorMessage.value = "Cannot get spec: Component reference missing.";
    return null;
  }

  try {
    const exportedCharts: IChart[] | null = await vueGraphicWalkerRef.value.exportCode();

    if (exportedCharts === null) {
      console.error("Failed to export chart specification (method returned null or failed).");
      errorMessage.value = "Failed to retrieve current chart configuration.";
      return null;
    }

    if (exportedCharts.length === 0) {
      console.log("No charts were exported from Graphic Walker.");
      return [];
    }

    return exportedCharts;
  } catch (error: any) {
    console.error("Error calling getCurrentSpec or processing result:", error);
    errorMessage.value = `Failed to process configuration: ${error.message || "Unknown error"}`;
    return null;
  }
};

// --- Save Spec Back to Store ---
const saveSpecToNodeStore = async (specsToSave: IChart[]) => {
  if (!nodeData.value) {
    console.error("Cannot save: Original node data context is missing.");
    errorMessage.value = "Cannot save: Missing original node data.";
    return false;
  }
  try {
    const saveData: NodeGraphicWalker = {
      ...nodeData.value,
      graphic_walker_input: {
        ...nodeData.value.graphic_walker_input,
        specList: specsToSave,
        dataModel: { data: [], fields: [] },
      },
    };

    await nodeStore.updateSettingsDirectly(saveData);
    console.log("Node settings updated successfully.");
    return true;
  } catch (error: any) {
    console.error("Error saving spec to node store:", error);
    errorMessage.value = `Failed to save configuration: ${error.message || "Unknown error"}`;
    return false;
  }
};

const pushNodeData = async () => {
  errorMessage.value = null; // Clear previous errors
  windowStore.setFullScreen("nodeSettings", false);
  const currentSpec = await getCurrentSpec();

  if (currentSpec === null) {
    console.log("Spec retrieval failed, skipping save.");
    return;
  }

  // Decide whether to save empty specs or not
  if (currentSpec.length === 0) {
    console.log("No chart configurations exported, skipping save.");
    return;
  }
  const saveSuccess = await saveSpecToNodeStore(currentSpec);
  if (saveSuccess) {
    console.log("Save process completed successfully.");
  } else {
    console.log("Save process failed.");
  }
};

// --- Expose Methods ---
defineExpose({
  loadNodeData,
  pushNodeData, // Expose the main save action
});
</script>

<template>
  <div class="explore-data-container">
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
        :spec-list="chartList"
      />
      <div v-else class="empty-data-message">
        Data loaded, but the dataset appears to be empty or lacks defined fields.
      </div>
    </div>

    <div v-else class="fallback-message">Please load data for the node.</div>
  </div>
</template>

<style scoped>
.explore-data-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}
.graphic-walker-wrapper {
  flex-grow: 1; /* Allow wrapper to fill space */
  min-height: 300px; /* Ensure minimum size */
  overflow: hidden; /* Prevent content spillover if needed */
}
/* Ensure the child fills the wrapper if necessary */
:deep(.graphic-walker-wrapper > div) {
  height: 100%;
}
.error-display {
  padding: 1rem;
  color: #a94442; /* Dark red */
  border: 1px solid #ebccd1; /* Light red border */
  background-color: #f2dede; /* Light red background */
  margin: 1rem;
  border-radius: 4px;
}
.empty-data-message,
.fallback-message {
  padding: 1rem;
  text-align: center;
  color: #777; /* Grey */
}
/* Add styles for the button if needed */
button {
  margin: 0.5rem 1rem;
  padding: 0.5rem 1rem;
  cursor: pointer;
}
button:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}
</style>
