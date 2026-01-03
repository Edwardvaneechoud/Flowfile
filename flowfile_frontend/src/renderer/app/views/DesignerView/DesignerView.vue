<template>
  <div v-if="!isLoading" class="header">
    <div class="header-top">
      <div class="left-section">
        <header-buttons ref="headerButtons" @open-flow="openFlow" @refresh-flow="refreshFlow" />
      </div>
    </div>
    <div class="header-bottom">
      <div class="middle-section">
        <flow-selector
          ref="flowSelector"
          @flow-changed="handleFlowChange"
          @close-tab="handleCloseFlow"
        />
      </div>
      <div class="right-section">
        <Status />
      </div>
    </div>
  </div>
  <!-- Show loading state while fetching flows -->
  <div v-if="isLoading" class="loading-state">
    <div class="loading-state-content">
      <p>Loading flows...</p>
    </div>
  </div>
  <!-- Show empty state only when loading is complete and no flows are found -->
  <div v-else-if="!isLoading && flowsActive.length === 0" class="empty-state">
    <div class="empty-state-content">
      <span class="material-icons empty-icon">account_tree</span>
      <h2>No Active Flows</h2>
      <p>There are currently no active flows in the system.</p>
      <el-button type="primary" class="action-button" @click="createFlowDialog">
        <span class="material-icons">add_circle</span>
        Create new flow
      </el-button>
      <el-button type="primary" class="action-button" @click="openFlowDialog">
        <span class="material-icons">folder_open</span>
        Open existing flow
      </el-button>
      <el-button type="primary" class="action-button" @click="openQuickCreateDialog">
        <span class="material-icons">folder_open</span>
        Quick create
      </el-button>
    </div>
  </div>
  <canvas-flow
    v-else
    ref="canvasFlow"
    class="canvas"
    @save="headerButtons?.openSaveModal()"
    @run="headerButtons?.runFlow()"
  />
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import HeaderButtons from "../../components/layout/Header/HeaderButtons.vue";
import Status from "../../features/designer/editor/status.vue";
import CanvasFlow from "./Canvas.vue";
import FlowSelector from "../FlowSelectorView/FlowSelectorView.vue";
import { FlowApi } from "../../api";
import { fetchNodes } from "../../features/designer/utils";
import type { NodeTemplate, FlowSettings } from "../../types";
import { useNodeStore } from "../../stores/column-store";

// Extract API functions
const getAllFlows = FlowApi.getAllFlows;
const importSavedFlow = FlowApi.importFlow;
const closeFlow = FlowApi.closeFlow;

const flowsActive = ref<FlowSettings[]>([]);
const isLoading = ref(true);
const canvasFlow = ref<InstanceType<typeof CanvasFlow>>();
const headerButtons = ref<InstanceType<typeof HeaderButtons>>();
const flowSelector = ref<InstanceType<typeof FlowSelector>>();
const nodeOptions = ref<NodeTemplate[]>([]);
const initialLoadComplete = ref(false);

const nodeStore = useNodeStore();

const fetchActiveFlows = async () => {
  try {
    const flows = await getAllFlows();
    flowsActive.value = flows;

    // Refresh the flow selector when active flows change
    if (flowSelector.value) {
      await flowSelector.value.loadFlows();
    }
    return flows;
  } catch (error) {
    console.error("Failed to load active flows:", error);
    return [];
  }
};

const openFlow = (eventData: { message: string; flowPath: string }) => {
  reloadCanvas(eventData.flowPath);
};

const reloadCanvas = async (flowPath: string) => {
  isLoading.value = true;
  try {
    console.log("reloadCanvas", flowPath);
    const flowId = await importSavedFlow(flowPath);
    if (flowId === undefined) {
      console.error("Failed to import flow from path:", flowPath);
      return;
    }
    nodeStore.setFlowId(flowId);
    if (canvasFlow.value) {
      await canvasFlow.value.loadFlow();
    }
    if (headerButtons.value) {
      await headerButtons.value.loadFlowSettings();
    }
    await fetchActiveFlows();
  } finally {
    isLoading.value = false;
  }
};

const handleCloseFlow = async (flowId: number) => {
  try {
    console.log("Closing flow:", flowId);

    // Check if we're closing the currently active flow
    const isCurrentFlow = nodeStore.flow_id === flowId;

    // Call the API to close the flow
    await closeFlow(flowId);

    // Clean up any flow-related data in the store
    nodeStore.clearFlowResults(flowId);
    nodeStore.clearFlowDescriptionCache(flowId);
    isLoading.value = true;

    // Refresh the flows list
    await fetchActiveFlows();

    if (isCurrentFlow) {
      if (flowsActive.value.length > 0) {
        // Switch to the first available flow
        const newFlowId = flowsActive.value[0].flow_id;
        console.log("Switching to flow:", newFlowId);
        await handleFlowChange(newFlowId);
      } else {
        // No flows left, reset the nodeStore
        nodeStore.setFlowId(-1);
      }
    }
  } catch (error) {
    console.error("Error closing flow:", error);
  } finally {
    isLoading.value = false;
  }
};

const handleFlowChange = async (flowId: number) => {
  if (isLoading.value && flowId === nodeStore.flow_id) {
    console.log("Already loading flow ID:", flowId);
    return;
  }

  isLoading.value = true;
  try {
    console.log("Handling flow change to:", flowId);
    nodeStore.setFlowId(flowId);
    if (canvasFlow.value) {
      await canvasFlow.value.loadFlow();
    }
    if (headerButtons.value) {
      await headerButtons.value.loadFlowSettings();
    }
  } finally {
    isLoading.value = false;
  }
};

const refreshFlow = async () => {
  isLoading.value = true;
  try {
    console.log("refreshFlow");
    await fetchActiveFlows(); // Refresh flows list
    if (canvasFlow.value && flowsActive.value.length > 0) {
      await canvasFlow.value.loadFlow();
    }
    console.log("refreshFlow end");
    if (headerButtons.value) {
      await headerButtons.value.loadFlowSettings();
    }
  } finally {
    isLoading.value = false;
  }
};

const createFlowDialog = () => {
  if (headerButtons.value) {
    headerButtons.value.openCreateDialog();
  }
};

const openFlowDialog = () => {
  if (headerButtons.value) {
    headerButtons.value.openOpenDialog();
  }
};

const openQuickCreateDialog = () => {
  if (headerButtons.value) {
    console.log("Opening quick create dialog");
    headerButtons.value.handleQuickCreateAction();
  }
};

const initialSetup = async () => {
  if (initialLoadComplete.value) {
    console.log("Initial setup already completed");
    return;
  }

  isLoading.value = true;
  console.log("Starting initial setup");

  try {
    const [nodes, flows] = await Promise.all([fetchNodes(), fetchActiveFlows()]);

    nodeOptions.value = nodes;
    if (flows.length > 0 && (!nodeStore.flow_id || nodeStore.flow_id <= 0)) {
      console.log("Setting initial flow ID to:", flows[0].flow_id);
      nodeStore.setFlowId(flows[0].flow_id);

      // Load the flow data
      if (canvasFlow.value) {
        await canvasFlow.value.loadFlow();
      }
      if (headerButtons.value) {
        await headerButtons.value.loadFlowSettings();
      }
    } else if (nodeStore.flow_id && nodeStore.flow_id > 0) {
      console.log("Using existing flow ID:", nodeStore.flow_id);
      if (canvasFlow.value) {
        await canvasFlow.value.loadFlow();
      }
      if (headerButtons.value) {
        await headerButtons.value.loadFlowSettings();
      }
    }

    initialLoadComplete.value = true;
    console.log("Initial setup completed");
  } catch (error) {
    console.error("Error during initial setup:", error);
  } finally {
    isLoading.value = false;
  }
};

onMounted(async () => {
  console.log("Component mounted, starting initialization");
  await initialSetup();
});
</script>

<style scoped>
.canvas {
  height: calc(100vh - 100px);
}

.header {
  background-color: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

/* Desktop layout - single row */
@media (min-width: 1025px) {
  .header {
    display: flex;
    justify-content: space-between;
    align-items: stretch;
    height: 50px;
  }

  .header-top {
    display: contents;
  }

  .header-bottom {
    display: contents;
  }

  .left-section {
    min-width: 250px;
    padding: 0 var(--spacing-4);
    display: flex;
    align-items: center;
  }

  .middle-section {
    flex: 1;
    display: flex;
    align-items: center;
    overflow: hidden;
  }

  .right-section {
    min-width: 150px;
    padding: 0 var(--spacing-4);
    display: flex;
    align-items: center;
    justify-content: flex-end;
  }

  .canvas {
    height: calc(100vh - 50px);
  }
}

/* Mobile/tablet layout - stacked */
@media (max-width: 1024px) {
  .header {
    height: auto;
    min-height: 80px;
  }

  .header-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
    height: 50px;
    border-bottom: 1px solid var(--color-border-primary);
  }

  .header-bottom {
    display: flex;
    height: 40px;
    padding: 0 var(--spacing-2);
  }

  .left-section {
    padding: 0 var(--spacing-3);
    display: flex;
    align-items: center;
  }

  .middle-section {
    flex: 1;
    display: flex;
    align-items: center;
    overflow: hidden;
    padding: 0 var(--spacing-2);
  }

  .right-section {
    padding: 0 var(--spacing-3);
    display: flex;
    align-items: center;
    justify-content: flex-end;
  }

  .canvas {
    height: calc(100vh - 90px);
  }
}

/* Very narrow screens */
@media (max-width: 480px) {
  .left-section {
    padding: 0 var(--spacing-2);
    min-width: auto;
  }

  .right-section {
    padding: 0 var(--spacing-2);
    min-width: auto;
  }

  .middle-section {
    padding: 0 var(--spacing-1);
  }
}

/* Loading state styles */
.loading-state {
  height: calc(100vh - 50px);
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: var(--color-background-secondary);
}

.loading-state-content {
  text-align: center;
  padding: var(--spacing-8);
}

.loading-state-content p {
  color: var(--color-text-secondary);
  margin-top: var(--spacing-4);
}

/* Empty state styles */
.empty-state {
  height: calc(100vh - 50px);
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: var(--color-background-secondary);
}

.empty-state-content {
  text-align: center;
  padding: var(--spacing-8);
}

.empty-icon {
  font-size: var(--font-size-4xl);
  color: var(--color-gray-400);
  margin-bottom: var(--spacing-4);
}

.empty-state h2 {
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-2);
}

.empty-state p {
  color: var(--color-text-secondary);
  margin-bottom: var(--spacing-6);
}

.action-button {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: 0 var(--spacing-2);
}

.action-button .material-icons {
  font-size: var(--font-size-2xl);
}
</style>
