<template>
  <div class="header">
    <HeaderButtons ref="headerButtons" @open-flow="openFlow" @refresh-flow="refreshFlow" />
    <div class="spacer"></div>
    <Status />
  </div>
  <div>
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
      </div>
    </div>
    <CanvasFlow v-else ref="canvasFlow" class="canvas" />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import HeaderButtons from "../features/designer/components/HeaderButtons/HeaderButtons.vue";
import {} from "../features/designer/components/Canvas/backendInterface";
import Status from "../features/designer/editor/status.vue";
import CanvasFlow from "../features/designer/components/Canvas/CanvasFlow.vue";
import {
  getAllFlows,
  importSavedFlow,
} from "../features/designer/components/Canvas/backendInterface";
import { fetchNodes } from "../features/designer/utils";
import { NodeTemplate } from "../features/designer/types";
import { FlowSettings } from "../features/designer/nodes/nodeLogic";

const flowsActive = ref<FlowSettings[]>([]);
const isLoading = ref(true);
const canvasFlow = ref<InstanceType<typeof CanvasFlow>>();
const headerButtons = ref<InstanceType<typeof HeaderButtons>>();
const nodeOptions = ref<NodeTemplate[]>([]);

const loadActiveFlows = async () => {
  isLoading.value = true;
  try {
    const flows = await getAllFlows();
    flowsActive.value = flows;
  } catch (error) {
    console.error("Failed to load active flows:", error);
    // Optionally show error message to user
    // ElMessage.error('Failed to load active flows');
  } finally {
    isLoading.value = false;
  }
};

const openFlow = (eventData: { message: string; flowPath: string }) => {
  console.log("openFlow");
  reloadCanvas(eventData.flowPath);
};

const reloadCanvas = async (flowPath: string) => {
  isLoading.value = true;
  try {
    console.log("reloadCanvas", flowPath);
    await importSavedFlow(flowPath);
    if (canvasFlow.value) {
      await canvasFlow.value.loadFlow();
    }
    if (headerButtons.value) {
      await headerButtons.value.loadFlowSettings();
    }
    await loadActiveFlows(); // Refresh flows after reloading canvas
  } finally {
    isLoading.value = false;
  }
};

const refreshFlow = async () => {
  isLoading.value = true;
  try {
    console.log("refreshFlow");
    await loadActiveFlows(); // Refresh flows list
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

onMounted(async () => {
  try {
    await Promise.all([
      fetchNodes().then((nodes) => (nodeOptions.value = nodes)),
      loadActiveFlows(),
    ]);
  } finally {
    isLoading.value = false;
  }
});
</script>

<style scoped>
.canvas {
  height: calc(100vh - 50px);
}

.header {
  display: flex;
  justify-content: flex-start;
  align-items: center;
  padding: 0 16px;
  height: 50px;
  background-color: #f5f5f5;
  border-bottom: 1px solid #ececec;
}

.spacer {
  flex-grow: 1;
}

.overlay {
  position: fixed;
  top: 0px;
  border-right: 1px solid #ececec;
  z-index: 2000;
  background-color: #ececec;
}

/* Loading state styles */
.loading-state {
  height: calc(100vh - 50px);
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #f9fafb;
}

.loading-state-content {
  text-align: center;
  padding: 2rem;
}

.loading-state-content p {
  color: #6b7280;
  margin-top: 1rem;
}

/* Empty state styles */
.empty-state {
  height: calc(100vh - 50px);
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #f9fafb;
}

.empty-state-content {
  text-align: center;
  padding: 2rem;
}

.empty-icon {
  font-size: 64px;
  color: #9ca3af;
  margin-bottom: 1rem;
}

.empty-state h2 {
  color: #374151;
  margin-bottom: 0.5rem;
}

.empty-state p {
  color: #6b7280;
  margin-bottom: 1.5rem;
}

.refresh-button {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
}

.refresh-button .material-icons {
  font-size: 18px;
}

.action-button {
  /* Changed from .refresh-button */
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  margin: 0 0.5rem; /* Added margin */
}

.action-button .material-icons {
  /* Changed from .refresh-button */
  font-size: 18px;
}
</style>
