<template>
  <div class="button-group">
    <el-button size="small" :disabled="nodeStore.isRunning" round @click="runFlow()">
      Run
    </el-button>
    <el-button v-if="nodeStore.isRunning" size="small" round @click="cancelFlow()">
      Cancel
    </el-button>
  </div>
</template>

<script setup lang="ts">
import axios from "axios";
import { defineProps, ref, onUnmounted } from "vue";
import { useNodeStore } from "../../../../stores/column-store";
import { RunInformation } from "../../baseNode/nodeInterfaces";
import { ElNotification } from "element-plus";
import { updateRunStatus } from "../../nodes/nodeLogic";
import { VueFlowStore } from '@vue-flow/core';

const nodeStore = useNodeStore();
const pollingInterval = ref<number | null>(null);

const props = defineProps({
  flowId: { type: Number, required: true },
  pollingConfig: {
    type: Object,
    default: () => ({
      interval: 2000,
      enabled: true,
      maxAttempts: Infinity,
    }),
  },
});

const freezeFlow = () => {
  let vueFlowElement: VueFlowStore = nodeStore.vueFlowInstance
  if (vueFlowElement) {
    vueFlowElement.nodesDraggable.value = false
    vueFlowElement.nodesConnectable.value = false
    vueFlowElement.elementsSelectable.value = false
  }
}

const unFreezeFlow = () => {
  let vueFlowElement: VueFlowStore = nodeStore.vueFlowInstance
  if (vueFlowElement) {
    vueFlowElement.nodesDraggable.value = true
    vueFlowElement.nodesConnectable.value = true
    vueFlowElement.elementsSelectable.value = true
  }
}

interface NotificationConfig {
  title: string;
  message: string;
  type: "success" | "error";
}

const showNotification = (title: string, message: string, type?: "success" | "error") => {
  ElNotification({
    title,
    message,
    type,
    position: "top-left",
  });
};

const startPolling = (checkFn: () => Promise<void>) => {
  if (pollingInterval.value === null && props.pollingConfig.enabled) {
    pollingInterval.value = setInterval(checkFn, props.pollingConfig.interval) as unknown as number;
  }
};

const stopPolling = () => {
  if (pollingInterval.value !== null) {
    clearInterval(pollingInterval.value);
    pollingInterval.value = null;
  }
};

const createNotificationConfig = (runInfo: RunInformation): NotificationConfig => ({
  title: runInfo.success ? "Success" : "Error",
  message: runInfo.success
    ? "The flow has completed"
    : "There were issues with the flow run, check the logging for more information",
  type: runInfo.success ? "success" : "error",
});

const checkRunStatus = async () => {
  try {
    const response = await updateRunStatus(props.flowId, nodeStore);

    if (response.status === 200) {
      stopPolling();
      unFreezeFlow()
      nodeStore.isRunning = false;

      const notificationConfig = createNotificationConfig(response.data);
      showNotification(
        notificationConfig.title,
        notificationConfig.message,
        notificationConfig.type,
      );
    } else if (response.status === 404) {
      stopPolling();
      unFreezeFlow()
      nodeStore.isRunning = false;
      nodeStore.runResults = {};
    }
  } catch (error) {
    console.error("Error checking run status:", error);
    stopPolling();
    unFreezeFlow()
    nodeStore.isRunning = false;
  }
};



const runFlow = async () => {
  freezeFlow()
  nodeStore.resetNodeResult();
  showNotification("Run started", "The flow started flowing");

  try {
    await axios.post("/flow/run/", null, {
      params: { flow_id: props.flowId },
      headers: { accept: "application/json" },
    });
    nodeStore.isRunning = true;
    nodeStore.showLogViewer();
    startPolling(checkRunStatus);
  } catch (error) {
    console.error("Error starting run:", error);
    unFreezeFlow()
    nodeStore.isRunning = false;
  }
};



const cancelFlow = async () => {
  try {
    await axios.post("/flow/cancel/", null, {
      params: { flow_id: props.flowId },
      headers: { accept: "application/json" },
    });
    showNotification("Cancelling", "The flow is being cancelled");
    unFreezeFlow()
    nodeStore.isRunning = false;
    stopPolling();
  } catch (error) {
    console.error("Error cancelling run:", error);
    showNotification("Error", "Failed to cancel the flow", "error");
  }
};

const emit = defineEmits(["logs-start", "logs-stop"]);

onUnmounted(() => {
  stopPolling();
});

defineExpose({
  startPolling,
  stopPolling,
  checkRunStatus,
  showNotification,
  runFlow,
  cancelFlow,
});
</script>

<style scoped>
.button-group {
  display: flex;
  gap: 10px;
}

.button-group .el-button {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 12px;
  height: 36px;
  border-radius: 6px !important;
  cursor: pointer;
  transition: all 0.2s ease;
  font-size: 13px;
  font-weight: 500 !important;
  letter-spacing: 0.01em;
  box-shadow: 0 1px 2px rgba(1, 5, 13, 0.08);
  flex-shrink: 0;
}

/* Run button - normal state */
.button-group .el-button:first-child:not([disabled]) {
  background-color: rgb(2, 27, 45) !important;
  border: 1px solid rgb(17, 64, 97) !important;
  color: white !important;
}

/* Run button - hover state */
.button-group .el-button:first-child:not([disabled]):hover {
  background-color: rgba(65, 55, 200, 0.95) !important;
  border-color: rgba(65, 55, 200, 1) !important;
}

/* Run button - disabled state */
.button-group .el-button:first-child[disabled] {
  background-color: rgba(16, 24, 40, 0.3) !important;
  border: 1px solid rgba(16, 24, 40, 0.2) !important;
  color: rgba(255, 255, 255, 0.8) !important;
  cursor: not-allowed;
}

/* Cancel button */
.button-group .el-button:nth-child(2) {
  background-color: rgba(220, 53, 69, 0.9) !important;
  border: 1px solid rgba(220, 53, 69, 0.95) !important;
  color: white !important;
}

/* Cancel button - hover state */
.button-group .el-button:nth-child(2):hover {
  background-color: rgba(200, 35, 51, 0.95) !important;
  border-color: rgba(200, 35, 51, 1) !important;
}

/* Active state for both buttons */
.button-group .el-button:active {
  transform: translateY(1px);
  box-shadow: none;
}
</style>
