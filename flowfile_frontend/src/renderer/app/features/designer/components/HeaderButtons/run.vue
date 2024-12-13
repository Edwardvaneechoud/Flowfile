<template>
  <div class="button-group">
    <el-button
      size="small"
      :style="
        nodeStore.isRunning
          ? 'background-color: lightgrey; color: white'
          : 'background-color: rgb(92, 92, 92); color: white; font-weight: bold'
      "
      round
      :disabled="nodeStore.isRunning"
      @click="runFlow()"
    >
      Run
    </el-button>
    <el-button
      v-if="nodeStore.isRunning"
      size="small"
      style="background-color: rgb(220, 53, 69); color: white; font-weight: bold"
      round
      @click="cancelFlow()"
    >
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

interface NotificationConfig {
  title: string;
  message: string;
  type: "success" | "error";
}

// Exposed notification functions
const showNotification = (title: string, message: string, type?: "success" | "error") => {
  ElNotification({
    title,
    message,
    type,
    position: "top-left",
  });
};

// Exposed polling control functions
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
      nodeStore.isRunning = false;

      const notificationConfig = createNotificationConfig(response.data);
      showNotification(
        notificationConfig.title,
        notificationConfig.message,
        notificationConfig.type,
      );
    } else if (response.status === 404) {
      stopPolling();
      nodeStore.runResults = {};
    }
  } catch (error) {
    console.error("Error checking run status:", error);
    stopPolling();
  }
};

// Main run function
const runFlow = async () => {
  nodeStore.resetNodeResult();
  console.log("start run");
  showNotification("Run started", "The flow started flowing");

  try {
    await axios.post("/flow/run/", null, {
      params: { flow_id: props.flowId },
      headers: { accept: "application/json" },
    });
    nodeStore.isRunning = true;
    startPolling(checkRunStatus);
  } catch (error) {
    console.error("Error starting run:", error);
  }
};

const cancelFlow = async () => {
  try {
    await axios.post("/flow/cancel/", null, {
      params: { flow_id: props.flowId },
      headers: { accept: "application/json" },
    });
    showNotification("Cancelling", "The flow is being cancelled");
    nodeStore.isRunning = false;
    stopPolling();
  } catch (error) {
    console.error("Error cancelling run:", error);
    showNotification("Error", "Failed to cancel the flow", "error");
  }
};

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

.el-button {
  flex-shrink: 0;
}
</style>
