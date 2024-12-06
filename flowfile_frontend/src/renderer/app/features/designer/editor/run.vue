<template>
  <div>
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
  </div>
</template>

<script setup lang="ts">
import axios from "axios";
import { defineProps, ref, onUnmounted } from "vue";
import { useNodeStore } from "../../../stores/column-store";
import { RunInformation } from "../baseNode/nodeInterfaces";
import { ElNotification } from "element-plus";

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

// Exposed notification functions
const showNotification = (title: string, message: string, type?: 'success' | 'error') => {
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
    pollingInterval.value = setInterval(
      checkFn,
      props.pollingConfig.interval,
    ) as unknown as number;
  }
};

const stopPolling = () => {
  if (pollingInterval.value !== null) {
    clearInterval(pollingInterval.value);
    pollingInterval.value = null;
  }
};

// Exposed run status check
const checkRunStatus = async () => {
  try {
    const response = await axios.get("/flow/run_status/", {
      params: { flow_id: props.flowId },
      headers: { accept: "application/json" },
    });

    if (response.status === 200) {
      console.log("stop polling");
      stopPolling();
      nodeStore.isRunning = false;
      
      try {
        const runResult = await axios.get("/flow/run_results/", {
          params: { flow_id: props.flowId },
          headers: { accept: "application/json" },
        });
        const runInformation = runResult.data as RunInformation;
        nodeStore.insertRunResult(runInformation);
        showNotification(
          runInformation.success ? "Success" : "Error",
          runInformation.success 
            ? "The flow has completed"
            : "There were issues with the flow run, check the logging for more information",
          runInformation.success ? "success" : "error"
        );
      } catch (error) {
        console.error("Error getting run results:", error);
      }
    } else {
      nodeStore.insertRunResult(response.data);
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

// Cleanup on component unmount
onUnmounted(() => {
  stopPolling();
});

// Expose functions for external use
defineExpose({
  startPolling,
  stopPolling,
  checkRunStatus,
  showNotification,
  runFlow,
});
</script>

<style scoped>
.el-button {
  flex-shrink: 0;
}
</style>