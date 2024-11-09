<template>
  <div>
    <el-button
      size="small"
      :style="
        isRunning
          ? 'background-color: lightgrey; color: white'
          : 'background-color: rgb(92, 92, 92); color: white; font-weight: bold'
      "
      round
      :disabled="isRunning"
      @click="runFlow()"
    >
      Run
    </el-button>
  </div>
</template>

<script setup lang="ts">
import axios from "axios";
import { defineProps, ref } from "vue";
import { useNodeStore } from "../../../stores/column-store";
import { RunInformation } from "../baseNode/nodeInterfaces";
import { ElNotification } from "element-plus";

const startRun = () => {
  ElNotification({
    title: "Run started",
    message: "The flow started flowing",
    position: "top-left",
  });
};
const isRunning = ref(false);
const pollingInterval = ref<number | null>(null);
const nodeStore = useNodeStore();
const props = defineProps({
  flowId: { type: Number, required: true },
});

const runFlow = async () => {
  nodeStore.resetNodeResult();
  console.log("start run");
  startRun();
  try {
    await axios.post("/flow/run/", null, {
      params: { flow_id: props.flowId },
      headers: { accept: "application/json" },
    });
    isRunning.value = true;
    nodeStore.isRunning = true;
    if (pollingInterval.value === null) {
      pollingInterval.value = setInterval(
        checkRunStatus,
        2000,
      ) as unknown as number;
    }
  } catch (error) {
    console.error("Error starting run:", error);
  }
};
const runCompleted = (success: boolean) => {
  if (success) {
    ElNotification({
      title: "Success",
      message: "The flow has completed",
      type: "success",
      position: "top-left",
    });
  } else {
    ElNotification({
      title: "Error",
      message:
        "There were issues with the flow run, check the logging for more information",
      type: "error",
      position: "top-left",
    });
  }
};
const checkRunStatus = async () => {
  try {
    const response = await axios.get("/flow/run_status/", {
      params: { flow_id: props.flowId },
      headers: { accept: "application/json" },
    });

    if (response.status === 200) {
      isRunning.value = false;
      console.log("stop polling");
      clearInterval(pollingInterval.value!); // Stop polling
      pollingInterval.value = null;
      nodeStore.isRunning = false;
      try {
        const runResult = await axios.get("/flow/run_results/", {
          params: { flow_id: props.flowId },
          headers: { accept: "application/json" },
        });
        const runInformation = runResult.data as RunInformation;
        nodeStore.insertRunResult(runResult.data as RunInformation);
        runCompleted(runInformation.success);
      } catch (error) {
        console.error("Error getting run results:", error);
      }
    } else {
      nodeStore.insertRunResult(response.data);
    }
    // If status is not 'completed', keep polling
  } catch (error) {
    console.error("Error checking run status:", error);
    // Handle error (e.g., by showing a notification or stopping polling)
  }
};
</script>

<style scoped>
.el-button {
  flex-shrink: 0; /* Prevent buttons from shrinking */
  /* Additional styling here if needed */
}
</style>
