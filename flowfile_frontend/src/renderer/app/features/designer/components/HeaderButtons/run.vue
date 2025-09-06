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
import { defineProps } from "vue";
import { useNodeStore } from "../../../../stores/column-store";
import { useFlowExecution } from "../../composables/useFlowExecution";

const nodeStore = useNodeStore();

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
  persistPolling: {
    type: Boolean,
    default: false, // RunButton doesn't need persistent polling by default
  },
});

// Use the composable
const { runFlow, cancelFlow, showNotification, startPolling, stopPolling, checkRunStatus } =
  useFlowExecution(props.flowId, props.pollingConfig, {
    persistPolling: props.persistPolling,
    pollingKey: `run_button_${props.flowId}`,
  });

const emit = defineEmits(["logs-start", "logs-stop"]);

// Expose methods if parent component needs them
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
