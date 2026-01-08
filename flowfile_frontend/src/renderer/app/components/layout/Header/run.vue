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
import { useNodeStore } from "../../../stores/column-store";
import { useFlowExecution } from "../../../composables/useFlowExecution";

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

defineEmits(["logs-start", "logs-stop"]);

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
  box-shadow: var(--shadow-xs);
  flex-shrink: 0;
}

/* Run button - normal state */
.button-group .el-button:first-child:not([disabled]) {
  background-color: var(--primary-blue) !important;
  border: 1px solid var(--primary-blue-light) !important;
  color: var(--color-text-inverse) !important;
}

/* Run button - hover state */
.button-group .el-button:first-child:not([disabled]):hover {
  background-color: var(--color-button-primary) !important;
  border-color: var(--color-button-primary) !important;
}

/* Run button - disabled state */
.button-group .el-button:first-child[disabled] {
  background-color: var(--color-gray-400) !important;
  border: 1px solid var(--color-gray-400) !important;
  color: var(--color-text-inverse) !important;
  cursor: not-allowed;
  opacity: 0.7;
}

/* Cancel button */
.button-group .el-button:nth-child(2) {
  background-color: var(--color-danger) !important;
  border: 1px solid var(--color-danger) !important;
  color: var(--color-text-inverse) !important;
}

/* Cancel button - hover state */
.button-group .el-button:nth-child(2):hover {
  background-color: var(--color-danger-hover) !important;
  border-color: var(--color-danger-hover) !important;
}

/* Active state for both buttons */
.button-group .el-button:active {
  transform: translateY(1px);
  box-shadow: none;
}
</style>
