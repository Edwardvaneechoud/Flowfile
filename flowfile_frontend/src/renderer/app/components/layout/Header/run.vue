<template>
  <div class="button-group">
    <el-button size="small" :disabled="nodeStore.isRunning" round @click="runFlow()">
      <span class="material-icons run-icon">play_arrow</span>
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
import { useTutorialStore } from "../../../stores/tutorial-store";

const nodeStore = useNodeStore();
const tutorialStore = useTutorialStore();

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

// Pass a getter so the composable always reads the *current* prop value.
// Without this, Save As re-keys nodeStore.flow_id but the run button keeps
// firing /flow/run/ and getFlowSettings against the old (template) id.
const {
  runFlow: executeFlow,
  cancelFlow,
  showNotification,
  startPolling,
  stopPolling,
  checkRunStatus,
} = useFlowExecution(() => props.flowId, props.pollingConfig, {
  persistPolling: props.persistPolling,
  pollingKey: `run_button_${props.flowId}`,
});

// Wrapper to also advance tutorial when run is clicked
const runFlow = () => {
  executeFlow();
  // Advance tutorial if we're on the "run-flow" step
  if (tutorialStore.isActive && tutorialStore.currentStep?.id === "run-flow") {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 500);
  }
};

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
  gap: 4px;
  padding: 0 10px;
  height: 28px;
  border-radius: 6px !important;
  cursor: pointer;
  transition: all 0.2s ease;
  font-size: 12px;
  font-weight: 500 !important;
  letter-spacing: 0.01em;
  box-shadow: var(--shadow-xs);
  flex-shrink: 0;
}

/* Run button - normal state */
.button-group .el-button:first-child:not([disabled]) {
  background-color: var(--color-accent-purple) !important;
  border: 1px solid var(--color-accent-purple) !important;
  color: #ffffff !important;
}

/* Run button - hover state */
.button-group .el-button:first-child:not([disabled]):hover {
  background-color: var(--color-accent-purple-hover) !important;
  border-color: var(--color-accent-purple-hover) !important;
}

.run-icon {
  font-size: 14px;
  line-height: 1;
}

/* Run button - disabled state */
.button-group .el-button:first-child[disabled] {
  background-color: var(--color-gray-400) !important;
  border: 1px solid var(--color-gray-400) !important;
  color: #ffffff !important;
  cursor: not-allowed;
  opacity: 0.7;
}

/* Cancel button */
.button-group .el-button:nth-child(2) {
  background-color: var(--color-danger) !important;
  border: 1px solid var(--color-danger) !important;
  color: #ffffff !important;
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
