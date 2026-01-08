<!-- nodeButton.vue - add this right after your button -->
<template>
  <div class="component-wrapper">
    <div class="status-indicator" :class="nodeResult?.statusIndicator">
      <span class="tooltip-text">{{ tooltipContent }}</span>
    </div>

    <button :class="['node-button', { selected: isSelected }]" @click="onClick">
      <img :src="getImageUrl(props.imageSrc)" :alt="props.title" width="50" />
    </button>
  </div>
</template>

<script setup lang="ts">
import { defineProps, defineEmits, computed, onMounted, ref, nextTick, watch } from "vue";
import type { Component } from "vue"; // <-- Import as a TYPE, not a value
import { getImageUrl } from "../../../features/designer/utils";
import { useNodeStore } from "../../../stores/column-store";
import { NodeTitleInfo } from "./nodeInterfaces";
const description = ref<string>("");
const nodeStore = useNodeStore();

const props = defineProps<{
  nodeId: number;
  imageSrc: string;
  title: string;
  drawerComponent?: Component | null;
  drawerProps?: Record<string, any>;
  nodeTitleInfo: NodeTitleInfo;
}>();

const isSelected = computed(() => {
  return nodeStore.node_id == props.nodeId;
});

interface ResultOutput {
  success?: boolean;
  statusIndicator: "success" | "failure" | "unknown" | "warning" | "running";
  error?: string;
  hasRun: boolean;
}

const nodeResult = computed<ResultOutput | undefined>(() => {
  const nodeResult = nodeStore.getNodeResult(props.nodeId);
  const nodeValidation = nodeStore.getNodeValidation(props.nodeId);

  // Check if node is currently running (has start timestamp but not completed)
  if (nodeResult && nodeResult.is_running) {
    return {
      success: undefined,
      statusIndicator: "running",
      hasRun: false,
      error: undefined,
    };
  }

  if (nodeResult && !nodeResult.is_running) {
    if (nodeValidation) {
      // Case 1: nodeResult is success, nodeValidation is not success, and validation is after result -> warning
      if (
        nodeResult.success === true &&
        !nodeValidation.isValid &&
        nodeValidation.validationTime > nodeResult.start_timestamp
      ) {
        return {
          success: true,
          statusIndicator: "warning",
          error: nodeValidation.error,
          hasRun: true,
        };
      }
      // Case 2: nodeResult and nodeValidation both success -> success
      if (nodeResult.success === true && nodeValidation.isValid) {
        return {
          success: true,
          statusIndicator: "success",
          error: nodeResult.error || nodeValidation.error,
          hasRun: true,
        };
      }
      if (
        nodeResult.success === false &&
        nodeValidation.isValid &&
        nodeValidation.validationTime > nodeResult.start_timestamp
      ) {
        return {
          success: false,
          statusIndicator: "unknown",
          error: nodeResult.error || nodeValidation.error,
          hasRun: true,
        };
      }
      if (
        nodeResult.success === false &&
        (!nodeValidation.isValid || !nodeValidation.validationTime)
      ) {
        return {
          success: false,
          statusIndicator: "failure",
          error: nodeResult.error || nodeValidation.error,
          hasRun: true,
        };
      }
    }
    // Handle completed but no validation case
    return {
      success: nodeResult.success ?? false,
      statusIndicator: nodeResult.success ? "success" : "failure",
      error: nodeResult.error,
      hasRun: true,
    };
  }

  // Handle incomplete node cases
  if (nodeValidation) {
    if (!nodeValidation.isValid) {
      return {
        success: false,
        statusIndicator: "warning",
        error: nodeValidation.error,
        hasRun: false,
      };
    }
    if (nodeValidation.isValid) {
      return {
        success: true,
        statusIndicator: "unknown",
        error: nodeValidation.error,
        hasRun: false,
      };
    }
  }

  return undefined; // Default case
});

const tooltipContent = computed(() => {
  switch (nodeResult.value?.statusIndicator) {
    case "success":
      return "Operation successful";
    case "failure":
      return "Operation failed: \n" + (nodeResult.value?.error || "No error message available");
    case "warning":
      return "Operation warning: \n" + (nodeResult.value?.error || "No warning message available");
    case "running":
      return "Operation in progress...";
    case "unknown":
    default:
      return "Status unknown";
  }
});

const getNodeDescription = async () => {
  description.value = await nodeStore.getNodeDescription(props.nodeId);
};

defineEmits(["click"]);

watch(
  () => nodeStore.node_id,
  (newNodeId) => {
    if (String(newNodeId) === String(props.nodeId) && props.drawerComponent) {
      nodeStore.openDrawer(props.drawerComponent, props.nodeTitleInfo);
    }
  },
  { immediate: true },
);

onMounted(() => {
  watch(
    () => props.nodeId,
    async (newVal) => {
      if (newVal !== -1) {
        // Assuming -1 is an uninitialized state
        await nextTick();
        getNodeDescription();
      }
    },
  );
});
</script>

<style scoped>
.status-indicator {
  position: relative;
  display: flex;
  align-items: center;
  margin-right: 8px;
}

.status-indicator::before {
  content: "";
  display: block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
}

.status-indicator.success::before {
  background-color: #4caf50;
}

.status-indicator.failure::before {
  background-color: #f44336;
}

.status-indicator.warning::before {
  background-color: #f09f5dd1;
}

.status-indicator.unknown::before {
  background-color: var(--color-text-muted);
}

.status-indicator.running::before {
  background-color: #0909ca;
  animation: pulse 1.5s cubic-bezier(0.4, 0, 0.6, 1) infinite;
  box-shadow: 0 0 10px #0909ca;
}

@keyframes pulse {
  0% {
    transform: scale(1);
    opacity: 1;
    box-shadow: 0 0 5px #0909ca;
  }
  50% {
    transform: scale(1.3);
    opacity: 0.6;
    box-shadow: 0 0 15px #0909ca;
  }
  100% {
    transform: scale(1);
    opacity: 1;
    box-shadow: 0 0 5px #0909ca;
  }
}

.tooltip-text {
  visibility: hidden;
  width: 120px;
  background-color: var(--color-gray-800);
  color: var(--color-text-inverse);
  text-align: center;
  border-radius: 6px;
  padding: 5px 0;
  position: absolute;
  z-index: 1;
  bottom: 100%;
  left: 50%;
  margin-left: -60px;
  opacity: 0;
  transition: opacity 0.3s;
}

.status-indicator:hover .tooltip-text {
  visibility: visible;
  opacity: 1;
}

.description-input:hover,
.description-input:focus {
  background-color: var(--color-background-tertiary);
  box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.2);
}
.component-wrapper {
  position: relative; /* This makes the absolute positioning of the child relative to this container */
  max-width: 60px;
  overflow: visible; /* Allows children to visually overflow */
}

.description-display {
  padding: 8px;
  width: 200px !important;
  max-height: 8px !important;
  background-color: var(--color-background-primary);
  border-radius: 4px;
  cursor: pointer;
}

.overlay {
  position: fixed; /* This is key for viewport-level positioning */
  width: 200px; /* Or whatever width you prefer */
  height: 200px; /* Or whatever height you prefer */
  left: 50%; /* Center horizontally */
  top: 50%; /* Center vertically */
  transform: translate(-50%, -50%); /* Adjust based on its own dimensions */
  z-index: 1000; /* High enough to float above everything else */
  /* Your existing styles for background, padding, etc. */
}

.node-button {
  background-color: #dedede;
  border-radius: 10px;
  border-width: 0px;
}

.node-button:hover {
  background-color: var(--color-background-hover);
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.overlay-content {
  padding: 20px;
  border-radius: 10px;
  box-shadow: var(--shadow-md);
  display: flex;
  flex-direction: column;
  align-items: stretch;
}

.overlay-prompt {
  margin-bottom: 10px;
  color: var(--color-text-primary);
  font-size: 16px;
}

.description-input {
  margin-bottom: 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  padding: 10px;
  font-size: 14px;
  height: 100px;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
}

.selected {
  border: 2px solid var(--color-accent);
}
</style>
