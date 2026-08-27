<template>
  <div class="component-wrapper">
    <div class="status-indicator" :class="nodeResult?.statusIndicator">
      <span class="tooltip-text">{{ tooltipContent }}</span>
    </div>

    <button :class="['node-button', { selected: isSelected }]" @click="onClick">
      <img :src="iconUrl" :alt="props.title" width="40" />
    </button>
  </div>
</template>

<script setup lang="ts">
import { defineProps, defineEmits, computed, onMounted, ref, nextTick, watch } from "vue";
import type { Component } from "vue"; // <-- Import as a TYPE, not a value
import { useNodeIconUrl } from "../../../composables/useCustomNodeIcon";
import { useNodeStore } from "../../../stores/column-store";
import { NodeTitleInfo } from "./nodeInterfaces";
import { deriveNodeStatus, nodeStatusTooltip, type NodeStatusOutput } from "./nodeStatus";
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

// Built-in glyphs resolve to bundled assets; custom-node icons come from the
// JWT-gated endpoint via an authed blob fetch.
const iconUrl = useNodeIconUrl(() => props.imageSrc);

const isSelected = computed(() => {
  return nodeStore.node_id == props.nodeId;
});

const nodeResult = computed<NodeStatusOutput | undefined>(() =>
  deriveNodeStatus(
    nodeStore.getNodeResult(props.nodeId),
    nodeStore.getNodeValidation(props.nodeId),
  ),
);

const tooltipContent = computed(() => nodeStatusTooltip(nodeResult.value));

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

/* Hollow ring: deliberately empty, distinct from the solid "unknown" dot. */
.status-indicator.skipped::before {
  box-sizing: border-box;
  background-color: transparent;
  border: 2px solid #8a8f98;
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
  width: max-content;
  max-width: 300px;
  background-color: var(--color-gray-800);
  color: var(--color-text-inverse);
  text-align: left;
  font-size: 12px;
  line-height: 1.4;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  border-radius: 6px;
  padding: 8px 12px;
  box-shadow: var(--shadow-md);
  position: absolute;
  z-index: 10;
  bottom: calc(100% + 6px);
  left: 50%;
  transform: translateX(-50%);
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
