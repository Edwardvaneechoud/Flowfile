<template>
  <div class="component-wrapper">
    <div class="status-indicator" :class="nodeResult?.statusIndicator">
      <span :id="`node-tooltip-${props.nodeId}`" class="tooltip-text">{{ tooltipContent }}</span>
    </div>

    <button
      :class="['node-button', { selected: isSelected, 'cached-node': showCachedIndicator }]"
      :title="showCachedIndicator ? cacheExplanation : undefined"
      :aria-describedby="showCachedIndicator ? `node-tooltip-${props.nodeId}` : undefined"
      @click="onClick"
    >
      <img :src="iconUrl" :alt="props.title" width="40" />
    </button>
    <span
      v-if="showCachedIndicator"
      class="cache-indicator material-icons"
      role="img"
      aria-label="Cached result"
      :title="cacheExplanation"
      >cached</span
    >
    <span
      v-if="placementNote"
      :class="['placement-badge', `placement-badge--${placementNote.kind}`]"
      :aria-label="placementNote.text"
    >
      <span data-testid="placement-badge">{{ placementNote.label }}</span>
      <span class="tooltip-text placement-tooltip" data-testid="placement-badge-tooltip">
        {{ placementNote.text }}
      </span>
    </span>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, nextTick, watch } from "vue";
import type { Component } from "vue"; // <-- Import as a TYPE, not a value
import { useNodeIconUrl } from "../../../composables/useCustomNodeIcon";
import { useNodeStore } from "../../../stores/column-store";
import { useFlowStore } from "../../../stores/flow-store";
import { useResultsStore } from "../../../stores/results-store";
import { nodePlacementNote } from "../../../utils/placement";
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

const cacheExplanation =
  "Cached: settings and upstream configuration match the stored result. Cache validity is checked when the flow runs.";
const showCachedIndicator = computed(
  () =>
    flowStore.isNodeCached(props.nodeId) &&
    !["running", "failure", "warning", "skipped"].includes(nodeResult.value?.statusIndicator ?? ""),
);
const tooltipContent = computed(
  () =>
    nodeStatusTooltip(nodeResult.value) +
    (showCachedIndicator.value ? `\n${cacheExplanation}` : ""),
);

const flowStore = useFlowStore();
const resultsStore = useResultsStore();
const placementNote = computed(() =>
  nodePlacementNote(resultsStore.placements[flowStore.flowId], props.nodeId),
);

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
.node-button.cached-node {
  --cache-ice: #d4f1fb;
  --cache-edge: #4b93af;
  --cache-shine: #ffffff80;
  position: relative;
  background: linear-gradient(145deg, var(--cache-ice), var(--color-background-primary));
}

[data-theme="dark"] .node-button.cached-node {
  --cache-ice: #254b64;
  --cache-edge: #8ac9e4;
  --cache-shine: #bdefff50;
}

/* A static frosted shell; the tool stays opaque and its hit area is unchanged. */
.node-button.cached-node::after {
  content: "";
  position: absolute;
  inset: -3px;
  border: 1px solid var(--cache-edge);
  border-radius: 11px;
  pointer-events: none;
  background:
    linear-gradient(135deg, transparent 30%, var(--cache-shine) 31% 34%, transparent 35%),
    linear-gradient(35deg, transparent 73%, var(--cache-shine) 74% 77%, transparent 78%),
    radial-gradient(ellipse at top left, var(--cache-shine), transparent 55%);
  box-shadow:
    inset 2px 2px 0 var(--cache-shine),
    inset -2px -2px 0 var(--cache-edge);
}

@media (forced-colors: active) {
  .node-button.cached-node {
    background: Canvas;
    outline: 1px solid CanvasText;
  }
  .node-button.cached-node::after {
    display: none;
  }
}

.cache-indicator {
  position: absolute;
  right: -4px;
  top: -4px;
  width: 16px;
  height: 16px;
  display: grid;
  place-items: center;
  font-size: 16px;
  line-height: 1;
  border: 1px solid var(--color-text-secondary);
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border-radius: 3px;
}

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

.placement-badge {
  position: absolute;
  bottom: -14px;
  right: -18px;
  z-index: 2;
  padding: 0 5px;
  border-radius: 8px;
  font-size: 10px;
  line-height: 16px;
  white-space: nowrap;
  color: var(--color-text-primary);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-warning);
}

.placement-badge--refused {
  border-color: var(--color-danger);
}

/* Below the badge, which sits under the node, so the tooltip never covers the node itself. */
.placement-tooltip {
  bottom: auto;
  top: calc(100% + 6px);
}

.placement-badge:hover .tooltip-text {
  visibility: visible;
  opacity: 1;
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
