<script setup lang="ts">
import { computed, inject, ref, type Ref } from "vue";
import { BaseEdge, EdgeLabelRenderer, getBezierPath, useVueFlow } from "@vue-flow/core";
import type { EdgeProps } from "@vue-flow/core";
import { useFlowStore } from "../../stores/flow-store";
import { useResultsStore } from "../../stores/results-store";
import { crossesBackends, crossingNote, runNodeLabel } from "../../utils/boundaryEdges";

const props = defineProps<EdgeProps>();

const { removeEdges } = useVueFlow();
const flowStore = useFlowStore();
const resultsStore = useResultsStore();
const hoveredEdgeId = inject<Ref<string | null>>("hoveredEdgeId");
const cancelEdgeLeave = inject<() => void>("cancelEdgeLeave");
const scheduleEdgeLeave = inject<(id: string) => void>("scheduleEdgeLeave");

const pathData = computed(() =>
  getBezierPath({
    sourceX: props.sourceX,
    sourceY: props.sourceY,
    sourcePosition: props.sourcePosition,
    targetX: props.targetX,
    targetY: props.targetY,
    targetPosition: props.targetPosition,
  }),
);

const path = computed(() => pathData.value[0]);
const labelX = computed(() => pathData.value[1]);
const labelY = computed(() => pathData.value[2]);

const isHovered = computed(() => hoveredEdgeId?.value === props.id);
const lastRun = computed(() => resultsStore.runResults[flowStore.flowId]);
const crosses = computed(() => crossesBackends(lastRun.value, props.source, props.target));
const badgeHovered = ref(false);
const sourceLabel = computed(() => runNodeLabel(lastRun.value, props.source));
const targetLabel = computed(() => runNodeLabel(lastRun.value, props.target));
const note = computed(() => crossingNote(lastRun.value, props.source, props.target));

function onDelete() {
  removeEdges([props.id]);
}

function onButtonEnter() {
  cancelEdgeLeave?.();
}

function onButtonLeave() {
  scheduleEdgeLeave?.(props.id);
}
</script>

<template>
  <BaseEdge
    :id="props.id"
    :path="path"
    :marker-start="props.markerStart"
    :marker-end="props.markerEnd"
    :label="props.label"
    :label-x="labelX"
    :label-y="labelY"
    :label-show-bg="props.labelShowBg"
    :label-style="props.labelStyle"
    :label-bg-style="props.labelBgStyle"
    :label-bg-padding="props.labelBgPadding"
    :label-bg-border-radius="props.labelBgBorderRadius"
    :interaction-width="props.interactionWidth"
  />
  <EdgeLabelRenderer>
    <span
      v-if="crosses"
      class="edge-boundary-badge"
      :class="{ 'is-hovered': badgeHovered }"
      data-testid="boundary-badge"
      :data-edge="`${props.source}-${props.target}`"
      :style="{ transform: `translate(-50%, -100%) translate(${labelX}px, ${labelY - 14}px)` }"
      aria-label="This data is copied between two compute backends"
      @mouseenter="badgeHovered = true"
      @mouseleave="badgeHovered = false"
    >
      <span class="material-icons">swap_horiz</span>
      <span
        v-if="badgeHovered"
        class="edge-boundary-tooltip"
        role="tooltip"
        data-testid="boundary-tooltip"
      >
        <span class="edge-boundary-tooltip-title">Data copied between backends</span>
        <span class="edge-boundary-tooltip-route">{{ sourceLabel }} → {{ targetLabel }}</span>
        <span class="edge-boundary-tooltip-note">{{ note }}</span>
      </span>
    </span>
    <button
      v-show="isHovered"
      class="edge-delete-btn"
      :style="{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }"
      title="Remove connection"
      type="button"
      @click.stop="onDelete"
      @pointerdown.stop
      @mouseenter="onButtonEnter"
      @mouseleave="onButtonLeave"
    >
      &times;
    </button>
  </EdgeLabelRenderer>
</template>

<style scoped>
.edge-delete-btn {
  position: absolute;
  pointer-events: all;
  width: 20px;
  height: 20px;
  padding: 0;
  border: 1px solid var(--color-border-primary);
  border-radius: 50%;
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.12);
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.edge-delete-btn:hover {
  background: #f56c6c;
  color: #fff;
  border-color: #f56c6c;
}

.edge-boundary-badge {
  position: absolute;
  pointer-events: all;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border: 1px solid var(--el-color-primary);
  border-radius: 50%;
  background: var(--color-background-primary);
  color: var(--el-color-primary);
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.12);
  cursor: help;
  transition:
    background-color 0.15s ease,
    color 0.15s ease,
    box-shadow 0.15s ease;
}

.edge-boundary-badge.is-hovered {
  z-index: 10;
  background: var(--el-color-primary);
  color: #fff;
  box-shadow:
    0 0 0 4px var(--el-color-primary-light-8),
    0 1px 4px rgba(0, 0, 0, 0.12);
}

.edge-boundary-badge .material-icons {
  font-size: 14px;
}

.edge-boundary-tooltip {
  position: absolute;
  bottom: calc(100% + 8px);
  left: 50%;
  transform: translateX(-50%);
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 6px 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  width: max-content;
  max-width: 320px;
  font-size: 12px;
  line-height: 1.4;
  pointer-events: none;
  animation: edge-boundary-tooltip-in 0.12s ease-out;
}

.edge-boundary-tooltip-title,
.edge-boundary-tooltip-route {
  white-space: nowrap;
}

.edge-boundary-tooltip-title {
  font-weight: 600;
  color: var(--el-color-primary);
}

.edge-boundary-tooltip-note {
  color: var(--color-text-secondary);
}

@keyframes edge-boundary-tooltip-in {
  from {
    opacity: 0;
    transform: translate(-50%, 4px);
  }
  to {
    opacity: 1;
    transform: translate(-50%, 0);
  }
}
</style>
