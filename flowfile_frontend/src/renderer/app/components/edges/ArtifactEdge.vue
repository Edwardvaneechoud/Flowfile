<template>
  <BaseEdge
    :id="id"
    :style="edgeStyle"
    :path="path[0]"
    :marker-end="markerEnd"
    :label-x="path[1]"
    :label-y="path[2]"
  />
  <EdgeLabelRenderer>
    <div
      :style="{
        position: 'absolute',
        transform: `translate(-50%, -50%) translate(${path[1]}px, ${path[2]}px)`,
        pointerEvents: 'all',
      }"
      class="artifact-edge-label nodrag nopan"
    >
      {{ data?.artifact_name || "" }}
    </div>
  </EdgeLabelRenderer>
</template>

<script setup lang="ts">
import { computed } from "vue";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type EdgeProps,
} from "@vue-flow/core";

const props = defineProps<EdgeProps>();

const path = computed(() =>
  getBezierPath({
    sourceX: props.sourceX,
    sourceY: props.sourceY,
    targetX: props.targetX,
    targetY: props.targetY,
    sourcePosition: props.sourcePosition,
    targetPosition: props.targetPosition,
    curvature: 0.4,
  }),
);

// The canvas applies `filter: invert(100%)` to all edges in the SVG container.
// We use the inverted color so the edge appears purple (#8b5cf6) after inversion.
const edgeStyle = {
  stroke: "#74a309",
  strokeWidth: 1.5,
  strokeDasharray: "6 4",
  opacity: 0.7,
};

const markerEnd = computed(() => props.markerEnd);
</script>

<style>
.artifact-edge-label {
  font-size: 10px;
  font-weight: 500;
  color: #8b5cf6;
  background-color: var(--color-background-primary, #fff);
  padding: 1px 6px;
  border-radius: 4px;
  border: 1px solid rgba(139, 92, 246, 0.3);
  white-space: nowrap;
  font-family: var(--font-family-base);
}
</style>
