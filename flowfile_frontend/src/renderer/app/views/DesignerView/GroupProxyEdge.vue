<script setup lang="ts">
// UI-only dashed edge for a collapsed group's rerouted connection.
import { computed } from "vue";
import { BaseEdge, getBezierPath } from "@vue-flow/core";
import type { EdgeProps } from "@vue-flow/core";

const props = defineProps<EdgeProps>();

const path = computed(
  () =>
    getBezierPath({
      sourceX: props.sourceX,
      sourceY: props.sourceY,
      sourcePosition: props.sourcePosition,
      targetX: props.targetX,
      targetY: props.targetY,
      targetPosition: props.targetPosition,
    })[0],
);
</script>

<template>
  <BaseEdge
    :id="props.id"
    :path="path"
    :marker-end="props.markerEnd"
    :style="{ stroke: '#94a3b8', strokeWidth: 1.5, strokeDasharray: '5 4' }"
  />
</template>
