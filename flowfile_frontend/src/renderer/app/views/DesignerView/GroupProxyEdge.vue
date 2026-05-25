<script setup lang="ts">
// UI-only edge shown while a group is collapsed: it reroutes a boundary connection to the
// collapsed pill. Non-interactive (no delete button, no label) and never persisted — see
// useNodeGroups.addGroupProxyEdges. Rendered dashed/muted to read as a proxy, not a real wire.
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
