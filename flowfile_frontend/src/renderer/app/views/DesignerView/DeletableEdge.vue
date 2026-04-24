<script setup lang="ts">
import { computed, inject, type Ref } from "vue";
import { BaseEdge, EdgeLabelRenderer, getBezierPath, useVueFlow } from "@vue-flow/core";
import type { EdgeProps } from "@vue-flow/core";

const props = defineProps<EdgeProps>();

const { removeEdges } = useVueFlow();
const hoveredEdgeId = inject<Ref<string | null>>("hoveredEdgeId");

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

function onDelete() {
  removeEdges([props.id]);
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
    <button
      v-show="isHovered"
      class="edge-delete-btn"
      :style="{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }"
      title="Remove connection"
      type="button"
      @click.stop="onDelete"
      @pointerdown.stop
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
  border: 1px solid #dcdfe6;
  border-radius: 50%;
  background: #fff;
  color: #606266;
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
</style>
