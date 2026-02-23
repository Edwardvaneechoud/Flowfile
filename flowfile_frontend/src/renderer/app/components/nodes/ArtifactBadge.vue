<template>
  <div v-if="summary" class="artifact-badges">
    <div
      v-if="summary.published_count > 0"
      class="artifact-badge artifact-badge-publish"
      @mouseenter="showPublishTooltip = true"
      @mouseleave="showPublishTooltip = false"
    >
      <svg
        width="10"
        height="10"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <path d="M12 19V5" />
        <path d="M5 12l7-7 7 7" />
      </svg>
      <span class="badge-count">{{ summary.published_count }}</span>
      <div v-if="showPublishTooltip" class="artifact-tooltip">
        <div class="tooltip-header">Published Artifacts</div>
        <div v-if="summary.kernel_id" class="tooltip-kernel">kernel: {{ summary.kernel_id }}</div>
        <div v-for="art in summary.published" :key="art.name" class="tooltip-item">
          <span class="tooltip-name">{{ art.name }}</span>
          <span v-if="art.type_name" class="tooltip-type">{{ art.type_name }}</span>
        </div>
      </div>
    </div>

    <div
      v-if="summary.consumed_count > 0"
      class="artifact-badge artifact-badge-consume"
      @mouseenter="showConsumeTooltip = true"
      @mouseleave="showConsumeTooltip = false"
    >
      <svg
        width="10"
        height="10"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <path d="M12 5v14" />
        <path d="M19 12l-7 7-7-7" />
      </svg>
      <span class="badge-count">{{ summary.consumed_count }}</span>
      <div v-if="showConsumeTooltip" class="artifact-tooltip">
        <div class="tooltip-header">Consumed Artifacts</div>
        <div v-if="summary.kernel_id" class="tooltip-kernel">kernel: {{ summary.kernel_id }}</div>
        <div v-for="art in summary.consumed" :key="art.name" class="tooltip-item">
          <span class="tooltip-name">{{ art.name }}</span>
          <span v-if="art.type_name" class="tooltip-type">{{ art.type_name }}</span>
          <span v-if="art.source_node_id != null" class="tooltip-source"
            >from node {{ art.source_node_id }}</span
          >
        </div>
      </div>
    </div>

    <div
      v-if="summary.deleted_count > 0"
      class="artifact-badge artifact-badge-delete"
      @mouseenter="showDeleteTooltip = true"
      @mouseleave="showDeleteTooltip = false"
    >
      <svg
        width="10"
        height="10"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <path d="M18 6L6 18" />
        <path d="M6 6l12 12" />
      </svg>
      <span class="badge-count">{{ summary.deleted_count }}</span>
      <div v-if="showDeleteTooltip" class="artifact-tooltip">
        <div class="tooltip-header">Deleted Artifacts</div>
        <div v-for="name in summary.deleted" :key="name" class="tooltip-item">
          <span class="tooltip-name">{{ name }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useFlowStore } from "../../stores/flow-store";

const props = defineProps<{
  nodeId: number;
}>();

const flowStore = useFlowStore();
const showPublishTooltip = ref(false);
const showConsumeTooltip = ref(false);
const showDeleteTooltip = ref(false);

const summary = computed(() => flowStore.getNodeArtifactSummary(props.nodeId));
</script>

<style scoped>
.artifact-badges {
  display: flex;
  gap: 3px;
  position: absolute;
  bottom: -8px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 10;
}

.artifact-badge {
  display: flex;
  align-items: center;
  gap: 2px;
  padding: 1px 4px;
  border-radius: 8px;
  font-size: 9px;
  font-weight: 600;
  cursor: default;
  position: relative;
  line-height: 1;
  font-family: var(--font-family-base);
  border: 1px solid;
}

.artifact-badge-publish {
  background-color: rgba(99, 102, 241, 0.15);
  color: #6366f1;
  border-color: rgba(99, 102, 241, 0.3);
}

.artifact-badge-consume {
  background-color: rgba(16, 185, 129, 0.15);
  color: #10b981;
  border-color: rgba(16, 185, 129, 0.3);
}

.artifact-badge-delete {
  background-color: rgba(239, 68, 68, 0.15);
  color: #ef4444;
  border-color: rgba(239, 68, 68, 0.3);
}

.badge-count {
  font-variant-numeric: tabular-nums;
}

.artifact-tooltip {
  position: absolute;
  bottom: calc(100% + 6px);
  left: 50%;
  transform: translateX(-50%);
  background-color: var(--color-gray-800, #1f2937);
  color: var(--color-text-inverse, #f9fafb);
  border-radius: 6px;
  padding: 8px 10px;
  font-size: 11px;
  min-width: 180px;
  max-width: 260px;
  z-index: 1000;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
  pointer-events: none;
}

.artifact-tooltip::after {
  content: "";
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border: 5px solid transparent;
  border-top-color: var(--color-gray-800, #1f2937);
}

.tooltip-header {
  font-weight: 600;
  margin-bottom: 4px;
  font-size: 11px;
  opacity: 0.9;
}

.tooltip-kernel {
  font-size: 10px;
  opacity: 0.6;
  margin-bottom: 4px;
  font-style: italic;
}

.tooltip-item {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  padding: 2px 0;
  border-top: 1px solid rgba(255, 255, 255, 0.1);
  align-items: baseline;
}

.tooltip-item:first-of-type {
  border-top: none;
}

.tooltip-name {
  font-weight: 500;
}

.tooltip-type {
  opacity: 0.7;
  font-size: 10px;
}

.tooltip-source {
  opacity: 0.5;
  font-size: 10px;
}
</style>
