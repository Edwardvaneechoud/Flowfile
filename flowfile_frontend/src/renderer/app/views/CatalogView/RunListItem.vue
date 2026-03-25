<template>
  <div class="run-list-item" :class="{ selected }" @click="$emit('select')">
    <span class="status-dot" :class="statusClass"></span>
    <div class="run-info">
      <span class="run-name">{{ run.flow_name }}</span>
      <span class="run-time">{{ formatDate(run.started_at) }}</span>
    </div>
    <div class="run-right">
      <el-tooltip :content="formatRunType(run.run_type)" placement="top" :show-after="400">
        <i :class="runTypeIcon(run.run_type)" class="run-type-icon"></i>
      </el-tooltip>
      <span class="run-duration">{{ formatDuration(run.duration_seconds) }}</span>
      <i
        v-if="run.has_snapshot"
        class="fa-solid fa-code-branch snapshot-icon"
        title="Has version snapshot"
      ></i>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { FlowRun } from "../../types";
import { formatDate, formatDuration, formatRunType, runTypeIcon } from "./catalog-formatters";

const props = defineProps<{
  run: FlowRun;
  selected: boolean;
}>();

defineEmits<{ select: [] }>();

const statusClass = computed(() => {
  if (props.run.success === null) return "pending";
  return props.run.success ? "success" : "failure";
});
</script>

<style scoped>
.run-list-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: background var(--transition-fast);
}

.run-list-item:hover {
  background: var(--color-background-hover);
}
.run-list-item.selected {
  background: rgba(59, 130, 246, 0.1);
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.status-dot.success {
  background: #22c55e;
}
.status-dot.failure {
  background: #ef4444;
}
.status-dot.pending {
  background: #eab308;
}

.run-info {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-width: 0;
}

.run-name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.run-time {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.run-right {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.run-duration {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  font-family: monospace;
}

.run-type-icon {
  font-size: 11px;
  color: var(--color-text-muted);
}

.snapshot-icon {
  font-size: 11px;
  color: var(--color-primary);
}
</style>
