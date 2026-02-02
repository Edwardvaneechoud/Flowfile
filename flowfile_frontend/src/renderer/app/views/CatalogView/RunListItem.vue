<template>
  <div class="run-list-item" :class="{ selected }" @click="$emit('select')">
    <span class="status-dot" :class="statusClass"></span>
    <div class="run-info">
      <span class="run-name">{{ run.flow_name }}</span>
      <span class="run-time">{{ formatDate(run.started_at) }}</span>
    </div>
    <div class="run-right">
      <span class="run-duration">{{ formatDuration(run.duration_seconds) }}</span>
      <i v-if="run.has_snapshot" class="fa-solid fa-code-branch snapshot-icon" title="Has version snapshot"></i>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { FlowRun } from "../../types";

const props = defineProps<{
  run: FlowRun;
  selected: boolean;
}>();

defineEmits<{ select: [] }>();

const statusClass = computed(() => {
  if (props.run.success === null) return "pending";
  return props.run.success ? "success" : "failure";
});

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString(undefined, {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

function formatDuration(seconds: number | null): string {
  if (seconds === null) return "--";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}
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

.run-list-item:hover { background: var(--color-background-hover); }
.run-list-item.selected { background: rgba(59, 130, 246, 0.1); }

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.status-dot.success { background: #22c55e; }
.status-dot.failure { background: #ef4444; }
.status-dot.pending { background: #eab308; }

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

.snapshot-icon {
  font-size: 11px;
  color: var(--color-primary);
}
</style>
