<template>
  <div v-if="activeRuns.length > 0" class="active-runs-banner">
    <div class="banner-header">
      <i class="fa-solid fa-spinner fa-spin" />
      <span>{{ activeRuns.length }} active run{{ activeRuns.length > 1 ? "s" : "" }}</span>
    </div>
    <div class="runs-list">
      <div v-for="run in activeRuns" :key="run.id" class="run-item">
        <div class="run-info">
          <span class="run-name">{{ run.flow_name }}</span>
          <span class="run-time">Started {{ formatDate(run.started_at) }}</span>
        </div>
        <el-button size="small" type="danger" text @click="$emit('cancel', run.id)">
          Cancel
        </el-button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { ActiveFlowRun } from "../../types";

defineProps<{
  activeRuns: ActiveFlowRun[];
}>();

defineEmits<{
  cancel: [runId: number];
}>();

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleTimeString();
}
</script>

<style scoped>
.active-runs-banner {
  background: var(--el-color-warning-light-9);
  border: 1px solid var(--el-color-warning-light-5);
  border-radius: 6px;
  padding: 10px 14px;
  margin-bottom: 12px;
}

.banner-header {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  font-weight: 600;
  color: var(--el-color-warning-dark-2);
  margin-bottom: 6px;
}

.runs-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.run-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 4px 0;
}

.run-info {
  display: flex;
  flex-direction: column;
}

.run-name {
  font-size: 13px;
  font-weight: 500;
}

.run-time {
  font-size: 11px;
  color: var(--el-text-color-secondary);
}
</style>
