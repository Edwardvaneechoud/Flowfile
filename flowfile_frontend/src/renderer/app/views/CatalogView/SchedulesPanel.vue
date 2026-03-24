<template>
  <div class="schedules-panel">
    <div class="panel-header">
      <h3>Schedules</h3>
      <el-button size="small" type="primary" @click="$emit('createSchedule')">
        <i class="fa-solid fa-plus" /> Add
      </el-button>
    </div>

    <div v-if="schedules.length === 0" class="empty-state">
      <i class="fa-solid fa-calendar-xmark" />
      <p>No schedules configured</p>
    </div>

    <div v-else class="schedule-list">
      <div v-for="schedule in schedules" :key="schedule.id" class="schedule-item">
        <div class="schedule-info">
          <div class="schedule-type">
            <i
              v-if="isRunning(schedule)"
              class="fa-solid fa-spinner fa-spin running-indicator"
              title="Running"
            />
            <i v-else :class="scheduleIcon(schedule)" />
            <span>{{ formatScheduleType(schedule) }}</span>
            <span v-if="isRunning(schedule)" class="running-label">Running</span>
          </div>
          <div v-if="schedule.description" class="schedule-description">
            {{ schedule.description }}
          </div>
          <div class="schedule-meta">
            <span v-if="schedule.last_triggered_at" class="last-triggered">
              Last: {{ formatDate(schedule.last_triggered_at) }}
            </span>
            <span v-else class="last-triggered">Never triggered</span>
          </div>
          <div class="schedule-flow flow-link" @click="$emit('viewFlow', schedule.registration_id)">
            {{ flowName(schedule) }}
          </div>
        </div>
        <div class="schedule-actions">
          <el-tooltip
            v-if="isRunning(schedule)"
            content="Cancel run"
            placement="top"
            :show-after="400"
          >
            <el-button
              size="small"
              type="warning"
              text
              @click="$emit('cancelScheduleRun', schedule)"
            >
              <i class="fa-solid fa-stop" />
            </el-button>
          </el-tooltip>
          <el-tooltip v-else content="Run Now" placement="top" :show-after="400">
            <el-button size="small" type="success" text @click="$emit('runNow', schedule.id)">
              <i class="fa-solid fa-play" />
            </el-button>
          </el-tooltip>
          <el-switch
            :model-value="schedule.enabled"
            size="small"
            @change="(val: boolean) => $emit('toggleSchedule', schedule.id, val)"
          />
          <el-button size="small" type="danger" text @click="$emit('deleteSchedule', schedule.id)">
            <i class="fa-solid fa-trash" />
          </el-button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowSchedule } from "../../types";
import { formatDate, formatScheduleType, scheduleIcon } from "./catalog-formatters";

defineProps<{
  schedules: FlowSchedule[];
}>();

defineEmits<{
  createSchedule: [];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [id: number];
  runNow: [id: number];
  cancelScheduleRun: [schedule: FlowSchedule];
  viewFlow: [registrationId: number];
}>();

const catalogStore = useCatalogStore();

function isRunning(schedule: FlowSchedule): boolean {
  return catalogStore.activeRuns.some((r) => r.registration_id === schedule.registration_id);
}

function flowName(schedule: FlowSchedule): string {
  return (
    catalogStore.allFlows.find((f) => f.id === schedule.registration_id)?.name ??
    `Flow #${schedule.registration_id}`
  );
}
</script>

<style scoped>
.schedules-panel {
  padding: 12px;
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.panel-header h3 {
  margin: 0;
  font-size: 14px;
  font-weight: 600;
}

.empty-state {
  text-align: center;
  padding: 24px;
  color: var(--el-text-color-secondary);
}

.empty-state i {
  font-size: 24px;
  margin-bottom: 8px;
}

.empty-state p {
  margin: 0;
  font-size: 13px;
}

.schedule-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.schedule-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 12px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  background: var(--el-bg-color);
}

.schedule-info {
  flex: 1;
  min-width: 0;
}

.schedule-type {
  font-size: 13px;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 6px;
}

.schedule-description {
  font-size: var(--font-size-xs, 11px);
  color: var(--el-text-color-secondary);
  margin-top: 2px;
}

.schedule-meta {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 2px;
}

.schedule-flow {
  font-size: 11px;
  color: var(--el-text-color-placeholder);
  margin-top: 2px;
}

.flow-link {
  cursor: pointer;
  transition: color 0.15s;
}

.flow-link:hover {
  color: var(--el-color-primary);
}

.schedule-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
}

.running-indicator {
  color: var(--el-color-primary);
}

.running-label {
  font-size: var(--font-size-xs, 11px);
  font-weight: 500;
  color: var(--el-color-primary);
  background: rgba(59, 130, 246, 0.1);
  padding: 1px 6px;
  border-radius: var(--border-radius-full, 8px);
}
</style>
