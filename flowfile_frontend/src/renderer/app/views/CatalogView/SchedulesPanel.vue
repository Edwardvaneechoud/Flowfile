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
      <div
        v-for="schedule in schedules"
        :key="schedule.id"
        class="schedule-item"
      >
        <div class="schedule-info">
          <div class="schedule-type">
            <i
              :class="
                schedule.schedule_type === 'interval'
                  ? 'fa-solid fa-clock'
                  : 'fa-solid fa-table'
              "
            />
            <span>{{ formatScheduleType(schedule) }}</span>
          </div>
          <div class="schedule-meta">
            <span v-if="schedule.last_triggered_at" class="last-triggered">
              Last: {{ formatDate(schedule.last_triggered_at) }}
            </span>
            <span v-else class="last-triggered">Never triggered</span>
          </div>
          <div class="schedule-flow">
            Flow #{{ schedule.registration_id }}
          </div>
        </div>
        <div class="schedule-actions">
          <el-switch
            :model-value="schedule.enabled"
            size="small"
            @change="(val: boolean) => $emit('toggleSchedule', schedule.id, val)"
          />
          <el-button
            size="small"
            type="danger"
            text
            @click="$emit('deleteSchedule', schedule.id)"
          >
            <i class="fa-solid fa-trash" />
          </el-button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { FlowSchedule } from "../../types";

defineProps<{
  schedules: FlowSchedule[];
}>();

defineEmits<{
  createSchedule: [];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [id: number];
}>();

function formatScheduleType(schedule: FlowSchedule): string {
  if (schedule.schedule_type === "interval" && schedule.interval_seconds) {
    const mins = Math.floor(schedule.interval_seconds / 60);
    if (mins < 60) return `Every ${mins}m`;
    const hrs = Math.floor(mins / 60);
    const remMins = mins % 60;
    return remMins > 0 ? `Every ${hrs}h ${remMins}m` : `Every ${hrs}h`;
  }
  if (schedule.schedule_type === "table_trigger") {
    return `Table trigger #${schedule.trigger_table_id}`;
  }
  return schedule.schedule_type;
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString();
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

.schedule-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
}
</style>
