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
            <i
              v-else
              :class="scheduleIcon(schedule)"
            />
            <span>{{ formatScheduleType(schedule) }}</span>
            <span v-if="isRunning(schedule)" class="running-label">Running</span>
          </div>
          <div class="schedule-meta">
            <span v-if="schedule.last_triggered_at" class="last-triggered">
              Last: {{ formatDate(schedule.last_triggered_at) }}
            </span>
            <span v-else class="last-triggered">Never triggered</span>
          </div>
          <div class="schedule-flow flow-link" @click="$emit('viewFlow', schedule.registration_id)">{{ flowName(schedule) }}</div>
        </div>
        <div class="schedule-actions">
          <el-tooltip content="Run Now" placement="top" :show-after="400">
            <el-button
              size="small"
              type="success"
              text
              :disabled="isRunning(schedule)"
              @click="$emit('runNow', schedule.id)"
            >
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
import { computed } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowSchedule } from "../../types";

defineProps<{
  schedules: FlowSchedule[];
}>();

defineEmits<{
  createSchedule: [];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [id: number];
  runNow: [id: number];
  viewFlow: [registrationId: number];
}>();

const catalogStore = useCatalogStore();

const activeRegistrationIds = computed(() => {
  return new Set(catalogStore.activeRuns.map((r) => r.registration_id).filter((id) => id !== null));
});

function isRunning(schedule: FlowSchedule): boolean {
  return activeRegistrationIds.value.has(schedule.registration_id);
}

function flowName(schedule: FlowSchedule): string {
  return (
    catalogStore.allFlows.find((f) => f.id === schedule.registration_id)?.name ??
    `Flow #${schedule.registration_id}`
  );
}

function scheduleIcon(schedule: FlowSchedule): string {
  if (schedule.schedule_type === "interval") return "fa-solid fa-clock";
  if (schedule.schedule_type === "table_set_trigger") return "fa-solid fa-layer-group";
  return "fa-solid fa-table";
}

function formatScheduleType(schedule: FlowSchedule): string {
  if (schedule.schedule_type === "interval" && schedule.interval_seconds) {
    const mins = Math.floor(schedule.interval_seconds / 60);
    if (mins < 60) return `Every ${mins}m`;
    const hrs = Math.floor(mins / 60);
    const remMins = mins % 60;
    return remMins > 0 ? `Every ${hrs}h ${remMins}m` : `Every ${hrs}h`;
  }
  if (schedule.schedule_type === "table_trigger") {
    const name = schedule.trigger_table_name ?? `#${schedule.trigger_table_id}`;
    return `On refresh: ${name}`;
  }
  if (schedule.schedule_type === "table_set_trigger") {
    const names = schedule.trigger_table_names ?? [];
    if (names.length > 0) return `Listens to: ${names.join(", ")}`;
    return `Listens to ${schedule.trigger_table_ids?.length ?? 0} tables`;
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
  color: #3b82f6;
}

.running-label {
  font-size: 11px;
  font-weight: 500;
  color: #3b82f6;
  background: rgba(59, 130, 246, 0.1);
  padding: 1px 6px;
  border-radius: 8px;
}
</style>
