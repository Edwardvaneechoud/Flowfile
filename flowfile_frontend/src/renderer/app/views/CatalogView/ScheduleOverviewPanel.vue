<template>
  <div class="schedule-overview">
    <h2>Schedules Overview</h2>

    <!-- Scheduler status bar -->
    <div
      class="scheduler-status-bar"
      :class="catalogStore.schedulerStatus?.active ? 'scheduler-active' : 'scheduler-inactive'"
    >
      <div class="scheduler-status-info">
        <span
          class="scheduler-dot"
          :class="catalogStore.schedulerStatus?.active ? 'dot-green' : 'dot-orange'"
        ></span>
        <span class="scheduler-status-text">
          {{ catalogStore.schedulerStatus?.active ? "Scheduler running" : "Scheduler not running" }}
        </span>
        <span
          v-if="catalogStore.schedulerStatus?.active && catalogStore.schedulerStatus?.heartbeat_at"
          class="scheduler-heartbeat"
        >
          Last heartbeat: {{ formatDate(catalogStore.schedulerStatus.heartbeat_at) }}
        </span>
      </div>
      <el-button
        v-if="catalogStore.schedulerStatus?.active"
        size="small"
        type="warning"
        text
        @click="catalogStore.stopScheduler()"
      >
        <i class="fa-solid fa-stop" /> Stop
      </el-button>
      <el-button v-else size="small" type="success" text @click="catalogStore.startScheduler()">
        <i class="fa-solid fa-play" /> Start
      </el-button>
    </div>

    <!-- Summary cards -->
    <div class="summary-cards">
      <div class="summary-card">
        <i class="fa-solid fa-calendar-days summary-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ catalogStore.schedules.length }}</span>
          <span class="summary-label">Total</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-circle-check summary-icon enabled-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ enabledCount }}</span>
          <span class="summary-label">Enabled</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-spinner summary-icon running-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ runningCount }}</span>
          <span class="summary-label">Running</span>
        </div>
      </div>
    </div>

    <!-- Empty state -->
    <div v-if="catalogStore.schedules.length === 0" class="empty-state">
      <i class="fa-solid fa-calendar-xmark empty-icon"></i>
      <h3>No schedules configured</h3>
      <p>Create a schedule to automatically run your flows on a timer or when a table updates.</p>
      <el-button type="primary" @click="$emit('createSchedule')">
        <i class="fa-solid fa-plus" /> Create Schedule
      </el-button>
    </div>

    <!-- Schedules list -->
    <div v-else class="schedules-table">
      <div class="table-header">
        <span class="col-status">Status</span>
        <span class="col-flow">Flow</span>
        <span class="col-type">Type</span>
        <span class="col-last">Last Triggered</span>
        <span class="col-actions">Actions</span>
      </div>
      <div
        v-for="schedule in enrichedSchedules"
        :key="schedule.id"
        class="table-row"
        :class="{ 'row-disabled': !schedule.enabled }"
      >
        <div class="col-status">
          <span v-if="schedule.isRunning" class="status-badge running">
            <i class="fa-solid fa-spinner fa-spin" /> Running
          </span>
          <span v-else-if="schedule.enabled" class="status-badge enabled">
            <i class="fa-solid fa-circle-check" /> Enabled
          </span>
          <span v-else class="status-badge paused">
            <i class="fa-solid fa-circle-pause" /> Disabled
          </span>
        </div>
        <div class="col-flow">
          <span class="flow-name flow-link" @click="$emit('viewFlow', schedule.registration_id)">{{
            schedule.flowName
          }}</span>
        </div>
        <div class="col-type">
          <i :class="scheduleIcon(schedule)" class="type-icon" />
          {{ formatScheduleType(schedule) }}
        </div>
        <div class="col-last">
          {{ schedule.last_triggered_at ? formatDate(schedule.last_triggered_at) : "Never" }}
        </div>
        <div class="col-actions">
          <el-tooltip content="Run Now" placement="top" :show-after="400">
            <el-button
              size="small"
              type="success"
              text
              :disabled="schedule.isRunning"
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
import { formatDate, formatScheduleType, scheduleIcon } from "./catalog-formatters";

const catalogStore = useCatalogStore();

defineEmits<{
  createSchedule: [];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [id: number];
  runNow: [id: number];
  viewFlow: [registrationId: number];
}>();

interface EnrichedSchedule extends FlowSchedule {
  flowName: string;
  isRunning: boolean;
}

const activeRegistrationIds = computed(() => {
  return new Set(catalogStore.activeRuns.map((r) => r.registration_id).filter((id) => id !== null));
});

const enrichedSchedules = computed((): EnrichedSchedule[] => {
  return catalogStore.schedules.map((s) => ({
    ...s,
    flowName:
      catalogStore.allFlows.find((f) => f.id === s.registration_id)?.name ??
      `Flow #${s.registration_id}`,
    isRunning: activeRegistrationIds.value.has(s.registration_id),
  }));
});

const enabledCount = computed(() => catalogStore.schedules.filter((s) => s.enabled).length);

const runningCount = computed(() => enrichedSchedules.value.filter((s) => s.isRunning).length);
</script>

<style scoped>
.schedule-overview {
  max-width: 900px;
}

.schedule-overview h2 {
  margin: 0 0 var(--spacing-5) 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

/* Scheduler status bar */
.scheduler-status-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
  margin-bottom: var(--spacing-4);
  font-size: var(--font-size-sm);
}

.scheduler-status-bar.scheduler-active {
  background: rgba(34, 197, 94, 0.05);
  border-color: rgba(34, 197, 94, 0.2);
}

.scheduler-status-bar.scheduler-inactive {
  background: rgba(249, 115, 22, 0.05);
  border-color: rgba(249, 115, 22, 0.2);
}

.scheduler-status-info {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.scheduler-dot {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.scheduler-dot.dot-green {
  background: #22c55e;
}

.scheduler-dot.dot-orange {
  background: #f97316;
}

.scheduler-status-text {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.scheduler-heartbeat {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

/* Summary cards */
.summary-cards {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-6);
}

.summary-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.summary-icon {
  font-size: var(--font-size-xl);
  color: var(--color-primary);
}

.enabled-icon {
  color: #22c55e;
}

.running-icon {
  color: #3b82f6;
}

.summary-info {
  display: flex;
  flex-direction: column;
}

.summary-value {
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-bold);
  color: var(--color-text-primary);
  line-height: 1.2;
}

.summary-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

/* Empty state */
.empty-state {
  text-align: center;
  padding: var(--spacing-8) var(--spacing-4);
  color: var(--color-text-secondary);
}

.empty-icon {
  font-size: 48px;
  color: var(--color-primary);
  opacity: 0.5;
  margin-bottom: var(--spacing-4);
}

.empty-state h3 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
  color: var(--color-text-primary);
}

.empty-state p {
  margin: 0 0 var(--spacing-5);
  font-size: var(--font-size-sm);
  max-width: 400px;
  margin-left: auto;
  margin-right: auto;
}

/* Schedules table */
.schedules-table {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.table-header {
  display: grid;
  grid-template-columns: 120px 1fr 160px 160px 130px;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.table-row {
  display: grid;
  grid-template-columns: 120px 1fr 160px 160px 130px;
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  align-items: center;
  transition: background var(--transition-fast);
}

.table-row:last-child {
  border-bottom: none;
}

.table-row:hover {
  background: var(--color-background-hover);
}

.table-row.row-disabled {
  opacity: 0.6;
}

/* Status badges */
.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
}

.status-badge.running {
  color: #3b82f6;
  background: rgba(59, 130, 246, 0.1);
}

.status-badge.enabled {
  color: #22c55e;
  background: rgba(34, 197, 94, 0.1);
}

.status-badge.paused {
  color: var(--color-text-muted);
  background: var(--color-background-secondary);
}

/* Columns */
.col-flow {
  min-width: 0;
}

.flow-name {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.flow-link {
  cursor: pointer;
  transition: color var(--transition-fast);
}

.flow-link:hover {
  color: var(--color-primary);
}

.col-type {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--color-text-secondary);
}

.type-icon {
  color: var(--color-primary);
  font-size: var(--font-size-xs);
}

.col-last {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.col-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}
</style>
