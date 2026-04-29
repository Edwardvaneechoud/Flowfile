<template>
  <div class="schedule-overview">
    <div class="schedule-header">
      <h2>Schedules Overview</h2>
      <el-button type="primary" size="small" @click="$emit('createSchedule')">
        <i class="fa-solid fa-plus" /> Create Schedule
      </el-button>
    </div>

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
      <template v-if="isStandalone || isDockerMode">
        <!-- No start/stop controls for standalone scheduler or Docker mode -->
      </template>
      <template v-else>
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
      </template>
    </div>

    <!-- Lifecycle warning (hidden in Docker mode where the scheduler is always running) -->
    <div v-if="!isDockerMode" class="scheduler-warning">
      <i class="fa-solid fa-circle-info"></i>
      <span v-if="isStandalone">
        The scheduler is running as a standalone service. Manage it from the process where it was
        started.
      </span>
      <span v-else>
        The scheduler runs inside the Flowfile process. Schedules will only be active while Flowfile
        is running.
        <el-popover placement="bottom" :width="340" trigger="hover" :show-after="200">
          <template #reference>
            <span class="standalone-link">Run as a standalone service?</span>
          </template>
          <div class="standalone-popover">
            <p>
              You can run the scheduler as an independent background service so it stays active even
              when the UI is closed:
            </p>
            <code class="standalone-cmd">pip install flowfile</code>
            <code class="standalone-cmd">flowfile run flowfile_scheduler</code>
          </div>
        </el-popover>
      </span>
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
    <EmptyState
      v-if="catalogStore.schedules.length === 0"
      icon="fa-solid fa-calendar-xmark"
      title="No schedules configured"
      description="Create a schedule to automatically run your flows on a timer or when a table updates."
    >
      <template #actions>
        <el-button type="primary" @click="$emit('createSchedule')">
          <i class="fa-solid fa-plus" /> Create Schedule
        </el-button>
      </template>
    </EmptyState>

    <!-- Schedules list -->
    <ScheduleTable
      v-else
      :schedules="enrichedSchedules"
      show-flow-column
      paginated
      :save-description="onSaveDescription"
      @select-schedule="$emit('selectSchedule', $event)"
      @run-now="$emit('runNow', $event)"
      @cancel-schedule-run="$emit('cancelScheduleRun', $event)"
      @toggle-schedule="(id, val) => $emit('toggleSchedule', id, val)"
      @delete-schedule="$emit('deleteSchedule', $event)"
      @view-flow="$emit('viewFlow', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import { CatalogApi } from "../../api/catalog.api";
import { authService } from "../../services/auth.service";
import { formatDate } from "./catalog-formatters";
import { EmptyState } from "../../components/common";
import ScheduleTable from "./components/ScheduleTable.vue";

const catalogStore = useCatalogStore();
const isDockerMode = computed(() => !authService.isInElectronMode());

defineEmits([
  "createSchedule",
  "toggleSchedule",
  "deleteSchedule",
  "runNow",
  "cancelScheduleRun",
  "viewFlow",
  "selectSchedule",
]);

const isStandalone = computed(
  () => catalogStore.schedulerStatus?.active && catalogStore.schedulerStatus?.is_embedded === false,
);

const enrichedSchedules = computed(() => catalogStore.enrichedSchedules);

const enabledCount = computed(() => catalogStore.schedules.filter((s) => s.enabled).length);

const runningCount = computed(() => enrichedSchedules.value.filter((s) => s.isRunning).length);

async function onSaveDescription(scheduleId: number, description: string | null) {
  await CatalogApi.updateSchedule(scheduleId, { description });
  await catalogStore.loadSchedules();
}
</script>

<style scoped>
.schedule-overview {
  max-width: 1000px;
  margin: 0 auto;
}

.schedule-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-5);
}

.schedule-overview h2 {
  margin: 0;
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
  background: color-mix(in srgb, var(--color-success) 5%, transparent);
  border-color: color-mix(in srgb, var(--color-success) 20%, transparent);
}

.scheduler-status-bar.scheduler-inactive {
  background: color-mix(in srgb, var(--color-warning) 5%, transparent);
  border-color: color-mix(in srgb, var(--color-warning) 20%, transparent);
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
  background: var(--color-success);
}

.scheduler-dot.dot-orange {
  background: var(--color-warning);
}

.scheduler-status-text {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.scheduler-heartbeat {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.scheduler-warning {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  margin-bottom: var(--spacing-4);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
  line-height: 1.4;
}

.scheduler-warning i {
  margin-top: 1px;
  flex-shrink: 0;
}

.standalone-link {
  color: var(--color-primary);
  cursor: pointer;
  text-decoration: underline;
  text-decoration-style: dotted;
  text-underline-offset: 2px;
}

.standalone-link:hover {
  text-decoration-style: solid;
}

/* Summary card overrides */
.summary-cards {
  grid-template-columns: repeat(3, 1fr);
  margin-bottom: var(--spacing-6);
}

.summary-card {
  padding: var(--spacing-4);
}

.summary-icon {
  font-size: var(--font-size-xl);
}

.summary-value {
  font-size: var(--font-size-xl);
}
</style>

<style>
.standalone-popover p {
  margin: 0 0 8px;
  font-size: 13px;
  color: var(--el-text-color-regular);
  line-height: 1.5;
}

.standalone-cmd {
  display: block;
  padding: 6px 10px;
  margin-bottom: 4px;
  background: var(--el-fill-color-light);
  border-radius: 4px;
  font-family: var(--font-family-mono);
  font-size: 12px;
  color: var(--el-text-color-primary);
  user-select: all;
}
</style>
