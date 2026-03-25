<template>
  <div class="run-overview">
    <h2>Run History</h2>

    <!-- Summary cards -->
    <div class="summary-cards">
      <div class="summary-card">
        <i class="fa-solid fa-clock-rotate-left summary-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ catalogStore.runsTotal }}</span>
          <span class="summary-label">Total</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-circle-check summary-icon success-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ successCount }}</span>
          <span class="summary-label">Successful</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-circle-xmark summary-icon failure-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ failedCount }}</span>
          <span class="summary-label">Failed</span>
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

    <!-- Filter bar -->
    <div class="filter-bar">
      <el-select
        :model-value="catalogStore.runsTriggerFilter"
        placeholder="All triggers"
        clearable
        size="small"
        style="width: 240px"
        @update:model-value="catalogStore.setTriggerFilter($event)"
      >
        <el-option label="Scheduled runs" value="scheduled" />
        <el-option label="Manual runs" value="manual" />
        <el-option label="On-demand runs" value="on_demand" />
        <el-option label="Designer runs" value="in_designer_run" />
        <el-option-group v-if="catalogStore.enrichedSchedules.length > 0" label="Specific Schedule">
          <el-option
            v-for="schedule in catalogStore.enrichedSchedules"
            :key="schedule.id"
            :label="`${schedule.flowName}: ${getScheduleDisplayName(schedule, schedule.id)}`"
            :value="`schedule:${schedule.id}`"
          />
        </el-option-group>
      </el-select>
    </div>

    <!-- Empty state -->
    <div v-if="catalogStore.runs.length === 0" class="empty-state">
      <i class="fa-solid fa-clock-rotate-left empty-icon"></i>
      <h3>No runs recorded yet</h3>
      <p>Run a registered flow to see its execution history here.</p>
    </div>

    <!-- Runs table -->
    <div v-else class="runs-table">
      <div class="table-header">
        <span class="col-status">Status</span>
        <span class="col-flow">Flow</span>
        <span class="col-trigger">Triggered By</span>
        <span class="col-schedule">Schedule</span>
        <span class="col-started">Started</span>
        <span class="col-duration">Duration</span>
        <span class="col-nodes">Nodes</span>
        <span class="col-version">Version</span>
      </div>
      <div
        v-for="run in catalogStore.runs"
        :key="run.id"
        class="table-row"
        @click="$emit('viewRun', run.id)"
      >
        <div class="col-status">
          <span class="status-badge" :class="runStatusClass(run)">
            <i v-if="run.success === null" class="fa-solid fa-spinner fa-spin" />
            <i v-else-if="run.success" class="fa-solid fa-circle-check" />
            <i v-else class="fa-solid fa-circle-xmark" />
            {{ runStatusText(run) }}
          </span>
        </div>
        <div class="col-flow">
          <span
            class="flow-name flow-link"
            @click.stop="run.registration_id && $emit('viewFlow', run.registration_id)"
          >
            {{ run.flow_name }}
          </span>
        </div>
        <div class="col-trigger">
          <i :class="runTypeIcon(run.run_type)" class="trigger-icon" />
          {{ formatRunType(run.run_type) }}
        </div>
        <div class="col-schedule">
          <template v-if="run.schedule_id">
            <span class="schedule-link" @click.stop="$emit('viewScheduleRuns', run.schedule_id)">
              {{
                getScheduleDisplayName(
                  catalogStore.getScheduleById(run.schedule_id),
                  run.schedule_id,
                )
              }}
            </span>
          </template>
          <span v-else class="no-schedule">--</span>
        </div>
        <div class="col-started">
          {{ formatDate(run.started_at) }}
        </div>
        <div class="col-duration">
          {{ formatDuration(run.duration_seconds) }}
        </div>
        <div class="col-nodes">{{ run.nodes_completed }} / {{ run.number_of_nodes }}</div>
        <div class="col-version">
          <span v-if="run.has_snapshot" class="snapshot-link">
            <i class="fa-solid fa-code-branch" /> View
          </span>
          <span v-else class="no-snapshot">--</span>
        </div>
      </div>
    </div>

    <!-- Pagination -->
    <div v-if="catalogStore.runsTotal > catalogStore.runsPageSize" class="pagination-bar">
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage <= 1"
        @click="catalogStore.setRunsPage(1)"
      >
        <i class="fa-solid fa-angles-left" />
      </button>
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage <= 1"
        @click="catalogStore.setRunsPage(catalogStore.runsPage - 1)"
      >
        <i class="fa-solid fa-angle-left" />
      </button>
      <span class="page-info">
        Page {{ catalogStore.runsPage }} of {{ catalogStore.runsTotalPages }}
      </span>
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage >= catalogStore.runsTotalPages"
        @click="catalogStore.setRunsPage(catalogStore.runsPage + 1)"
      >
        <i class="fa-solid fa-angle-right" />
      </button>
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage >= catalogStore.runsTotalPages"
        @click="catalogStore.setRunsPage(catalogStore.runsTotalPages)"
      >
        <i class="fa-solid fa-angles-right" />
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowRun } from "../../types";
import {
  formatDate,
  formatDuration,
  formatRunType,
  getScheduleDisplayName,
  runTypeIcon,
} from "./catalog-formatters";

const catalogStore = useCatalogStore();

defineEmits<{
  viewRun: [runId: number];
  viewFlow: [registrationId: number];
  viewScheduleRuns: [scheduleId: number];
}>();

const successCount = computed(() => catalogStore.runsTotalSuccess);
const failedCount = computed(() => catalogStore.runsTotalFailed);
const runningCount = computed(() => catalogStore.runsTotalRunning);

function runStatusClass(run: FlowRun): string {
  if (run.success === null) return "running";
  return run.success ? "success" : "failure";
}

function runStatusText(run: FlowRun): string {
  if (run.success === null) return "Running";
  return run.success ? "Success" : "Failed";
}
</script>

<style scoped>
.run-overview {
  max-width: 1000px;
  margin: 0 auto;
}

.run-overview h2 {
  margin: 0 0 var(--spacing-5) 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

/* Summary card padding override */
.summary-card {
  padding: var(--spacing-4);
}

.summary-icon {
  font-size: var(--font-size-xl);
}

.summary-value {
  font-size: var(--font-size-xl);
}

.summary-cards {
  margin-bottom: var(--spacing-6);
}

/* Empty state override */
.empty-state {
  padding: var(--spacing-8) var(--spacing-4);
  color: var(--color-text-secondary);
}

.empty-icon {
  font-size: 48px;
  color: var(--color-primary);
  opacity: 0.5;
  margin-bottom: var(--spacing-4);
}

.empty-state p {
  max-width: 400px;
  margin-left: auto;
  margin-right: auto;
}

/* Filter bar */
.filter-bar {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-4);
}

/* Schedule column */
.col-schedule {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.schedule-link {
  color: var(--color-primary);
  cursor: pointer;
  transition: opacity var(--transition-fast);
}

.schedule-link:hover {
  opacity: 0.8;
  text-decoration: underline;
}

.no-schedule {
  color: var(--color-text-muted);
}

/* Grid column template */
.table-header,
.table-row {
  grid-template-columns: 100px 1fr 110px 1fr 150px 90px 80px 70px;
}
</style>
