<template>
  <div class="run-history-table">
    <!-- Summary cards -->
    <div v-if="!isCollapsed" class="summary-cards">
      <div class="summary-card">
        <i class="fa-solid fa-clock-rotate-left summary-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ total }}</span>
          <span class="summary-label">Total</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-circle-check summary-icon success-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ totalSuccess }}</span>
          <span class="summary-label">Successful</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-circle-xmark summary-icon failure-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ totalFailed }}</span>
          <span class="summary-label">Failed</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-spinner summary-icon running-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ totalRunning }}</span>
          <span class="summary-label">Running</span>
        </div>
      </div>
    </div>

    <!-- Filter bar -->
    <div v-if="!isCollapsed" class="filter-bar">
      <el-select
        :model-value="triggerFilter"
        placeholder="All triggers"
        clearable
        size="small"
        style="width: 240px"
        @update:model-value="handleFilterChange"
      >
        <el-option label="Scheduled runs" value="scheduled" />
        <el-option label="Manual runs" value="manual" />
        <el-option label="On-demand runs" value="on_demand" />
        <el-option label="Designer runs" value="in_designer_run" />
        <el-option-group
          v-if="!scheduleId && catalogStore.enrichedSchedules.length > 0"
          label="Specific Schedule"
        >
          <el-option
            v-for="s in catalogStore.enrichedSchedules"
            :key="s.id"
            :label="`${s.flowName}: ${getScheduleDisplayName(s, s.id)}`"
            :value="`schedule:${s.id}`"
          />
        </el-option-group>
      </el-select>
    </div>

    <!-- Empty state -->
    <div v-if="runs.length === 0" class="empty-state">
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
        <span class="col-actions"></span>
      </div>
      <div v-for="run in runs" :key="run.id" class="table-row">
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
        <div class="col-actions">
          <button
            class="btn-icon-inline"
            title="View run details"
            @click.stop="$emit('viewRun', run.id)"
          >
            <i class="fa-solid fa-arrow-right"></i>
          </button>
        </div>
      </div>
    </div>

    <!-- Expand / Collapse toggle -->
    <div v-if="collapsible && allRuns.length > COLLAPSED_LIMIT" class="expand-bar">
      <button class="expand-btn" @click="expanded = !expanded">
        <i :class="expanded ? 'fa-solid fa-chevron-up' : 'fa-solid fa-chevron-down'" />
        {{ expanded ? "Show less" : `Show all ${total} runs` }}
      </button>
    </div>

    <!-- Pagination (only when expanded or non-collapsible) -->
    <div v-if="!isCollapsed && total > catalogStore.runsPageSize" class="pagination-bar">
      <button class="page-btn" :disabled="page <= 1" @click="goToPage(1)">
        <i class="fa-solid fa-angles-left" />
      </button>
      <button class="page-btn" :disabled="page <= 1" @click="goToPage(page - 1)">
        <i class="fa-solid fa-angle-left" />
      </button>
      <span class="page-info"> Page {{ page }} of {{ totalPages }} </span>
      <button class="page-btn" :disabled="page >= totalPages" @click="goToPage(page + 1)">
        <i class="fa-solid fa-angle-right" />
      </button>
      <button class="page-btn" :disabled="page >= totalPages" @click="goToPage(totalPages)">
        <i class="fa-solid fa-angles-right" />
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowRun } from "../../types";
import {
  formatDate,
  formatDuration,
  formatRunType,
  getScheduleDisplayName,
  runTypeIcon,
} from "./catalog-formatters";

const COLLAPSED_LIMIT = 5;

const props = defineProps<{
  registrationId?: number;
  scheduleId?: number;
  collapsible?: boolean;
}>();

const expanded = ref(false);

defineEmits<{
  viewRun: [runId: number];
  viewFlow: [registrationId: number];
  viewScheduleRuns: [scheduleId: number];
}>();

const catalogStore = useCatalogStore();

const isScheduleMode = computed(() => props.scheduleId !== undefined);

const allRuns = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRuns : catalogStore.runs,
);

const isCollapsed = computed(() => props.collapsible && !expanded.value);

const runs = computed(() =>
  isCollapsed.value ? allRuns.value.slice(0, COLLAPSED_LIMIT) : allRuns.value,
);
const total = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRunsTotal : catalogStore.runsTotal,
);
const totalSuccess = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRunsTotalSuccess : catalogStore.runsTotalSuccess,
);
const totalFailed = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRunsTotalFailed : catalogStore.runsTotalFailed,
);
const totalRunning = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRunsTotalRunning : catalogStore.runsTotalRunning,
);
const page = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRunsPage : catalogStore.runsPage,
);
const totalPages = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRunsTotalPages : catalogStore.runsTotalPages,
);
const triggerFilter = computed(() =>
  isScheduleMode.value ? catalogStore.scheduleRunsTriggerFilter : catalogStore.runsTriggerFilter,
);

function handleFilterChange(value: string | null) {
  if (isScheduleMode.value) {
    catalogStore.setScheduleTriggerFilter(value, props.scheduleId!);
  } else {
    catalogStore.setTriggerFilter(value);
  }
}

function goToPage(p: number) {
  if (isScheduleMode.value) {
    catalogStore.setScheduleRunsPage(p, props.scheduleId!);
  } else {
    catalogStore.setRunsPage(p, props.registrationId);
  }
}

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

/* Expand / Collapse */
.expand-bar {
  display: flex;
  justify-content: center;
  padding: var(--spacing-3) 0;
}

.expand-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.expand-btn:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

/* Grid column template */
.table-header,
.table-row {
  grid-template-columns: 100px 1fr 110px 1fr 150px 90px 80px 70px 40px;
}
</style>
