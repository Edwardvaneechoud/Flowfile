<template>
  <div class="schedule-detail">
    <button class="back-btn" @click="$emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>

    <!-- Header -->
    <div class="detail-header">
      <div class="header-main">
        <div class="header-name">
          <template v-if="isEditingName">
            <input
              ref="nameInput"
              v-model="editName"
              class="edit-name-input"
              placeholder="Schedule name..."
              @keydown.enter="saveName"
              @keydown.escape="cancelRename"
              @blur="saveName"
            />
          </template>
          <template v-else>
            <h2>{{ displayName }}</h2>
            <button class="btn-icon-inline" title="Rename" @click="startRename">
              <i class="fa-solid fa-pen"></i>
            </button>
          </template>
        </div>
        <p class="description-line">
          <i :class="scheduleIcon(schedule)" class="type-icon"></i>
          {{ formatScheduleType(schedule) }}
        </p>
      </div>
      <div class="header-actions">
        <button
          v-if="isScheduleRunning && !isFlowRunning"
          class="btn btn-warning btn-sm"
          @click="$emit('cancelScheduleRun', schedule)"
        >
          <i class="fa-solid fa-stop"></i>
          Cancel Run
        </button>
        <button
          v-else-if="!isFlowRunning"
          class="btn btn-success btn-sm"
          @click="$emit('runNow', schedule.id)"
        >
          <i class="fa-solid fa-play"></i>
          Run Now
        </button>
        <el-switch
          :model-value="schedule.enabled"
          size="default"
          active-text="Enabled"
          inactive-text="Disabled"
          @change="(val: boolean) => $emit('toggleSchedule', schedule.id, val)"
        />
        <button class="btn btn-danger btn-sm" @click="$emit('deleteSchedule', schedule.id)">
          <i class="fa-solid fa-trash"></i>
          Delete
        </button>
      </div>
    </div>

    <!-- Metadata Grid -->
    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Type</span>
        <span class="meta-value">
          <i :class="scheduleIcon(schedule)" class="type-icon"></i>
          {{ scheduleTypeName }}
        </span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Flow</span>
        <span class="meta-value flow-link" @click="$emit('viewFlow', schedule.registration_id)">
          {{ flowName }}
        </span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Last Triggered</span>
        <span class="meta-value">{{
          schedule.last_triggered_at ? formatDate(schedule.last_triggered_at) : "Never"
        }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Created</span>
        <span class="meta-value">{{ formatDate(schedule.created_at) }}</span>
      </div>
      <div
        v-if="schedule.schedule_type === 'interval' && schedule.interval_seconds"
        class="meta-card"
      >
        <span class="meta-label">Interval</span>
        <span class="meta-value">{{ formatScheduleType(schedule) }}</span>
      </div>
      <div v-if="schedule.schedule_type === 'table_trigger'" class="meta-card">
        <span class="meta-label">Trigger Table</span>
        <span class="meta-value">{{
          schedule.trigger_table_name ?? `Table #${schedule.trigger_table_id}`
        }}</span>
      </div>
      <div v-if="schedule.schedule_type === 'table_set_trigger'" class="meta-card">
        <span class="meta-label">Trigger Tables</span>
        <span class="meta-value">{{
          schedule.trigger_table_names?.join(", ") ||
          `${schedule.trigger_table_ids?.length ?? 0} tables`
        }}</span>
      </div>
    </div>

    <!-- Description -->
    <div class="section">
      <h3><i class="fa-solid fa-align-left section-icon"></i> Description</h3>
      <div class="description-block">
        <template v-if="isEditingDescription">
          <input
            ref="descriptionInput"
            v-model="editDescription"
            class="edit-description-input"
            placeholder="Add description..."
            maxlength="200"
            @keydown.enter="saveDescription"
            @keydown.escape="cancelEditDescription"
            @blur="saveDescription"
          />
        </template>
        <template v-else>
          <span
            class="description-text"
            :class="{ placeholder: !schedule.description }"
            @click="startEditDescription"
          >
            {{ schedule.description || "Click to add a description..." }}
          </span>
          <button class="btn-icon-inline" title="Edit description" @click="startEditDescription">
            <i class="fa-solid fa-pen"></i>
          </button>
        </template>
      </div>
    </div>

    <!-- Run History -->
    <div class="section">
      <h3><i class="fa-solid fa-clock-rotate-left section-icon"></i> Run History</h3>

      <!-- Run summary cards -->
      <div class="summary-cards">
        <div class="summary-card">
          <i class="fa-solid fa-clock-rotate-left summary-icon"></i>
          <div class="summary-info">
            <span class="summary-value">{{ catalogStore.scheduleRunsTotal }}</span>
            <span class="summary-label">Total</span>
          </div>
        </div>
        <div class="summary-card">
          <i class="fa-solid fa-circle-check summary-icon success-icon"></i>
          <div class="summary-info">
            <span class="summary-value">{{ catalogStore.scheduleRunsTotalSuccess }}</span>
            <span class="summary-label">Successful</span>
          </div>
        </div>
        <div class="summary-card">
          <i class="fa-solid fa-circle-xmark summary-icon failure-icon"></i>
          <div class="summary-info">
            <span class="summary-value">{{ catalogStore.scheduleRunsTotalFailed }}</span>
            <span class="summary-label">Failed</span>
          </div>
        </div>
        <div class="summary-card">
          <i class="fa-solid fa-spinner summary-icon running-icon"></i>
          <div class="summary-info">
            <span class="summary-value">{{ catalogStore.scheduleRunsTotalRunning }}</span>
            <span class="summary-label">Running</span>
          </div>
        </div>
      </div>

      <div v-if="catalogStore.scheduleRunsTotal === 0" class="empty-runs">
        No runs recorded yet.
      </div>
      <div v-else class="overview-table runs-grid">
        <div class="table-header">
          <span class="col-status">Status</span>
          <span class="col-trigger">Triggered By</span>
          <span class="col-started">Started</span>
          <span class="col-duration">Duration</span>
          <span class="col-nodes">Nodes</span>
          <span class="col-version">Version</span>
        </div>
        <div
          v-for="run in visibleRuns"
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
          <div class="col-trigger">
            <i :class="runTypeIcon(run.run_type)" class="trigger-icon" />
            {{ formatRunType(run.run_type) }}
          </div>
          <div class="col-started">{{ formatDate(run.started_at) }}</div>
          <div class="col-duration">{{ formatDuration(run.duration_seconds) }}</div>
          <div class="col-nodes">{{ run.nodes_completed }} / {{ run.number_of_nodes }}</div>
          <div class="col-version">
            <span v-if="run.has_snapshot" class="snapshot-link">
              <i class="fa-solid fa-code-branch" /> View
            </span>
            <span v-else class="no-snapshot">--</span>
          </div>
        </div>
      </div>

      <!-- Expand / Collapse toggle -->
      <button
        v-if="catalogStore.scheduleRunsTotal > collapsedRunCount"
        class="runs-toggle-btn"
        @click="runsExpanded = !runsExpanded"
      >
        <i :class="runsExpanded ? 'fa-solid fa-chevron-up' : 'fa-solid fa-chevron-down'" />
        {{ runsExpanded ? "Show less" : `Show all ${catalogStore.scheduleRunsTotal} runs` }}
      </button>

      <!-- Pagination (only when expanded) -->
      <div
        v-if="runsExpanded && catalogStore.scheduleRunsTotal > catalogStore.runsPageSize"
        class="pagination-bar"
      >
        <button
          class="page-btn"
          :disabled="catalogStore.scheduleRunsPage <= 1"
          @click="catalogStore.setScheduleRunsPage(1, schedule.id)"
        >
          <i class="fa-solid fa-angles-left" />
        </button>
        <button
          class="page-btn"
          :disabled="catalogStore.scheduleRunsPage <= 1"
          @click="catalogStore.setScheduleRunsPage(catalogStore.scheduleRunsPage - 1, schedule.id)"
        >
          <i class="fa-solid fa-angle-left" />
        </button>
        <span class="page-info">
          Page {{ catalogStore.scheduleRunsPage }} of
          {{ catalogStore.scheduleRunsTotalPages }}
        </span>
        <button
          class="page-btn"
          :disabled="catalogStore.scheduleRunsPage >= catalogStore.scheduleRunsTotalPages"
          @click="catalogStore.setScheduleRunsPage(catalogStore.scheduleRunsPage + 1, schedule.id)"
        >
          <i class="fa-solid fa-angle-right" />
        </button>
        <button
          class="page-btn"
          :disabled="catalogStore.scheduleRunsPage >= catalogStore.scheduleRunsTotalPages"
          @click="
            catalogStore.setScheduleRunsPage(catalogStore.scheduleRunsTotalPages, schedule.id)
          "
        >
          <i class="fa-solid fa-angles-right" />
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import { ElMessage } from "element-plus";
import { useCatalogStore } from "../../stores/catalog-store";
import { CatalogApi } from "../../api/catalog.api";
import type { FlowRun, FlowSchedule } from "../../types";
import {
  formatDate,
  formatDuration,
  formatRunType,
  formatScheduleType,
  getScheduleDisplayName,
  runTypeIcon,
  scheduleIcon,
} from "./catalog-formatters";

const catalogStore = useCatalogStore();

const props = defineProps<{
  schedule: FlowSchedule;
  runs: FlowRun[];
}>();

defineEmits<{
  close: [];
  viewRun: [runId: number];
  viewFlow: [registrationId: number];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [scheduleId: number];
  runNow: [scheduleId: number];
  cancelScheduleRun: [schedule: FlowSchedule];
}>();

const runsExpanded = ref(false);
const collapsedRunCount = 5;

const isEditingName = ref(false);
const editName = ref("");
const nameInput = ref<HTMLInputElement | null>(null);

const isEditingDescription = ref(false);
const editDescription = ref("");
const descriptionInput = ref<HTMLInputElement | null>(null);

const displayName = computed(() => getScheduleDisplayName(props.schedule, props.schedule.id));

const flowName = computed(() => {
  const flow = catalogStore.allFlows.find((f) => f.id === props.schedule.registration_id);
  return flow?.name ?? `Flow #${props.schedule.registration_id}`;
});

const scheduleTypeName = computed(() => {
  if (props.schedule.schedule_type === "interval") return "Interval";
  if (props.schedule.schedule_type === "table_trigger") return "Table Trigger";
  if (props.schedule.schedule_type === "table_set_trigger") return "Table Set Trigger";
  return props.schedule.schedule_type;
});

const isScheduleRunning = computed(() =>
  catalogStore.activeRuns.some((r) => r.registration_id === props.schedule.registration_id),
);

const isFlowRunning = computed(() =>
  catalogStore.activeRuns.some((r) => r.registration_id === props.schedule.registration_id),
);

const visibleRuns = computed(() => {
  if (runsExpanded.value) return props.runs;
  return props.runs.slice(0, collapsedRunCount);
});

function runStatusClass(run: FlowRun): string {
  if (run.success === null) return "pending";
  return run.success ? "success" : "failure";
}

function runStatusText(run: FlowRun): string {
  if (run.success === null) return "Running";
  return run.success ? "Success" : "Failed";
}

function startRename() {
  editName.value = props.schedule.name ?? "";
  isEditingName.value = true;
  nextTick(() => {
    nameInput.value?.focus();
    nameInput.value?.select();
  });
}

function cancelRename() {
  isEditingName.value = false;
}

async function saveName() {
  if (!isEditingName.value) return;
  const trimmed = editName.value.trim();
  const oldName = props.schedule.name ?? "";
  isEditingName.value = false;
  if (trimmed !== oldName) {
    try {
      await CatalogApi.updateSchedule(props.schedule.id, { name: trimmed || null });
      await Promise.all([
        catalogStore.loadScheduleDetail(props.schedule.id),
        catalogStore.loadSchedules(),
      ]);
    } catch (e: any) {
      ElMessage.error(e?.response?.data?.detail ?? "Failed to rename schedule");
    }
  }
}

function startEditDescription() {
  isEditingDescription.value = true;
  editDescription.value = props.schedule.description ?? "";
  nextTick(() => {
    descriptionInput.value?.focus();
  });
}

function cancelEditDescription() {
  isEditingDescription.value = false;
}

async function saveDescription() {
  if (!isEditingDescription.value) return;
  const trimmed = editDescription.value.trim();
  const oldDescription = props.schedule.description ?? "";
  isEditingDescription.value = false;
  if (trimmed !== oldDescription) {
    try {
      await CatalogApi.updateSchedule(props.schedule.id, { description: trimmed || null });
      await Promise.all([
        catalogStore.loadScheduleDetail(props.schedule.id),
        catalogStore.loadSchedules(),
      ]);
    } catch (e: any) {
      ElMessage.error(e?.response?.data?.detail ?? "Failed to update description");
    }
  }
}
</script>

<style scoped>
.schedule-detail {
  max-width: 1000px;
  margin: 0 auto;
}

.detail-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: var(--spacing-5);
}

.header-main h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.header-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.edit-name-input {
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-primary);
  border-radius: var(--border-radius-md);
  padding: var(--spacing-1) var(--spacing-2);
  outline: none;
  width: 100%;
}

.description-line {
  margin: var(--spacing-1) 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
}

.header-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

/* Description block */
.description-block {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.description-block .edit-description-input {
  flex: 1;
}

/* ========== Grid column templates ========== */
.runs-grid .table-header,
.runs-grid .table-row {
  grid-template-columns: 100px 120px 150px 100px 90px 80px;
}

.empty-runs {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  padding: var(--spacing-4);
  text-align: center;
}

.runs-toggle-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-1);
  width: 100%;
  padding: var(--spacing-2) 0;
  margin-top: var(--spacing-2);
  background: none;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.runs-toggle-btn:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
  background: var(--color-background-hover);
}
</style>
