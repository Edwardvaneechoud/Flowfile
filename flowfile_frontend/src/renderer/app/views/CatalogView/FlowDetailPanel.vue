<template>
  <div class="flow-detail">
    <button class="back-btn" @click="$emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>
    <!-- Header -->
    <div class="detail-header">
      <div class="header-main">
        <div class="header-name">
          <template v-if="isEditing">
            <input
              ref="editInput"
              v-model="editName"
              class="edit-name-input"
              @keydown.enter="saveRename"
              @keydown.escape="cancelRename"
              @blur="saveRename"
            />
          </template>
          <template v-else>
            <h2>{{ flow.name }}</h2>
            <button class="btn-icon-inline" title="Rename" @click="startRename">
              <i class="fa-solid fa-pen"></i>
            </button>
          </template>
        </div>
        <p v-if="flow.description" class="description">{{ flow.description }}</p>
      </div>
      <div class="header-actions">
        <button
          v-if="flow.file_exists && !isFlowRunning"
          class="action-btn-run"
          @click="$emit('runFlow', flow.id)"
        >
          <i class="fa-solid fa-play"></i>
          Run Flow
        </button>
        <button
          v-else-if="isFlowRunning"
          class="action-btn-cancel"
          @click="$emit('cancelFlowRun', flow.id)"
        >
          <i class="fa-solid fa-stop"></i>
          Cancel Run
        </button>
        <button
          v-if="flow.file_exists"
          class="action-btn-primary"
          @click="$emit('openFlow', flow.flow_path)"
        >
          <i class="fa-solid fa-up-right-from-square"></i>
          Open in Designer
        </button>
        <button
          class="action-btn-lg"
          :class="{ active: flow.is_favorite }"
          @click="$emit('toggleFavorite', flow.id)"
        >
          <i :class="flow.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
          {{ flow.is_favorite ? "Favorited" : "Favorite" }}
        </button>
        <button
          class="btn-danger-outline"
          title="Delete flow"
          @click="$emit('deleteFlow', flow.id)"
        >
          <i class="fa-solid fa-trash"></i>
          Delete
        </button>
      </div>
    </div>

    <!-- File missing banner -->
    <div v-if="!flow.file_exists" class="missing-banner">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div>
        <strong>Flow file not found</strong>
        <p>
          The file <code>{{ flow.flow_path }}</code> no longer exists on disk. The flow cannot be
          opened or executed, but its run history is still available below.
        </p>
      </div>
    </div>

    <!-- Metadata Grid -->
    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Total Runs</span>
        <span class="meta-value">{{ flow.run_count }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Last Run</span>
        <span class="meta-value">{{
          flow.last_run_at ? formatDate(flow.last_run_at) : "Never"
        }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Artifacts</span>
        <span class="meta-value">{{ flow.artifact_count ?? 0 }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Status</span>
        <span class="meta-value" :class="statusClass">{{ statusText }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Path</span>
        <span class="meta-value mono">{{ flow.flow_path }}</span>
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
            <span class="summary-value">{{ runs.length }}</span>
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

      <div v-if="runs.length === 0" class="empty-runs">No runs recorded yet.</div>
      <div v-else class="overview-table runs-grid">
        <div class="table-header">
          <span class="col-status">Status</span>
          <span class="col-trigger">Triggered By</span>
          <span class="col-started">Started</span>
          <span class="col-duration">Duration</span>
          <span class="col-nodes">Nodes</span>
          <span class="col-version">Version</span>
        </div>
        <div v-for="run in runs" :key="run.id" class="table-row" @click="$emit('viewRun', run.id)">
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
    </div>

    <!-- Data Lineage -->
    <div
      v-if="
        (flow.tables_produced && flow.tables_produced.length > 0) ||
        (flow.tables_read && flow.tables_read.length > 0)
      "
      class="section"
    >
      <h3><i class="fa-solid fa-diagram-project section-icon"></i> Data Lineage</h3>
      <div class="lineage-grid">
        <!-- Tables Read -->
        <div v-if="flow.tables_read && flow.tables_read.length > 0" class="lineage-group">
          <div class="lineage-group-header">
            <span class="lineage-label read-label">
              <i class="fa-solid fa-arrow-right-to-bracket"></i> Reads
            </span>
            <span class="lineage-count">{{ flow.tables_read.length }}</span>
          </div>
          <div class="lineage-items">
            <div
              v-for="table in flow.tables_read"
              :key="table.id"
              class="lineage-item read-item"
              @click="$emit('selectTable', table.id)"
            >
              <i class="fa-solid fa-table lineage-item-icon"></i>
              <span class="lineage-item-name">{{ table.name }}</span>
              <i class="fa-solid fa-chevron-right lineage-item-arrow"></i>
            </div>
          </div>
        </div>
        <!-- Tables Produced -->
        <div v-if="flow.tables_produced && flow.tables_produced.length > 0" class="lineage-group">
          <div class="lineage-group-header">
            <span class="lineage-label produced-label">
              <i class="fa-solid fa-arrow-right-from-bracket"></i> Produces
            </span>
            <span class="lineage-count">{{ flow.tables_produced.length }}</span>
          </div>
          <div class="lineage-items">
            <div
              v-for="table in flow.tables_produced"
              :key="table.id"
              class="lineage-item produced-item"
              @click="$emit('selectTable', table.id)"
            >
              <i class="fa-solid fa-table lineage-item-icon"></i>
              <span class="lineage-item-name">{{ table.name }}</span>
              <i class="fa-solid fa-chevron-right lineage-item-arrow"></i>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Artifacts Section -->
    <div class="section">
      <h3><i class="fa-solid fa-cube section-icon"></i> Global Artifacts</h3>
      <div v-if="artifacts.length === 0" class="empty-state">
        <i class="fa-solid fa-cube empty-state-icon"></i>
        <span>No artifacts published yet</span>
      </div>
      <table v-else class="artifacts-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Version</th>
            <th>Type</th>
            <th>Size</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="artifact in artifacts" :key="artifact.id" class="artifact-row">
            <td>
              <div class="artifact-name">
                <i class="fa-solid fa-cube artifact-icon"></i>
                {{ artifact.name }}
              </div>
              <div v-if="artifact.description" class="artifact-desc">
                {{ artifact.description }}
              </div>
            </td>
            <td>v{{ artifact.version }}</td>
            <td>
              <span class="type-badge">{{ formatType(artifact) }}</span>
            </td>
            <td>{{ formatSize(artifact.size_bytes) }}</td>
            <td>{{ artifact.created_at ? formatDate(artifact.created_at) : "--" }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Schedules Section -->
    <div class="section">
      <div class="section-header">
        <h3><i class="fa-solid fa-calendar-days section-icon"></i> Schedules</h3>
        <el-button
          v-if="flow.file_exists"
          size="small"
          type="primary"
          @click="$emit('addSchedule', flow.id)"
        >
          <i class="fa-solid fa-plus" /> Add
        </el-button>
      </div>

      <!-- Schedule summary cards -->
      <div class="summary-cards summary-cards-3">
        <div class="summary-card">
          <i class="fa-solid fa-calendar-days summary-icon"></i>
          <div class="summary-info">
            <span class="summary-value">{{ catalogStore.flowSchedules.length }}</span>
            <span class="summary-label">Total</span>
          </div>
        </div>
        <div class="summary-card">
          <i class="fa-solid fa-circle-check summary-icon enabled-icon"></i>
          <div class="summary-info">
            <span class="summary-value">{{ scheduleEnabledCount }}</span>
            <span class="summary-label">Enabled</span>
          </div>
        </div>
        <div class="summary-card">
          <i class="fa-solid fa-spinner summary-icon running-icon"></i>
          <div class="summary-info">
            <span class="summary-value">{{ scheduleRunningCount }}</span>
            <span class="summary-label">Running</span>
          </div>
        </div>
      </div>

      <div v-if="catalogStore.flowSchedules.length === 0" class="empty-state">
        <i class="fa-solid fa-calendar-xmark empty-state-icon"></i>
        <span>No schedules configured</span>
      </div>
      <div v-else class="overview-table schedules-grid">
        <div class="table-header">
          <span class="col-status">Status</span>
          <span class="col-description">Description</span>
          <span class="col-type">Type</span>
          <span class="col-last">Last Triggered</span>
          <span class="col-actions">Actions</span>
        </div>
        <div
          v-for="schedule in catalogStore.flowSchedules"
          :key="schedule.id"
          class="table-row"
          :class="{ 'row-disabled': !schedule.enabled }"
        >
          <div class="col-status">
            <span v-if="isScheduleRunning(schedule)" class="status-badge running">
              <i class="fa-solid fa-spinner fa-spin" /> Running
            </span>
            <span v-else-if="schedule.enabled" class="status-badge enabled">
              <i class="fa-solid fa-circle-check" /> Enabled
            </span>
            <span v-else class="status-badge paused">
              <i class="fa-solid fa-circle-pause" /> Disabled
            </span>
          </div>
          <div class="col-description">
            <template v-if="editingScheduleId === schedule.id">
              <input
                ref="descriptionInput"
                v-model="editDescription"
                class="edit-description-input"
                placeholder="Add description..."
                maxlength="200"
                @keydown.enter="saveDescription(schedule.id)"
                @keydown.escape="cancelEditDescription"
                @blur="saveDescription(schedule.id)"
              />
            </template>
            <template v-else>
              <span
                class="description-text"
                :class="{ placeholder: !schedule.description }"
                @click="startEditDescription(schedule)"
              >
                {{ schedule.description || "Add description..." }}
              </span>
              <button
                class="btn-icon-inline"
                title="Edit description"
                @click="startEditDescription(schedule)"
              >
                <i class="fa-solid fa-pen"></i>
              </button>
            </template>
          </div>
          <div class="col-type">
            <i :class="scheduleIcon(schedule)" class="type-icon" />
            {{ formatScheduleType(schedule) }}
          </div>
          <div class="col-last">
            {{ schedule.last_triggered_at ? formatDate(schedule.last_triggered_at) : "Never" }}
          </div>
          <div class="col-actions">
            <el-tooltip
              v-if="isScheduleRunning(schedule)"
              content="Cancel run"
              placement="top"
              :show-after="400"
            >
              <el-button
                size="small"
                type="warning"
                text
                @click="handleCancelScheduleRun(schedule)"
              >
                <i class="fa-solid fa-stop" />
              </el-button>
            </el-tooltip>
            <el-tooltip v-else content="Run Now" placement="top" :show-after="400">
              <el-button
                size="small"
                type="success"
                text
                :disabled="isFlowRunning"
                @click="handleRunNow(schedule.id)"
              >
                <i class="fa-solid fa-play" />
              </el-button>
            </el-tooltip>
            <el-switch
              :model-value="schedule.enabled"
              size="small"
              @change="(val: boolean) => handleToggleSchedule(schedule.id, val)"
            />
            <el-button size="small" type="danger" text @click="handleDeleteSchedule(schedule.id)">
              <i class="fa-solid fa-trash" />
            </el-button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useCatalogStore } from "../../stores/catalog-store";
import { CatalogApi } from "../../api/catalog.api";
import type { FlowRegistration, FlowRun, FlowSchedule, GlobalArtifact } from "../../types";
import {
  formatDate,
  formatDuration,
  formatRunType,
  formatScheduleType,
  runTypeIcon,
  scheduleIcon,
} from "./catalog-formatters";

const catalogStore = useCatalogStore();

const props = defineProps<{
  flow: FlowRegistration;
  runs: FlowRun[];
  artifacts: GlobalArtifact[];
}>();

const emit = defineEmits<{
  close: [];
  viewRun: [runId: number];
  toggleFavorite: [flowId: number];
  openFlow: [flowPath: string];
  selectTable: [tableId: number];
  deleteFlow: [flowId: number];
  renameFlow: [flowId: number, newName: string];
  addSchedule: [flowId: number];
  runFlow: [flowId: number];
  cancelFlowRun: [flowId: number];
}>();

const isEditing = ref(false);
const editName = ref("");
const editInput = ref<HTMLInputElement | null>(null);
const editingScheduleId = ref<number | null>(null);
const editDescription = ref("");
const descriptionInput = ref<HTMLInputElement | null>(null);

function startRename() {
  editName.value = props.flow.name;
  isEditing.value = true;
  nextTick(() => {
    editInput.value?.focus();
    editInput.value?.select();
  });
}

function saveRename() {
  if (!isEditing.value) return;
  const trimmed = editName.value.trim();
  isEditing.value = false;
  if (trimmed && trimmed !== props.flow.name) {
    emit("renameFlow", props.flow.id, trimmed);
  }
}

function cancelRename() {
  isEditing.value = false;
}

const isFlowRunning = computed(() => {
  return catalogStore.activeRuns.some((r) => r.registration_id === props.flow.id);
});

// Run summary counts
const successCount = computed(() => props.runs.filter((r) => r.success === true).length);
const failedCount = computed(() => props.runs.filter((r) => r.success === false).length);
const runningCount = computed(() => props.runs.filter((r) => r.success === null).length);

// Schedule summary counts
const scheduleEnabledCount = computed(
  () => catalogStore.flowSchedules.filter((s) => s.enabled).length,
);
const scheduleRunningCount = computed(
  () =>
    catalogStore.flowSchedules.filter((s) =>
      catalogStore.activeRuns.some((r) => r.registration_id === s.registration_id),
    ).length,
);

function isScheduleRunning(schedule: FlowSchedule): boolean {
  return catalogStore.activeRuns.some((r) => r.registration_id === schedule.registration_id);
}

async function handleCancelScheduleRun(schedule: FlowSchedule) {
  const activeRuns = catalogStore.activeRuns.filter(
    (r) => r.registration_id === schedule.registration_id,
  );
  for (const run of activeRuns) {
    await catalogStore.cancelRun(run.id);
  }
  await Promise.all([catalogStore.loadActiveRuns(), catalogStore.loadRuns()]);
}

const statusClass = computed(() => {
  if (props.flow.last_run_success === null) return "";
  return props.flow.last_run_success ? "text-success" : "text-danger";
});

const statusText = computed(() => {
  if (props.flow.last_run_success === null) return "No runs";
  return props.flow.last_run_success ? "Success" : "Failed";
});

function runStatusClass(run: FlowRun): string {
  if (run.success === null) return "pending";
  return run.success ? "success" : "failure";
}

function runStatusText(run: FlowRun): string {
  if (run.success === null) return "Running";
  return run.success ? "Success" : "Failed";
}

function formatType(artifact: GlobalArtifact): string {
  if (artifact.python_type) {
    const parts = artifact.python_type.split(".");
    return parts[parts.length - 1];
  }
  return artifact.serialization_format ?? "unknown";
}

function formatSize(bytes: number | null): string {
  if (bytes === null) return "--";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function startEditDescription(schedule: FlowSchedule) {
  editingScheduleId.value = schedule.id;
  editDescription.value = schedule.description ?? "";
  nextTick(() => {
    descriptionInput.value?.focus();
  });
}

function cancelEditDescription() {
  editingScheduleId.value = null;
}

async function saveDescription(scheduleId: number) {
  if (editingScheduleId.value !== scheduleId) return;
  const trimmed = editDescription.value.trim();
  const schedule = catalogStore.flowSchedules.find((s) => s.id === scheduleId);
  const oldDescription = schedule?.description ?? "";
  editingScheduleId.value = null;
  if (trimmed !== oldDescription) {
    try {
      await CatalogApi.updateSchedule(scheduleId, { description: trimmed || null });
      await catalogStore.loadFlowSchedules(props.flow.id);
    } catch (e: any) {
      ElMessage.error(e?.response?.data?.detail ?? "Failed to update description");
    }
  }
}

async function handleRunNow(scheduleId: number) {
  try {
    await CatalogApi.triggerScheduleNow(scheduleId);
    await Promise.all([catalogStore.loadActiveRuns(), catalogStore.loadRuns()]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to trigger run");
  }
}

async function handleToggleSchedule(id: number, enabled: boolean) {
  try {
    await CatalogApi.updateSchedule(id, { enabled });
    await catalogStore.loadFlowSchedules(props.flow.id);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to update schedule");
  }
}

async function handleDeleteSchedule(id: number) {
  try {
    await ElMessageBox.confirm(
      "Are you sure you want to delete this schedule?",
      "Delete Schedule",
      {
        confirmButtonText: "Delete",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );
  } catch {
    return; // User cancelled
  }
  try {
    await CatalogApi.deleteSchedule(id);
    await catalogStore.loadFlowSchedules(props.flow.id);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to delete schedule");
  }
}
</script>

<style scoped>
.flow-detail {
  width: 100%;
}

.back-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  cursor: pointer;
  transition: all var(--transition-fast);
  margin-bottom: var(--spacing-3);
}

.back-btn:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
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

.description {
  margin: var(--spacing-1) 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.header-actions {
  display: flex;
  gap: var(--spacing-2);
}

.action-btn-lg {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.action-btn-lg:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}
.action-btn-lg.active {
  color: var(--color-primary);
  border-color: var(--color-primary);
}

.header-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.btn-icon-inline {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-md);
  transition: all var(--transition-fast);
  flex-shrink: 0;
}

.btn-icon-inline:hover {
  background: var(--color-background-hover);
  color: var(--color-primary);
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

.btn-danger-outline {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-3);
  background: transparent;
  color: #ef4444;
  border: 1px solid #ef4444;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.btn-danger-outline:hover {
  background: rgba(239, 68, 68, 0.1);
}

.action-btn-run {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  background: #22c55e;
  color: #fff;
  border: none;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: opacity var(--transition-fast);
}

.action-btn-run:hover {
  opacity: 0.9;
}

.action-btn-run:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.action-btn-cancel {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  background: transparent;
  color: #ef4444;
  border: 1px solid #ef4444;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.action-btn-cancel:hover {
  background: rgba(239, 68, 68, 0.1);
}

.action-btn-primary {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  background: var(--color-primary);
  color: #fff;
  border: none;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: opacity var(--transition-fast);
}

.action-btn-primary:hover {
  opacity: 0.9;
}

/* ========== Meta Grid ========== */
.meta-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-5);
}

.meta-card {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.meta-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.meta-value {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.meta-value.mono {
  font-family: monospace;
  font-size: var(--font-size-xs);
  word-break: break-all;
}

.text-success {
  color: #22c55e;
}
.text-danger {
  color: #ef4444;
}

/* ========== Sections ========== */
.section {
  margin-bottom: var(--spacing-5);
}

.section h3 {
  display: flex;
  align-items: center;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-3) 0;
}

/* ========== Summary Cards ========== */
.summary-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-4);
}

.summary-cards-3 {
  grid-template-columns: repeat(3, 1fr);
}

.summary-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.summary-icon {
  font-size: var(--font-size-lg);
  color: var(--color-primary);
}

.success-icon {
  color: #22c55e;
}

.failure-icon {
  color: #ef4444;
}

.running-icon {
  color: #3b82f6;
}

.enabled-icon {
  color: #22c55e;
}

.summary-info {
  display: flex;
  flex-direction: column;
}

.summary-value {
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-bold);
  color: var(--color-text-primary);
  line-height: 1.2;
}

.summary-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

/* ========== Overview Table ========== */
.overview-table {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.table-header {
  display: grid;
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
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  align-items: center;
  cursor: pointer;
  transition: background var(--transition-fast);
}

.runs-grid .table-header,
.runs-grid .table-row {
  grid-template-columns: 100px 120px 150px 100px 90px 80px;
}

.schedules-grid .table-header,
.schedules-grid .table-row {
  grid-template-columns: 100px 1fr 150px 130px 120px;
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

.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.status-badge.success {
  background: rgba(34, 197, 94, 0.1);
  color: #22c55e;
}

.status-badge.failure {
  background: rgba(239, 68, 68, 0.1);
  color: #ef4444;
}

.status-badge.pending {
  background: rgba(234, 179, 8, 0.1);
  color: #eab308;
}

.status-badge.running {
  background: rgba(59, 130, 246, 0.1);
  color: #3b82f6;
}

.status-badge.enabled {
  background: rgba(34, 197, 94, 0.1);
  color: #22c55e;
}

.status-badge.paused {
  background: rgba(156, 163, 175, 0.15);
  color: var(--color-text-muted);
}

.col-trigger {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.trigger-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.type-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.col-started,
.col-duration,
.col-last {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.col-nodes {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  font-family: monospace;
}

.col-type {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.col-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
}

.snapshot-link {
  color: var(--color-primary);
  cursor: pointer;
  font-size: var(--font-size-xs);
}

.no-snapshot {
  color: var(--color-text-muted);
}

.empty-runs {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  padding: var(--spacing-4);
  text-align: center;
}

/* ========== Artifacts Table ========== */
.artifacts-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
}

.artifacts-table th {
  text-align: left;
  padding: var(--spacing-2) var(--spacing-3);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  border-bottom: 1px solid var(--color-border-primary);
  font-size: var(--font-size-xs);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.artifacts-table td {
  padding: var(--spacing-2) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  color: var(--color-text-primary);
}

/* ========== Artifact Rows ========== */
.artifact-row td {
  vertical-align: top;
}
.artifact-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  font-weight: var(--font-weight-medium);
}
.artifact-icon {
  color: var(--color-primary);
  font-size: var(--font-size-xs);
}
.artifact-desc {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  margin-top: 2px;
}
.type-badge {
  display: inline-block;
  padding: 1px 6px;
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
  font-family: monospace;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  color: var(--color-text-secondary);
}

/* ========== Section Icons ========== */
.section-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  margin-right: var(--spacing-1);
}

/* ========== Data Lineage ========== */
.lineage-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: var(--spacing-4);
}

.lineage-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.lineage-group-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding-bottom: var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.lineage-label {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.read-label {
  color: var(--color-text-secondary);
}

.produced-label {
  color: var(--color-text-secondary);
}

.lineage-count {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  background: var(--color-background-secondary);
  padding: 0 var(--spacing-2);
  border-radius: var(--border-radius-full);
  line-height: 1.8;
}

.lineage-items {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.lineage-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.lineage-item:hover {
  border-color: var(--color-primary);
  background: var(--color-background-hover);
}

.lineage-item-icon {
  font-size: var(--font-size-xs);
  flex-shrink: 0;
}

.read-item .lineage-item-icon {
  color: var(--color-text-muted);
}

.produced-item .lineage-item-icon {
  color: var(--color-text-muted);
}

.lineage-item-name {
  flex: 1;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.lineage-item-arrow {
  font-size: 10px;
  color: var(--color-text-muted);
  opacity: 0;
  transition: opacity var(--transition-fast);
}

.lineage-item:hover .lineage-item-arrow {
  opacity: 1;
  color: var(--color-primary);
}

/* ========== Empty State ========== */
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-5) var(--spacing-4);
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  background: var(--color-background-secondary);
  border: 1px dashed var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.empty-state-icon {
  font-size: var(--font-size-xl);
  opacity: 0.4;
}

/* ========== Missing File Banner ========== */
.missing-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  margin-bottom: var(--spacing-4);
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: var(--border-radius-md);
  color: var(--color-text-primary);
}

.missing-banner > i {
  color: #f59e0b;
  font-size: var(--font-size-lg);
  margin-top: 2px;
  flex-shrink: 0;
}

.missing-banner strong {
  font-size: var(--font-size-sm);
  display: block;
  margin-bottom: var(--spacing-1);
}

.missing-banner p {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.missing-banner code {
  font-family: monospace;
  font-size: var(--font-size-xs);
  background: rgba(0, 0, 0, 0.06);
  padding: 1px 4px;
  border-radius: var(--border-radius-sm);
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: var(--spacing-2);
}

.section-header h3 {
  margin: 0;
}

/* ========== Schedule Table Extras ========== */
.col-description {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  min-width: 0;
}

.col-description .btn-icon-inline {
  opacity: 0;
  transition: opacity var(--transition-fast);
}

.table-row:hover .col-description .btn-icon-inline {
  opacity: 1;
}

.description-text {
  cursor: pointer;
  transition: color var(--transition-fast);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.description-text:hover {
  color: var(--color-text-primary);
}

.description-text.placeholder {
  font-style: italic;
  opacity: 0.6;
}

.edit-description-input {
  width: 100%;
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-xs);
  outline: none;
}
</style>
