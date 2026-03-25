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
      <RunHistoryTable
        :schedule-id="schedule.id"
        collapsible
        @view-run="$emit('viewRun', $event)"
        @view-flow="$emit('viewFlow', $event)"
        @view-schedule-runs="$emit('viewScheduleRuns', $event)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import { ElMessage } from "element-plus";
import { useCatalogStore } from "../../stores/catalog-store";
import { CatalogApi } from "../../api/catalog.api";
import type { FlowSchedule } from "../../types";
import {
  formatDate,
  formatScheduleType,
  getScheduleDisplayName,
  scheduleIcon,
} from "./catalog-formatters";
import RunHistoryTable from "./RunHistoryTable.vue";

const catalogStore = useCatalogStore();

const props = defineProps<{
  schedule: FlowSchedule;
}>();

defineEmits<{
  close: [];
  viewRun: [runId: number];
  viewFlow: [registrationId: number];
  viewScheduleRuns: [scheduleId: number];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [scheduleId: number];
  runNow: [scheduleId: number];
  cancelScheduleRun: [schedule: FlowSchedule];
}>();

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
</style>
