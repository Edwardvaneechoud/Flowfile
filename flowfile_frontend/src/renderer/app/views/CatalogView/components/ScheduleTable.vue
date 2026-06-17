<template>
  <div class="schedule-table" :class="{ 'with-flow': showFlowColumn, 'no-flow': !showFlowColumn }">
    <div class="table-header">
      <span class="col-status">Status</span>
      <span v-if="showFlowColumn" class="col-flow">Flow</span>
      <span class="col-name">Name</span>
      <span class="col-description">Description</span>
      <span class="col-type">Type</span>
      <span class="col-last">Last Triggered</span>
      <span class="col-actions">Actions</span>
      <span v-if="showFlowColumn" class="col-arrow" />
    </div>

    <div
      v-for="schedule in visibleSchedules"
      :key="schedule.id"
      class="table-row"
      :class="{ 'row-disabled': !schedule.enabled }"
      @click="emit('selectSchedule', schedule.id)"
    >
      <div class="col-status">
        <ScheduleStatusBadge :running="isRunning(schedule)" :enabled="schedule.enabled" />
      </div>
      <div v-if="showFlowColumn" class="col-flow">
        <span
          class="flow-name flow-link"
          @click.stop="emit('viewFlow', (schedule as EnrichedFlowSchedule).registration_id)"
        >
          {{ (schedule as EnrichedFlowSchedule).flowName }}
        </span>
      </div>
      <div class="col-name">
        {{ getScheduleDisplayName(schedule, schedule.id) }}
      </div>
      <div class="col-description" @click.stop>
        <template v-if="editingId === schedule.id">
          <input
            ref="descriptionInputRef"
            v-model="editValue"
            class="edit-description-input"
            placeholder="Add description..."
            maxlength="200"
            @keydown.enter="saveEditor(schedule)"
            @keydown.escape="cancelEditor"
            @blur="saveEditor(schedule)"
          />
        </template>
        <template v-else>
          <span
            class="description-text"
            :class="{ placeholder: !schedule.description }"
            @click="startEditor(schedule)"
          >
            {{ schedule.description || "Add description..." }}
          </span>
          <button class="btn-icon-inline" title="Edit description" @click="startEditor(schedule)">
            <i class="fa-solid fa-pen" />
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
      <div class="col-actions" @click.stop>
        <el-tooltip
          v-if="isRunning(schedule)"
          content="Cancel run"
          placement="top"
          :show-after="400"
        >
          <el-button size="small" type="warning" text @click="emit('cancelScheduleRun', schedule)">
            <i class="fa-solid fa-stop" />
          </el-button>
        </el-tooltip>
        <el-tooltip v-else content="Run Now" placement="top" :show-after="400">
          <el-button
            size="small"
            type="success"
            text
            :disabled="disableRunNow"
            @click="emit('runNow', schedule.id)"
          >
            <i class="fa-solid fa-play" />
          </el-button>
        </el-tooltip>
        <el-switch
          :model-value="schedule.enabled"
          size="small"
          @change="(val: boolean) => emit('toggleSchedule', schedule.id, val)"
        />
        <el-button size="small" type="danger" text @click="emit('deleteSchedule', schedule.id)">
          <i class="fa-solid fa-trash" />
        </el-button>
      </div>
      <div v-if="showFlowColumn" class="col-arrow">
        <button
          class="btn-icon-inline"
          title="View schedule details"
          @click.stop="emit('selectSchedule', schedule.id)"
        >
          <i class="fa-solid fa-arrow-right" />
        </button>
      </div>
    </div>

    <div v-if="paginated && totalPages > 1" class="pagination-bar">
      <button class="page-btn" :disabled="currentPage <= 1" @click="currentPage = 1">
        <i class="fa-solid fa-angles-left" />
      </button>
      <button class="page-btn" :disabled="currentPage <= 1" @click="currentPage--">
        <i class="fa-solid fa-angle-left" />
      </button>
      <span class="page-info">Page {{ currentPage }} of {{ totalPages }}</span>
      <button class="page-btn" :disabled="currentPage >= totalPages" @click="currentPage++">
        <i class="fa-solid fa-angle-right" />
      </button>
      <button
        class="page-btn"
        :disabled="currentPage >= totalPages"
        @click="currentPage = totalPages"
      >
        <i class="fa-solid fa-angles-right" />
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import type { FlowSchedule } from "../../../types";
import {
  formatDate,
  formatScheduleType,
  getScheduleDisplayName,
  scheduleIcon,
} from "../catalog-formatters";
import { useInlineDescriptionEdit } from "../../../composables/useInlineDescriptionEdit";
import ScheduleStatusBadge from "./ScheduleStatusBadge.vue";

interface EnrichedFlowSchedule extends FlowSchedule {
  flowName: string;
  isRunning: boolean;
}

type ScheduleLike = FlowSchedule | EnrichedFlowSchedule;

const props = withDefaults(
  defineProps<{
    schedules: ScheduleLike[];
    showFlowColumn?: boolean;
    paginated?: boolean;
    pageSize?: number;
    disableRunNow?: boolean;
    isRunningFn?: (schedule: ScheduleLike) => boolean;
    saveDescription: (id: number, description: string | null) => Promise<void>;
  }>(),
  {
    showFlowColumn: false,
    paginated: false,
    pageSize: 25,
    disableRunNow: false,
    isRunningFn: undefined,
  },
);

const emit = defineEmits<{
  selectSchedule: [id: number];
  runNow: [id: number];
  cancelScheduleRun: [schedule: ScheduleLike];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [id: number];
  viewFlow: [registrationId: number];
}>();

const {
  editingId,
  editValue,
  inputRef: descriptionInputRef,
  start: startEditor,
  cancel: cancelEditor,
  save: saveEditor,
} = useInlineDescriptionEdit({
  save: (id, description) => props.saveDescription(id, description),
});

const currentPage = ref(1);
const totalPages = computed(() => Math.max(1, Math.ceil(props.schedules.length / props.pageSize)));
const visibleSchedules = computed(() => {
  if (!props.paginated) return props.schedules;
  const start = (currentPage.value - 1) * props.pageSize;
  return props.schedules.slice(start, start + props.pageSize);
});

function isRunning(schedule: ScheduleLike): boolean {
  if (props.isRunningFn) return props.isRunningFn(schedule);
  if ("isRunning" in schedule) return schedule.isRunning;
  return false;
}
</script>

<style scoped>
.schedule-table.with-flow .table-header,
.schedule-table.with-flow .table-row {
  grid-template-columns: 120px 1fr 1fr 1fr 160px 160px 160px 40px;
}

.schedule-table.no-flow .table-header,
.schedule-table.no-flow .table-row {
  grid-template-columns: 100px 1fr minmax(120px, 1fr) 150px 130px 120px;
}

.col-name {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.col-flow {
  display: flex;
  flex-direction: column;
}

.col-actions {
  gap: var(--spacing-2);
}

.type-icon {
  color: var(--color-primary);
}
</style>
