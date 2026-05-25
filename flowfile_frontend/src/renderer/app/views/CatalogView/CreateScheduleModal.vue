<template>
  <el-dialog
    :model-value="visible"
    width="540px"
    align-center
    class="schedule-dialog"
    @close="$emit('close')"
  >
    <template #header>
      <div class="dialog-header">
        <span class="dialog-header-icon"><i class="fa-solid fa-calendar-plus"></i></span>
        <div class="dialog-header-text">
          <span class="dialog-title">Create schedule</span>
          <span class="dialog-subtitle">Run a flow automatically</span>
        </div>
      </div>
    </template>

    <el-form label-position="top" class="schedule-form" @submit.prevent="handleCreate">
      <el-form-item label="Flow">
        <el-select
          v-model="form.registration_id"
          placeholder="Select a flow"
          filterable
          style="width: 100%"
        >
          <el-option
            v-for="flow in availableFlows"
            :key="flow.id"
            :label="flow.name"
            :value="flow.id"
          />
        </el-select>
      </el-form-item>

      <el-form-item label="When should it run?">
        <div class="type-cards">
          <button
            v-for="t in SCHEDULE_TYPES"
            :key="t.value"
            type="button"
            class="type-card"
            :class="{ active: form.schedule_type === t.value }"
            @click="form.schedule_type = t.value"
          >
            <span class="type-card-icon"><i :class="t.icon"></i></span>
            <span class="type-card-text">
              <span class="type-card-title">{{ t.title }}</span>
              <span class="type-card-sub">{{ t.subtitle }}</span>
            </span>
            <i
              v-if="form.schedule_type === t.value"
              class="fa-solid fa-circle-check type-card-check"
            ></i>
          </button>
        </div>
      </el-form-item>

      <div v-if="form.schedule_type === 'cron'" class="options-panel">
        <el-form-item label="Repeats">
          <el-select v-model="cron.frequency" style="width: 100%">
            <el-option
              v-for="opt in FREQUENCY_OPTIONS"
              :key="opt.value"
              :label="opt.label"
              :value="opt.value"
            />
          </el-select>
        </el-form-item>

        <el-form-item v-if="cron.frequency === 'weekly'" label="On these days">
          <el-checkbox-group v-model="cron.weekdays" class="weekday-group">
            <el-checkbox-button v-for="d in WEEKDAYS" :key="d.value" :value="d.value">
              {{ d.label }}
            </el-checkbox-button>
          </el-checkbox-group>
        </el-form-item>

        <div class="value-row">
          <el-form-item v-if="cron.frequency === 'minutes'" label="Every" class="value-control">
            <div class="inline-row">
              <el-input-number
                v-model="cron.everyN"
                :min="1"
                :max="59"
                controls-position="right"
                style="width: 110px"
              />
              <span class="unit">minutes</span>
            </div>
          </el-form-item>

          <el-form-item v-else-if="cron.frequency === 'hourly'" label="Every" class="value-control">
            <div class="inline-row">
              <el-input-number
                v-model="cron.everyN"
                :min="1"
                :max="23"
                controls-position="right"
                style="width: 110px"
              />
              <span class="unit">hour(s)</span>
            </div>
          </el-form-item>

          <el-form-item
            v-else-if="cron.frequency === 'monthly'"
            label="On day"
            class="value-control"
          >
            <div class="inline-row">
              <el-input-number
                v-model="cron.dayOfMonth"
                :min="1"
                :max="31"
                controls-position="right"
                style="width: 100px"
              />
              <span class="unit">at</span>
              <el-time-picker
                v-model="cron.time"
                format="HH:mm"
                value-format="HH:mm"
                placeholder="Time"
                style="width: 120px"
              />
            </div>
          </el-form-item>

          <el-form-item
            v-else-if="cron.frequency === 'custom'"
            label="Cron expression"
            class="value-control value-control--wide"
          >
            <el-input v-model="cron.expression" placeholder="e.g. 0 9 * * 1-5" />
          </el-form-item>

          <el-form-item v-else label="At" class="value-control">
            <el-time-picker
              v-model="cron.time"
              format="HH:mm"
              value-format="HH:mm"
              placeholder="Select time"
              style="width: 150px"
            />
          </el-form-item>

          <div class="cron-preview" :class="{ invalid: !cronPreview }">
            <i
              :class="cronPreview ? 'fa-solid fa-calendar-check' : 'fa-solid fa-circle-exclamation'"
            ></i>
            <span v-if="cronPreview">{{ cronPreview }}</span>
            <span v-else>Enter a valid schedule.</span>
          </div>
        </div>

        <div v-if="cron.frequency === 'custom'" class="hint-text">
          Standard 5-field cron: minute, hour, day-of-month, month, day-of-week.
        </div>
      </div>

      <el-form-item v-if="form.schedule_type === 'table_trigger'" label="Trigger table">
        <el-select
          v-model="form.trigger_table_id"
          :placeholder="tables.length === 0 ? 'No catalog tables available' : 'Select a table'"
          :disabled="tables.length === 0"
          filterable
          style="width: 100%"
        >
          <el-option-group v-if="readTables.length > 0" label="Read by this flow">
            <el-option
              v-for="table in readTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag
                v-if="table.table_type === 'virtual'"
                size="small"
                type="info"
                style="margin-left: 8px"
                >virtual</el-tag
              >
            </el-option>
          </el-option-group>
          <el-option-group
            v-if="otherTables.length > 0"
            :label="readTables.length > 0 ? 'Other tables' : 'All tables'"
          >
            <el-option
              v-for="table in otherTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag
                v-if="table.table_type === 'virtual'"
                size="small"
                type="info"
                style="margin-left: 8px"
                >virtual</el-tag
              >
            </el-option>
          </el-option-group>
        </el-select>
        <div v-if="tables.length === 0" class="hint-text">No catalog tables registered yet.</div>
        <div v-else class="hint-text">The flow will run when this table is refreshed.</div>
      </el-form-item>

      <el-form-item v-if="form.schedule_type === 'table_set_trigger'" label="Trigger tables">
        <el-select
          v-model="form.trigger_table_ids"
          :placeholder="
            tables.length === 0 ? 'No catalog tables available' : 'Select tables (at least 2)'
          "
          :disabled="tables.length === 0"
          filterable
          multiple
          style="width: 100%"
        >
          <el-option-group v-if="readTables.length > 0" label="Read by this flow">
            <el-option
              v-for="table in readTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag
                v-if="table.table_type === 'virtual'"
                size="small"
                type="info"
                style="margin-left: 8px"
                >virtual</el-tag
              >
            </el-option>
          </el-option-group>
          <el-option-group
            v-if="otherTables.length > 0"
            :label="readTables.length > 0 ? 'Other tables' : 'All tables'"
          >
            <el-option
              v-for="table in otherTables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <el-tag
                v-if="table.table_type === 'virtual'"
                size="small"
                type="info"
                style="margin-left: 8px"
                >virtual</el-tag
              >
            </el-option>
          </el-option-group>
        </el-select>
        <div v-if="tables.length === 0" class="hint-text">No catalog tables registered yet.</div>
        <div v-else class="hint-text">
          The flow will run when all selected tables have been refreshed.
        </div>
      </el-form-item>

      <el-divider class="details-divider" content-position="left">Details (optional)</el-divider>

      <el-form-item label="Name">
        <el-input v-model="form.name" :maxlength="100" placeholder="e.g. Nightly sales refresh" />
      </el-form-item>

      <el-form-item label="Description">
        <el-input
          v-model="form.description"
          type="textarea"
          :rows="2"
          :maxlength="200"
          show-word-limit
          placeholder="e.g. Nightly sales data refresh"
        />
      </el-form-item>
    </el-form>

    <template #footer>
      <el-button @click="$emit('close')">Cancel</el-button>
      <el-button type="primary" :disabled="!isValid" @click="handleCreate">
        Create schedule
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import type { CatalogTable, FlowRegistration, FlowScheduleCreate } from "../../types";
import {
  FREQUENCY_OPTIONS,
  WEEKDAYS,
  buildCron,
  defaultCronState,
  describeCron,
  localTimezone,
} from "./cron-builder";

const props = defineProps<{
  visible: boolean;
  flows: FlowRegistration[];
  tables: CatalogTable[];
  preselectedFlowId?: number | null;
}>();

const emit = defineEmits(["close", "create"]);

type ScheduleKind = "cron" | "table_trigger" | "table_set_trigger";

const SCHEDULE_TYPES: { value: ScheduleKind; icon: string; title: string; subtitle: string }[] = [
  {
    value: "cron",
    icon: "fa-solid fa-calendar-day",
    title: "On a schedule",
    subtitle: "Run at times you choose",
  },
  {
    value: "table_trigger",
    icon: "fa-solid fa-table",
    title: "When a table updates",
    subtitle: "Run when a table is refreshed",
  },
  {
    value: "table_set_trigger",
    icon: "fa-solid fa-layer-group",
    title: "When several tables update",
    subtitle: "Run once all chosen tables refresh",
  },
];

// Captured once: the user's IANA zone, so "every night at 2 AM" runs at their local time.
const timezone = localTimezone();
const cron = ref(defaultCronState());

const form = ref<{
  registration_id: number | null;
  schedule_type: ScheduleKind;
  trigger_table_id: number | null;
  trigger_table_ids: number[];
  name: string;
  description: string;
}>({
  registration_id: props.preselectedFlowId ?? null,
  schedule_type: "cron",
  trigger_table_id: null,
  trigger_table_ids: [],
  name: "",
  description: "",
});

const builtCron = computed(() => buildCron(cron.value));
const cronPreview = computed(() => describeCron(builtCron.value, timezone));

const availableFlows = computed(() => props.flows.filter((f) => f.file_exists));

const selectedFlow = computed(
  () => props.flows.find((f) => f.id === form.value.registration_id) ?? null,
);

const readTableIds = computed(() => {
  return new Set(selectedFlow.value?.tables_read?.map((t) => t.id) ?? []);
});

const readTables = computed(() => props.tables.filter((t) => readTableIds.value.has(t.id)));

const otherTables = computed(() => props.tables.filter((t) => !readTableIds.value.has(t.id)));

watch(
  () => form.value.registration_id,
  () => {
    form.value.trigger_table_id = null;
    form.value.trigger_table_ids = [];
  },
);

watch(
  () => props.visible,
  (open) => {
    if (open) {
      form.value.registration_id = props.preselectedFlowId ?? null;
      form.value.schedule_type = "cron";
      form.value.trigger_table_id = null;
      form.value.trigger_table_ids = [];
      form.value.name = "";
      form.value.description = "";
      cron.value = defaultCronState();
    }
  },
);

const isValid = computed(() => {
  if (!form.value.registration_id) return false;
  if (form.value.schedule_type === "cron") return !!cronPreview.value;
  if (form.value.schedule_type === "table_trigger") return !!form.value.trigger_table_id;
  if (form.value.schedule_type === "table_set_trigger")
    return form.value.trigger_table_ids.length >= 2;
  return true;
});

function handleCreate() {
  if (!isValid.value || !form.value.registration_id) return;

  const body: FlowScheduleCreate = {
    registration_id: form.value.registration_id,
    schedule_type: form.value.schedule_type,
    name: form.value.name.trim() || null,
    description: form.value.description.trim() || null,
  };

  if (form.value.schedule_type === "cron") {
    body.cron_expression = builtCron.value;
    body.cron_timezone = timezone;
  } else if (form.value.schedule_type === "table_trigger") {
    body.trigger_table_id = form.value.trigger_table_id;
  } else if (form.value.schedule_type === "table_set_trigger") {
    body.trigger_table_ids = form.value.trigger_table_ids;
  }

  emit("create", body);
}
</script>

<style scoped>
/* Dialog chrome */
.schedule-dialog :deep(.el-dialog) {
  border-radius: var(--border-radius-lg);
  overflow: hidden;
}
.schedule-dialog :deep(.el-dialog__header) {
  margin-right: 0;
  padding-bottom: var(--spacing-2);
  border-bottom: 1px solid var(--el-border-color-lighter);
}
.schedule-dialog :deep(.el-dialog__body) {
  padding-top: var(--spacing-3);
  padding-bottom: var(--spacing-1);
}

/* Compact form rhythm */
.schedule-form :deep(.el-form-item) {
  margin-bottom: var(--spacing-3);
}
.schedule-form :deep(.el-form-item__label) {
  padding-bottom: 3px;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--el-text-color-regular);
  line-height: 1.3;
}

.dialog-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}
.dialog-header-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  flex-shrink: 0;
  border-radius: var(--border-radius-md);
  background: var(--el-color-primary-light-9);
  color: var(--el-color-primary);
  font-size: 15px;
}
.dialog-header-text {
  display: flex;
  flex-direction: column;
  line-height: 1.25;
}
.dialog-title {
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--el-text-color-primary);
}
.dialog-subtitle {
  font-size: var(--font-size-sm);
  color: var(--el-text-color-secondary);
}

/* Schedule-type cards */
.type-cards {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
.type-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--el-border-color);
  border-radius: var(--border-radius-md);
  background: var(--el-bg-color);
  cursor: pointer;
  text-align: left;
  transition:
    border-color 0.15s ease,
    background 0.15s ease,
    box-shadow 0.15s ease;
}
.type-card:hover {
  border-color: var(--el-color-primary-light-5);
  background: var(--el-fill-color-light);
}
.type-card.active {
  border-color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
  box-shadow: inset 0 0 0 1px var(--el-color-primary);
}
.type-card-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 30px;
  height: 30px;
  flex-shrink: 0;
  border-radius: var(--border-radius-md);
  background: var(--el-fill-color);
  color: var(--el-text-color-regular);
  font-size: 14px;
  transition:
    background 0.15s ease,
    color 0.15s ease;
}
.type-card.active .type-card-icon {
  background: var(--el-color-primary);
  color: #fff;
}
.type-card-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
  flex: 1;
  min-width: 0;
}
.type-card-title {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-medium);
  color: var(--el-text-color-primary);
}
.type-card-sub {
  font-size: var(--font-size-sm);
  color: var(--el-text-color-secondary);
}
.type-card-check {
  color: var(--el-color-primary);
  font-size: 16px;
  flex-shrink: 0;
}

/* Cron options panel */
.options-panel {
  padding: var(--spacing-3) var(--spacing-3) var(--spacing-1);
  margin-bottom: var(--spacing-3);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: var(--border-radius-md);
  background: var(--el-fill-color-lighter);
}
.options-panel :deep(.el-form-item) {
  margin-bottom: var(--spacing-2);
}

.inline-row {
  display: flex;
  gap: var(--spacing-2);
  align-items: center;
}
.unit {
  font-size: var(--font-size-md);
  color: var(--el-text-color-secondary);
}
.weekday-group {
  display: flex;
  flex-wrap: nowrap;
  gap: 4px;
  width: 100%;
}
.weekday-group :deep(.el-checkbox-button) {
  flex: 1 1 0;
}
.weekday-group :deep(.el-checkbox-button__inner) {
  width: 100%;
  padding: 7px 0;
  text-align: center;
  font-size: var(--font-size-sm);
}

/* Value control + live preview share one row */
.value-row {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-1);
}
.value-row :deep(.el-form-item) {
  margin-bottom: 0;
}
.value-control {
  flex: 0 0 auto;
}
.value-control--wide {
  flex: 1 1 100%;
}

.cron-preview {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex: 1 1 190px;
  min-width: 0;
  padding: 7px 10px;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-medium);
  background: var(--el-color-primary-light-9);
  color: var(--el-color-primary);
}
.cron-preview span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.cron-preview.invalid {
  background: var(--el-color-error-light-9);
  color: var(--el-color-error);
}

.details-divider {
  margin: var(--spacing-1) 0 var(--spacing-3);
}
.details-divider :deep(.el-divider__text) {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--el-text-color-secondary);
  background: var(--el-bg-color);
}

.hint-text {
  font-size: var(--font-size-sm);
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}
</style>
