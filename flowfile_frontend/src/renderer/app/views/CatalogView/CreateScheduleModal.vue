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
        <span class="dialog-header-icon">
          <i :class="isEdit ? 'fa-solid fa-pen-to-square' : 'fa-solid fa-calendar-plus'"></i>
        </span>
        <div class="dialog-header-text">
          <span class="dialog-title">{{ isEdit ? "Edit schedule" : "Create schedule" }}</span>
          <span class="dialog-subtitle">{{
            isEdit ? "Change when this flow runs" : "Run a flow automatically"
          }}</span>
        </div>
      </div>
    </template>

    <el-form label-position="top" class="schedule-form" @submit.prevent="handleCreate">
      <el-form-item v-if="!isEdit" label="Flow">
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

      <el-form-item v-if="!isEdit" label="When should it run?">
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

          <div class="cron-preview" :class="{ invalid: !cronValid && !customCronChecking }">
            <i :class="cronPreviewIcon"></i>
            <span v-if="isCustom && customCronChecking">Checking…</span>
            <span v-else-if="cronPreviewText">{{ cronPreviewText }}</span>
            <span v-else>{{ customCronError || "Enter a valid schedule." }}</span>
          </div>
        </div>

        <div
          v-if="cron.frequency === 'custom' && aiStore.hasConfiguredProvider"
          class="cron-ai-row"
        >
          <el-input
            v-model="aiDescription"
            placeholder="Describe it, e.g. every weekday at 9am"
            :disabled="aiGenerating"
            clearable
            @keyup.enter="generateCron"
          >
            <template #prefix>
              <i class="fa-solid fa-wand-magic-sparkles cron-ai-spark"></i>
            </template>
          </el-input>
          <el-button
            type="primary"
            :loading="aiGenerating"
            :disabled="!aiDescription.trim() || aiGenerating"
            @click="generateCron"
          >
            Generate
          </el-button>
        </div>

        <div
          v-if="cron.frequency === 'custom'"
          class="hint-text"
          :class="{ 'cron-ai-error': !!aiError }"
        >
          {{ aiError || "Standard 5-field cron: minute, hour, day-of-month, month, day-of-week." }}
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

      <template v-if="!isEdit">
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
      </template>
    </el-form>

    <template #footer>
      <el-button @click="$emit('close')">Cancel</el-button>
      <el-button type="primary" :disabled="!isValid" @click="handleCreate">
        {{ isEdit ? "Save changes" : "Create schedule" }}
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, onBeforeUnmount } from "vue";
import type {
  CatalogTable,
  FlowRegistration,
  FlowSchedule,
  FlowScheduleCreate,
  FlowScheduleUpdate,
} from "../../types";
import {
  FREQUENCY_OPTIONS,
  WEEKDAYS,
  buildCron,
  defaultCronState,
  describeCron,
  localTimezone,
  parseCron,
} from "./cron-builder";
import { useAiStore } from "../../stores/ai-store";
import { AiDisabledError, generateCronExpression } from "../../api/ai.api";
import { CatalogApi } from "../../api/catalog.api";

const props = defineProps<{
  visible: boolean;
  flows: FlowRegistration[];
  tables: CatalogTable[];
  preselectedFlowId?: number | null;
  // "edit" reuses the same builder to retime an existing cron schedule (timing only).
  mode?: "create" | "edit";
  editSchedule?: FlowSchedule | null;
}>();

const emit = defineEmits(["close", "create", "update"]);

const isEdit = computed(() => props.mode === "edit");

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

// The IANA zone the schedule runs in. Create: the user's local zone (so "every night at
// 2 AM" runs at their local time). Edit: seeded from the schedule's stored zone (see the
// visible-watcher) so re-saving never silently relocates it to the editor's browser zone.
const timezone = ref(localTimezone());
const cron = ref(defaultCronState());

// AI "describe in words" → cron, shown only in Custom mode when a provider is configured.
const aiStore = useAiStore();
const aiDescription = ref("");
const aiGenerating = ref(false);
const aiError = ref<string | null>(null);

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
const cronPreview = computed(() => describeCron(builtCron.value, timezone.value));

const isCustom = computed(() => cron.value.frequency === "custom");

// Custom mode lets users type any cron, but the preview parser (cronstrue) and
// the backend's croniter accept slightly different grammars. Ask the backend to
// validate — it is the single source of truth for whether the schedule will be
// accepted. Builder modes always emit croniter-valid cron and keep the cronstrue
// preview as their gate, so no round-trip is needed there.
const customCronValid = ref(false);
const customCronError = ref<string | null>(null);
const customCronChecking = ref(false);
let cronCheckTimer: ReturnType<typeof setTimeout> | null = null;
let cronCheckSeq = 0;

function validateCustomCron() {
  if (cronCheckTimer) clearTimeout(cronCheckTimer);
  customCronError.value = null;
  const expr = builtCron.value;
  if (!expr) {
    customCronValid.value = false;
    customCronChecking.value = false;
    return;
  }
  customCronChecking.value = true;
  const seq = ++cronCheckSeq;
  cronCheckTimer = setTimeout(async () => {
    try {
      const result = await CatalogApi.validateCron({
        cron_expression: expr,
        cron_timezone: timezone.value,
      });
      if (seq !== cronCheckSeq) return; // superseded by a newer keystroke
      customCronValid.value = result.valid;
      customCronError.value = result.valid ? null : result.error || "Invalid cron expression.";
    } catch {
      if (seq !== cronCheckSeq) return;
      // Backend unreachable — fall back to cronstrue so we don't hard-block.
      customCronValid.value = !!cronPreview.value;
    } finally {
      if (seq === cronCheckSeq) customCronChecking.value = false;
    }
  }, 350);
}

watch([isCustom, builtCron], ([custom]) => {
  if (custom) {
    validateCustomCron();
  } else {
    if (cronCheckTimer) clearTimeout(cronCheckTimer);
    customCronChecking.value = false;
    customCronError.value = null;
  }
});

onBeforeUnmount(() => {
  if (cronCheckTimer) clearTimeout(cronCheckTimer);
});

// Valid for submission: backend verdict in Custom mode, cronstrue describability otherwise.
const cronValid = computed(() => (isCustom.value ? customCronValid.value : !!cronPreview.value));

// Preview text: human-readable description when available; a neutral confirmation
// for a valid custom expr cronstrue can't describe; empty when invalid.
const cronPreviewText = computed(() => {
  if (isCustom.value) {
    if (!customCronValid.value) return "";
    return cronPreview.value || `Valid schedule (${timezone.value})`;
  }
  return cronPreview.value;
});

const cronPreviewIcon = computed(() => {
  if (isCustom.value && customCronChecking.value) return "fa-solid fa-circle-notch fa-spin";
  return cronValid.value ? "fa-solid fa-calendar-check" : "fa-solid fa-circle-exclamation";
});

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
    if (!open) return;
    // Shared resets for both create and edit.
    customCronValid.value = false;
    customCronError.value = null;
    customCronChecking.value = false;
    aiDescription.value = "";
    aiError.value = null;
    if (!aiStore.providers.length) void aiStore.loadProviders();

    if (isEdit.value && props.editSchedule) {
      // Edit: seed the builder from the existing schedule; preserve its stored timezone.
      const s = props.editSchedule;
      timezone.value = s.cron_timezone || localTimezone();
      form.value.schedule_type = "cron";
      cron.value = s.cron_expression ? parseCron(s.cron_expression) : defaultCronState();
      // Re-opening the same custom expression won't change builtCron, so the
      // [isCustom, builtCron] watch won't re-fire — kick the async check explicitly.
      if (cron.value.frequency === "custom") validateCustomCron();
    } else {
      timezone.value = localTimezone();
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
  if (isEdit.value) return cronValid.value; // edit is cron-only; no flow selection
  if (!form.value.registration_id) return false;
  if (form.value.schedule_type === "cron") return cronValid.value;
  if (form.value.schedule_type === "table_trigger") return !!form.value.trigger_table_id;
  if (form.value.schedule_type === "table_set_trigger")
    return form.value.trigger_table_ids.length >= 2;
  return true;
});

async function generateCron() {
  const description = aiDescription.value.trim();
  if (!description || aiGenerating.value) return;
  aiGenerating.value = true;
  aiError.value = null;
  try {
    // Cron-from-text is a simple surface — resolveSurface routes it to the
    // simple tier when the user has split models on, else the main selection.
    const { provider, model } = aiStore.resolveSurface("cron");
    const request = {
      description,
      provider: provider ?? undefined,
      model,
    };
    let resp = await generateCronExpression(request);
    // A cold first call can time out while the provider SDK imports; that attempt
    // warms it, so a single retry resolves without the user having to click again.
    if (resp.degraded && resp.reason === "timeout") {
      resp = await generateCronExpression(request);
    }
    if (resp.degraded || !resp.cronExpression) {
      aiError.value =
        resp.reason === "no_expression" && resp.explanation
          ? resp.explanation
          : "Couldn't read that as a schedule. Try rephrasing.";
      return;
    }
    cron.value.expression = resp.cronExpression;
  } catch (err) {
    aiError.value =
      err instanceof AiDisabledError
        ? "AI is currently unavailable."
        : "Couldn't reach the AI service — please try again.";
  } finally {
    aiGenerating.value = false;
  }
}

function handleCreate() {
  if (!isValid.value) return;

  if (isEdit.value) {
    const body: FlowScheduleUpdate = {
      cron_expression: builtCron.value,
      cron_timezone: timezone.value,
    };
    emit("update", body);
    return;
  }

  if (!form.value.registration_id) return;

  const body: FlowScheduleCreate = {
    registration_id: form.value.registration_id,
    schedule_type: form.value.schedule_type,
    name: form.value.name.trim() || null,
    description: form.value.description.trim() || null,
  };

  if (form.value.schedule_type === "cron") {
    body.cron_expression = builtCron.value;
    body.cron_timezone = timezone.value;
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

/* AI "describe in words" → cron row (Custom mode only) */
.cron-ai-row {
  display: flex;
  gap: var(--spacing-2);
  align-items: center;
  margin-top: var(--spacing-2);
}
.cron-ai-row :deep(.el-input) {
  flex: 1 1 auto;
}
.cron-ai-spark {
  font-size: 15px;
  color: var(--el-color-primary);
}
.cron-ai-error {
  color: var(--el-color-error);
}
</style>
