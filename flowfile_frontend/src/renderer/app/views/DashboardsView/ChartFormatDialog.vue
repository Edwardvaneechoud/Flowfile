<template>
  <el-dialog
    :model-value="modelValue"
    title="Format chart"
    width="480px"
    append-to-body
    draggable
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-form label-width="110px" size="small" @submit.prevent>
      <el-form-item label="Numbers">
        <div class="fmt-field">
          <el-select v-model="draft.numberPreset" data-testid="chart-format-number">
            <el-option
              v-for="p in NUMBER_FORMAT_PRESETS"
              :key="p.id"
              :value="p.id"
              :label="p.label"
            />
            <el-option :value="CUSTOM_FORMAT" label="Custom…" />
          </el-select>
          <el-input
            v-if="draft.numberPreset === CUSTOM_FORMAT"
            v-model="draft.numberCustom"
            placeholder="d3 format, e.g. $,.2f"
            maxlength="64"
            data-testid="chart-format-number-custom"
          />
          <div v-if="numberError" class="fmt-error" data-testid="chart-format-number-error">
            {{ numberError }}
          </div>
          <div v-else-if="numberSample" class="fmt-sample" data-testid="chart-format-number-sample">
            {{ SAMPLE_NUMBER }} → {{ numberSample }}
          </div>
        </div>
      </el-form-item>
      <el-form-item label="Dates">
        <div class="fmt-field">
          <el-select v-model="draft.datePreset" data-testid="chart-format-date">
            <el-option
              v-for="p in DATE_FORMAT_PRESETS"
              :key="p.id"
              :value="p.id"
              :label="p.label"
            />
            <el-option :value="CUSTOM_FORMAT" label="Custom…" />
          </el-select>
          <el-input
            v-if="draft.datePreset === CUSTOM_FORMAT"
            v-model="draft.dateCustom"
            placeholder="d3 time format, e.g. %d/%m/%Y"
            maxlength="64"
            data-testid="chart-format-date-custom"
          />
          <div v-if="dateError" class="fmt-error" data-testid="chart-format-date-error">
            {{ dateError }}
          </div>
          <div v-else-if="dateSample" class="fmt-sample">31 March 2024 → {{ dateSample }}</div>
          <div class="fmt-hint">
            Tooltips and date labels; the ticks of a time axis pick their own format.
          </div>
        </div>
      </el-form-item>
      <el-form-item label="Horizontal axis">
        <el-input
          v-model="draft.xTitle"
          :placeholder="defaultTitles.x ?? 'No field on this axis'"
          :disabled="defaultTitles.x == null && !draft.xTitle"
          maxlength="120"
          clearable
          data-testid="chart-format-x-title"
        />
      </el-form-item>
      <el-form-item label="Vertical axis">
        <div class="fmt-field">
          <el-input
            v-model="draft.yTitle"
            :placeholder="defaultTitles.y ?? 'No field on this axis'"
            :disabled="defaultTitles.y == null && !draft.yTitle"
            maxlength="120"
            clearable
            data-testid="chart-format-y-title"
          />
          <div class="fmt-hint">
            Renames the field the axis shows, in tooltips too. An aggregated measure keeps its
            aggregation, so "Revenue" reads sum(Revenue).
          </div>
        </div>
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button class="fmt-reset" :disabled="!hasAny" @click="resetAll">Reset all</el-button>
      <el-button @click="emit('update:modelValue', false)">Cancel</el-button>
      <el-button
        type="primary"
        :disabled="!!numberError || !!dateError"
        data-testid="chart-format-save"
        @click="onSave"
        >Save</el-button
      >
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onUnmounted, reactive, watch } from "vue";
import type { DashboardChartFormat } from "../../types";
import {
  CUSTOM_FORMAT,
  DATE_FORMAT_PRESETS,
  NUMBER_FORMAT_PRESETS,
  SAMPLE_DATE,
  SAMPLE_NUMBER,
  formatSampleDate,
  formatSampleNumber,
  normalizeChartFormat,
  numberFormatError,
  presetIdFor,
  timeFormatError,
  type FormatPreset,
} from "./chartPresentation";

const props = defineProps<{
  modelValue: boolean;
  chartFormat: DashboardChartFormat | null | undefined;
  /** What each axis shows without an override; null when its shelf is empty. */
  defaultTitles: { x: string | null; y: string | null };
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  /** The draft while it is edited, so the tile behind can show it. */
  (e: "preview", value: DashboardChartFormat | null): void;
  (e: "save", value: DashboardChartFormat | null): void;
}>();

const draft = reactive({
  numberPreset: "auto",
  numberCustom: "",
  datePreset: "auto",
  dateCustom: "",
  xTitle: "",
  yTitle: "",
});

const seed = () => {
  const saved = props.chartFormat ?? {};
  draft.numberPreset = presetIdFor(saved.number_format, NUMBER_FORMAT_PRESETS);
  draft.numberCustom = draft.numberPreset === CUSTOM_FORMAT ? (saved.number_format ?? "") : "";
  draft.datePreset = presetIdFor(saved.time_format, DATE_FORMAT_PRESETS);
  draft.dateCustom = draft.datePreset === CUSTOM_FORMAT ? (saved.time_format ?? "") : "";
  draft.xTitle = saved.x_title ?? "";
  draft.yTitle = saved.y_title ?? "";
};

// Mounted already open (v-if + v-model in one tick), so el-dialog's @open never fires.
watch(
  () => props.modelValue,
  (open) => {
    if (open) seed();
  },
  { immediate: true },
);

const chosen = (id: string, custom: string, presets: FormatPreset[]): string | null =>
  id === CUSTOM_FORMAT ? custom : (presets.find((p) => p.id === id)?.format ?? null);

const numberFormat = computed(() =>
  chosen(draft.numberPreset, draft.numberCustom, NUMBER_FORMAT_PRESETS),
);
const timeFormat = computed(() => chosen(draft.datePreset, draft.dateCustom, DATE_FORMAT_PRESETS));
// Custom starts from the preset it replaces, so a small change needs no retyping.
const presetFormat = (id: string, presets: FormatPreset[]) =>
  presets.find((p) => p.id === id)?.format ?? "";
watch(
  () => draft.numberPreset,
  (next, previous) => {
    if (next === CUSTOM_FORMAT && !draft.numberCustom)
      draft.numberCustom = presetFormat(previous, NUMBER_FORMAT_PRESETS);
  },
);
watch(
  () => draft.datePreset,
  (next, previous) => {
    if (next === CUSTOM_FORMAT && !draft.dateCustom)
      draft.dateCustom = presetFormat(previous, DATE_FORMAT_PRESETS);
  },
);

const numberError = computed(() => numberFormatError(numberFormat.value));
const dateError = computed(() => timeFormatError(timeFormat.value));
const numberSample = computed(() => formatSampleNumber(numberFormat.value, SAMPLE_NUMBER));
const dateSample = computed(() => formatSampleDate(timeFormat.value, SAMPLE_DATE));

const result = computed(() =>
  normalizeChartFormat({
    number_format: numberFormat.value,
    time_format: timeFormat.value,
    x_title: draft.xTitle,
    y_title: draft.yTitle,
  }),
);
const hasAny = computed(() => result.value != null);

// A short pause, so typing a custom format does not redraw the chart on every key.
let previewTimer: ReturnType<typeof setTimeout> | null = null;
watch(result, (next) => {
  if (previewTimer) clearTimeout(previewTimer);
  previewTimer = setTimeout(() => emit("preview", next), 250);
});
onUnmounted(() => {
  if (previewTimer) clearTimeout(previewTimer);
});

const resetAll = () => {
  draft.numberPreset = "auto";
  draft.datePreset = "auto";
  draft.numberCustom = "";
  draft.dateCustom = "";
  draft.xTitle = "";
  draft.yTitle = "";
};

const onSave = () => {
  if (numberError.value || dateError.value) return;
  if (previewTimer) clearTimeout(previewTimer);
  emit("save", result.value);
  emit("update:modelValue", false);
};
</script>

<style scoped>
.fmt-field {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
.fmt-sample,
.fmt-hint {
  font-size: 11px;
  line-height: 1.4;
  color: var(--el-text-color-secondary);
}
.fmt-sample {
  font-family: var(--el-font-family-monospace, monospace);
}
.fmt-error {
  font-size: 11px;
  line-height: 1.4;
  color: var(--el-color-danger);
}
.fmt-reset {
  float: left;
}
</style>
