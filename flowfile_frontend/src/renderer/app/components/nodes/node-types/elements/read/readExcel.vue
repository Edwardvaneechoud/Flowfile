<template>
  <div v-if="isLoaded && localExcelTable" class="excel-settings">
    <div class="field">
      <div class="field-label">
        Sheet name
        <span class="field-optional">optional</span>
      </div>
      <div class="sheet-row">
        <drop-down
          v-model="localExcelTable.sheet_name"
          label="Sheet name"
          placeholder="First sheet (default)"
          :column-options="sheetNames"
          :is-loading="!sheetNamesLoaded"
        />
        <span v-if="showWarning" class="warning-sign" title="This sheet is not in the workbook">
          ⚠️
        </span>
      </div>
    </div>

    <div class="checkbox-row">
      <el-checkbox v-model="localExcelTable.has_headers" label="Has headers" />
      <el-checkbox v-model="localExcelTable.type_inference" label="Type inference" />
    </div>

    <div class="optional">
      <button
        type="button"
        class="disclosure"
        :aria-expanded="showOptionalSettings"
        :aria-controls="rangeGroupId"
        @click="toggleOptionalSettings"
      >
        <svg class="disclosure-chevron" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
          <path
            fill-rule="evenodd"
            d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z"
            clip-rule="evenodd"
          />
        </svg>
        Read range
        <span class="field-optional">optional</span>
      </button>

      <div v-if="showOptionalSettings" :id="rangeGroupId" class="range-group">
        <div class="range-row">
          <span class="range-label">Rows</span>
          <el-input-number
            v-model="startRow"
            class="range-field"
            size="small"
            :min="0"
            :precision="0"
            :controls="false"
            placeholder="First"
            aria-label="First row"
          />
          <span class="range-sep">to</span>
          <el-input-number
            v-model="endRow"
            class="range-field"
            size="small"
            :min="0"
            :precision="0"
            :controls="false"
            placeholder="Last"
            aria-label="Last row"
          />
        </div>

        <div class="range-row">
          <span class="range-label">Columns</span>
          <el-input-number
            v-model="startColumn"
            class="range-field"
            size="small"
            :min="0"
            :precision="0"
            :controls="false"
            placeholder="First"
            aria-label="First column"
          />
          <span class="range-sep">to</span>
          <el-input-number
            v-model="endColumn"
            class="range-field"
            size="small"
            :min="0"
            :precision="0"
            :controls="false"
            placeholder="Last"
            aria-label="Last column"
          />
        </div>

        <p class="range-hint">Leave a field blank to read to the edge of the sheet.</p>
      </div>
    </div>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, watch, onMounted } from "vue";
import { InputExcelTable } from "../../../baseNode/nodeInput";
import dropDown from "../../../baseNode/page_objects/dropDown.vue";
import { excelOptionalSettingsOpen, getXlsxSheetNamesForPath } from "./utils";
import { CodeLoader } from "vue-content-loader";

const props = defineProps<{
  modelValue: InputExcelTable;
  path: string;
}>();

const isLoaded = ref(false);
const emit = defineEmits(["update:modelValue"]);
const localExcelTable = ref({ ...props.modelValue });

const showOptionalSettings = excelOptionalSettingsOpen;
const rangeGroupId = `excel-range-${Math.random().toString(36).slice(2, 9)}`;

type RangeField = "start_row" | "end_row" | "start_column" | "end_column";

/**
 * The schema encodes "unbounded" as 0, which is indistinguishable from a real
 * offset of 0 in the UI. Blank fields carry that meaning instead, so nobody has
 * to type a sentinel; the 0 goes back on the wire untouched. el-input-number
 * models an empty field as null, so that is the blank on this side.
 */
const rangeBound = (field: RangeField) =>
  computed<number | null>({
    get: () => localExcelTable.value[field] || null,
    set: (value) => {
      const isBounded = typeof value === "number" && Number.isFinite(value) && value > 0;
      localExcelTable.value[field] = isBounded ? Math.trunc(value as number) : 0;
    },
  });

const startRow = rangeBound("start_row");
const endRow = rangeBound("end_row");
const startColumn = rangeBound("start_column");
const endColumn = rangeBound("end_column");

const sheetNames = ref<string[]>([]);
const sheetNamesLoaded = ref(false);

const getSheetNames = async () => {
  sheetNames.value = await getXlsxSheetNamesForPath(props.path);
  sheetNamesLoaded.value = true;
};

const toggleOptionalSettings = () => {
  showOptionalSettings.value = !showOptionalSettings.value;
};

const showWarning = computed(() => {
  if (!sheetNamesLoaded.value || !localExcelTable.value.sheet_name) {
    return false;
  }
  return !sheetNames.value.includes(localExcelTable.value.sheet_name);
});

onMounted(() => {
  if (props.path) {
    getSheetNames();
  }
  isLoaded.value = true;
});

watch(
  () => localExcelTable.value,
  (newValue) => {
    emit("update:modelValue", { ...newValue });
  },
  { deep: true },
);
</script>

<style scoped>
.excel-settings {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  /* Aligns the content edge with the "File Specs" bar above it. */
  padding: var(--spacing-2-5) var(--spacing-2-5) var(--spacing-2);
}

.field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1-5);
}

.field-label {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-1-5);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.field-optional {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-normal);
  color: var(--color-text-muted);
}

.sheet-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.sheet-row > :first-child {
  flex: 1 1 auto;
  min-width: 0;
}

.warning-sign {
  flex: none;
  font-size: var(--font-size-base);
  line-height: 1;
}

.checkbox-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2) var(--spacing-5);
}

.optional {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2-5);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
}

.disclosure {
  display: flex;
  align-items: center;
  align-self: flex-start;
  gap: var(--spacing-1-5);
  margin-left: calc(var(--spacing-1) * -1);
  padding: var(--spacing-1) var(--spacing-2);
  border: none;
  border-radius: var(--border-radius-sm);
  background: none;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  line-height: 1.4;
  transition:
    color var(--transition-fast) var(--transition-timing),
    background-color var(--transition-fast) var(--transition-timing);
}

.disclosure:hover {
  color: var(--color-text-primary);
  background-color: var(--color-background-soft);
}

.disclosure:focus-visible {
  outline: 2px solid var(--color-border-focus);
  outline-offset: 1px;
}

.disclosure-chevron {
  flex: none;
  width: 14px;
  height: 14px;
  color: var(--color-text-tertiary);
  transition: transform var(--transition-fast) var(--transition-timing);
}

.disclosure[aria-expanded="true"] .disclosure-chevron {
  transform: rotate(90deg);
}

.range-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.range-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.range-label {
  flex: 0 0 64px;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.range-field {
  width: 84px;
}

.range-sep {
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.range-hint {
  margin: 0;
  font-size: var(--font-size-xs);
  line-height: var(--line-height-normal);
  color: var(--color-text-tertiary);
}
</style>
