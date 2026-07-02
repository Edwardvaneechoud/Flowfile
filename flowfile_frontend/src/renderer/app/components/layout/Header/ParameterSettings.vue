<template>
  <div>
    <div v-if="localParams.length > 0">
      <div v-for="(param, index) in localParams" :key="index" class="param-row-wrap">
        <div class="param-row">
          <el-input
            v-model="param.name"
            placeholder="Name"
            size="small"
            class="param-name-input"
            @change="commit"
          />
          <el-select
            v-model="param.type"
            size="small"
            class="param-type-select"
            @change="handleTypeChange(param)"
          >
            <el-option v-for="t in FLOW_PARAM_TYPES" :key="t" :label="t" :value="t" />
          </el-select>
          <div class="param-value-input">
            <el-checkbox
              v-if="param.type === 'boolean'"
              :model-value="param.default_value === 'true'"
              @update:model-value="
                (v: string | number | boolean) => setDefault(param, v ? 'true' : 'false')
              "
            />
            <el-input-number
              v-else-if="param.type === 'integer'"
              :model-value="defaultAsNumber(param)"
              :step="1"
              :precision="0"
              size="small"
              controls-position="right"
              placeholder="Default"
              @update:model-value="
                (v: number | undefined) => setDefault(param, v == null ? '' : String(v))
              "
            />
            <el-input-number
              v-else-if="param.type === 'float'"
              :model-value="defaultAsNumber(param)"
              size="small"
              controls-position="right"
              placeholder="Default"
              @update:model-value="
                (v: number | undefined) => setDefault(param, v == null ? '' : String(v))
              "
            />
            <el-select
              v-else-if="param.type === 'enum'"
              :model-value="param.default_value || undefined"
              size="small"
              placeholder="Default"
              @update:model-value="(v: string) => setDefault(param, v)"
            >
              <el-option
                v-for="opt in param.enum_values ?? []"
                :key="opt"
                :label="opt"
                :value="opt"
              />
            </el-select>
            <el-input
              v-else
              v-model="param.default_value"
              placeholder="Default value"
              size="small"
              @change="commit"
            />
          </div>
          <el-input
            v-if="param.type === 'enum'"
            :model-value="enumValuesToText(param.enum_values)"
            placeholder="Values (comma-separated)"
            size="small"
            class="param-enum-input"
            @change="(v: string) => setEnumValues(param, v)"
          />
          <el-input
            v-model="param.description"
            placeholder="Description (optional)"
            size="small"
            class="param-desc-input"
            @change="commit"
          />
          <el-button
            type="danger"
            size="small"
            :icon="'Delete'"
            circle
            @click="removeParameter(index)"
          />
        </div>
        <div v-if="issueFor(index)" class="param-row-issue">{{ issueFor(index) }}</div>
      </div>
    </div>
    <div v-else class="param-empty">No parameters defined.</div>
    <el-button size="small" style="margin-top: var(--spacing-3)" @click="addParameter">
      + Add Parameter
    </el-button>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { FlowParameter } from "../../../types/flow.types";
import {
  FLOW_PARAM_TYPES,
  normalizeParameter,
  coerceDefaultForType,
  enumValuesFromText,
  enumValuesToText,
  validateParameters,
} from "./parameterSettings";

const props = defineProps<{
  modelValue?: FlowParameter[] | null;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: FlowParameter[]): void;
  (e: "change"): void;
}>();

// Local editable copy: rows are committed back to the parent on change/blur,
// which avoids mutating the prop array directly.
const localParams = ref<FlowParameter[]>([]);

watch(
  () => props.modelValue,
  (params) => {
    localParams.value = (params ?? []).map(normalizeParameter);
  },
  { immediate: true },
);

const issues = computed(() => validateParameters(localParams.value));
const issueFor = (index: number): string | undefined =>
  issues.value.find((issue) => issue.index === index)?.message;

const commit = () => {
  // Hold the settings POST while any row would be rejected by the backend
  // (enum without values, duplicate names); the row shows why inline.
  if (issues.value.length > 0) return;
  emit(
    "update:modelValue",
    localParams.value.map((param) => ({ ...param })),
  );
  emit("change");
};

const setDefault = (param: FlowParameter, value: string) => {
  param.default_value = value;
  commit();
};

const defaultAsNumber = (param: FlowParameter): number | undefined => {
  if (param.default_value == null || param.default_value === "") return undefined;
  const num = Number(param.default_value);
  return Number.isFinite(num) ? num : undefined;
};

const handleTypeChange = (param: FlowParameter) => {
  param.default_value = coerceDefaultForType(
    param.default_value,
    param.type ?? "string",
    param.enum_values ?? [],
  );
  commit();
};

const setEnumValues = (param: FlowParameter, text: string) => {
  param.enum_values = enumValuesFromText(text);
  param.default_value = coerceDefaultForType(param.default_value, "enum", param.enum_values);
  commit();
};

const addParameter = () => {
  localParams.value.push({
    name: "",
    default_value: "",
    description: "",
    type: "string",
    enum_values: [],
  });
  commit();
};

const removeParameter = (index: number) => {
  localParams.value.splice(index, 1);
  commit();
};
</script>

<style scoped>
.param-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
}

.param-name-input {
  flex: 1.4;
  min-width: 80px;
}

.param-type-select {
  flex: 1;
  min-width: 84px;
}

.param-value-input {
  flex: 1.6;
  min-width: 100px;
}

.param-value-input > * {
  width: 100%;
}

.param-enum-input {
  flex: 1.6;
  min-width: 110px;
}

.param-desc-input {
  flex: 2;
  min-width: 110px;
}

.param-empty {
  font-size: var(--font-size-sm);
  color: var(--color-text-muted, #999);
  font-style: italic;
}

.param-row-issue {
  font-size: var(--font-size-sm);
  color: var(--color-danger, #f56c6c);
  margin: calc(-1 * var(--spacing-1)) 0 var(--spacing-2);
}
</style>
