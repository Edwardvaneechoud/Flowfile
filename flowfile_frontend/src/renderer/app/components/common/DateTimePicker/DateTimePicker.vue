<template>
  <div class="date-time-picker">
    <el-date-picker
      :model-value="localValue"
      type="datetime"
      :placeholder="placeholder"
      :clearable="clearable"
      :size="size"
      :disabled="disabled"
      :disabled-date="disabledDate"
      :teleported="teleported"
      :format="displayFormat"
      @update:model-value="onUpdate"
    />
    <p v-if="showUtcHint && utcHint" class="date-time-picker-hint">= {{ utcHint }}</p>
  </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";
import { isoToLocalDate, localDateToIso, formatUtcHint } from "./isoDateTime";

const props = withDefaults(
  defineProps<{
    /** ISO-8601 UTC instant ("2026-07-31T12:00:00Z"), or null when unset. Never "". */
    modelValue: string | null;
    placeholder?: string;
    clearable?: boolean;
    /** Include seconds in the editor and in the emitted value. */
    showSeconds?: boolean;
    size?: "large" | "default" | "small";
    disabled?: boolean;
    disabledDate?: (d: Date) => boolean;
    teleported?: boolean;
    /** Render the "= <UTC> UTC" caption under the field. */
    showUtcHint?: boolean;
  }>(),
  {
    placeholder: "Select date and time",
    clearable: true,
    showSeconds: false,
    size: "small",
    disabled: false,
    disabledDate: undefined,
    teleported: true,
    showUtcHint: true,
  },
);

const emit = defineEmits<{ (e: "update:modelValue", value: string | null): void }>();

const localValue = computed(() => isoToLocalDate(props.modelValue));

const displayFormat = computed(() =>
  props.showSeconds ? "YYYY-MM-DD HH:mm:ss" : "YYYY-MM-DD HH:mm",
);

const utcHint = computed(() => formatUtcHint(props.modelValue));

function onUpdate(value: Date | null) {
  emit("update:modelValue", localDateToIso(value));
}
</script>

<style scoped>
.date-time-picker {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.date-time-picker-hint {
  margin: 0;
  font-size: 11px;
  color: var(--color-text-tertiary);
}
</style>
