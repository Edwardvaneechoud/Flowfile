<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-input-number
      :model-value="modelValue"
      :min="schema.min_value"
      :max="schema.max_value"
      :placeholder="schema.placeholder || 'Enter number...'"
      controls-position="right"
      size="large"
      style="width: 100%"
      @update:model-value="$emit('update:modelValue', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import type { PropType } from "vue";
import type { NumericInputComponent } from "../interface";

defineProps({
  schema: {
    type: Object as PropType<NumericInputComponent>,
    required: true,
  },
  modelValue: {
    type: Number,
    default: 0,
  },
});

defineEmits(["update:modelValue"]);
</script>
