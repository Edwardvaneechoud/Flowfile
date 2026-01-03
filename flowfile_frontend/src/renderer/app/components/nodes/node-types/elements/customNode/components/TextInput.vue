<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-input
      :model-value="modelValue"
      :placeholder="schema.placeholder || 'Enter value...'"
      clearable
      size="large"
      @update:model-value="$emit('update:modelValue', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import type { PropType } from "vue";
import type { TextInputComponent } from "../interface";

defineProps({
  schema: {
    type: Object as PropType<TextInputComponent>,
    required: true,
  },
  modelValue: {
    type: String,
    default: "",
  },
});

defineEmits(["update:modelValue"]);
</script>
