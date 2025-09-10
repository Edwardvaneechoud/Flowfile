<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-input
      :model-value="modelValue"
      @update:modelValue="$emit('update:modelValue', $event)"
      :placeholder="schema.placeholder || 'Enter value...'"
      clearable
      size="large"
    />
  </div>
</template>

<script setup lang="ts">
import type { PropType } from 'vue';
import type { TextInputComponent } from '../interface';

defineProps({
  schema: {
    type: Object as PropType<TextInputComponent>,
    required: true,
  },
  modelValue: String,
});

defineEmits(['update:modelValue']);
</script>
