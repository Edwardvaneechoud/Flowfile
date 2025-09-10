<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-switch
      :model-value="modelValue"
      @update:modelValue="$emit('update:modelValue', $event)"
      size="large"
      inline-prompt
    />
  </div>
</template>

<script setup lang="ts">
import type { PropType } from 'vue';
import type { ToggleSwitchComponent } from '../interface';

defineProps({
  schema: {
    type: Object as PropType<ToggleSwitchComponent>,
    required: true,
  },
  modelValue: Boolean,
});

defineEmits(['update:modelValue']);
</script>
