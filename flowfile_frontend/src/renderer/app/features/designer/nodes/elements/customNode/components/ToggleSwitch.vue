<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-switch
      :model-value="modelValue"
      size="large"
      inline-prompt
      @update:modelValue="$emit('update:modelValue', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import type { PropType } from "vue";
import type { ToggleSwitchComponent } from "../interface";

defineProps({
  schema: {
    type: Object as PropType<ToggleSwitchComponent>,
    required: true,
  },
  modelValue: Boolean,
});

defineEmits(["update:modelValue"]);
</script>
