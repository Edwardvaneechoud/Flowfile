<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <div class="slider-wrapper">
      <el-slider
        :model-value="modelValue"
        :min="schema.min_value"
        :max="schema.max_value"
        :step="schema.step || 1"
        :show-input="true"
        size="large"
        style="width: 100%"
        @update:modelValue="$emit('update:modelValue', $event)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import type { PropType } from "vue";
import type { SliderInputComponent } from "../interface";

defineProps({
  schema: {
    type: Object as PropType<SliderInputComponent>,
    required: true,
  },
  modelValue: Number,
});

defineEmits(["update:modelValue"]);
</script>

<style scoped>
.slider-wrapper {
  padding: 0 10px;
}
</style>
