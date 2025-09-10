<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <el-select
      :model-value="modelValue"
      @update:modelValue="$emit('update:modelValue', $event)"
      multiple
      filterable
      placeholder="Select one or more options"
      style="width: 100%;"
      size="large"
    >
      <el-option
        v-for="item in options"
        :key="item"
        :label="item"
        :value="item"
      />
    </el-select>
  </div>
</template>

<script setup lang="ts">
import { computed, PropType } from 'vue';
import type { MultiSelectComponent } from '../interface';

const props = defineProps({
  schema: {
    type: Object as PropType<MultiSelectComponent>,
    required: true,
  },
  modelValue: {
    type: Array as PropType<string[]>,
    default: () => [],
  },
  incomingColumns: {
    type: Array as PropType<string[]>,
    default: () => [],
  },
});

defineEmits(['update:modelValue']);

const options = computed(() => {
  // Check if the schema.options is an object (and not an array) first.
  // This acts as a type guard for TypeScript.
  if (props.schema.options && !Array.isArray(props.schema.options) && props.schema.options.__type__ === 'IncomingColumns') {
    return props.incomingColumns;
  }
  
  // If it's not the special object, it must be a string array (or undefined).
  if (Array.isArray(props.schema.options)) {
    return props.schema.options;
  }

  // Fallback to an empty array if options are not defined correctly.
  return [];
});
</script>

