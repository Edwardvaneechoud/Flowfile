<template>
  <div class="component-container">
    <label class="listbox-subtitle">
      {{ schema.label }}
      <span v-if="schema.required" class="required-indicator">*</span>
    </label>
    <el-select
      :model-value="modelValue"
      :multiple="schema.multiple"
      filterable
      :placeholder="schema.multiple ? 'Select columns...' : 'Select a column...'"
      style="width: 100%"
      size="large"
      @update:modelValue="$emit('update:modelValue', $event)"
    >
      <el-option
        v-for="column in filteredColumns"
        :key="column.name"
        :label="column.name"
        :value="column.name"
      >
        <span>{{ column.name }}</span>
        <span class="column-type">{{ column.data_type }}</span>
      </el-option>
    </el-select>
  </div>
</template>

<script setup lang="ts">
import { computed, PropType } from "vue";
import type { ColumnSelectorComponent } from "../interface";
import type { FileColumn } from "../../../../baseNode/nodeInterfaces";

const props = defineProps({
  schema: {
    type: Object as PropType<ColumnSelectorComponent>,
    required: true,
  },
  modelValue: {
    type: [String, Array] as PropType<string | string[]>,
    default: () => [],
  },
  incomingColumns: {
    type: Array as PropType<FileColumn[]>,
    default: () => [],
  },
});

defineEmits(["update:modelValue"]);

const filteredColumns = computed(() => {
  // If data_types is "ALL" or not specified, return all columns
  console.log("props.schem.data_types", props.schema);
  console.log("incoming columns", props.incomingColumns);
  if (!props.schema.data_types || props.schema.data_types === "ALL") {
    return props.incomingColumns;
  }

  // Filter columns based on data types
  if (Array.isArray(props.schema.data_types)) {
    return props.incomingColumns.filter((column) => {
      return props.schema.data_types.includes(column.data_type);
    });
  }

  return props.incomingColumns;
});
</script>

<style scoped>
.required-indicator {
  color: #f56c6c;
  margin-left: 4px;
}

.column-type {
  font-size: 0.75rem;
  color: #909399;
  margin-left: 8px;
}
</style>
