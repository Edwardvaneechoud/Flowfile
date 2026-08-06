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
      size="small"
      :loading="loading"
      loading-text="Loading columns…"
      @update:model-value="$emit('update:modelValue', $event)"
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
  loading: {
    type: Boolean,
    default: false,
  },
});

defineEmits(["update:modelValue"]);

const filteredColumns = computed(() => {
  const dataTypes = props.schema.data_types;
  if (!dataTypes || dataTypes === "ALL") {
    return props.incomingColumns;
  }

  if (Array.isArray(dataTypes)) {
    // A data_types entry is a specific polars type ("Int64") or a group ("Numeric"), so a column
    // matches on its concrete dtype or its data_type_group. Parameterized dtypes arrive stringified
    // with their inner type ("Array(Float32, shape=(384,))", "Datetime(time_unit='us')"), so they
    // also match on the base token — mirroring get_readable_datatype_group() in core.
    const baseType = (dtype: string) => (dtype ?? "").split("(")[0];
    return props.incomingColumns.filter(
      (column) =>
        dataTypes.includes(column.data_type) ||
        dataTypes.includes(baseType(column.data_type)) ||
        dataTypes.includes(column.data_type_group),
    );
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
