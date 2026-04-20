<template>
  <div>
    <p v-if="label" class="label">{{ label }}</p>
    <el-select
      v-model="selected"
      multiple
      filterable
      :placeholder="placeholder"
      size="small"
      style="width: 100%"
    >
      <el-option
        v-for="col in filteredSchema"
        :key="col.name"
        :label="col.name"
        :value="col.name"
      >
        <span class="col-name">{{ col.name }}</span>
        <span class="col-type">{{ col.data_type }}</span>
      </el-option>
    </el-select>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref, watch } from "vue";
import type { FileColumn } from "@/types/node.types";

type DataTypeFilter = "Numeric" | "String" | "Date" | "All";

const props = defineProps<{
  modelValue: string[];
  schema: FileColumn[] | undefined | null;
  dataTypeFilter?: DataTypeFilter;
  label?: string;
  placeholder?: string;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string[]): void;
}>();

const selected = ref<string[]>([...(props.modelValue ?? [])]);

watch(
  () => props.modelValue,
  (v) => {
    selected.value = [...(v ?? [])];
  },
);

watch(selected, (v) => {
  emit("update:modelValue", v);
});

const filteredSchema = computed<FileColumn[]>(() => {
  const cols = props.schema ?? [];
  const f = props.dataTypeFilter ?? "All";
  if (f === "All") return cols;
  return cols.filter((c) => c.data_type_group === f);
});
</script>

<style scoped>
.label {
  font-weight: bold;
  margin-bottom: 6px;
  color: var(--color-text-primary);
}
.col-name {
  font-family: inherit;
}
.col-type {
  margin-left: 8px;
  color: var(--color-text-secondary, #9ca3af);
  font-size: 11px;
}
</style>
