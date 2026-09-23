<template>
  <el-select
    :id="id"
    :model-value="modelValue"
    size="small"
    multiple
    filterable
    placeholder="Select key columns"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-option v-for="col in columns" :key="col" :label="col" :value="col" />
  </el-select>
  <p v-if="error" class="field-error">{{ error }}</p>
</template>

<script lang="ts" setup>
/**
 * The key-column picker of a Delta merge write (upsert / update / delete / scd2).
 *
 * Shared by the catalog writer and the cloud storage writer. The label stays with the host,
 * because the two drawers style their labels differently.
 */
defineProps<{
  modelValue?: string[];
  columns: string[];
  id?: string;
  error?: string | null;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string[]): void;
}>();
</script>

<style scoped>
.field-error {
  margin: 0;
  font-size: 11px;
  color: var(--el-color-danger);
}
</style>
