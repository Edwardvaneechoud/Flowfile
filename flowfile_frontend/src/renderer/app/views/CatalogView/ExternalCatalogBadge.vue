<template>
  <el-tooltip :content="tooltip" placement="top" :show-after="300">
    <span class="external-badge" data-testid="external-badge">
      <i class="fa-solid fa-link"></i>
      Iceberg REST
    </span>
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { ExternalCatalogInfo } from "../../types";

const props = defineProps<{ external: ExternalCatalogInfo }>();

const tooltip = computed(
  () =>
    `Kept by the Iceberg REST catalog at ${props.external.uri}` +
    (props.external.warehouse ? ` (warehouse ${props.external.warehouse})` : ""),
);
</script>

<style scoped>
.external-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-left: 8px;
  padding: 1px 8px;
  font-size: 11px;
  font-weight: 600;
  color: var(--el-color-success, #67c23a);
  background: var(--el-color-success-light-9, rgba(103, 194, 58, 0.1));
  border-radius: 999px;
  white-space: nowrap;
}
.external-badge i {
  font-size: 10px;
}
</style>
