<template>
  <el-tooltip v-if="storageUri" :content="tooltip" placement="top" :show-after="300">
    <span class="storage-badge">
      <i class="fa-solid fa-cloud"></i>
      {{ schemeLabel }}
    </span>
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  storageUri?: string | null;
  connectionName?: string | null;
}>();

const schemeLabel = computed(() => {
  const u = props.storageUri ?? "";
  if (u.startsWith("s3")) return "S3";
  if (u.startsWith("gs") || u.startsWith("gcs")) return "GCS";
  if (u.startsWith("az") || u.startsWith("abfs") || u.startsWith("adl")) return "Azure";
  return "Cloud";
});

const tooltip = computed(
  () =>
    `Object storage: ${props.storageUri}` +
    (props.connectionName ? ` (connection: ${props.connectionName})` : ""),
);
</script>

<style scoped>
.storage-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-left: 8px;
  padding: 1px 8px;
  font-size: 11px;
  font-weight: 600;
  color: var(--el-color-primary, #409eff);
  background: var(--el-color-primary-light-9, rgba(64, 158, 255, 0.1));
  border-radius: 999px;
  white-space: nowrap;
}
.storage-badge i {
  font-size: 10px;
}
</style>
