<template>
  <el-tooltip :content="title" placement="top" :show-after="300">
    <span class="hub-badge" data-testid="hub-badge">
      <i class="fa-solid fa-building"></i>
      Hub
    </span>
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { HUB_NAMESPACE_TITLE, HUB_TABLE_TITLE } from "./hubRows";

const props = defineProps<{ kind: "namespace" | "table" }>();

const title = computed(() => (props.kind === "table" ? HUB_TABLE_TITLE : HUB_NAMESPACE_TITLE));
</script>

<style scoped>
.hub-badge {
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
.hub-badge i {
  font-size: 10px;
}
</style>
