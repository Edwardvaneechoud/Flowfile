<template>
  <span v-if="isShared(resource)" class="shared-badge" :title="sharedTitle(resource)">
    <i class="fa-solid fa-share-nodes"></i>
    {{ sharedLabel(resource) }}
  </span>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useResourceSharing } from "../../composables/useResourceSharing";
import type { AccessInfo } from "../../types/sharing.types";

const props = defineProps<{ access?: AccessInfo | null }>();

const { isShared, sharedLabel, sharedTitle } = useResourceSharing();
const resource = computed(() => ({ access: props.access }));
</script>

<style scoped>
.shared-badge {
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
.shared-badge i {
  font-size: 10px;
}
</style>
