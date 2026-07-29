<template>
  <el-alert
    v-if="state.status === 'conflict'"
    type="warning"
    :closable="false"
    show-icon
    class="autosave-banner"
  >
    <template #title>This {{ resourceLabel }} was changed somewhere else</template>
    <div class="autosave-banner-body">
      <span>
        Another window or another person edited it. Autosave is paused — you're looking at your
        version.
      </span>
      <span class="autosave-banner-actions">
        <el-button size="small" :loading="busy" @click="emit('reload')">Load latest</el-button>
        <el-button size="small" type="primary" :loading="busy" @click="emit('overwrite')">
          Keep mine
        </el-button>
      </span>
    </div>
  </el-alert>
  <el-alert
    v-else-if="state.status === 'blocked'"
    type="warning"
    :closable="false"
    show-icon
    class="autosave-banner"
    :title="blockedTitle"
    :description="blockedDescription"
  />
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { AutosaveState } from "../../composables/autosaveEngine";
import { CATALOG_PERMISSION_MESSAGE } from "../../composables/saveError";

const props = defineProps<{
  state: AutosaveState;
  resourceLabel: string;
  busy?: boolean;
}>();

const emit = defineEmits<{
  (e: "reload"): void;
  (e: "overwrite"): void;
}>();

const blockedTitle = computed(() => {
  if (props.state.blockKind === "gone") return `This ${props.resourceLabel} no longer exists`;
  if (props.state.blockKind === "auth") return "Signed out — changes aren't being saved";
  return "Read-only — changes aren't being saved";
});

const blockedDescription = computed(() => {
  if (props.state.blockKind === "gone") {
    return "It was deleted elsewhere, so your changes can't be saved.";
  }
  if (props.state.blockKind === "auth") {
    return "Your session expired. Sign in again to keep your work.";
  }
  return CATALOG_PERMISSION_MESSAGE;
});
</script>

<style scoped>
.autosave-banner {
  flex-shrink: 0;
}
.autosave-banner-body {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}
.autosave-banner-actions {
  display: inline-flex;
  gap: 8px;
  flex-shrink: 0;
}
</style>
