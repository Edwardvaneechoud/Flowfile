<template>
  <span
    v-if="queued || staleReason"
    class="nb-status-badge"
    :class="queued ? 'is-queued' : 'is-stale'"
    :data-reason="queued ? 'queued' : staleReason"
    :title="queued ? undefined : staleTitle(staleReason!)"
  >
    {{ queued ? "Queued" : staleLabel(staleReason!) }}
  </span>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { staleLabel, staleTitle } from "./notebookRuntimeState";
import type { CellRuntime, StaleReason } from "./notebookRuntimeState";

const props = withDefaults(
  defineProps<{
    runtime?: CellRuntime | null;
    hasOutput: boolean;
  }>(),
  { runtime: null },
);

const queued = computed(() => props.runtime?.status === "queued");

// A stale mark on a cell with nothing to show yet would be a badge about an invisible result.
const staleReason = computed<StaleReason | null>(() =>
  props.hasOutput ? (props.runtime?.staleReason ?? null) : null,
);
</script>

<style scoped>
.nb-status-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 11px;
  line-height: 16px;
  white-space: nowrap;
}
.nb-status-badge.is-queued {
  background: var(--el-fill-color-light, #f5f7fa);
  color: var(--el-text-color-secondary, #909399);
}
.nb-status-badge.is-stale {
  border: 1px solid var(--el-color-warning-light-7, #f3d19e);
  background: var(--el-color-warning-light-9, #fdf6ec);
  color: var(--el-color-warning, #e6a23c);
}
</style>
