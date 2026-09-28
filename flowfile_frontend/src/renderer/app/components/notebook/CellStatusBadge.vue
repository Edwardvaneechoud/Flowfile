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
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  line-height: 16px;
  white-space: nowrap;
}
.nb-status-badge.is-queued {
  background: var(--color-background-tertiary);
  color: var(--color-text-secondary);
}
.nb-status-badge.is-stale {
  border: 1px solid color-mix(in srgb, var(--color-warning) 35%, transparent);
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
}
</style>
