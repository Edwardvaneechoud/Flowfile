<template>
  <span
    v-if="syncState"
    class="nb-status-badge"
    :class="`is-${syncState}`"
    data-testid="nb-sync-state"
    :data-sync-state="syncState"
    :title="syncTitle(syncState)"
  >
    {{ syncLabel(syncState) }}
  </span>
  <span
    v-else-if="queued || staleReason"
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
import { staleLabel, staleTitle, syncLabel, syncTitle } from "./notebookRuntimeState";
import type { CellRuntime, StaleReason, SyncState } from "./notebookRuntimeState";

const props = withDefaults(
  defineProps<{
    runtime?: CellRuntime | null;
    hasOutput: boolean;
    /** A canvas notebook cell's sync state; shown instead of the kernel staleness. */
    syncState?: SyncState | null;
  }>(),
  { runtime: null, syncState: null },
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
.nb-status-badge.is-stale,
.nb-status-badge.is-edited {
  border: 1px solid color-mix(in srgb, var(--color-warning) 35%, transparent);
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
}
.nb-status-badge.is-synced {
  padding: 1px 0;
  color: var(--color-text-tertiary);
}
.nb-status-badge.is-error {
  border: 1px solid color-mix(in srgb, var(--color-danger) 30%, transparent);
  background: var(--color-danger-light);
  color: var(--color-danger-dark);
}
</style>
