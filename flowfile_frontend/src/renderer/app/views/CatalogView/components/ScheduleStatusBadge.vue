<template>
  <span v-if="running" class="status-badge running">
    <i class="fa-solid fa-spinner fa-spin" /> Running
  </span>
  <span v-else-if="enabled" class="status-badge enabled">
    <i class="fa-solid fa-circle-check" /> Enabled
  </span>
  <span v-else class="status-badge paused"> <i class="fa-solid fa-circle-pause" /> Disabled </span>
</template>

<script setup lang="ts">
defineProps<{
  running: boolean;
  enabled: boolean;
}>();
</script>

<style scoped>
.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.status-badge.running {
  background: color-mix(in srgb, var(--color-info) 12%, transparent);
  color: var(--color-info);
}

.status-badge.enabled {
  background: color-mix(in srgb, var(--color-success) 12%, transparent);
  color: var(--color-success);
}

.status-badge.paused {
  background: rgba(156, 163, 175, 0.15);
  color: var(--color-text-muted);
}
</style>
