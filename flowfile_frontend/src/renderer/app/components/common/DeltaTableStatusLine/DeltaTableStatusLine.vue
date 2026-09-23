<template>
  <div class="table-status">
    <slot name="before" />
    <template v-if="exists">
      <div class="status-line status-exists">
        <i class="fa-solid fa-circle-info"></i>
        <span v-if="rowCount != null"
          >Table exists — {{ rowCount.toLocaleString() }} rows. Writing here {{ verb }}.</span
        >
        <span v-else>Existing table — v{{ version }}</span>
      </div>
      <slot />
      <div v-if="partitionColumns.length" class="status-line status-partitions">
        <span class="status-partitions-label">Partitioned by</span>
        <span v-for="col in partitionColumns" :key="col" class="status-chip">{{ col }}</span>
      </div>
    </template>
    <div v-else class="status-line status-new">
      <i class="fa-solid fa-circle-plus"></i>
      <span>New table — will be created.</span>
    </div>
  </div>
</template>

<script lang="ts" setup>
/**
 * Whether a Delta write target already exists, and how it is partitioned.
 *
 * Shared by the catalog writer, which knows the row count and what the chosen mode does to the
 * table (``rowCount`` + ``verb``), and the cloud storage writer, which knows the table version.
 * The ``before`` slot renders above the summary, the default slot under it (existing tables
 * only); slotted lines take the ``status-line`` / ``status-exists`` styles. The host sets the
 * background, since the block contrasts with whatever surface it sits on.
 */
defineProps<{
  exists: boolean;
  partitionColumns: string[];
  rowCount?: number | null;
  verb?: string;
  version?: number | null;
}>();
</script>

<style scoped>
.table-status {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 8px 10px;
  border-radius: 4px;
  font-size: 11px;
}

.status-line,
:slotted(.status-line) {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}

.status-exists,
:slotted(.status-exists) {
  color: var(--color-text-secondary);
}

.status-exists i,
:slotted(.status-exists i) {
  color: var(--color-primary);
}

.status-new {
  color: var(--color-text-tertiary);
}

.status-new i {
  color: var(--color-success, #22c55e);
}

.status-partitions-label {
  color: var(--color-text-tertiary);
}

.status-chip {
  display: inline-flex;
  align-items: center;
  padding: 1px 6px;
  background: rgba(59, 130, 246, 0.12);
  color: var(--color-primary);
  border-radius: 4px;
  font-weight: 500;
}
</style>
