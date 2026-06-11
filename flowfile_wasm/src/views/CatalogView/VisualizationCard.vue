<script setup lang="ts">
import type { SavedVisual } from '../../types/visuals'

defineProps<{ viz: SavedVisual }>()
const emit = defineEmits<{ open: []; delete: [] }>()

function fmt(ts: number): string {
  return new Date(ts).toLocaleDateString()
}
</script>

<template>
  <div class="viz-card" @click="emit('open')">
    <div class="viz-card-thumb">
      <img v-if="viz.thumbnail_data_url" :src="viz.thumbnail_data_url" alt="" />
      <i v-else class="fa-solid fa-chart-column"></i>
    </div>
    <div class="viz-card-body">
      <div class="viz-card-name" :title="viz.name">{{ viz.name }}</div>
      <div class="viz-card-meta">{{ viz.dataset_name }} · {{ fmt(viz.updatedAt) }}</div>
    </div>
    <button class="viz-card-del" title="Delete visualization" @click.stop="emit('delete')">
      <i class="fa-solid fa-trash"></i>
    </button>
  </div>
</template>

<style scoped>
.viz-card {
  position: relative;
  display: flex;
  flex-direction: column;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md, 8px);
  background: var(--color-background-primary);
  overflow: hidden;
  cursor: pointer;
  transition: all var(--transition-fast, 0.15s);
}
.viz-card:hover {
  border-color: var(--color-accent);
  box-shadow: var(--shadow-sm, 0 1px 4px rgba(0, 0, 0, 0.1));
}
.viz-card-thumb {
  height: 120px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-light);
  overflow: hidden;
}
.viz-card-thumb img {
  width: 100%;
  height: 100%;
  object-fit: contain;
}
.viz-card-thumb i {
  font-size: 34px;
  color: var(--color-text-muted);
}
.viz-card-body {
  padding: var(--spacing-2, 8px) var(--spacing-3, 12px);
  min-width: 0;
}
.viz-card-name {
  font-size: var(--font-size-sm, 13px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viz-card-meta {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viz-card-del {
  position: absolute;
  top: 6px;
  right: 6px;
  width: 26px;
  height: 26px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: none;
  border-radius: var(--border-radius-sm, 4px);
  background: var(--color-background-primary);
  color: var(--color-text-muted);
  cursor: pointer;
  opacity: 0;
  transition: all var(--transition-fast, 0.15s);
}
.viz-card:hover .viz-card-del {
  opacity: 1;
}
.viz-card-del:hover {
  color: var(--color-danger);
}
.viz-card-del i {
  font-size: 13px;
}
</style>
