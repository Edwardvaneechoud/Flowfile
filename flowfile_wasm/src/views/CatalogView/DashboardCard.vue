<script setup lang="ts">
import type { Dashboard } from '../../types/visuals'

defineProps<{ dashboard: Dashboard }>()
const emit = defineEmits<{ open: []; edit: []; delete: [] }>()

function fmt(ts: number): string {
  return new Date(ts).toLocaleDateString()
}
</script>

<template>
  <div class="dash-card" @click="emit('open')">
    <div class="dash-card-icon"><i class="fa-solid fa-table-cells-large"></i></div>
    <div class="dash-card-body">
      <div class="dash-card-name" :title="dashboard.name">{{ dashboard.name }}</div>
      <div class="dash-card-meta">
        {{ dashboard.layout.tiles.length }} tile{{ dashboard.layout.tiles.length === 1 ? '' : 's' }}
        · {{ fmt(dashboard.updatedAt) }}
      </div>
    </div>
    <button class="dash-card-act" title="Edit dashboard" @click.stop="emit('edit')">
      <i class="fa-solid fa-pen-to-square"></i>
    </button>
    <button class="dash-card-del" title="Delete dashboard" @click.stop="emit('delete')">
      <i class="fa-solid fa-trash"></i>
    </button>
  </div>
</template>

<style scoped>
.dash-card {
  position: relative;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px 16px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md, 8px);
  background: var(--color-background-primary);
  cursor: pointer;
  transition: all var(--transition-fast, 0.15s);
}
.dash-card:hover {
  border-color: var(--color-accent);
  box-shadow: var(--shadow-sm, 0 1px 4px rgba(0, 0, 0, 0.1));
}
.dash-card-icon {
  width: 40px;
  height: 40px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 8px;
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}
.dash-card-icon i {
  font-size: 18px;
}
.dash-card-body {
  flex: 1;
  min-width: 0;
}
.dash-card-name {
  font-size: 14px;
  font-weight: 600;
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.dash-card-meta {
  font-size: 12px;
  color: var(--color-text-muted);
}
.dash-card-act,
.dash-card-del {
  width: 28px;
  height: 28px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  opacity: 0;
  transition: all var(--transition-fast, 0.15s);
}
.dash-card:hover .dash-card-act,
.dash-card:hover .dash-card-del {
  opacity: 1;
}
.dash-card-act:hover {
  color: var(--color-accent);
}
.dash-card-del:hover {
  color: var(--color-danger);
}
.dash-card-act i,
.dash-card-del i {
  font-size: 14px;
}
</style>
