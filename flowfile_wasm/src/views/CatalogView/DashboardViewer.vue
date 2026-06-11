<script setup lang="ts">
import { computed } from 'vue'
import { useDashboardsStore } from '../../stores/dashboards-store'
import { useGraphicWalkerAppearance } from '../../composables/useGraphicWalkerAppearance'
import DashboardCanvas from './DashboardCanvas.vue'
import { EMPTY_DASHBOARD_LAYOUT } from '../../types/visuals'
import type { DashboardLayout } from '../../types/visuals'

// Read-only dashboard view. The canvas runs in mode="view": tiles render but
// can't be dragged, resized, removed, or text-edited. Click "Edit" to unlock.
const emit = defineEmits<{ back: []; edit: [] }>()
const store = useDashboardsStore()
const appearance = useGraphicWalkerAppearance()

const layout = computed<DashboardLayout>(() => store.current?.layout ?? EMPTY_DASHBOARD_LAYOUT)
const name = computed(() => store.current?.name ?? 'Dashboard')
</script>

<template>
  <div class="dash-viewer">
    <div class="dash-toolbar">
      <div class="dash-toolbar-left">
        <button class="btn-secondary" @click="emit('back')">
          <i class="fa-solid fa-arrow-left"></i> Back
        </button>
        <span class="dash-view-name">{{ name }}</span>
      </div>
      <div class="dash-toolbar-right">
        <button class="btn-primary" @click="emit('edit')">
          <i class="fa-solid fa-pen-to-square"></i> Edit
        </button>
      </div>
    </div>
    <div class="dash-view-body">
      <DashboardCanvas :layout="layout" mode="view" :appearance="appearance" />
    </div>
  </div>
</template>

<style scoped>
.dash-viewer {
  position: fixed;
  inset: 0;
  z-index: 9990;
  display: flex;
  flex-direction: column;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
}
.dash-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 16px;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
}
.dash-toolbar-left {
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 1;
  min-width: 0;
}
.dash-view-name {
  font-size: 15px;
  font-weight: 600;
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.btn-secondary {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 7px 12px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  font-size: 14px;
  cursor: pointer;
}
.btn-secondary:hover {
  background: var(--color-background-hover);
}
.btn-secondary i {
  font-size: 13px;
}
.btn-primary {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 7px 16px;
  background: var(--color-accent);
  color: #fff;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
}
.btn-primary:hover {
  background: var(--color-accent-hover, #1d4ed8);
}
.btn-primary i {
  font-size: 13px;
}
.dash-view-body {
  flex: 1;
  min-width: 0;
  min-height: 0;
  overflow: hidden;
}
</style>
