<script setup lang="ts">
import { computed, ref } from 'vue'
import { useDashboardsStore } from '../../stores/dashboards-store'
import { useGraphicWalkerAppearance } from '../../composables/useGraphicWalkerAppearance'
import DashboardCanvas from './DashboardCanvas.vue'
import DashboardSidebarPicker from './DashboardSidebarPicker.vue'
import { EMPTY_DASHBOARD_LAYOUT } from '../../types/visuals'
import type { DashboardLayout, DashboardTile, SavedVisual } from '../../types/visuals'

const emit = defineEmits<{ close: [] }>()
const store = useDashboardsStore()
const appearance = useGraphicWalkerAppearance()

const dirty = ref(false)
const nameDraft = ref(store.current?.name ?? 'Untitled dashboard')

const layout = computed<DashboardLayout>(() => store.current?.layout ?? EMPTY_DASHBOARD_LAYOUT)

const addedVizIds = computed(
  () =>
    new Set(
      (store.current?.layout.tiles ?? [])
        .map((t) => t.viz_id)
        .filter((v): v is string => v != null),
    ),
)

function genTileId(): string {
  return (
    globalThis.crypto?.randomUUID?.() ??
    `tile-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  )
}

function findFreeRow(l: DashboardLayout): number {
  if (!l.tiles.length) return 0
  return Math.max(...l.tiles.map((t) => t.y + t.h))
}

function buildVizTile(vizId: string, x: number, y: number): DashboardTile {
  return { id: genTileId(), type: 'viz', viz_id: vizId, chart_index: 0, x, y, w: 6, h: 6 }
}

function buildTextTile(x: number, y: number): DashboardTile {
  return {
    id: genTileId(),
    type: 'text',
    viz_id: null,
    chart_index: 0,
    text_md: "## New section\n\nDescribe what's below. **Markdown** supported.",
    x,
    y,
    w: 12,
    h: 3,
  }
}

function onLayoutChange(next: DashboardLayout) {
  store.setLayout(next)
  dirty.value = true
}

function onAddTile(viz: SavedVisual) {
  if (!store.current) return
  const l = store.current.layout
  onLayoutChange({ ...l, tiles: [...l.tiles, buildVizTile(viz.id, 0, findFreeRow(l))] })
}

function onAddVizAt({ vizId, x, y }: { vizId: string; x: number; y: number }) {
  if (!store.current) return
  const l = store.current.layout
  const row = y < 0 ? findFreeRow(l) : y
  onLayoutChange({ ...l, tiles: [...l.tiles, buildVizTile(vizId, x, row)] })
}

function onAddTextTile() {
  if (!store.current) return
  const l = store.current.layout
  onLayoutChange({ ...l, tiles: [...l.tiles, buildTextTile(0, findFreeRow(l))] })
}

function onAddTextAt({ x, y }: { x: number; y: number }) {
  if (!store.current) return
  const l = store.current.layout
  const row = y < 0 ? findFreeRow(l) : y
  onLayoutChange({ ...l, tiles: [...l.tiles, buildTextTile(x, row)] })
}

function onSave() {
  if (!store.current || !nameDraft.value.trim()) return
  store.setName(nameDraft.value.trim())
  store.save()
  dirty.value = false
}

function onBack() {
  if (dirty.value && !confirm('Discard unsaved changes?')) return
  // The panel decides where to land (read-only view for a saved dashboard, or
  // the library for a brand-new one) and reloads to drop unsaved edits.
  emit('close')
}
</script>

<template>
  <div class="dash-editor">
    <div class="dash-toolbar">
      <div class="dash-toolbar-left">
        <button class="btn-secondary" @click="onBack">
          <i class="fa-solid fa-arrow-left"></i> Back
        </button>
        <input
          v-model="nameDraft"
          class="dash-name"
          placeholder="Dashboard name"
          maxlength="120"
          @input="dirty = true"
        />
        <span v-if="dirty" class="dash-dirty">unsaved</span>
      </div>
      <div class="dash-toolbar-right">
        <button class="btn-primary" :disabled="!nameDraft.trim()" @click="onSave">Save</button>
      </div>
    </div>

    <div class="dash-body">
      <aside class="dash-sidebar">
        <DashboardSidebarPicker
          :added-viz-ids="addedVizIds"
          @add="onAddTile"
          @add-text="onAddTextTile"
        />
      </aside>
      <main class="dash-canvas">
        <DashboardCanvas
          :layout="layout"
          mode="edit"
          :appearance="appearance"
          @update:layout="onLayoutChange"
          @add-viz-at="onAddVizAt"
          @add-text-at="onAddTextAt"
        />
      </main>
    </div>
  </div>
</template>

<style scoped>
.dash-editor {
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
.dash-name {
  max-width: 320px;
  padding: 6px 10px;
  font-size: 14px;
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
}
.dash-name:focus {
  outline: none;
  border-color: var(--color-accent);
}
.dash-dirty {
  font-size: 11px;
  color: var(--color-warning, #d97706);
  text-transform: uppercase;
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
  padding: 7px 16px;
  background: var(--color-accent);
  color: #fff;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
}
.btn-primary:hover:not(:disabled) {
  background: var(--color-accent-hover, #1d4ed8);
}
.btn-primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.dash-body {
  flex: 1;
  display: flex;
  min-height: 0;
}
.dash-sidebar {
  width: 280px;
  flex-shrink: 0;
  border-right: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
  display: flex;
  flex-direction: column;
  min-height: 0;
}
.dash-canvas {
  flex: 1;
  min-width: 0;
  overflow: hidden;
}
</style>
