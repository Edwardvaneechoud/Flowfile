<script setup lang="ts">
import { computed, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useVisualsStore } from '../../stores/visuals-store'
import VisualizationCard from './VisualizationCard.vue'
import VisualizationSourcePicker from './VisualizationSourcePicker.vue'
import VisualizationEditor from './VisualizationEditor.vue'
import FullscreenOverlay from './FullscreenOverlay.vue'
import type { SavedVisual, VizSourceDescriptor } from '../../types/visuals'

const visualsStore = useVisualsStore()
const { visuals } = storeToRefs(visualsStore)

const search = ref('')
const filtered = computed(() => {
  const q = search.value.trim().toLowerCase()
  if (!q) return visuals.value
  return visuals.value.filter(
    (v) => v.name.toLowerCase().includes(q) || v.dataset_name.toLowerCase().includes(q),
  )
})

const pickerOpen = ref(false)
const editorOpen = ref(false)
const editingViz = ref<SavedVisual | null>(null)
const pendingSource = ref<VizSourceDescriptor | null>(null)

function onNew() {
  pickerOpen.value = true
}

function onPicked(source: VizSourceDescriptor) {
  pendingSource.value = source
  editingViz.value = null
  editorOpen.value = true
}

function onOpen(viz: SavedVisual) {
  editingViz.value = viz
  pendingSource.value = {
    source_type: viz.source_type,
    dataset_name: viz.dataset_name,
    source_kind: viz.source_kind,
  }
  editorOpen.value = true
}

function onDelete(viz: SavedVisual) {
  if (!confirm(`Delete "${viz.name}"? This cannot be undone.`)) return
  visualsStore.remove(viz.id)
}

function closeEditor() {
  editorOpen.value = false
  editingViz.value = null
  pendingSource.value = null
}

const editorTitle = () =>
  editingViz.value ? `Editing "${editingViz.value.name}"` : 'New visualization'
</script>

<template>
  <div class="charts-panel">
    <div class="charts-header">
      <div class="charts-heading">
        <h2 class="charts-title">Visualizations</h2>
        <p class="charts-subtitle">Saved charts across the catalog. Click any chart to open it.</p>
      </div>
      <div class="charts-actions">
        <input v-model="search" class="charts-filter" type="text" placeholder="Filter by name or source" />
        <button class="btn-primary" @click="onNew">
          <i class="fa-solid fa-plus"></i>
          <span>New chart</span>
        </button>
      </div>
    </div>

    <div v-if="filtered.length" class="charts-grid">
      <VisualizationCard
        v-for="viz in filtered"
        :key="viz.id"
        :viz="viz"
        @open="onOpen(viz)"
        @delete="onDelete(viz)"
      />
    </div>
    <div v-else-if="visuals.length" class="charts-empty">
      <p>No charts match your filter.</p>
    </div>
    <div v-else class="charts-empty">
      <i class="fa-solid fa-chart-line"></i>
      <p>No charts yet.</p>
      <p class="charts-empty-hint">Click "New chart" to visualize a catalog table.</p>
    </div>

    <VisualizationSourcePicker v-model="pickerOpen" @picked="onPicked" />

    <FullscreenOverlay :open="editorOpen" :title="editorTitle()" @close="closeEditor">
      <VisualizationEditor
        v-if="editorOpen && pendingSource"
        :source="pendingSource"
        :viz="editingViz"
        @saved="closeEditor"
        @cancel="closeEditor"
      />
    </FullscreenOverlay>
  </div>
</template>

<style scoped>
.charts-panel {
  height: 100%;
  overflow-y: auto;
  padding: var(--spacing-5, 20px) var(--spacing-6, 24px);
}
.charts-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: var(--spacing-5, 20px);
}
.charts-title {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: var(--color-text-primary);
}
.charts-subtitle {
  margin: 2px 0 0;
  font-size: 13px;
  color: var(--color-text-secondary);
}
.charts-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.charts-filter {
  width: 220px;
  padding: 8px 12px;
  font-size: 13px;
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
}
.charts-filter:focus {
  outline: none;
  border-color: var(--color-accent);
}
.btn-primary {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: var(--color-accent);
  color: #fff;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  white-space: nowrap;
}
.btn-primary:hover {
  background: var(--color-accent-hover, #1d4ed8);
}
.btn-primary i {
  font-size: 14px;
}
.charts-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 16px;
}
.charts-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 64px 16px;
  color: var(--color-text-muted);
  text-align: center;
}
.charts-empty i {
  font-size: 42px;
  opacity: 0.5;
}
.charts-empty p {
  margin: 0;
  font-size: 14px;
}
.charts-empty-hint {
  font-size: 12px !important;
}
</style>
