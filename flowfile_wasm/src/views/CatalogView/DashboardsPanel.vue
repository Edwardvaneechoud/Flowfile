<script setup lang="ts">
import { ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useDashboardsStore } from '../../stores/dashboards-store'
import DashboardCard from './DashboardCard.vue'
import DashboardEditor from './DashboardEditor.vue'
import DashboardViewer from './DashboardViewer.vue'

// library = grid of saved dashboards; view = read-only; edit = unlocked canvas.
type Mode = 'library' | 'view' | 'edit'

const store = useDashboardsStore()
const { library } = storeToRefs(store)
const mode = ref<Mode>('library')

function onNew() {
  store.newBlankDashboard()
  mode.value = 'edit'
}

function onView(id: string) {
  if (store.loadDashboard(id)) mode.value = 'view'
}

function onEditFromCard(id: string) {
  if (store.loadDashboard(id)) mode.value = 'edit'
}

function onDelete(id: string, name: string) {
  if (!confirm(`Delete "${name}"? This cannot be undone.`)) return
  store.deleteDashboard(id)
}

// From the read-only viewer.
function viewerEdit() {
  mode.value = 'edit'
}
function viewerBack() {
  store.reset()
  mode.value = 'library'
}

// From the editor's Back/Discard. An existing dashboard returns to its
// read-only view (reloaded to drop unsaved edits); a never-saved one goes back
// to the library.
function editorClose() {
  const id = store.current?.id
  if (id) {
    store.loadDashboard(id)
    mode.value = 'view'
  } else {
    store.reset()
    mode.value = 'library'
  }
}
</script>

<template>
  <div class="dashboards-panel">
    <template v-if="mode === 'library'">
      <div class="dash-header">
        <div>
          <h2 class="dash-title">Dashboards</h2>
          <p class="dash-subtitle">Combine saved visuals into a layout. Stored in this browser.</p>
        </div>
        <button class="btn-primary" @click="onNew">
          <i class="fa-solid fa-plus"></i>
          <span>New dashboard</span>
        </button>
      </div>

      <div v-if="library.length" class="dash-grid">
        <DashboardCard
          v-for="d in library"
          :key="d.id"
          :dashboard="d"
          @open="onView(d.id)"
          @edit="onEditFromCard(d.id)"
          @delete="onDelete(d.id, d.name)"
        />
      </div>
      <div v-else class="dash-empty">
        <i class="fa-solid fa-table-cells-large"></i>
        <p>No dashboards yet.</p>
        <p class="dash-empty-hint">Create a few visuals, then combine them here.</p>
      </div>
    </template>

    <DashboardViewer v-if="mode === 'view'" @back="viewerBack" @edit="viewerEdit" />
    <DashboardEditor v-if="mode === 'edit'" @close="editorClose" />
  </div>
</template>

<style scoped>
.dashboards-panel {
  height: 100%;
  overflow-y: auto;
  padding: var(--spacing-5, 20px) var(--spacing-6, 24px);
}
.dash-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: var(--spacing-5, 20px);
}
.dash-title {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: var(--color-text-primary);
}
.dash-subtitle {
  margin: 2px 0 0;
  font-size: 13px;
  color: var(--color-text-secondary);
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
.dash-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 12px;
}
.dash-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 64px 16px;
  color: var(--color-text-muted);
  text-align: center;
}
.dash-empty i {
  font-size: 42px;
  opacity: 0.5;
}
.dash-empty p {
  margin: 0;
  font-size: 14px;
}
.dash-empty-hint {
  font-size: 12px !important;
}
</style>
