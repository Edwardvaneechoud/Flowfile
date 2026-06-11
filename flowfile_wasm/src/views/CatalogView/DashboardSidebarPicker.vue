<script setup lang="ts">
import { computed, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useVisualsStore } from '../../stores/visuals-store'
import { useDashboardDragAndDrop } from '../../composables/useDashboardDragAndDrop'
import type { SavedVisual } from '../../types/visuals'

defineProps<{ addedVizIds: Set<string> }>()
const emit = defineEmits<{ add: [viz: SavedVisual]; 'add-text': [] }>()

const visualsStore = useVisualsStore()
const { visuals } = storeToRefs(visualsStore)
const { onVizDragStart, onTextDragStart } = useDashboardDragAndDrop()

const search = ref('')
const filtered = computed(() => {
  const q = search.value.trim().toLowerCase()
  return q ? visuals.value.filter((v) => v.name.toLowerCase().includes(q)) : visuals.value
})
</script>

<template>
  <div class="picker">
    <div class="picker-top">
      <input v-model="search" class="picker-search" type="text" placeholder="Search visuals..." />
      <button class="picker-text-btn" draggable="true" @dragstart="onTextDragStart" @click="emit('add-text')">
        <i class="fa-solid fa-align-left"></i>
        <span>Add text</span>
      </button>
    </div>

    <div class="picker-scroll">
      <p v-if="!visuals.length" class="picker-empty">
        No visuals yet. Create one on the Visuals tab first.
      </p>
      <p v-else-if="!filtered.length" class="picker-empty">No visuals match your search.</p>
      <ul v-else class="picker-list">
        <li
          v-for="viz in filtered"
          :key="viz.id"
          class="picker-item"
          :class="{ added: addedVizIds.has(viz.id) }"
          draggable="true"
          :title="`${viz.name} — drag onto the canvas`"
          @dragstart="onVizDragStart($event, viz.id)"
        >
          <div class="picker-thumb">
            <img v-if="viz.thumbnail_data_url" :src="viz.thumbnail_data_url" alt="" />
            <i v-else class="fa-solid fa-chart-column"></i>
          </div>
          <div class="picker-info">
            <div class="picker-name">{{ viz.name }}</div>
            <div class="picker-source">{{ viz.dataset_name }}</div>
          </div>
          <button class="picker-add" title="Add to dashboard" @click.stop="emit('add', viz)">
            <i class="fa-solid fa-plus"></i>
          </button>
        </li>
      </ul>
    </div>
  </div>
</template>

<style scoped>
.picker {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
}
.picker-top {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 12px;
  border-bottom: 1px solid var(--color-border-light);
}
.picker-search {
  width: 100%;
  padding: 7px 10px;
  font-size: 13px;
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
}
.picker-search:focus {
  outline: none;
  border-color: var(--color-accent);
}
.picker-text-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 7px 10px;
  border: 1px dashed var(--color-border-primary);
  border-radius: 6px;
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  cursor: grab;
  font-size: 13px;
}
.picker-text-btn:hover {
  border-color: var(--color-accent);
  color: var(--color-text-primary);
}
.picker-text-btn i {
  font-size: 13px;
}
.picker-scroll {
  flex: 1;
  overflow-y: auto;
  padding: 8px;
}
.picker-empty {
  margin: 0;
  padding: 16px 8px;
  font-size: 12px;
  color: var(--color-text-muted);
  text-align: center;
}
.picker-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.picker-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px;
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  background: var(--color-background-primary);
  cursor: grab;
}
.picker-item:hover {
  border-color: var(--color-accent);
}
.picker-item.added {
  opacity: 0.6;
}
.picker-thumb {
  width: 40px;
  height: 40px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  background: var(--color-background-secondary);
  overflow: hidden;
}
.picker-thumb img {
  width: 100%;
  height: 100%;
  object-fit: contain;
}
.picker-thumb i {
  font-size: 17px;
  color: var(--color-text-muted);
}
.picker-info {
  flex: 1;
  min-width: 0;
}
.picker-name {
  font-size: 13px;
  font-weight: 500;
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.picker-source {
  font-size: 11px;
  color: var(--color-text-muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.picker-add {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  flex-shrink: 0;
  border: none;
  border-radius: 4px;
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  cursor: pointer;
}
.picker-add:hover {
  background: var(--color-accent);
  color: #fff;
}
.picker-add i {
  font-size: 13px;
}
</style>
