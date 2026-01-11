<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Sort Columns</label>
      <div class="help-text">Click columns to add sort order</div>
    </div>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <div v-else class="column-list">
      <div
        v-for="col in columns"
        :key="col.name"
        class="column-item"
        @click="addSortColumn(col.name)"
      >
        <span class="column-name">{{ col.name }}</span>
        <span class="column-type">{{ col.data_type }}</span>
      </div>
    </div>

    <div v-if="sortCols.length > 0" class="sort-list">
      <div class="form-group">
        <label>Sort Order</label>
      </div>
      <div
        v-for="(sort, idx) in sortCols"
        :key="idx"
        class="sort-item"
        draggable="true"
        @dragstart="onDragStart(idx)"
        @dragover.prevent
        @drop="onDrop(idx)"
      >
        <div class="drag-handle">⋮⋮</div>
        <span class="sort-column">{{ sort.column }}</span>
        <button
          class="sort-direction"
          :class="{ desc: sort.descending }"
          @click="toggleDirection(idx)"
        >
          {{ sort.descending ? '↓ DESC' : '↑ ASC' }}
        </button>
        <button class="remove-btn" @click="removeSortColumn(idx)">×</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { SortSettings, SortColumn, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: SortSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: SortSettings): void
}>()

const flowStore = useFlowStore()

const sortCols = ref<SortColumn[]>(props.settings.sort_input?.sort_cols || [])
const dragIndex = ref<number | null>(null)

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

watch(() => props.settings.sort_input?.sort_cols, (newCols) => {
  if (newCols) {
    sortCols.value = [...newCols]
  }
}, { deep: true })

function addSortColumn(name: string) {
  if (sortCols.value.some(s => s.column === name)) return

  sortCols.value.push({
    column: name,
    descending: false
  })
  emitUpdate()
}

function toggleDirection(index: number) {
  sortCols.value[index].descending = !sortCols.value[index].descending
  emitUpdate()
}

function removeSortColumn(index: number) {
  sortCols.value.splice(index, 1)
  emitUpdate()
}

function onDragStart(index: number) {
  dragIndex.value = index
}

function onDrop(targetIndex: number) {
  if (dragIndex.value === null || dragIndex.value === targetIndex) return

  const item = sortCols.value.splice(dragIndex.value, 1)[0]
  sortCols.value.splice(targetIndex, 0, item)
  dragIndex.value = null
  emitUpdate()
}

function emitUpdate() {
  const settings: SortSettings = {
    ...props.settings,
    is_setup: sortCols.value.length > 0,
    sort_input: {
      sort_cols: [...sortCols.value]
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
.settings-form {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.form-group label {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

.help-text {
  font-size: 12px;
  color: var(--text-secondary);
}

.no-columns {
  padding: 16px;
  text-align: center;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
}

.column-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--border-light);
  border-radius: var(--radius-sm);
}

.column-item {
  display: flex;
  justify-content: space-between;
  padding: 8px 12px;
  cursor: pointer;
  transition: background 0.15s;
}

.column-item:hover {
  background: var(--bg-hover);
}

.column-name {
  font-size: 13px;
}

.column-type {
  font-size: 11px;
  color: var(--text-secondary);
}

.sort-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.sort-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  cursor: grab;
}

.drag-handle {
  color: var(--text-muted);
  cursor: grab;
}

.sort-column {
  flex: 1;
  font-size: 13px;
  font-weight: 500;
}

.sort-direction {
  padding: 4px 8px;
  font-size: 11px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  cursor: pointer;
  transition: all 0.15s;
}

.sort-direction:hover {
  border-color: var(--accent-color);
}

.sort-direction.desc {
  background: var(--accent-color);
  color: white;
  border-color: var(--accent-color);
}

.remove-btn {
  background: none;
  border: none;
  color: var(--error-color);
  font-size: 18px;
  cursor: pointer;
  padding: 0 4px;
}
</style>
