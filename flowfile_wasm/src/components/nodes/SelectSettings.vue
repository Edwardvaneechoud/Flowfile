<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Select and reorder columns</label>
      <div class="help-text">Drag to reorder. Uncheck to remove columns.</div>
    </div>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <div v-else class="column-list">
      <div
        v-for="(col, index) in localColumns"
        :key="col.old_name"
        class="column-item"
        draggable="true"
        @dragstart="onDragStart(index)"
        @dragover.prevent="onDragOver(index)"
        @drop="onDrop(index)"
      >
        <div class="drag-handle">⋮⋮</div>
        <label class="checkbox-label">
          <input type="checkbox" v-model="col.keep" @change="emitUpdate" />
        </label>
        <div class="column-info">
          <span class="column-name">{{ col.old_name }}</span>
          <span class="column-type">{{ col.data_type }}</span>
        </div>
        <input
          type="text"
          v-model="col.new_name"
          @input="emitUpdate"
          class="rename-input"
          placeholder="New name..."
        />
      </div>
    </div>

    <div class="actions">
      <button class="btn btn-secondary" @click="selectAll">Select All</button>
      <button class="btn btn-secondary" @click="deselectAll">Deselect All</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { SelectSettings, SelectColumn, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: SelectSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: SelectSettings): void
}>()

const flowStore = useFlowStore()

interface LocalColumn extends SelectColumn {
  old_name: string
  new_name: string
  keep: boolean
  position: number
  data_type: string
}

const localColumns = ref<LocalColumn[]>([])
const dragIndex = ref<number | null>(null)

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

watch(columns, (newColumns) => {
  if (newColumns.length > 0 && localColumns.value.length === 0) {
    initializeColumns()
  }
}, { immediate: true })

watch(() => props.settings.select_input, (newInput) => {
  if (newInput && newInput.length > 0) {
    localColumns.value = newInput.map((col, idx) => ({
      old_name: col.old_name,
      new_name: col.new_name,
      keep: col.keep,
      position: col.position ?? idx,
      data_type: col.data_type || 'unknown'
    }))
  }
}, { deep: true })

function initializeColumns() {
  localColumns.value = columns.value.map((col, idx) => ({
    old_name: col.name,
    new_name: col.name,
    keep: true,
    position: idx,
    data_type: col.data_type
  }))
  emitUpdate()
}

function onDragStart(index: number) {
  dragIndex.value = index
}

function onDragOver(_index: number) {
  // Visual feedback handled by CSS
}

function onDrop(targetIndex: number) {
  if (dragIndex.value === null || dragIndex.value === targetIndex) return

  const item = localColumns.value.splice(dragIndex.value, 1)[0]
  localColumns.value.splice(targetIndex, 0, item)

  // Update positions
  localColumns.value.forEach((col, idx) => {
    col.position = idx
  })

  dragIndex.value = null
  emitUpdate()
}

function selectAll() {
  localColumns.value.forEach(col => col.keep = true)
  emitUpdate()
}

function deselectAll() {
  localColumns.value.forEach(col => col.keep = false)
  emitUpdate()
}

function emitUpdate() {
  const settings: SelectSettings = {
    ...props.settings,
    is_setup: true,
    select_input: localColumns.value.map((col, idx) => ({
      old_name: col.old_name,
      new_name: col.new_name,
      keep: col.keep,
      position: idx,
      data_type: col.data_type
    })),
    keep_missing: props.settings.keep_missing
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
  padding: 20px;
  text-align: center;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
}

.column-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-height: 400px;
  overflow-y: auto;
}

.column-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  cursor: grab;
}

.column-item:hover {
  background: var(--bg-hover);
}

.drag-handle {
  color: var(--text-muted);
  cursor: grab;
  user-select: none;
}

.checkbox-label input {
  width: 16px;
  height: 16px;
}

.column-info {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.column-name {
  font-size: 13px;
  font-weight: 500;
}

.column-type {
  font-size: 11px;
  color: var(--text-secondary);
}

.rename-input {
  width: 120px;
  padding: 4px 8px;
  font-size: 12px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
}

.rename-input:focus {
  outline: none;
  border-color: var(--accent-color);
}

.actions {
  display: flex;
  gap: 8px;
}

.btn {
  padding: 6px 12px;
  font-size: 12px;
  border: none;
  border-radius: var(--radius-sm);
  cursor: pointer;
}

.btn-secondary {
  background: var(--bg-tertiary);
  color: var(--text-primary);
}

.btn-secondary:hover {
  background: var(--border-color);
}
</style>
