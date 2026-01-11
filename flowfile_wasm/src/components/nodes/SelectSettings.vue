<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Columns</div>

    <div v-if="columns.length === 0 && localColumns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <div v-else-if="localColumns.length > 0" class="table-wrapper">
      <table class="styled-table">
        <thead>
          <tr>
            <th style="width: 30px;"></th>
            <th>Field</th>
            <th>Output Field Name</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(col, index) in localColumns"
            :key="col.old_name + '-' + index"
            :class="{ 'row-disabled': !col.keep }"
            draggable="true"
            @dragstart="onDragStart(index)"
            @dragover.prevent="onDragOver(index)"
            @drop="onDrop(index)"
          >
            <td style="width: 30px; text-align: center;">
              <input type="checkbox" :checked="col.keep" @change="toggleKeep(index)" />
            </td>
            <td>{{ col.old_name }}</td>
            <td>
              <input
                type="text"
                :value="col.new_name"
                @input="updateNewName(index, ($event.target as HTMLInputElement).value)"
                class="input-sm"
              />
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div style="margin-top: 12px; display: flex; gap: 8px;">
      <button class="btn btn-small btn-secondary" @click="selectAll">Select All</button>
      <button class="btn btn-small btn-secondary" @click="deselectAll">Deselect All</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { SelectSettings, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: SelectSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: SelectSettings): void
}>()

const flowStore = useFlowStore()

interface LocalColumn {
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

// Initialize from settings immediately if available
if (props.settings.select_input && props.settings.select_input.length > 0) {
  localColumns.value = props.settings.select_input.map((col, idx) => ({
    old_name: col.old_name,
    new_name: col.new_name,
    keep: col.keep,
    position: col.position ?? idx,
    data_type: col.data_type || 'unknown'
  }))
}

// Watch for input columns becoming available (only if we don't have settings yet)
watch(columns, (newColumns) => {
  // Only initialize from columns if we don't have any local columns yet
  if (newColumns.length > 0 && localColumns.value.length === 0) {
    localColumns.value = newColumns.map((col, idx) => ({
      old_name: col.name,
      new_name: col.name,
      keep: true,
      position: idx,
      data_type: col.data_type
    }))
    emitUpdate()
  } else if (newColumns.length > 0 && localColumns.value.length > 0) {
    // Check if we need to add new columns that appeared
    const existingNames = new Set(localColumns.value.map(c => c.old_name))
    const newCols = newColumns.filter(c => !existingNames.has(c.name))
    if (newCols.length > 0) {
      // Add new columns at the end
      const startPos = localColumns.value.length
      newCols.forEach((col, idx) => {
        localColumns.value.push({
          old_name: col.name,
          new_name: col.name,
          keep: true,
          position: startPos + idx,
          data_type: col.data_type
        })
      })
      emitUpdate()
    }
  }
}, { immediate: true })

function toggleKeep(index: number) {
  localColumns.value[index].keep = !localColumns.value[index].keep
  emitUpdate()
}

function updateNewName(index: number, value: string) {
  localColumns.value[index].new_name = value
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
    keep_missing: props.settings.keep_missing ?? false
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
/* Component uses global styles from main.css */
.row-disabled {
  opacity: 0.5;
}

.styled-table tr {
  cursor: grab;
}
</style>
