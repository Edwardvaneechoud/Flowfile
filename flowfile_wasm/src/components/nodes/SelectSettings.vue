<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Columns</div>

    <div v-if="columns.length === 0 && availableColumns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <div v-else-if="availableColumns.length > 0" class="table-wrapper">
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
            v-for="(col, index) in availableColumns"
            :key="col.old_name + '-' + index"
            :class="{ 'row-disabled': !col.keep }"
            draggable="true"
            @dragstart="onDragStart(col.old_name)"
            @dragover.prevent="onDragOver(index)"
            @drop="onDrop(col.old_name)"
          >
            <td style="width: 30px; text-align: center;">
              <input type="checkbox" :checked="col.keep" @change="toggleKeep(col.old_name)" />
            </td>
            <td>{{ col.old_name }}</td>
            <td>
              <input
                type="text"
                :value="col.new_name"
                @input="updateNewName(col.old_name, ($event.target as HTMLInputElement).value)"
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
  is_available: boolean
}

const localColumns = ref<LocalColumn[]>([])

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

// Computed property for available columns only (for display)
const availableColumns = computed(() => {
  return localColumns.value.filter(col => col.is_available !== false)
})

// Initialize from settings immediately if available
function initFromSettings() {
  if (props.settings.select_input && props.settings.select_input.length > 0) {
    localColumns.value = props.settings.select_input.map((col: any, idx: number) => ({
      old_name: col.old_name,
      new_name: col.new_name,
      keep: col.keep,
      position: col.position ?? idx,
      data_type: col.data_type || 'unknown',
      is_available: col.is_available !== false
    }))
  }
}
initFromSettings()

// Watch for settings changes from the store (schema propagation updates)
watch(() => props.settings.select_input, (newSelectInput) => {
  if (newSelectInput && newSelectInput.length > 0) {
    // Preserve user changes (keep, new_name) while updating availability
    const currentByName = new Map(localColumns.value.map(c => [c.old_name, c]))

    localColumns.value = newSelectInput.map((col: any, idx: number) => {
      const existing = currentByName.get(col.old_name)
      return {
        old_name: col.old_name,
        new_name: existing?.new_name ?? col.new_name,
        keep: existing?.keep ?? col.keep,
        position: col.position ?? idx,
        data_type: col.data_type || 'unknown',
        is_available: col.is_available !== false
      }
    })
  }
}, { deep: true })

// Watch for input columns becoming available (only if we don't have settings yet)
watch(columns, (newColumns) => {
  // Only initialize from columns if we don't have any local columns yet
  if (newColumns.length > 0 && localColumns.value.length === 0) {
    localColumns.value = newColumns.map((col, idx) => ({
      old_name: col.name,
      new_name: col.name,
      keep: true,
      position: idx,
      data_type: col.data_type,
      is_available: true
    }))
    emitUpdate()
  }
}, { immediate: true })

// Helper to find column index by name
function findColumnIndex(name: string): number {
  return localColumns.value.findIndex(c => c.old_name === name)
}

function toggleKeep(columnName: string) {
  const index = findColumnIndex(columnName)
  if (index !== -1) {
    localColumns.value[index].keep = !localColumns.value[index].keep
    emitUpdate()
  }
}

function updateNewName(columnName: string, value: string) {
  const index = findColumnIndex(columnName)
  if (index !== -1) {
    localColumns.value[index].new_name = value
    emitUpdate()
  }
}

const dragColumnName = ref<string | null>(null)

function onDragStart(columnName: string) {
  dragColumnName.value = columnName
}

function onDragOver(_index: number) {
  // Visual feedback handled by CSS
}

function onDrop(targetColumnName: string) {
  if (dragColumnName.value === null || dragColumnName.value === targetColumnName) return

  const sourceIndex = findColumnIndex(dragColumnName.value)
  const targetIndex = findColumnIndex(targetColumnName)

  if (sourceIndex === -1 || targetIndex === -1) return

  const item = localColumns.value.splice(sourceIndex, 1)[0]
  localColumns.value.splice(targetIndex, 0, item)

  // Update positions
  localColumns.value.forEach((col, idx) => {
    col.position = idx
  })

  dragColumnName.value = null
  emitUpdate()
}

function selectAll() {
  // Only select available columns
  localColumns.value.forEach(col => {
    if (col.is_available) {
      col.keep = true
    }
  })
  emitUpdate()
}

function deselectAll() {
  // Only deselect available columns
  localColumns.value.forEach(col => {
    if (col.is_available) {
      col.keep = false
    }
  })
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
