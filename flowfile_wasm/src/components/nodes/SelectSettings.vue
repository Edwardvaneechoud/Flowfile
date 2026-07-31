<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Columns</div>

    <div v-if="!hasInputConnection" class="no-columns">
      No input connected. Connect an input node first.
    </div>

    <div v-else-if="columns.length === 0 && availableColumns.length === 0" class="no-columns">
      Connect an input to see its columns.
    </div>

    <template v-else-if="availableColumns.length > 0">
      <div class="column-picker-toolbar">
        <span class="column-picker-count">{{ keptCount }} of {{ availableColumns.length }} kept</span>

        <div class="column-picker-search">
          <!-- type="search" keeps the rename fields the only text inputs here -->
          <input
            v-model="filterText"
            type="search"
            placeholder="Filter columns…"
            aria-label="Filter columns"
            v-bind="NO_AUTOFILL"
          />
        </div>

        <div class="column-picker-actions">
          <button
            class="btn"
            type="button"
            title="Keep all columns"
            aria-label="Keep all columns"
            @mousedown.prevent
            @click="selectAll"
          >
            ☑
          </button>
          <button
            class="btn"
            type="button"
            title="Keep no columns"
            aria-label="Keep no columns"
            @mousedown.prevent
            @click="deselectAll"
          >
            ☐
          </button>
        </div>
      </div>

      <div class="table-wrapper">
        <table class="styled-table column-picker">
          <!-- Inline because scoped CSS does not reach <col> -->
          <colgroup>
            <col style="width: 22px" />
            <col />
            <col />
            <col style="width: 92px" />
            <col style="width: 52px" />
          </colgroup>
          <thead>
            <tr>
              <th aria-label="Reorder"></th>
              <th>Field</th>
              <th>Output name</th>
              <th>Data type</th>
              <th class="is-centered">Keep</th>
            </tr>
          </thead>
          <tbody @dragend="onDragEnd">
            <tr
              v-for="(col, index) in visibleColumns"
              :key="col.old_name + '-' + index"
              :class="{ 'row-disabled': !col.keep, 'is-drop-target': dragOverName === col.old_name }"
              @dragover.prevent="onDragOver(col.old_name)"
              @drop="onDrop(col.old_name)"
            >
              <td
                class="drag-handle-cell"
                :draggable="!isFiltered"
                :title="dragHandleTitle"
                @dragstart="onDragStart(col.old_name)"
              >
                ⠿
              </td>
              <td class="picker-name" :title="col.old_name">{{ col.old_name }}</td>
              <td>
                <input
                  type="text"
                  :value="col.new_name"
                  :aria-label="`Output name for ${col.old_name}`"
                  class="inline-input"
                  v-bind="NO_AUTOFILL"
                  @input="updateNewName(col.old_name, ($event.target as HTMLInputElement).value)"
                />
              </td>
              <td class="type-cell">
                <span
                  class="type-badge"
                  :class="dataTypeBadgeClass(dataTypeGroup(col.data_type))"
                  :title="col.data_type"
                >
                  {{ dataTypeLabel(col.data_type) }}
                </span>
              </td>
              <td class="is-centered">
                <input
                  type="checkbox"
                  :checked="col.keep"
                  :aria-label="`Keep ${col.old_name}`"
                  @change="toggleKeep(col.old_name)"
                />
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="visibleColumns.length === 0" class="no-columns">
        No columns match “{{ filterText }}”.
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import { dataTypeGroup, dataTypeBadgeClass, dataTypeLabel } from '../../utils/dtypeGroup'
import { NO_AUTOFILL } from '../../utils/noAutofill'
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

const hasInputConnection = computed(() => {
  const node = flowStore.getNode(props.nodeId)
  if (!node) return false
  return node.inputIds.length > 0 || node.leftInputId !== undefined
})

const availableColumns = computed(() => {
  return localColumns.value.filter(col => col.is_available !== false)
})

const filterText = ref('')

const isFiltered = computed(() => filterText.value.trim().length > 0)

const visibleColumns = computed(() => {
  const query = filterText.value.trim().toLowerCase()
  if (!query) return availableColumns.value
  return availableColumns.value.filter(
    col =>
      col.old_name.toLowerCase().includes(query) || col.new_name.toLowerCase().includes(query)
  )
})

const keptCount = computed(() => availableColumns.value.filter(col => col.keep).length)

// A drop between two non-adjacent visible rows is ambiguous, so block it.
const dragHandleTitle = computed(() =>
  isFiltered.value ? 'Clear the filter to reorder columns' : 'Drag to reorder'
)

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

watch(columns, (newColumns) => {
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
const dragOverName = ref<string | null>(null)

function onDragStart(columnName: string) {
  dragColumnName.value = columnName
}

function onDragOver(columnName: string) {
  if (dragColumnName.value === null) return
  dragOverName.value = columnName === dragColumnName.value ? null : columnName
}

function onDragEnd() {
  dragColumnName.value = null
  dragOverName.value = null
}

function onDrop(targetColumnName: string) {
  dragOverName.value = null
  if (dragColumnName.value === null || dragColumnName.value === targetColumnName) return

  const sourceIndex = findColumnIndex(dragColumnName.value)
  const targetIndex = findColumnIndex(targetColumnName)

  if (sourceIndex === -1 || targetIndex === -1) return

  const item = localColumns.value.splice(sourceIndex, 1)[0]
  localColumns.value.splice(targetIndex, 0, item)

  localColumns.value.forEach((col, idx) => {
    col.position = idx
  })

  dragColumnName.value = null
  emitUpdate()
}

function selectAll() {
  localColumns.value.forEach(col => {
    if (col.is_available) {
      col.keep = true
    }
  })
  emitUpdate()
}

function deselectAll() {
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
/* Component uses global styles from main.css (.column-picker, .type-badge, …) */
.row-disabled {
  opacity: 0.5;
}

/* Reset .styled-table's 35%/30%/35%; widths belong to the <colgroup> alone, or
   the header and body disagree. Global rule stays — 3 other panels rely on it. */
.column-picker th:first-child,
.column-picker td:first-child,
.column-picker th:nth-child(2),
.column-picker td:nth-child(2),
.column-picker th:last-child,
.column-picker td:last-child {
  width: auto;
}

/* 22px is unreachable while the cell carries side padding. */
.column-picker th:first-child,
.column-picker td.drag-handle-cell {
  padding-left: 0;
  padding-right: 0;
}
</style>
