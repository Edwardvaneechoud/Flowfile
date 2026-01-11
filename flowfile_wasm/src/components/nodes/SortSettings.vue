<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Columns</div>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <ul v-else class="listbox">
      <li
        v-for="col in columns"
        :key="col.name"
        :class="{ 'is-selected': selectedColumns.includes(col.name) }"
        @click="handleItemClick(col.name)"
        @contextmenu.prevent="openContextMenu($event, col.name)"
      >
        {{ col.name }} ({{ col.data_type }})
      </li>
    </ul>

    <div class="listbox-subtitle" style="margin-top: 12px;">Settings</div>

    <div v-if="sortCols.length === 0" class="no-data">
      No sort columns selected. Click or right-click on columns above to add.
    </div>

    <div v-else class="table-wrapper">
      <table class="styled-table">
        <thead>
          <tr>
            <th>Field</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(sort, idx) in sortCols"
            :key="idx"
            @contextmenu.prevent="openRowContextMenu($event, idx)"
          >
            <td>{{ sort.column }}</td>
            <td>
              <select :value="sort.descending" @change="updateDescending(idx, ($event.target as HTMLSelectElement).value === 'true')" class="select-sm">
                <option :value="false">Ascending</option>
                <option :value="true">Descending</option>
              </select>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Context Menu for columns -->
    <div
      v-if="showContextMenu"
      class="context-menu"
      :style="{ top: contextMenuPosition.y + 'px', left: contextMenuPosition.x + 'px' }"
    >
      <button @click="setSortSettings(false)">Ascending</button>
      <button @click="setSortSettings(true)">Descending</button>
    </div>

    <!-- Context Menu for rows -->
    <div
      v-if="showContextMenuRemove"
      class="context-menu"
      :style="{ top: contextMenuPosition.y + 'px', left: contextMenuPosition.x + 'px' }"
    >
      <button @click="removeRow">Remove</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
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

// Initialize directly from props - no watch needed
const sortCols = ref<SortColumn[]>(
  props.settings.sort_input?.sort_cols
    ? props.settings.sort_input.sort_cols.map(s => ({ column: s.column, descending: s.descending }))
    : []
)
const selectedColumns = ref<string[]>([])
const showContextMenu = ref(false)
const showContextMenuRemove = ref(false)
const contextMenuPosition = ref({ x: 0, y: 0 })
const contextMenuColumn = ref<string | null>(null)
const contextMenuRowIndex = ref<number | null>(null)

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

function handleItemClick(columnName: string) {
  const idx = selectedColumns.value.indexOf(columnName)
  if (idx === -1) {
    selectedColumns.value = [columnName]
  } else {
    selectedColumns.value = []
  }
}

function openContextMenu(event: MouseEvent, columnName: string) {
  event.preventDefault()
  if (!selectedColumns.value.includes(columnName)) {
    selectedColumns.value = [columnName]
  }
  contextMenuPosition.value = { x: event.clientX, y: event.clientY }
  contextMenuColumn.value = columnName
  showContextMenu.value = true
}

function openRowContextMenu(event: MouseEvent, index: number) {
  event.preventDefault()
  contextMenuPosition.value = { x: event.clientX, y: event.clientY }
  contextMenuRowIndex.value = index
  showContextMenuRemove.value = true
}

function hideContextMenu() {
  showContextMenu.value = false
  showContextMenuRemove.value = false
  contextMenuColumn.value = null
}

function setSortSettings(descending: boolean) {
  const column = contextMenuColumn.value
  if (column) {
    sortCols.value.push({ column, descending })
    emitUpdate()
  }
  hideContextMenu()
}

function updateDescending(index: number, descending: boolean) {
  sortCols.value[index].descending = descending
  emitUpdate()
}

function removeRow() {
  if (contextMenuRowIndex.value !== null) {
    sortCols.value.splice(contextMenuRowIndex.value, 1)
    emitUpdate()
  }
  showContextMenuRemove.value = false
  contextMenuRowIndex.value = null
}

function emitUpdate() {
  const settings: SortSettings = {
    ...props.settings,
    is_setup: sortCols.value.length > 0,
    sort_input: {
      sort_cols: sortCols.value.map(s => ({
        column: s.column,
        descending: s.descending
      }))
    }
  }
  emit('update:settings', settings)
}

onMounted(() => {
  document.addEventListener('click', hideContextMenu)
})

onUnmounted(() => {
  document.removeEventListener('click', hideContextMenu)
})
</script>

<style scoped>
/* Component uses global styles from main.css */
</style>
