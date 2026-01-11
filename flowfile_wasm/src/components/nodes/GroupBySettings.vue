<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Available Columns</div>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <ul v-else class="listbox">
      <li
        v-for="col in columns"
        :key="col.name"
        :class="{ 'is-selected': selectedColumns.includes(col.name) }"
        @click="toggleColumn(col.name)"
        @contextmenu.prevent="showContextMenu($event, col.name)"
      >
        {{ col.name }} ({{ col.data_type }})
      </li>
    </ul>

    <div class="listbox-subtitle" style="margin-top: 12px;">Settings</div>

    <div v-if="aggCols.length === 0" class="no-aggs">
      No aggregations defined. Right-click on columns above to add.
    </div>

    <div v-else class="table-wrapper">
      <table class="styled-table">
        <thead>
          <tr>
            <th>Field</th>
            <th>Action</th>
            <th>Output Field Name</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(agg, idx) in aggCols" :key="idx" @contextmenu.prevent="openRowContextMenu($event, idx)">
            <td>{{ agg.old_name }}</td>
            <td>
              <select v-model="agg.agg" @change="emitUpdate" class="select-sm">
                <option v-for="opt in aggOptions" :key="opt" :value="opt">{{ opt }}</option>
              </select>
            </td>
            <td>
              <input type="text" v-model="agg.new_name" @input="emitUpdate" class="input-sm" />
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Context Menu for columns -->
    <div
      v-if="contextMenu.show"
      class="context-menu"
      :style="{ top: contextMenu.y + 'px', left: contextMenu.x + 'px' }"
    >
      <button @click="addAgg('groupby')">Group by</button>
      <button @click="addAgg('sum')">Sum</button>
      <button @click="addAgg('count')">Count</button>
      <button @click="addAgg('mean')">Mean</button>
      <button @click="addAgg('min')">Min</button>
      <button @click="addAgg('max')">Max</button>
      <button @click="addAgg('median')">Median</button>
      <button @click="addAgg('first')">First</button>
      <button @click="addAgg('last')">Last</button>
      <button @click="addAgg('n_unique')">N Unique</button>
      <button @click="addAgg('concat')">Concat</button>
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
import { ref, computed, watch, onMounted, onUnmounted } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { GroupBySettings, AggColumn, AggType, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: GroupBySettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: GroupBySettings): void
}>()

const flowStore = useFlowStore()

const selectedColumns = ref<string[]>([])
const aggCols = ref<AggColumn[]>(props.settings.groupby_input?.agg_cols || [])
const contextMenu = ref({ show: false, x: 0, y: 0, column: '' })
const showContextMenuRemove = ref(false)
const contextMenuPosition = ref({ x: 0, y: 0 })
const contextMenuRowIndex = ref<number | null>(null)

const aggOptions: AggType[] = ['groupby', 'sum', 'count', 'mean', 'min', 'max', 'median', 'first', 'last', 'n_unique', 'concat']

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

watch(() => props.settings.groupby_input?.agg_cols, (newAggs) => {
  if (newAggs) {
    aggCols.value = [...newAggs]
  }
}, { deep: true })

function toggleColumn(name: string) {
  const idx = selectedColumns.value.indexOf(name)
  if (idx === -1) {
    selectedColumns.value.push(name)
  } else {
    selectedColumns.value.splice(idx, 1)
  }
}

function showContextMenu(event: MouseEvent, column: string) {
  contextMenu.value = {
    show: true,
    x: event.clientX,
    y: event.clientY,
    column
  }
}

function hideContextMenu() {
  contextMenu.value.show = false
  showContextMenuRemove.value = false
}

function openRowContextMenu(event: MouseEvent, index: number) {
  event.preventDefault()
  contextMenuPosition.value = { x: event.clientX, y: event.clientY }
  contextMenuRowIndex.value = index
  showContextMenuRemove.value = true
}

function removeRow() {
  if (contextMenuRowIndex.value !== null) {
    aggCols.value.splice(contextMenuRowIndex.value, 1)
    emitUpdate()
  }
  showContextMenuRemove.value = false
  contextMenuRowIndex.value = null
}

function addAgg(aggType: AggType) {
  const column = contextMenu.value.column
  const newName = aggType === 'groupby' ? column : `${column}_${aggType}`

  aggCols.value.push({
    old_name: column,
    new_name: newName,
    agg: aggType
  })

  hideContextMenu()
  emitUpdate()
}

function emitUpdate() {
  const settings: GroupBySettings = {
    ...props.settings,
    is_setup: true,
    groupby_input: {
      agg_cols: [...aggCols.value]
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
