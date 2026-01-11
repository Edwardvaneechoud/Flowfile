<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Available Columns</label>
      <div class="help-text">Right-click to add aggregations</div>
    </div>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <div v-else class="column-list">
      <div
        v-for="col in columns"
        :key="col.name"
        class="column-item"
        :class="{ selected: selectedColumns.includes(col.name) }"
        @click="toggleColumn(col.name)"
        @contextmenu.prevent="showContextMenu($event, col.name)"
      >
        <span class="column-name">{{ col.name }}</span>
        <span class="column-type">{{ col.data_type }}</span>
      </div>
    </div>

    <div class="form-group">
      <label>Aggregations</label>
    </div>

    <div v-if="aggCols.length === 0" class="no-aggs">
      No aggregations defined. Right-click on columns above to add.
    </div>

    <table v-else class="agg-table">
      <thead>
        <tr>
          <th>Column</th>
          <th>Aggregation</th>
          <th>Output Name</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(agg, idx) in aggCols" :key="idx">
          <td>{{ agg.old_name }}</td>
          <td>
            <select v-model="agg.agg" @change="emitUpdate" class="select-sm">
              <option v-for="opt in aggOptions" :key="opt" :value="opt">{{ opt }}</option>
            </select>
          </td>
          <td>
            <input type="text" v-model="agg.new_name" @input="emitUpdate" class="input-sm" />
          </td>
          <td>
            <button class="remove-btn" @click="removeAgg(idx)">×</button>
          </td>
        </tr>
      </tbody>
    </table>

    <!-- Context Menu -->
    <div
      v-if="contextMenu.show"
      class="context-menu"
      :style="{ top: contextMenu.y + 'px', left: contextMenu.x + 'px' }"
    >
      <div class="context-menu-item" @click="addAgg('groupby')">Group By</div>
      <div class="context-menu-divider"></div>
      <div class="context-menu-item" @click="addAgg('sum')">Sum</div>
      <div class="context-menu-item" @click="addAgg('count')">Count</div>
      <div class="context-menu-item" @click="addAgg('mean')">Mean</div>
      <div class="context-menu-item" @click="addAgg('min')">Min</div>
      <div class="context-menu-item" @click="addAgg('max')">Max</div>
      <div class="context-menu-item" @click="addAgg('median')">Median</div>
      <div class="context-menu-item" @click="addAgg('first')">First</div>
      <div class="context-menu-item" @click="addAgg('last')">Last</div>
      <div class="context-menu-item" @click="addAgg('n_unique')">N Unique</div>
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

function removeAgg(index: number) {
  aggCols.value.splice(index, 1)
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

.no-columns, .no-aggs {
  padding: 16px;
  text-align: center;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
  font-size: 13px;
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

.column-item.selected {
  background: var(--accent-light);
}

.column-name {
  font-size: 13px;
}

.column-type {
  font-size: 11px;
  color: var(--text-secondary);
}

.agg-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.agg-table th,
.agg-table td {
  padding: 8px;
  text-align: left;
  border-bottom: 1px solid var(--border-light);
}

.agg-table th {
  background: var(--bg-tertiary);
  font-weight: 500;
}

.select-sm, .input-sm {
  padding: 4px 8px;
  font-size: 12px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
}

.input-sm {
  width: 100%;
}

.remove-btn {
  background: none;
  border: none;
  color: var(--error-color);
  font-size: 18px;
  cursor: pointer;
  padding: 0 4px;
}

.remove-btn:hover {
  opacity: 0.8;
}

.context-menu {
  position: fixed;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  box-shadow: var(--shadow-lg);
  min-width: 120px;
  z-index: 1000;
  overflow: hidden;
}

.context-menu-item {
  padding: 8px 12px;
  font-size: 13px;
  cursor: pointer;
  transition: background 0.15s;
}

.context-menu-item:hover {
  background: var(--bg-hover);
}

.context-menu-divider {
  height: 1px;
  background: var(--border-light);
  margin: 4px 0;
}
</style>
