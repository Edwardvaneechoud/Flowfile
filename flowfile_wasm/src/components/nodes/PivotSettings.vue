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
        :class="{
          'is-selected': isColumnUsed(col.name),
          'is-index': indexColumns.includes(col.name),
          'is-pivot': pivotColumn === col.name,
          'is-value': valueColumn === col.name
        }"
        @contextmenu.prevent="showContextMenu($event, col.name)"
      >
        <span class="col-name">{{ col.name }}</span>
        <span class="col-type">({{ col.data_type }})</span>
        <span v-if="indexColumns.includes(col.name)" class="col-role">Index</span>
        <span v-if="pivotColumn === col.name" class="col-role">Pivot</span>
        <span v-if="valueColumn === col.name" class="col-role">Value</span>
      </li>
    </ul>

    <div class="listbox-subtitle" style="margin-top: 12px;">Pivot Configuration</div>

    <div class="config-section">
      <div class="config-row">
        <label>Index Columns</label>
        <div class="chip-list">
          <span v-for="col in indexColumns" :key="col" class="chip">
            {{ col }}
            <button class="chip-remove" @click="removeIndexColumn(col)">&times;</button>
          </span>
          <span v-if="indexColumns.length === 0" class="placeholder">Right-click columns to add as index</span>
        </div>
      </div>

      <div class="config-row">
        <label>Pivot Column</label>
        <select v-model="pivotColumn" @change="emitUpdate" class="select-sm">
          <option value="">Select column...</option>
          <option v-for="col in availableForPivot" :key="col.name" :value="col.name">
            {{ col.name }}
          </option>
        </select>
        <span class="help-text">Values become new column names</span>
      </div>

      <div class="config-row">
        <label>Value Column</label>
        <select v-model="valueColumn" @change="emitUpdate" class="select-sm">
          <option value="">Select column...</option>
          <option v-for="col in availableForValue" :key="col.name" :value="col.name">
            {{ col.name }}
          </option>
        </select>
        <span class="help-text">Values to aggregate</span>
      </div>

      <div class="config-row">
        <label>Aggregation</label>
        <div class="checkbox-group">
          <label v-for="agg in availableAggregations" :key="agg" class="checkbox-label">
            <input
              type="checkbox"
              :checked="aggregations.includes(agg)"
              @change="toggleAggregation(agg)"
            />
            {{ agg }}
          </label>
        </div>
      </div>
    </div>

    <!-- Context Menu for columns -->
    <div
      v-if="contextMenu.show"
      class="context-menu"
      :style="{ top: contextMenu.y + 'px', left: contextMenu.x + 'px' }"
    >
      <button @click="setAsIndex">Add as Index Column</button>
      <button @click="setAsPivot">Set as Pivot Column</button>
      <button @click="setAsValue">Set as Value Column</button>
      <button v-if="indexColumns.includes(contextMenu.column)" @click="removeFromIndex">Remove from Index</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { PivotSettings, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: PivotSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: PivotSettings): void
}>()

const flowStore = useFlowStore()

// Local state initialized from props
const indexColumns = ref<string[]>(
  props.settings.pivot_input?.index_columns
    ? [...props.settings.pivot_input.index_columns]
    : []
)
const pivotColumn = ref<string>(props.settings.pivot_input?.pivot_column || '')
const valueColumn = ref<string>(props.settings.pivot_input?.value_col || '')
const aggregations = ref<string[]>(
  props.settings.pivot_input?.aggregations
    ? [...props.settings.pivot_input.aggregations]
    : ['sum']
)

const contextMenu = ref({ show: false, x: 0, y: 0, column: '' })

const availableAggregations = ['sum', 'mean', 'min', 'max', 'count', 'first', 'last', 'median']

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

const availableForPivot = computed(() => {
  // Pivot column should not be an index column
  return columns.value.filter(c => !indexColumns.value.includes(c.name))
})

const availableForValue = computed(() => {
  // Value column should not be an index column or the pivot column
  return columns.value.filter(c =>
    !indexColumns.value.includes(c.name) && c.name !== pivotColumn.value
  )
})

function isColumnUsed(name: string): boolean {
  return indexColumns.value.includes(name) ||
    pivotColumn.value === name ||
    valueColumn.value === name
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

function setAsIndex() {
  const column = contextMenu.value.column
  if (!indexColumns.value.includes(column)) {
    // Remove from other roles if set
    if (pivotColumn.value === column) pivotColumn.value = ''
    if (valueColumn.value === column) valueColumn.value = ''

    indexColumns.value.push(column)
    emitUpdate()
  }
  hideContextMenu()
}

function setAsPivot() {
  const column = contextMenu.value.column
  // Remove from index if it's there
  const idx = indexColumns.value.indexOf(column)
  if (idx !== -1) indexColumns.value.splice(idx, 1)
  if (valueColumn.value === column) valueColumn.value = ''

  pivotColumn.value = column
  emitUpdate()
  hideContextMenu()
}

function setAsValue() {
  const column = contextMenu.value.column
  // Remove from index if it's there
  const idx = indexColumns.value.indexOf(column)
  if (idx !== -1) indexColumns.value.splice(idx, 1)
  if (pivotColumn.value === column) pivotColumn.value = ''

  valueColumn.value = column
  emitUpdate()
  hideContextMenu()
}

function removeFromIndex() {
  const column = contextMenu.value.column
  const idx = indexColumns.value.indexOf(column)
  if (idx !== -1) {
    indexColumns.value.splice(idx, 1)
    emitUpdate()
  }
  hideContextMenu()
}

function removeIndexColumn(column: string) {
  const idx = indexColumns.value.indexOf(column)
  if (idx !== -1) {
    indexColumns.value.splice(idx, 1)
    emitUpdate()
  }
}

function toggleAggregation(agg: string) {
  const idx = aggregations.value.indexOf(agg)
  if (idx !== -1) {
    // Don't allow removing last aggregation
    if (aggregations.value.length > 1) {
      aggregations.value.splice(idx, 1)
    }
  } else {
    aggregations.value.push(agg)
  }
  emitUpdate()
}

function emitUpdate() {
  const isConfigured = pivotColumn.value !== '' && valueColumn.value !== '' && aggregations.value.length > 0

  const settings: PivotSettings = {
    ...props.settings,
    is_setup: isConfigured,
    pivot_input: {
      index_columns: [...indexColumns.value],
      pivot_column: pivotColumn.value,
      value_col: valueColumn.value,
      aggregations: [...aggregations.value]
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

.config-section {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.config-row {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.config-row label {
  font-weight: 500;
  font-size: 12px;
  color: var(--text-secondary);
}

.help-text {
  font-size: 11px;
  color: var(--text-muted);
}

.chip-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  min-height: 28px;
  padding: 4px;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--bg-tertiary);
}

.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  background: var(--accent-light);
  border-radius: 12px;
  font-size: 12px;
  color: var(--text-primary);
}

.chip-remove {
  background: none;
  border: none;
  cursor: pointer;
  padding: 0 2px;
  font-size: 14px;
  color: var(--text-secondary);
}

.chip-remove:hover {
  color: var(--error-color);
}

.placeholder {
  color: var(--text-muted);
  font-size: 12px;
  font-style: italic;
}

.checkbox-group {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  font-weight: normal;
  cursor: pointer;
  color: var(--text-primary);
}

.col-name {
  font-weight: 500;
}

.col-type {
  color: var(--text-muted);
  font-size: 11px;
}

.col-role {
  margin-left: auto;
  padding: 1px 6px;
  border-radius: 8px;
  font-size: 10px;
  font-weight: 600;
}

.is-index .col-role {
  background: var(--accent-light);
  color: var(--accent-color);
}

.is-pivot .col-role {
  background: rgba(245, 124, 0, 0.15);
  color: #f57c00;
}

.is-value .col-role {
  background: rgba(56, 142, 60, 0.15);
  color: #388e3c;
}

.listbox li {
  display: flex;
  align-items: center;
  gap: 4px;
}

/* Override select styling */
.select-sm {
  background: var(--bg-tertiary);
  color: var(--text-primary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  padding: 6px 8px;
  font-size: 13px;
}

.select-sm:focus {
  outline: none;
  border-color: var(--accent-color);
}
</style>
