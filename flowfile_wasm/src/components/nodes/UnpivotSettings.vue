<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Selection Mode</div>

    <div class="mode-toggle">
      <label class="radio-label">
        <input
          type="radio"
          value="column"
          v-model="selectorMode"
          @change="emitUpdate"
        />
        Select columns manually
      </label>
      <label class="radio-label">
        <input
          type="radio"
          value="data_type"
          v-model="selectorMode"
          @change="emitUpdate"
        />
        Select by data type
      </label>
    </div>

    <!-- Data Type Selector -->
    <div v-if="selectorMode === 'data_type'" class="config-section">
      <div class="config-row">
        <label>Data Type to Unpivot</label>
        <select v-model="dataTypeSelector" @change="emitUpdate" class="select-sm">
          <option value="numeric">Numeric (Int, Float)</option>
          <option value="float">Float only</option>
          <option value="string">String</option>
          <option value="date">Date/Time</option>
          <option value="all">All columns</option>
        </select>
        <span class="help-text">All columns of this type will be unpivoted</span>
      </div>
    </div>

    <div class="listbox-subtitle" style="margin-top: 12px;">Available Columns</div>

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
          'is-value': valueColumns.includes(col.name)
        }"
        @contextmenu.prevent="showContextMenu($event, col.name)"
      >
        <span class="col-name">{{ col.name }}</span>
        <span class="col-type">({{ col.data_type }})</span>
        <span v-if="indexColumns.includes(col.name)" class="col-role">Index</span>
        <span v-if="valueColumns.includes(col.name)" class="col-role">Unpivot</span>
      </li>
    </ul>

    <div class="listbox-subtitle" style="margin-top: 12px;">Unpivot Configuration</div>

    <div class="config-section">
      <div class="config-row">
        <label>Index Columns (kept as identifiers)</label>
        <div class="chip-list">
          <span v-for="col in indexColumns" :key="col" class="chip chip-index">
            {{ col }}
            <button class="chip-remove" @click="removeIndexColumn(col)">&times;</button>
          </span>
          <span v-if="indexColumns.length === 0" class="placeholder">Right-click columns to add as index</span>
        </div>
      </div>

      <div v-if="selectorMode === 'column'" class="config-row">
        <label>Columns to Unpivot</label>
        <div class="chip-list">
          <span v-for="col in valueColumns" :key="col" class="chip chip-value">
            {{ col }}
            <button class="chip-remove" @click="removeValueColumn(col)">&times;</button>
          </span>
          <span v-if="valueColumns.length === 0" class="placeholder">Right-click columns to add to unpivot</span>
        </div>
      </div>

      <div class="output-preview">
        <div class="listbox-subtitle">Output Columns</div>
        <ul class="output-list">
          <li v-for="col in indexColumns" :key="col">{{ col }} (from index)</li>
          <li><strong>variable</strong> - column names</li>
          <li><strong>value</strong> - column values</li>
        </ul>
      </div>
    </div>

    <!-- Context Menu for columns -->
    <div
      v-if="contextMenu.show"
      class="context-menu"
      :style="{ top: contextMenu.y + 'px', left: contextMenu.x + 'px' }"
    >
      <button @click="setAsIndex">Add as Index Column</button>
      <button v-if="selectorMode === 'column'" @click="setAsValue">Add to Unpivot</button>
      <button v-if="indexColumns.includes(contextMenu.column)" @click="removeFromIndex">Remove from Index</button>
      <button v-if="valueColumns.includes(contextMenu.column)" @click="removeFromValue">Remove from Unpivot</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { UnpivotSettings, ColumnSchema, UnpivotDataTypeSelector, UnpivotSelectorMode } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: UnpivotSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: UnpivotSettings): void
}>()

const flowStore = useFlowStore()

// Local state initialized from props
const indexColumns = ref<string[]>(
  props.settings.unpivot_input?.index_columns
    ? [...props.settings.unpivot_input.index_columns]
    : []
)
const valueColumns = ref<string[]>(
  props.settings.unpivot_input?.value_columns
    ? [...props.settings.unpivot_input.value_columns]
    : []
)
const selectorMode = ref<UnpivotSelectorMode>(
  props.settings.unpivot_input?.data_type_selector_mode || 'column'
)
const dataTypeSelector = ref<UnpivotDataTypeSelector | undefined>(
  props.settings.unpivot_input?.data_type_selector
)

const contextMenu = ref({ show: false, x: 0, y: 0, column: '' })

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

function isColumnUsed(name: string): boolean {
  return indexColumns.value.includes(name) || valueColumns.value.includes(name)
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
    // Remove from value columns if there
    const valueIdx = valueColumns.value.indexOf(column)
    if (valueIdx !== -1) valueColumns.value.splice(valueIdx, 1)

    indexColumns.value.push(column)
    emitUpdate()
  }
  hideContextMenu()
}

function setAsValue() {
  const column = contextMenu.value.column
  if (!valueColumns.value.includes(column)) {
    // Remove from index columns if there
    const indexIdx = indexColumns.value.indexOf(column)
    if (indexIdx !== -1) indexColumns.value.splice(indexIdx, 1)

    valueColumns.value.push(column)
    emitUpdate()
  }
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

function removeFromValue() {
  const column = contextMenu.value.column
  const idx = valueColumns.value.indexOf(column)
  if (idx !== -1) {
    valueColumns.value.splice(idx, 1)
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

function removeValueColumn(column: string) {
  const idx = valueColumns.value.indexOf(column)
  if (idx !== -1) {
    valueColumns.value.splice(idx, 1)
    emitUpdate()
  }
}

function emitUpdate() {
  // Determine if configuration is complete
  const hasColumns = selectorMode.value === 'data_type'
    ? !!dataTypeSelector.value
    : valueColumns.value.length > 0

  const settings: UnpivotSettings = {
    ...props.settings,
    is_setup: hasColumns,
    unpivot_input: {
      index_columns: [...indexColumns.value],
      value_columns: selectorMode.value === 'column' ? [...valueColumns.value] : [],
      data_type_selector: selectorMode.value === 'data_type' ? dataTypeSelector.value : undefined,
      data_type_selector_mode: selectorMode.value
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

.mode-toggle {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 12px;
}

.radio-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  cursor: pointer;
}

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
  color: #666;
}

.help-text {
  font-size: 11px;
  color: #999;
}

.chip-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  min-height: 28px;
  padding: 4px;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  background: #fafafa;
}

.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 12px;
}

.chip-index {
  background: #e3f2fd;
  color: #1976d2;
}

.chip-value {
  background: #fff3e0;
  color: #f57c00;
}

.chip-remove {
  background: none;
  border: none;
  cursor: pointer;
  padding: 0 2px;
  font-size: 14px;
  color: #666;
}

.chip-remove:hover {
  color: #f44336;
}

.placeholder {
  color: #999;
  font-size: 12px;
  font-style: italic;
}

.output-preview {
  margin-top: 8px;
  padding: 8px;
  background: #f5f5f5;
  border-radius: 4px;
}

.output-list {
  margin: 0;
  padding: 0 0 0 16px;
  font-size: 12px;
}

.output-list li {
  padding: 2px 0;
}

.col-name {
  font-weight: 500;
}

.col-type {
  color: #888;
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
  background: #e3f2fd;
  color: #1976d2;
}

.is-value .col-role {
  background: #fff3e0;
  color: #f57c00;
}

.listbox li {
  display: flex;
  align-items: center;
  gap: 4px;
}
</style>
