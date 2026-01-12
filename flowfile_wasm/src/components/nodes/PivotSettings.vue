<template>
  <div class="listbox-wrapper">
    <!-- Column List -->
    <ul class="listbox">
      <li
        v-for="col in columns"
        :key="col.name"
        :class="{ 'is-selected': isColumnAssigned(col.name) }"
        draggable="true"
        @click="handleItemClick(col.name)"
        @contextmenu.prevent="openContextMenu(col.name, $event)"
        @dragstart="onDragStart(col.name, $event)"
      >
        {{ col.name }} ({{ col.data_type }})
      </li>
    </ul>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <!-- Context Menu -->
    <ContextMenu
      v-if="showContextMenu"
      :position="contextMenuPosition"
      :options="contextMenuOptions"
      @select="handleContextMenuSelect"
      @close="closeContextMenu"
    />

    <!-- Index Keys Section -->
    <SettingsSection
      title="Index Keys"
      :items="indexColumns"
      :droppable="true"
      placeholder="Drag columns here or right-click to add"
      @remove-item="removeColumn('index', $event)"
      @drop="onDropInSection('index')"
    />

    <!-- Pivot Column Section -->
    <SettingsSection
      title="Pivot Column"
      :items="pivotColumn ? [pivotColumn] : []"
      :droppable="true"
      placeholder="Drag a column here (values become new column names)"
      @remove-item="removeColumn('pivot', $event)"
      @drop="onDropInSection('pivot')"
    />

    <!-- Value Column Section -->
    <SettingsSection
      title="Value Column"
      :items="valueColumn ? [valueColumn] : []"
      :droppable="true"
      placeholder="Drag a column here (values to aggregate)"
      @remove-item="removeColumn('value', $event)"
      @drop="onDropInSection('value')"
    />

    <!-- Aggregations Section -->
    <div class="listbox-wrapper">
      <div class="listbox-subtitle">Select aggregations</div>
      <div class="aggregations-container">
        <label
          v-for="agg in availableAggregations"
          :key="agg"
          class="checkbox-label"
        >
          <input
            type="checkbox"
            :checked="aggregations.includes(agg)"
            @change="toggleAggregation(agg)"
          />
          {{ agg }}
        </label>
      </div>
    </div>

    <!-- Validation -->
    <div v-if="!isConfigured" class="validation-message">
      <span class="validation-icon">!</span>
      <span>Please select a pivot column, value column, and at least one aggregation</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { PivotSettings, ColumnSchema } from '../../types'
import ContextMenu from '../common/ContextMenu.vue'
import type { ContextMenuOption } from '../common/ContextMenu.vue'
import SettingsSection from '../common/SettingsSection.vue'

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

// Context menu state
const showContextMenu = ref(false)
const contextMenuPosition = ref({ x: 0, y: 0 })
const selectedColumn = ref<string>('')
const contextMenuOptions = ref<ContextMenuOption[]>([])

// Drag and drop state
const draggedColumnName = ref<string | null>(null)

const availableAggregations = ['sum', 'count', 'min', 'max', 'n_unique', 'mean', 'median', 'first', 'last', 'concat']

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

const isConfigured = computed(() => {
  return pivotColumn.value !== '' && valueColumn.value !== '' && aggregations.value.length > 0
})

function isColumnAssigned(name: string): boolean {
  return indexColumns.value.includes(name) ||
    pivotColumn.value === name ||
    valueColumn.value === name
}

function handleItemClick(columnName: string) {
  selectedColumn.value = columnName
}

function openContextMenu(columnName: string, event: MouseEvent) {
  selectedColumn.value = columnName
  contextMenuPosition.value = { x: event.clientX, y: event.clientY }

  contextMenuOptions.value = [
    {
      label: 'Add to Index',
      action: 'index',
      disabled: isColumnAssigned(columnName)
    },
    {
      label: 'Set as Pivot',
      action: 'pivot',
      disabled: isColumnAssigned(columnName)
    },
    {
      label: 'Set as Value',
      action: 'value',
      disabled: isColumnAssigned(columnName)
    }
  ]

  showContextMenu.value = true
}

function handleContextMenuSelect(action: string) {
  const column = selectedColumn.value
  if (action === 'index' && !indexColumns.value.includes(column)) {
    removeColumnIfExists(column)
    indexColumns.value.push(column)
    emitUpdate()
  } else if (action === 'pivot') {
    removeColumnIfExists(column)
    pivotColumn.value = column
    emitUpdate()
  } else if (action === 'value') {
    removeColumnIfExists(column)
    valueColumn.value = column
    emitUpdate()
  }
  closeContextMenu()
}

function closeContextMenu() {
  showContextMenu.value = false
}

function removeColumnIfExists(column: string) {
  indexColumns.value = indexColumns.value.filter(col => col !== column)
  if (pivotColumn.value === column) pivotColumn.value = ''
  if (valueColumn.value === column) valueColumn.value = ''
}

function removeColumn(type: 'index' | 'pivot' | 'value', column: string) {
  if (type === 'index') {
    indexColumns.value = indexColumns.value.filter(col => col !== column)
  } else if (type === 'pivot') {
    pivotColumn.value = ''
  } else if (type === 'value') {
    valueColumn.value = ''
  }
  emitUpdate()
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

// Drag and drop handlers
function onDragStart(columnName: string, event: DragEvent) {
  draggedColumnName.value = columnName
  event.dataTransfer?.setData('text/plain', columnName)
}

function onDropInSection(section: 'index' | 'pivot' | 'value') {
  if (draggedColumnName.value) {
    removeColumnIfExists(draggedColumnName.value)

    if (section === 'index' && !indexColumns.value.includes(draggedColumnName.value)) {
      indexColumns.value.push(draggedColumnName.value)
    } else if (section === 'pivot') {
      pivotColumn.value = draggedColumnName.value
    } else if (section === 'value') {
      valueColumn.value = draggedColumnName.value
    }

    draggedColumnName.value = null
    emitUpdate()
  }
}

function emitUpdate() {
  const settings: PivotSettings = {
    ...props.settings,
    is_setup: isConfigured.value,
    pivot_input: {
      index_columns: [...indexColumns.value],
      pivot_column: pivotColumn.value,
      value_col: valueColumn.value,
      aggregations: [...aggregations.value]
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
.aggregations-container {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
  cursor: pointer;
  color: var(--color-text-primary);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--radius-sm);
  transition: background-color var(--transition-fast);
}

.checkbox-label:hover {
  background-color: var(--color-background-tertiary);
}

.checkbox-label input[type="checkbox"] {
  width: 14px;
  height: 14px;
  accent-color: var(--color-accent);
}

.validation-message {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  margin-top: var(--spacing-2);
  background-color: var(--color-warning-light);
  border: 1px solid var(--color-warning);
  border-radius: var(--radius-sm);
  font-size: var(--font-size-sm);
  color: var(--color-warning-darker);
}

.validation-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  background-color: var(--color-warning);
  color: white;
  border-radius: 50%;
  font-weight: bold;
  font-size: var(--font-size-xs);
}

.listbox li {
  cursor: grab;
}

.listbox li:active {
  cursor: grabbing;
}
</style>
