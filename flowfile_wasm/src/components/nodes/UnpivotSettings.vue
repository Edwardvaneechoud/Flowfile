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

    <!-- Value Selector Mode Toggle -->
    <div class="listbox-wrapper">
      <div class="switch-container">
        <span>Value selector</span>
        <div class="toggle-switch">
          <button
            :class="{ active: selectorMode === 'column' }"
            @click="setSelectorMode('column')"
          >
            Column
          </button>
          <button
            :class="{ active: selectorMode === 'data_type' }"
            @click="setSelectorMode('data_type')"
          >
            Data Type
          </button>
        </div>
      </div>

      <!-- Columns to unpivot (when in column mode) -->
      <SettingsSection
        v-if="selectorMode === 'column'"
        title="Columns to unpivot"
        :items="valueColumns"
        :droppable="true"
        placeholder="Drag columns here or right-click to add"
        @remove-item="removeColumn('value', $event)"
        @drop="onDropInSection('value')"
      />

      <!-- Data type selector (when in data_type mode) -->
      <div v-else class="listbox-wrapper">
        <div class="listbox-subtitle">Dynamic data type selector</div>
        <select v-model="dataTypeSelector" @change="emitUpdate" class="select">
          <option value="">Select data type...</option>
          <option v-for="item in dataTypeSelectorOptions" :key="item" :value="item">
            {{ item }}
          </option>
        </select>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { UnpivotSettings, ColumnSchema, UnpivotDataTypeSelector, UnpivotSelectorMode } from '../../types'
import ContextMenu from '../common/ContextMenu.vue'
import type { ContextMenuOption } from '../common/ContextMenu.vue'
import SettingsSection from '../common/SettingsSection.vue'

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

// Context menu state
const showContextMenu = ref(false)
const contextMenuPosition = ref({ x: 0, y: 0 })
const selectedColumn = ref<string>('')
const contextMenuOptions = ref<ContextMenuOption[]>([])

// Drag and drop state
const draggedColumnName = ref<string | null>(null)

const dataTypeSelectorOptions: UnpivotDataTypeSelector[] = ['all', 'numeric', 'string', 'date', 'float']

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

function isColumnAssigned(name: string): boolean {
  return indexColumns.value.includes(name) || valueColumns.value.includes(name)
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
      label: 'Add to Value',
      action: 'value',
      disabled: isColumnAssigned(columnName) || selectorMode.value !== 'column'
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
  } else if (action === 'value' && !valueColumns.value.includes(column)) {
    removeColumnIfExists(column)
    valueColumns.value.push(column)
    emitUpdate()
  }
  closeContextMenu()
}

function closeContextMenu() {
  showContextMenu.value = false
}

function removeColumnIfExists(column: string) {
  indexColumns.value = indexColumns.value.filter(col => col !== column)
  valueColumns.value = valueColumns.value.filter(col => col !== column)
}

function removeColumn(type: 'index' | 'value', column: string) {
  if (type === 'index') {
    indexColumns.value = indexColumns.value.filter(col => col !== column)
  } else if (type === 'value') {
    valueColumns.value = valueColumns.value.filter(col => col !== column)
  }
  emitUpdate()
}

function setSelectorMode(mode: UnpivotSelectorMode) {
  selectorMode.value = mode
  emitUpdate()
}

// Drag and drop handlers
function onDragStart(columnName: string, event: DragEvent) {
  draggedColumnName.value = columnName
  event.dataTransfer?.setData('text/plain', columnName)
}

function onDropInSection(section: 'index' | 'value') {
  if (draggedColumnName.value) {
    removeColumnIfExists(draggedColumnName.value)

    if (section === 'index' && !indexColumns.value.includes(draggedColumnName.value)) {
      indexColumns.value.push(draggedColumnName.value)
    } else if (section === 'value' && !valueColumns.value.includes(draggedColumnName.value)) {
      valueColumns.value.push(draggedColumnName.value)
    }

    draggedColumnName.value = null
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
</script>

<style scoped>
.switch-container {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  margin: var(--spacing-3) 0;
}

.switch-container span {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  font-weight: var(--font-weight-medium);
}

.toggle-switch {
  display: flex;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--radius-sm);
  overflow: hidden;
}

.toggle-switch button {
  padding: var(--spacing-1) var(--spacing-3);
  border: none;
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.toggle-switch button:not(:last-child) {
  border-right: 1px solid var(--color-border-primary);
}

.toggle-switch button.active {
  background: var(--color-accent);
  color: var(--color-text-inverse);
}

.toggle-switch button:hover:not(.active) {
  background: var(--color-background-tertiary);
}

.select {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  font-size: var(--font-size-sm);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--radius-sm);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  cursor: pointer;
}

.select:focus {
  outline: none;
  border-color: var(--color-accent);
}

.listbox li {
  cursor: grab;
}

.listbox li:active {
  cursor: grabbing;
}
</style>
