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
              <td class="type-cell" :title="col.data_type">
                <select
                  class="type-select"
                  :class="dataTypeBadgeClass(dataTypeGroup(col.data_type))"
                  :value="dataTypeLabel(col.data_type)"
                  :aria-label="`Data type for ${col.old_name}`"
                  @change="updateDataType(col.old_name, ($event.target as HTMLSelectElement).value)"
                >
                  <option v-for="type in typeOptions(col)" :key="type" :value="type">
                    {{ type }}
                  </option>
                </select>
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

/** Offered cast targets; mirrors the desktop app's list, plus Date. */
const CAST_TYPES = [
  'String', 'Int64', 'Int32', 'Int16', 'Float64', 'Float32', 'Boolean', 'Date', 'Datetime'
]

interface LocalColumn {
  old_name: string
  new_name: string
  keep: boolean
  position: number
  /** The type the column becomes — the incoming one until a cast is picked. */
  data_type: string
  /** The type that arrives; empty when no input schema is available yet. */
  source_data_type: string
  /** The saved data_type_change, used only while the source type is unknown. */
  declared_change: boolean
  is_available: boolean
}

const localColumns = ref<LocalColumn[]>([])

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

const inputTypeByName = computed(
  () => new Map(columns.value.map(col => [col.name, col.data_type]))
)

/**
 * Whether this column asks for a cast. Same rule as the desktop app: the target
 * differs from the type arriving. While the input schema is unknown the saved
 * flag is all we have, so it is carried through untouched.
 */
function typeChanged(col: LocalColumn): boolean {
  if (!col.source_data_type) return col.declared_change
  return dataTypeLabel(col.data_type) !== dataTypeLabel(col.source_data_type)
}

/** The standard targets, plus whatever this column already holds or asks for. */
function typeOptions(col: LocalColumn): string[] {
  const options = [...CAST_TYPES]
  for (const raw of [col.source_data_type, col.data_type]) {
    const label = raw ? dataTypeLabel(raw) : 'unknown'
    if (!options.includes(label)) options.unshift(label)
  }
  return options
}

/** One saved entry as local state, resolving its source type against the schema. */
function toLocalColumn(entry: any, index: number, previous?: LocalColumn): LocalColumn {
  const schemaType = inputTypeByName.value.get(entry.old_name)
  // A local pick wins over the saved entry, exactly as the rename and keep do.
  const changed = previous ? typeChanged(previous) : Boolean(entry.data_type_change)
  const target = (changed && previous ? previous.data_type : entry.data_type) || schemaType || ''
  return {
    old_name: entry.old_name,
    new_name: previous?.new_name ?? entry.new_name,
    keep: previous?.keep ?? entry.keep,
    position: entry.position ?? index,
    // An untouched column follows its source when the upstream type changes.
    data_type: changed ? target : schemaType || target,
    source_data_type: schemaType ?? (changed ? previous?.source_data_type || '' : target),
    declared_change: changed,
    is_available: entry.is_available !== false
  }
}

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
    localColumns.value = props.settings.select_input.map((col: any, idx: number) =>
      toLocalColumn(col, idx)
    )
  }
}
initFromSettings()

watch(() => props.settings.select_input, (newSelectInput) => {
  if (newSelectInput && newSelectInput.length > 0) {
    // Preserve user changes (keep, new_name, data type) while updating availability
    const currentByName = new Map(localColumns.value.map(c => [c.old_name, c]))
    localColumns.value = newSelectInput.map((col: any, idx: number) =>
      toLocalColumn(col, idx, currentByName.get(col.old_name))
    )
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
      source_data_type: col.data_type,
      declared_change: false,
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

function updateDataType(columnName: string, value: string) {
  const index = findColumnIndex(columnName)
  if (index !== -1) {
    const col = localColumns.value[index]
    col.data_type = value
    // With no input schema there is nothing to compare against; a deliberate
    // pick is a change until a schema says otherwise.
    col.declared_change = col.source_data_type ? typeChanged(col) : true
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
      data_type: col.data_type,
      data_type_change: typeChanged(col)
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

/* The picker wears the .badge-* colours, so the column still reads at a glance;
   it deliberately sets no color/background of its own or it would fight them. */
.type-select {
  width: 100%;
  max-width: 100%;
  border: 1px solid transparent;
  border-radius: var(--radius-full);
  padding: 0 var(--spacing-1);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  line-height: 1.6;
  cursor: pointer;
}

.type-select:hover,
.type-select:focus {
  border-color: var(--border-color);
}

/* 22px is unreachable while the cell carries side padding. */
.column-picker th:first-child,
.column-picker td.drag-handle-cell {
  padding-left: 0;
  padding-right: 0;
}
</style>
