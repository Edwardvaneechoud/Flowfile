<template>
  <div class="manual-input-settings">
    <!-- Controls Section -->
    <div class="controls-section">
      <div class="button-group">
        <button class="btn btn-sm" @click="addColumn">
          <span class="icon">+</span> Add Column
        </button>
        <button class="btn btn-sm" @click="addRow">
          <span class="icon">+</span> Add Row
        </button>
        <button class="btn btn-sm" @click="toggleJsonEditor">
          {{ showJsonEditor ? 'Hide JSON' : 'Edit JSON' }}
        </button>
      </div>
      <div class="table-info">
        <span class="info-badge">{{ columns.length }} columns</span>
        <span class="info-badge">{{ rows.length }} rows</span>
      </div>
    </div>

    <!-- Table Editor -->
    <div class="table-container">
      <table class="data-table">
        <thead>
          <tr>
            <th class="row-number-header">#</th>
            <th v-for="col in columns" :key="col.id" class="column-header-cell">
              <div class="column-header">
                <input
                  v-model="col.name"
                  class="input-header"
                  type="text"
                  :placeholder="`Column ${col.id}`"
                  @blur="saveData"
                />
                <button class="delete-btn" title="Delete column" @click="deleteColumn(col.id)">
                  ×
                </button>
              </div>
              <select v-model="col.dataType" class="type-select" @change="saveData">
                <option value="String">String</option>
                <option value="Int64">Integer</option>
                <option value="Float64">Float</option>
                <option value="Boolean">Boolean</option>
              </select>
            </th>
            <th class="actions-header"></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, rowIndex) in rows" :key="row.id" class="data-row">
            <td class="row-number">{{ rowIndex + 1 }}</td>
            <td v-for="col in columns" :key="col.id" class="data-cell">
              <input
                v-model="row.values[col.id]"
                class="input-cell"
                type="text"
                @blur="saveData"
                @keydown.tab="handleTab($event, rowIndex, col.id)"
              />
            </td>
            <td class="row-actions">
              <button class="delete-btn" title="Delete row" @click="deleteRow(row.id)">
                ×
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- JSON Editor (collapsed by default) -->
    <div v-if="showJsonEditor" class="json-section">
      <div class="json-header">
        <span class="json-title">JSON Editor</span>
        <span class="json-hint">Edit data as JSON array</span>
      </div>
      <textarea
        v-model="jsonInput"
        class="json-textarea"
        placeholder='[{"column1": "value1", "column2": "value2"}]'
      ></textarea>
      <div class="json-actions">
        <button class="btn btn-primary btn-sm" @click="applyJson">
          Apply JSON to Table
        </button>
      </div>
    </div>

    <div v-if="errorMessage" class="error-message">{{ errorMessage }}</div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeSettings, NodeManualInputSettings, RawData, MinimalFieldInfo } from '../../types'

interface Column {
  id: number
  name: string
  dataType: string
}

interface Row {
  id: number
  values: Record<number, string>
}

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeSettings): void
}>()

const flowStore = useFlowStore()

// State
const columns = ref<Column[]>([])
const rows = ref<Row[]>([])
const showJsonEditor = ref(false)
const jsonInput = ref('')
const errorMessage = ref('')

let nextColumnId = 1
let nextRowId = 1

// Initialize with empty table if no data
function initializeEmptyTable() {
  columns.value = [{ id: 1, name: 'Column 1', dataType: 'String' }]
  rows.value = [{ id: 1, values: { 1: '' } }]
  nextColumnId = 2
  nextRowId = 2
}

// Convert table to JSON for display
const tableAsJson = computed(() => {
  return rows.value.map(row => {
    const obj: Record<string, string> = {}
    columns.value.forEach(col => {
      obj[col.name] = row.values[col.id] || ''
    })
    return obj
  })
})

// Load data from settings
function loadData() {
  const settings = props.settings as any
  console.log('[ManualInputSettings] loadData called with:', settings)

  // Try raw_data_format (flowfile_core schema)
  if (settings?.raw_data_format?.columns?.length > 0) {
    const rd = settings.raw_data_format as RawData
    console.log('[ManualInputSettings] Loading from raw_data_format:', rd)

    columns.value = rd.columns.map((f: MinimalFieldInfo, idx: number) => ({
      id: idx + 1,
      name: f.name,
      dataType: f.data_type || 'String'
    }))

    if (rd.data && rd.data.length > 0) {
      rows.value = rd.data.map((rowData, rowIdx) => {
        const values: Record<number, string> = {}
        columns.value.forEach((col, colIdx) => {
          values[col.id] = String(rowData[colIdx] ?? '')
        })
        return { id: rowIdx + 1, values }
      })
      nextRowId = rows.value.length + 1
    } else {
      rows.value = [{ id: 1, values: {} }]
      columns.value.forEach(col => {
        rows.value[0].values[col.id] = ''
      })
      nextRowId = 2
    }

    nextColumnId = columns.value.length + 1
    jsonInput.value = JSON.stringify(tableAsJson.value, null, 2)
    return
  }

  // Try manual_input format (legacy schema)
  if (settings?.manual_input?.data) {
    console.log('[ManualInputSettings] Loading from manual_input')
    const mi = settings.manual_input
    try {
      loadFromCsv(mi.data, mi.has_headers ?? true, mi.delimiter || ',')
      jsonInput.value = JSON.stringify(tableAsJson.value, null, 2)
      return
    } catch (e) {
      console.error('[ManualInputSettings] Failed to parse CSV:', e)
    }
  }

  // Try file contents
  const storedContent = flowStore.fileContents.get(props.nodeId)
  if (storedContent) {
    console.log('[ManualInputSettings] Loading from fileContents')
    try {
      loadFromCsv(storedContent, true, ',')
      jsonInput.value = JSON.stringify(tableAsJson.value, null, 2)
      return
    } catch (e) {
      console.error('[ManualInputSettings] Failed to parse stored content:', e)
    }
  }

  // Initialize empty table
  console.log('[ManualInputSettings] No data found, initializing empty table')
  initializeEmptyTable()
  jsonInput.value = '[]'
}

function loadFromCsv(csvData: string, hasHeaders: boolean, delimiter: string) {
  const lines = csvData.trim().split('\n')
  if (lines.length === 0) {
    initializeEmptyTable()
    return
  }

  const actualDelimiter = delimiter === '\\t' ? '\t' : delimiter

  if (hasHeaders) {
    const headerLine = lines[0]
    const colNames = headerLine.split(actualDelimiter).map(c => c.trim())
    columns.value = colNames.map((name, idx) => ({
      id: idx + 1,
      name: name || `Column ${idx + 1}`,
      dataType: 'String'
    }))

    rows.value = lines.slice(1).map((line, rowIdx) => {
      const values: Record<number, string> = {}
      const cells = line.split(actualDelimiter)
      columns.value.forEach((col, colIdx) => {
        values[col.id] = cells[colIdx]?.trim() ?? ''
      })
      return { id: rowIdx + 1, values }
    })
  } else {
    const firstLine = lines[0].split(actualDelimiter)
    columns.value = firstLine.map((_, idx) => ({
      id: idx + 1,
      name: `Column ${idx + 1}`,
      dataType: 'String'
    }))

    rows.value = lines.map((line, rowIdx) => {
      const values: Record<number, string> = {}
      const cells = line.split(actualDelimiter)
      columns.value.forEach((col, colIdx) => {
        values[col.id] = cells[colIdx]?.trim() ?? ''
      })
      return { id: rowIdx + 1, values }
    })
  }

  nextColumnId = columns.value.length + 1
  nextRowId = rows.value.length + 1

  // Infer data types
  columns.value.forEach(col => {
    col.dataType = inferDataType(col.id)
  })
}

function inferDataType(colId: number): string {
  const values = rows.value.map(r => r.values[colId]).filter(v => v !== '' && v !== null && v !== undefined)
  if (values.length === 0) return 'String'

  const allBooleans = values.every(v => v === 'true' || v === 'false')
  if (allBooleans) return 'Boolean'

  const allIntegers = values.every(v => /^-?\d+$/.test(v))
  if (allIntegers) return 'Int64'

  const allNumbers = values.every(v => !isNaN(parseFloat(v)))
  if (allNumbers) return 'Float64'

  return 'String'
}

// Save data to settings
function saveData() {
  console.log('[ManualInputSettings] saveData called')
  errorMessage.value = ''

  // Build RawData structure matching flowfile_core format
  const columnsData: MinimalFieldInfo[] = columns.value.map(col => ({
    name: col.name,
    data_type: col.dataType
  }))

  // Columnar data format (each inner array is a column's values)
  // This matches flowfile_core's RawData.data format
  const data: any[][] = columns.value.map(col =>
    rows.value.map(row => row.values[col.id] || '')
  )

  const rawData: RawData = { columns: columnsData, data }

  // Also build CSV for legacy execution engine
  const csvData = convertToCsv()

  const newSettings: NodeManualInputSettings = {
    node_id: props.settings.node_id ?? props.nodeId,
    cache_results: (props.settings as any).cache_results ?? true,
    pos_x: (props.settings as any).pos_x ?? 0,
    pos_y: (props.settings as any).pos_y ?? 0,
    is_setup: rows.value.length > 0 && columns.value.length > 0,
    description: (props.settings as any).description ?? '',
    raw_data_format: rawData,
    // Keep legacy format for backward compatibility
    manual_input: {
      data: csvData,
      columns: columns.value.map(c => c.name),
      has_headers: true,
      delimiter: ','
    }
  } as any

  console.log('[ManualInputSettings] Emitting settings:', newSettings)
  emit('update:settings', newSettings)

  // Store CSV for execution
  flowStore.setFileContent(props.nodeId, csvData)

  // Update source node schema for reactive schema propagation
  flowStore.setSourceNodeSchema(props.nodeId, columnsData)

  // Update JSON display
  jsonInput.value = JSON.stringify(tableAsJson.value, null, 2)
}

function convertToCsv(): string {
  const header = columns.value.map(c => escapeCSV(c.name)).join(',')
  const dataRows = rows.value.map(row =>
    columns.value.map(col => escapeCSV(row.values[col.id] || '')).join(',')
  )
  return [header, ...dataRows].join('\n')
}

function escapeCSV(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`
  }
  return value
}

// Table manipulation
function addColumn() {
  const newCol: Column = {
    id: nextColumnId,
    name: `Column ${nextColumnId}`,
    dataType: 'String'
  }
  columns.value.push(newCol)
  rows.value.forEach(row => {
    row.values[newCol.id] = ''
  })
  nextColumnId++
  saveData()
}

function addRow() {
  const newRow: Row = {
    id: nextRowId,
    values: {}
  }
  columns.value.forEach(col => {
    newRow.values[col.id] = ''
  })
  rows.value.push(newRow)
  nextRowId++
  saveData()
}

function deleteColumn(id: number) {
  if (columns.value.length <= 1) {
    errorMessage.value = 'Cannot delete the last column'
    return
  }
  const idx = columns.value.findIndex(c => c.id === id)
  if (idx !== -1) {
    columns.value.splice(idx, 1)
    rows.value.forEach(row => {
      delete row.values[id]
    })
    saveData()
  }
}

function deleteRow(id: number) {
  if (rows.value.length <= 1) {
    errorMessage.value = 'Cannot delete the last row'
    return
  }
  const idx = rows.value.findIndex(r => r.id === id)
  if (idx !== -1) {
    rows.value.splice(idx, 1)
    saveData()
  }
}

function handleTab(event: KeyboardEvent, rowIndex: number, colId: number) {
  const colIndex = columns.value.findIndex(c => c.id === colId)
  // If last cell, add new row
  if (colIndex === columns.value.length - 1 && rowIndex === rows.value.length - 1) {
    event.preventDefault()
    addRow()
  }
}

function toggleJsonEditor() {
  showJsonEditor.value = !showJsonEditor.value
  if (showJsonEditor.value) {
    jsonInput.value = JSON.stringify(tableAsJson.value, null, 2)
  }
}

function applyJson() {
  try {
    const data = JSON.parse(jsonInput.value)
    if (!Array.isArray(data)) {
      errorMessage.value = 'JSON must be an array of objects'
      return
    }

    if (data.length === 0) {
      initializeEmptyTable()
      saveData()
      return
    }

    // Get column names from first object
    const colNames = Object.keys(data[0])
    columns.value = colNames.map((name, idx) => ({
      id: idx + 1,
      name,
      dataType: 'String'
    }))

    rows.value = data.map((item, rowIdx) => {
      const values: Record<number, string> = {}
      colNames.forEach((key, colIdx) => {
        values[colIdx + 1] = String(item[key] ?? '')
      })
      return { id: rowIdx + 1, values }
    })

    nextColumnId = columns.value.length + 1
    nextRowId = rows.value.length + 1

    // Infer types
    columns.value.forEach(col => {
      col.dataType = inferDataType(col.id)
    })

    errorMessage.value = ''
    saveData()
  } catch (e) {
    errorMessage.value = 'Invalid JSON format'
  }
}

// Lifecycle
onMounted(() => {
  loadData()
})

watch(() => props.settings, (newSettings, oldSettings) => {
  // Only reload if settings changed from outside (not from our own emit)
  const newSetup = (newSettings as any)?.is_setup
  const oldSetup = (oldSettings as any)?.is_setup
  if (newSetup !== oldSetup) {
    loadData()
  }
}, { deep: true })
</script>

<style scoped>
.manual-input-settings {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

/* Controls */
.controls-section {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}

.button-group {
  display: flex;
  gap: 6px;
}

.btn {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 6px 10px;
  font-size: 12px;
  font-weight: 500;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-tertiary);
  color: var(--text-primary);
  cursor: pointer;
  transition: all 0.15s;
}

.btn:hover {
  border-color: var(--border-color);
  opacity: 0.8;
}

.btn-primary {
  background: var(--accent-color);
  border-color: var(--accent-color);
  color: white;
}

.btn-primary:hover {
  opacity: 0.9;
}

.btn-sm {
  padding: 4px 8px;
  font-size: 11px;
}

.btn .icon {
  font-size: 14px;
  font-weight: bold;
}

.table-info {
  display: flex;
  gap: 8px;
}

.info-badge {
  font-size: 11px;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  padding: 2px 8px;
  border-radius: var(--radius-sm);
}

/* Table Container */
.table-container {
  max-height: 300px;
  overflow: auto;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

/* Header */
.row-number-header {
  width: 32px;
  min-width: 32px;
  text-align: center;
  font-size: 10px;
  font-weight: 500;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-color);
  border-right: 1px solid var(--border-light);
  position: sticky;
  left: 0;
  top: 0;
  z-index: 2;
}

.column-header-cell {
  min-width: 120px;
  padding: 0;
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-color);
  border-right: 1px solid var(--border-light);
  position: sticky;
  top: 0;
  z-index: 1;
}

.column-header {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 6px 8px 4px;
}

.input-header {
  flex: 1;
  border: none;
  background: transparent;
  font-size: 12px;
  font-weight: 500;
  color: var(--text-primary);
  padding: 2px 4px;
  border-radius: var(--radius-sm);
}

.input-header:focus {
  outline: none;
  background: var(--bg-secondary);
}

.delete-btn {
  width: 18px;
  height: 18px;
  padding: 0;
  border: none;
  background: transparent;
  color: var(--text-secondary);
  cursor: pointer;
  font-size: 14px;
  line-height: 1;
  border-radius: var(--radius-sm);
  opacity: 0.6;
  transition: all 0.15s;
}

.delete-btn:hover {
  opacity: 1;
}

.type-select {
  width: calc(100% - 16px);
  margin: 0 8px 6px;
  padding: 2px 4px;
  font-size: 10px;
  border: 1px solid var(--border-light);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  color: var(--text-secondary);
}

.actions-header {
  width: 28px;
  min-width: 28px;
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-color);
  position: sticky;
  top: 0;
}

/* Data rows */

.row-number {
  width: 32px;
  min-width: 32px;
  text-align: center;
  font-size: 10px;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-light);
  border-right: 1px solid var(--border-light);
  position: sticky;
  left: 0;
}

.data-cell {
  min-width: 120px;
  padding: 0;
  border-bottom: 1px solid var(--border-light);
  border-right: 1px solid var(--border-light);
}

.input-cell {
  width: 100%;
  border: none;
  background: transparent;
  font-size: 12px;
  color: var(--text-primary);
  padding: 6px 8px;
}

.input-cell:focus {
  outline: none;
  background: var(--bg-primary);
}

.row-actions {
  width: 28px;
  min-width: 28px;
  text-align: center;
  border-bottom: 1px solid var(--border-light);
  padding: 2px;
}

.data-row .delete-btn {
  opacity: 0;
}

.data-row:hover .delete-btn {
  opacity: 0.6;
}

/* JSON Editor */
.json-section {
  padding: 12px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  border: 1px solid var(--border-light);
}

.json-header {
  display: flex;
  align-items: baseline;
  gap: 8px;
  margin-bottom: 8px;
}

.json-title {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-primary);
}

.json-hint {
  font-size: 11px;
  color: var(--text-secondary);
}

.json-textarea {
  width: 100%;
  min-height: 120px;
  padding: 8px;
  font-family: monospace;
  font-size: 11px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  color: var(--text-primary);
  resize: vertical;
}

.json-textarea:focus {
  outline: none;
}

.json-actions {
  margin-top: 8px;
  display: flex;
  justify-content: flex-end;
}

/* Error */
.error-message {
  color: var(--error-color);
  background: rgba(244, 67, 54, 0.1);
  padding: 8px 12px;
  border-radius: var(--radius-sm);
  font-size: 12px;
}
</style>
