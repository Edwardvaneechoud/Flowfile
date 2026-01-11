<template>
  <div class="manual-input-settings">
    <div class="section">
      <label class="section-label">Data Input</label>
      <p class="section-hint">Enter data as CSV or paste from spreadsheet (tab-separated)</p>

      <div class="input-mode">
        <label class="radio-label">
          <input type="radio" v-model="inputMode" value="csv" />
          CSV Format
        </label>
        <label class="radio-label">
          <input type="radio" v-model="inputMode" value="json" />
          JSON Format
        </label>
      </div>

      <textarea
        v-model="dataInput"
        class="data-textarea"
        :placeholder="placeholderText"
        @input="parseData"
      ></textarea>
    </div>

    <div class="section">
      <label class="section-label">Options</label>

      <div class="option-row">
        <label class="checkbox-label">
          <input type="checkbox" v-model="hasHeaders" @change="parseData" />
          First row is header
        </label>
      </div>

      <div class="option-row" v-if="inputMode === 'csv'">
        <label class="option-label">Delimiter</label>
        <select v-model="delimiter" class="select" @change="parseData">
          <option value=",">Comma (,)</option>
          <option value=";">Semicolon (;)</option>
          <option value="\t">Tab</option>
          <option value="|">Pipe (|)</option>
        </select>
      </div>
    </div>

    <div v-if="parseError" class="error-message">
      {{ parseError }}
    </div>

    <div v-if="previewData.length > 0" class="section">
      <label class="section-label">Preview ({{ previewData.length }} rows)</label>
      <div class="preview-table-wrapper">
        <table class="preview-table">
          <thead>
            <tr>
              <th v-for="col in columns" :key="col">{{ col }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, idx) in previewData.slice(0, 5)" :key="idx">
              <td v-for="col in columns" :key="col">{{ row[col] }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="actions">
      <button class="btn btn-primary" @click="applySettings" :disabled="!isValid">
        Apply
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeSettings, NodeManualInputSettings, RawData, MinimalFieldInfo } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeSettings): void
}>()

const flowStore = useFlowStore()

const inputMode = ref<'csv' | 'json'>('csv')
const dataInput = ref('')
const hasHeaders = ref(true)
const delimiter = ref(',')
const parseError = ref('')
const previewData = ref<Record<string, any>[]>([])
const columns = ref<string[]>([])

const isValid = computed(() => {
  return previewData.value.length > 0 && columns.value.length > 0 && !parseError.value
})

const placeholderText = computed(() => {
  if (inputMode.value === 'csv') {
    return 'name,age,city\nAlice,30,NYC\nBob,25,LA'
  }
  return '[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]'
})

function parseData() {
  parseError.value = ''
  previewData.value = []
  columns.value = []

  if (!dataInput.value.trim()) return

  try {
    if (inputMode.value === 'json') {
      const parsed = JSON.parse(dataInput.value)
      if (Array.isArray(parsed) && parsed.length > 0) {
        previewData.value = parsed
        columns.value = Object.keys(parsed[0])
      } else {
        parseError.value = 'JSON must be an array of objects'
      }
    } else {
      // CSV parsing
      const lines = dataInput.value.trim().split('\n')
      if (lines.length === 0) return

      const actualDelimiter = delimiter.value === '\\t' ? '\t' : delimiter.value

      if (hasHeaders.value) {
        columns.value = lines[0].split(actualDelimiter).map(c => c.trim())
        for (let i = 1; i < lines.length; i++) {
          const values = lines[i].split(actualDelimiter)
          const row: Record<string, any> = {}
          columns.value.forEach((col, idx) => {
            row[col] = values[idx]?.trim() ?? ''
          })
          previewData.value.push(row)
        }
      } else {
        // Generate column names
        const firstRow = lines[0].split(actualDelimiter)
        columns.value = firstRow.map((_, idx) => `column_${idx + 1}`)

        for (const line of lines) {
          const values = line.split(actualDelimiter)
          const row: Record<string, any> = {}
          columns.value.forEach((col, idx) => {
            row[col] = values[idx]?.trim() ?? ''
          })
          previewData.value.push(row)
        }
      }
    }
  } catch (e) {
    parseError.value = e instanceof Error ? e.message : 'Failed to parse data'
  }
}

function applySettings() {
  if (!isValid.value) return

  // Convert to CSV format for storage (used by execution)
  const csvData = inputMode.value === 'json'
    ? convertJsonToCsv(previewData.value, columns.value)
    : dataInput.value

  // Build RawData structure matching flowfile_core schema
  const fields: MinimalFieldInfo[] = columns.value.map(col => ({
    name: col,
    data_type: inferDataType(previewData.value, col)
  }))

  // Convert to array of arrays format
  const dataRows: any[][] = previewData.value.map(row =>
    columns.value.map(col => row[col])
  )

  const rawData: RawData = {
    fields,
    data: dataRows
  }

  const newSettings: NodeManualInputSettings = {
    node_id: props.settings.node_id ?? props.nodeId,
    cache_results: (props.settings as any).cache_results ?? true,
    pos_x: (props.settings as any).pos_x ?? 0,
    pos_y: (props.settings as any).pos_y ?? 0,
    is_setup: true,
    description: (props.settings as any).description ?? '',
    raw_data: rawData,
    // Keep legacy format for backwards compatibility
    manual_input: {
      data: csvData,
      columns: columns.value,
      has_headers: hasHeaders.value,
      delimiter: delimiter.value
    }
  } as any

  emit('update:settings', newSettings)

  // Store the data for execution
  flowStore.setFileContent(props.nodeId, csvData)
}

function inferDataType(data: Record<string, any>[], column: string): string {
  // Sample first few non-null values to infer type
  for (const row of data.slice(0, 10)) {
    const val = row[column]
    if (val === null || val === undefined || val === '') continue

    if (typeof val === 'number') {
      return Number.isInteger(val) ? 'Int64' : 'Float64'
    }
    if (typeof val === 'boolean') {
      return 'Boolean'
    }
    // Try to parse as number
    const num = parseFloat(val)
    if (!isNaN(num)) {
      return Number.isInteger(num) ? 'Int64' : 'Float64'
    }
  }
  return 'Utf8'  // Default to string
}

function convertJsonToCsv(data: Record<string, any>[], cols: string[]): string {
  const header = cols.join(',')
  const rows = data.map(row => cols.map(col => {
    const val = row[col]
    if (typeof val === 'string' && (val.includes(',') || val.includes('"'))) {
      return `"${val.replace(/"/g, '""')}"`
    }
    return val ?? ''
  }).join(','))
  return [header, ...rows].join('\n')
}

function loadData() {
  const settings = props.settings as any

  // Try new schema first (raw_data)
  if (settings?.raw_data?.fields && settings?.raw_data?.data) {
    const rd = settings.raw_data as RawData
    columns.value = rd.fields.map(f => f.name)
    previewData.value = rd.data.map(row => {
      const obj: Record<string, any> = {}
      columns.value.forEach((col, idx) => {
        obj[col] = row[idx]
      })
      return obj
    })
    // Reconstruct CSV for editing
    dataInput.value = convertJsonToCsv(previewData.value, columns.value)
    console.log('[ManualInputSettings] Loaded from raw_data schema')
  }
  // Fallback to legacy schema (manual_input)
  else if (settings?.manual_input) {
    const mi = settings.manual_input
    dataInput.value = mi.data || ''
    hasHeaders.value = mi.has_headers ?? true
    delimiter.value = mi.delimiter || ','
    parseData()
    console.log('[ManualInputSettings] Loaded from legacy manual_input schema')
  }
}

onMounted(() => {
  loadData()
})

watch(() => props.settings, () => {
  loadData()
}, { deep: true })
</script>

<style scoped>
.manual-input-settings {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.section {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.section-label {
  font-weight: 500;
  font-size: 13px;
  color: var(--text-primary);
}

.section-hint {
  font-size: 12px;
  color: var(--text-secondary);
  margin: 0;
}

.input-mode {
  display: flex;
  gap: 16px;
}

.radio-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  cursor: pointer;
}

.data-textarea {
  width: 100%;
  min-height: 150px;
  padding: 10px;
  font-family: monospace;
  font-size: 12px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  color: var(--text-primary);
  resize: vertical;
}

.data-textarea:focus {
  outline: none;
  border-color: var(--accent-color);
}

.option-row {
  display: flex;
  align-items: center;
  gap: 12px;
}

.option-label {
  font-size: 13px;
  color: var(--text-secondary);
  min-width: 80px;
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  cursor: pointer;
}

.select {
  flex: 1;
  padding: 6px 10px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  color: var(--text-primary);
}

.error-message {
  color: var(--error-color);
  background: rgba(244, 67, 54, 0.1);
  padding: 8px 12px;
  border-radius: var(--radius-sm);
  font-size: 12px;
}

.preview-table-wrapper {
  max-height: 150px;
  overflow: auto;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
}

.preview-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.preview-table th,
.preview-table td {
  padding: 6px 8px;
  text-align: left;
  border-bottom: 1px solid var(--border-light);
  white-space: nowrap;
}

.preview-table th {
  background: var(--bg-tertiary);
  font-weight: 500;
  position: sticky;
  top: 0;
}

.actions {
  display: flex;
  justify-content: flex-end;
  padding-top: 8px;
  border-top: 1px solid var(--border-light);
}

.btn {
  padding: 8px 16px;
  font-size: 13px;
  font-weight: 500;
  border: none;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.2s;
}

.btn-primary {
  background: var(--accent-color);
  color: white;
}

.btn-primary:hover:not(:disabled) {
  background: var(--accent-hover);
}

.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
</style>
