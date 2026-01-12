<template>
  <div class="settings-form">
    <!-- File Name -->
    <div class="form-group">
      <label>File Name</label>
      <input
        type="text"
        :value="outputSettings.name"
        @input="updateSetting('name', ($event.target as HTMLInputElement).value)"
        placeholder="output"
        class="input"
      />
    </div>

    <!-- File Type -->
    <div class="form-group">
      <label>File Type</label>
      <select
        :value="outputSettings.file_type"
        @change="updateFileType(($event.target as HTMLSelectElement).value as 'csv' | 'excel' | 'parquet')"
        class="select"
      >
        <option value="csv">CSV</option>
        <option value="excel">Excel</option>
        <option value="parquet">Parquet</option>
      </select>
    </div>

    <!-- Write Mode -->
    <div class="form-group">
      <label>Write Mode</label>
      <select
        :value="outputSettings.write_mode"
        @change="updateSetting('write_mode', ($event.target as HTMLSelectElement).value)"
        class="select"
      >
        <option value="overwrite">Overwrite</option>
        <option value="create">Create New</option>
        <option value="append" v-if="outputSettings.file_type === 'csv'">Append (CSV only)</option>
      </select>
    </div>

    <!-- Format-Specific Settings -->
    <div v-if="outputSettings.file_type === 'csv'" class="format-settings">
      <div class="form-group">
        <label>Delimiter</label>
        <select
          :value="(outputSettings.table_settings as OutputCsvTable).delimiter"
          @change="updateTableSetting('delimiter', ($event.target as HTMLSelectElement).value)"
          class="select"
        >
          <option value=",">Comma (,)</option>
          <option value=";">Semicolon (;)</option>
          <option value="\t">Tab</option>
          <option value="|">Pipe (|)</option>
        </select>
      </div>

      <div class="form-group">
        <label>Encoding</label>
        <select
          :value="(outputSettings.table_settings as OutputCsvTable).encoding"
          @change="updateTableSetting('encoding', ($event.target as HTMLSelectElement).value)"
          class="select"
        >
          <option value="utf-8">UTF-8</option>
          <option value="utf-16">UTF-16</option>
          <option value="latin1">Latin-1</option>
          <option value="ascii">ASCII</option>
        </select>
      </div>
    </div>

    <div v-if="outputSettings.file_type === 'excel'" class="format-settings">
      <div class="form-group">
        <label>Sheet Name</label>
        <input
          type="text"
          :value="(outputSettings.table_settings as OutputExcelTable).sheet_name"
          @input="updateTableSetting('sheet_name', ($event.target as HTMLInputElement).value)"
          placeholder="Sheet1"
          class="input"
        />
      </div>
    </div>

    <!-- Column Selection (Optional) -->
    <div v-if="inputSchema && inputSchema.length > 0" class="form-group">
      <label class="checkbox-label">
        <input
          type="checkbox"
          :checked="selectSpecificColumns"
          @change="toggleColumnSelection"
        />
        <span>Select specific columns</span>
      </label>
    </div>

    <div v-if="selectSpecificColumns && inputSchema" class="column-list">
      <div class="column-list-header">Columns to Write</div>
      <div
        v-for="column in inputSchema"
        :key="column.name"
        class="column-item"
      >
        <label class="checkbox-label">
          <input
            type="checkbox"
            :checked="isColumnSelected(column.name)"
            @change="toggleColumn(column.name)"
          />
          <span>{{ column.name }}</span>
          <span class="column-type">{{ column.data_type }}</span>
        </label>
      </div>
    </div>

    <!-- Download Button -->
    <div v-if="outputReady" class="form-group download-section">
      <button @click="downloadOutput" class="btn btn-primary btn-download">
        📥 Download {{ outputInfo?.file_name }}
      </button>
      <div class="output-info">
        <span>{{ outputInfo?.row_count }} rows, {{ outputInfo?.column_count }} columns</span>
      </div>
    </div>

    <div v-if="!outputReady && props.settings.is_setup" class="info-message">
      Run the flow to generate the output file
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type {
  NodeOutputSettings,
  OutputSettings,
  OutputCsvTable,
  OutputExcelTable,
  OutputParquetTable,
  ColumnSchema
} from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeOutputSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeOutputSettings): void
}>()

const flowStore = useFlowStore()
const selectSpecificColumns = ref(false)

// Get input schema from upstream node
const inputSchema = computed<ColumnSchema[] | undefined>(() => {
  const inputId = props.settings.depending_on_id
  if (!inputId) return undefined
  const result = flowStore.nodeResults.get(inputId)
  return result?.schema
})

// Get output info from node result
const nodeResult = computed(() => {
  return flowStore.nodeResults.get(props.nodeId)
})

const outputReady = computed(() => {
  return nodeResult.value?.success && (nodeResult.value as any)?.output_ready
})

const outputInfo = computed(() => {
  return (nodeResult.value as any)?.output_info
})

// Initialize output settings with defaults
const outputSettings = ref<OutputSettings>(
  props.settings.output_settings || {
    name: 'output',
    file_type: 'csv',
    fields: undefined,
    write_mode: 'overwrite',
    table_settings: {
      file_type: 'csv',
      delimiter: ',',
      encoding: 'utf-8'
    }
  }
)

// Initialize selectSpecificColumns based on fields
if (outputSettings.value.fields && outputSettings.value.fields.length > 0) {
  selectSpecificColumns.value = true
}

function updateSetting(key: keyof OutputSettings, value: any) {
  outputSettings.value = {
    ...outputSettings.value,
    [key]: value
  }
  emitUpdate()
}

function updateFileType(fileType: 'csv' | 'excel' | 'parquet') {
  let table_settings: OutputCsvTable | OutputExcelTable | OutputParquetTable

  if (fileType === 'csv') {
    table_settings = {
      file_type: 'csv',
      delimiter: ',',
      encoding: 'utf-8'
    }
  } else if (fileType === 'excel') {
    table_settings = {
      file_type: 'excel',
      sheet_name: 'Sheet1'
    }
  } else {
    table_settings = {
      file_type: 'parquet'
    }
  }

  outputSettings.value = {
    ...outputSettings.value,
    file_type: fileType,
    table_settings
  }

  // Reset write_mode if append is selected but file type is not CSV
  if (outputSettings.value.write_mode === 'append' && fileType !== 'csv') {
    outputSettings.value.write_mode = 'overwrite'
  }

  emitUpdate()
}

function updateTableSetting(key: string, value: any) {
  outputSettings.value = {
    ...outputSettings.value,
    table_settings: {
      ...outputSettings.value.table_settings,
      [key]: value
    }
  }
  emitUpdate()
}

function toggleColumnSelection() {
  selectSpecificColumns.value = !selectSpecificColumns.value
  if (selectSpecificColumns.value) {
    // Initialize with all columns selected
    outputSettings.value.fields = inputSchema.value?.map(c => c.name) || []
  } else {
    // Clear field selection
    outputSettings.value.fields = undefined
  }
  emitUpdate()
}

function isColumnSelected(columnName: string): boolean {
  if (!outputSettings.value.fields) return false
  return outputSettings.value.fields.includes(columnName)
}

function toggleColumn(columnName: string) {
  if (!outputSettings.value.fields) {
    outputSettings.value.fields = [columnName]
  } else {
    const index = outputSettings.value.fields.indexOf(columnName)
    if (index > -1) {
      outputSettings.value.fields = outputSettings.value.fields.filter(c => c !== columnName)
    } else {
      outputSettings.value.fields = [...outputSettings.value.fields, columnName]
    }
  }
  emitUpdate()
}

function emitUpdate() {
  const updatedSettings: NodeOutputSettings = {
    ...props.settings,
    is_setup: true,
    output_settings: outputSettings.value
  }
  emit('update:settings', updatedSettings)
}

async function downloadOutput() {
  try {
    // Call Python function to get the output file
    const result = await flowStore.pyodideStore.runPythonWithResult(`
get_output_file(${props.nodeId})
    `)

    if (!result) {
      console.error('No output file found for node', props.nodeId)
      return
    }

    const { name, content, mime_type, extension } = result

    // Create blob and download
    let blob: Blob
    if (typeof content === 'string') {
      // CSV content
      blob = new Blob([content], { type: mime_type })
    } else {
      // Binary content (Parquet, Excel) - convert from Python bytes
      const uint8Array = new Uint8Array(content)
      blob = new Blob([uint8Array], { type: mime_type })
    }

    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${name}.${extension}`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  } catch (error) {
    console.error('Error downloading output file:', error)
  }
}

// Watch for changes in props.settings to sync
watch(
  () => props.settings.output_settings,
  (newSettings) => {
    if (newSettings) {
      outputSettings.value = newSettings
    }
  },
  { deep: true }
)
</script>

<style scoped>
.settings-form {
  padding: 16px;
}

.form-group {
  margin-bottom: 16px;
}

.form-group label {
  display: block;
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
  margin-bottom: 6px;
}

.input,
.select {
  width: 100%;
  padding: 8px 12px;
  font-size: 13px;
  border: 1px solid var(--border-medium);
  border-radius: 4px;
  background: var(--bg-secondary);
  color: var(--text-primary);
}

.input:focus,
.select:focus {
  outline: none;
  border-color: var(--primary);
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  font-size: 13px;
  color: var(--text-primary);
}

.checkbox-label input[type='checkbox'] {
  cursor: pointer;
}

.format-settings {
  padding: 12px;
  background: var(--bg-tertiary);
  border-radius: 4px;
  margin-bottom: 16px;
}

.column-list {
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--border-medium);
  border-radius: 4px;
  background: var(--bg-secondary);
}

.column-list-header {
  padding: 8px 12px;
  font-size: 12px;
  font-weight: 600;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--border-medium);
  position: sticky;
  top: 0;
  z-index: 1;
}

.column-item {
  padding: 6px 12px;
  border-bottom: 1px solid var(--border-light);
}

.column-item:last-child {
  border-bottom: none;
}

.column-type {
  margin-left: auto;
  font-size: 11px;
  color: var(--text-tertiary);
  font-family: monospace;
}

.download-section {
  margin-top: 24px;
  padding-top: 16px;
  border-top: 1px solid var(--border-medium);
}

.btn {
  padding: 8px 16px;
  font-size: 13px;
  font-weight: 500;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.15s;
}

.btn-primary {
  background: var(--primary);
  color: white;
}

.btn-primary:hover {
  background: var(--primary-dark);
}

.btn-download {
  width: 100%;
  padding: 12px 16px;
  font-size: 14px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.output-info {
  margin-top: 8px;
  text-align: center;
  font-size: 12px;
  color: var(--text-secondary);
}

.info-message {
  padding: 12px;
  font-size: 13px;
  color: var(--text-secondary);
  background: var(--bg-tertiary);
  border-radius: 4px;
  text-align: center;
}
</style>
