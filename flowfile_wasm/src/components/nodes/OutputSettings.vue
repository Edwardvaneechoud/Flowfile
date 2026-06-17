<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Output Settings</div>

    <div v-if="!hasInputConnection" class="no-columns">
      No input connected. Connect an input node first.
    </div>

    <div v-else class="output-settings">
      <!-- File Name -->
      <div class="form-group">
        <label class="filter-label">File Name</label>
        <input
          type="text"
          :value="outputSettings.name"
          @input="updateFileName(($event.target as HTMLInputElement).value)"
          class="input"
          placeholder="output.csv"
        />
      </div>

      <!-- Format -->
      <div class="form-group">
        <label class="filter-label">Format</label>
        <select
          :value="outputSettings.file_type"
          @change="updateFormat(($event.target as HTMLSelectElement).value as OutputFileType)"
          class="select"
        >
          <option value="csv">CSV</option>
          <option value="excel">Excel (.xlsx)</option>
          <option value="parquet">Parquet</option>
        </select>
        <span v-if="outputSettings.file_type === 'excel'" class="hint">
          Downloads xlsxwriter from PyPI on first use
        </span>
        <span v-else-if="outputSettings.file_type === 'parquet'" class="hint">
          Downloads the Parquet engine from cdn.jsdelivr.net on first use
        </span>
      </div>

      <!-- CSV Options -->
      <div v-if="outputSettings.file_type === 'csv'" class="format-options">
        <div class="form-group">
          <label class="filter-label">Delimiter</label>
          <select
            :value="csvSettings.delimiter"
            @change="updateDelimiter(($event.target as HTMLSelectElement).value)"
            class="select"
          >
            <option v-for="opt in delimiterOptions" :key="opt.value" :value="opt.value">
              {{ opt.label }}
            </option>
          </select>
        </div>

        <div class="form-group">
          <label class="filter-label">Encoding</label>
          <select
            :value="csvSettings.encoding"
            @change="updateEncoding(($event.target as HTMLSelectElement).value)"
            class="select"
          >
            <option v-for="opt in encodingOptions" :key="opt" :value="opt">
              {{ opt }}
            </option>
          </select>
        </div>
      </div>

      <!-- Excel Options -->
      <div v-else-if="outputSettings.file_type === 'excel'" class="format-options">
        <div class="form-group">
          <label class="filter-label">Sheet Name</label>
          <input
            type="text"
            :value="excelSettings.sheet_name"
            @input="updateSheetName(($event.target as HTMLInputElement).value)"
            class="input"
            placeholder="Sheet1"
          />
        </div>
      </div>

      <!-- Download Section -->
      <div class="download-section">
        <div v-if="downloadInfo" class="download-info">
          <div class="download-ready">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
              <polyline points="22 4 12 14.01 9 11.01"></polyline>
            </svg>
            <span>Ready to download ({{ downloadInfo.row_count.toLocaleString() }} rows)</span>
          </div>
          <button class="btn btn-primary download-btn" @click="triggerDownload">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
              <polyline points="7 10 12 15 17 10"></polyline>
              <line x1="12" y1="15" x2="12" y2="3"></line>
            </svg>
            Download {{ outputSettings.name }}
          </button>
        </div>
        <div v-else class="download-pending">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="12" cy="12" r="10"></circle>
            <polyline points="12 6 12 12 16 14"></polyline>
          </svg>
          <span>Run the flow to prepare data for download</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { OutputNodeSettings, OutputCsvTable, OutputExcelTable, OutputFileType, OutputSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: OutputNodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: OutputNodeSettings): void
}>()

const flowStore = useFlowStore()

const delimiterOptions = [
  { value: ',', label: 'Comma (,)' },
  { value: ';', label: 'Semicolon (;)' },
  { value: '|', label: 'Pipe (|)' },
  { value: 'tab', label: 'Tab' }
]

const encodingOptions = ['utf-8', 'ISO-8859-1', 'ASCII']

const outputSettings = ref<OutputSettings>({
  name: props.settings.output_settings?.name || 'output.csv',
  directory: props.settings.output_settings?.directory || '.',
  file_type: props.settings.output_settings?.file_type || 'csv',
  write_mode: props.settings.output_settings?.write_mode || 'overwrite',
  table_settings: props.settings.output_settings?.table_settings || {
    file_type: 'csv',
    delimiter: ',',
    encoding: 'utf-8'
  },
  polars_method:
    props.settings.output_settings?.polars_method ||
    (props.settings.output_settings?.file_type === 'parquet' ? 'sink_parquet' : 'sink_csv')
})

const csvSettings = computed(() => {
  if (outputSettings.value.table_settings.file_type === 'csv') {
    return outputSettings.value.table_settings as OutputCsvTable
  }
  return { file_type: 'csv' as const, delimiter: ',', encoding: 'utf-8' }
})

const excelSettings = computed(() => {
  if (outputSettings.value.table_settings.file_type === 'excel') {
    return outputSettings.value.table_settings as OutputExcelTable
  }
  return { file_type: 'excel' as const, sheet_name: 'Sheet1' }
})

const EXTENSIONS: Record<OutputFileType, string> = { csv: '.csv', excel: '.xlsx', parquet: '.parquet' }

function enforceExtension(name: string, fileType: OutputFileType): string {
  const ext = EXTENSIONS[fileType]
  if (name.toLowerCase().endsWith(ext)) return name
  const baseName = name.replace(/\.[^.]*$/, '') || 'output'
  return baseName + ext
}

function updateFormat(fileType: OutputFileType) {
  outputSettings.value.file_type = fileType
  outputSettings.value.name = enforceExtension(outputSettings.value.name, fileType)
  if (fileType === 'excel') {
    outputSettings.value.table_settings = { file_type: 'excel', sheet_name: 'Sheet1' }
    outputSettings.value.polars_method = 'sink_csv'  // unused for excel; kept for core-compat shape
  } else if (fileType === 'parquet') {
    outputSettings.value.table_settings = { file_type: 'parquet' }
    outputSettings.value.polars_method = 'sink_parquet'
  } else {
    outputSettings.value.table_settings = { file_type: 'csv', delimiter: ',', encoding: 'utf-8' }
    outputSettings.value.polars_method = 'sink_csv'
  }
  emitUpdate()
}

function updateSheetName(value: string) {
  if (outputSettings.value.table_settings.file_type === 'excel') {
    (outputSettings.value.table_settings as OutputExcelTable).sheet_name = value || 'Sheet1'
    emitUpdate()
  }
}

const hasInputConnection = computed(() => {
  const node = flowStore.getNode(props.nodeId)
  if (!node) return false
  return node.inputIds.length > 0 || node.leftInputId !== undefined
})

const downloadInfo = computed(() => {
  const result = flowStore.getNodeResult(props.nodeId)
  return result?.download
})

watch(() => props.settings.output_settings, (newSettings) => {
  if (newSettings) {
    outputSettings.value = { ...newSettings }
  }
}, { deep: true })

function updateFileName(value: string) {
  outputSettings.value.name = enforceExtension(value, outputSettings.value.file_type)
  emitUpdate()
}

function updateDelimiter(value: string) {
  if (outputSettings.value.table_settings.file_type === 'csv') {
    (outputSettings.value.table_settings as OutputCsvTable).delimiter = value
    emitUpdate()
  }
}

function updateEncoding(value: string) {
  if (outputSettings.value.table_settings.file_type === 'csv') {
    (outputSettings.value.table_settings as OutputCsvTable).encoding = value
    emitUpdate()
  }
}

function emitUpdate() {
  const settings: OutputNodeSettings = {
    ...props.settings,
    is_setup: true,
    output_settings: { ...outputSettings.value }
  }
  emit('update:settings', settings)
}

async function triggerDownload() {
  if (!downloadInfo.value) return

  const downloadEntry = await flowStore.getDownloadContent(props.nodeId)
  if (!downloadEntry) {
    console.error('Download content not found in storage')
    return
  }

  const { content, mimeType, fileName } = downloadEntry

  // Blob accepts both string (CSV) and Uint8Array (xlsx/parquet) directly
  const blob = new Blob([content], { type: mimeType })

  // Use the current file name from settings (user may have changed it)
  const finalFileName = outputSettings.value.name || fileName

  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = finalFileName
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}
</script>

<style scoped>
.output-settings {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-2) 0;
}

.hint {
  font-size: 12px;
  color: var(--text-secondary);
}

.format-options {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-3);
  background: var(--color-background-tertiary);
  border-radius: var(--radius-md);
  margin-top: var(--spacing-1);
}

.download-section {
  margin-top: var(--spacing-4);
  padding: var(--spacing-4);
  background: var(--color-background-tertiary);
  border-radius: var(--radius-md);
}

.download-ready {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  color: var(--color-success);
  font-size: var(--font-size-sm);
  margin-bottom: var(--spacing-3);
}

.download-pending {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.download-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  width: 100%;
}

.download-info {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}
</style>
