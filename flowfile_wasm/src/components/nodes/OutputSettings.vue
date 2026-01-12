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

      <!-- File Type -->
      <div class="form-group">
        <label class="filter-label">File Type</label>
        <select
          :value="outputSettings.file_type"
          @change="updateFileType(($event.target as HTMLSelectElement).value as OutputFileType)"
          class="select"
        >
          <option v-for="type in fileTypes" :key="type.value" :value="type.value">
            {{ type.label }}
          </option>
        </select>
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

      <!-- Parquet Info -->
      <div v-if="outputSettings.file_type === 'parquet'" class="format-options">
        <div class="help-text">
          Parquet files will be downloaded as binary data with optimal compression.
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
import type { OutputNodeSettings, OutputFileType, OutputCsvTable, OutputSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: OutputNodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: OutputNodeSettings): void
}>()

const flowStore = useFlowStore()

// File type options (CSV and Parquet only)
const fileTypes = [
  { value: 'csv', label: 'CSV (.csv)' },
  { value: 'parquet', label: 'Parquet (.parquet)' }
]

// CSV options
const delimiterOptions = [
  { value: ',', label: 'Comma (,)' },
  { value: ';', label: 'Semicolon (;)' },
  { value: '|', label: 'Pipe (|)' },
  { value: 'tab', label: 'Tab' }
]

const encodingOptions = ['utf-8', 'ISO-8859-1', 'ASCII']

// Initialize output settings from props
const outputSettings = ref<OutputSettings>({
  name: props.settings.output_settings?.name || 'output.csv',
  directory: props.settings.output_settings?.directory || '.',
  file_type: props.settings.output_settings?.file_type || 'csv',
  write_mode: props.settings.output_settings?.write_mode || 'overwrite',
  table_settings: props.settings.output_settings?.table_settings || {
    file_type: 'csv',
    delimiter: ',',
    encoding: 'utf-8'
  }
})

// Computed properties for format-specific settings
const csvSettings = computed(() => {
  if (outputSettings.value.table_settings.file_type === 'csv') {
    return outputSettings.value.table_settings as OutputCsvTable
  }
  return { file_type: 'csv' as const, delimiter: ',', encoding: 'utf-8' }
})

// Check if this node has an input connection
const hasInputConnection = computed(() => {
  const node = flowStore.getNode(props.nodeId)
  if (!node) return false
  return node.inputIds.length > 0 || node.leftInputId !== undefined
})

// Get download info from node result
const downloadInfo = computed(() => {
  const result = flowStore.getNodeResult(props.nodeId)
  return result?.download
})

// Watch for settings changes from parent
watch(() => props.settings.output_settings, (newSettings) => {
  if (newSettings) {
    outputSettings.value = { ...newSettings }
  }
}, { deep: true })

// Update functions
function updateFileName(value: string) {
  outputSettings.value.name = value

  // Auto-detect file type from extension
  const extension = value.split('.').pop()?.toLowerCase()
  if (extension) {
    const extMap: Record<string, OutputFileType> = {
      'csv': 'csv',
      'parquet': 'parquet'
    }
    if (extMap[extension] && extMap[extension] !== outputSettings.value.file_type) {
      updateFileType(extMap[extension])
      return // updateFileType already emits
    }
  }

  emitUpdate()
}

function updateFileType(value: OutputFileType) {
  outputSettings.value.file_type = value

  // Update table_settings based on file type
  switch (value) {
    case 'csv':
      outputSettings.value.table_settings = {
        file_type: 'csv',
        delimiter: ',',
        encoding: 'utf-8'
      }
      break
    case 'parquet':
      outputSettings.value.table_settings = {
        file_type: 'parquet'
      }
      break
  }

  // Update file extension
  const baseName = outputSettings.value.name.split('.')[0]
  const extMap: Record<OutputFileType, string> = {
    'csv': '.csv',
    'parquet': '.parquet'
  }
  outputSettings.value.name = baseName + extMap[value]

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

function triggerDownload() {
  if (!downloadInfo.value) return

  const { content, file_name, file_type, mime_type } = downloadInfo.value

  let blob: Blob

  if (file_type === 'parquet') {
    // Decode base64 content for parquet
    const binaryString = atob(content)
    const bytes = new Uint8Array(binaryString.length)
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i)
    }
    blob = new Blob([bytes], { type: mime_type })
  } else {
    // Text content for CSV
    blob = new Blob([content], { type: mime_type })
  }

  // Use the current file name from settings
  const finalFileName = outputSettings.value.name || file_name

  // Create download link
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
