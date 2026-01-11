<template>
  <div class="settings-form">
    <div class="form-group">
      <label>CSV File</label>
      <div class="file-input-wrapper">
        <input
          type="file"
          accept=".csv,.txt"
          @change="handleFileSelect"
          class="file-input"
        />
        <span v-if="fileName" class="file-name">{{ fileName }}</span>
        <span v-else class="file-placeholder">Choose a CSV file...</span>
      </div>
    </div>

    <div class="form-group">
      <label class="checkbox-label">
        <input
          type="checkbox"
          :checked="tableSettings?.has_headers ?? true"
          @change="updateTableSetting('has_headers', ($event.target as HTMLInputElement).checked)"
        />
        <span>Has Headers</span>
      </label>
    </div>

    <div class="form-group">
      <label>Delimiter</label>
      <select
        :value="tableSettings?.delimiter ?? ','"
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
      <label>Skip Rows</label>
      <input
        type="number"
        :value="tableSettings?.starting_from_line ?? 0"
        @change="updateTableSetting('starting_from_line', parseInt(($event.target as HTMLInputElement).value) || 0)"
        min="0"
        class="input"
      />
    </div>

    <div v-if="fileWarning" class="warning-message">{{ fileWarning }}</div>
    <div v-if="fileError" class="error-message">{{ fileError }}</div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeReadSettings, NodeSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeSettings): void
}>()

const flowStore = useFlowStore()
const fileName = ref('')
const fileError = ref('')
const fileWarning = ref('')

// File size limits (in bytes)
const FILE_SIZE_WARNING_MB = 100
const FILE_SIZE_LIMIT_MB = 200
const FILE_SIZE_WARNING = FILE_SIZE_WARNING_MB * 1024 * 1024  // 100MB
const FILE_SIZE_LIMIT = FILE_SIZE_LIMIT_MB * 1024 * 1024      // 200MB

// Support both new schema (NodeReadSettings) and legacy (ReadCsvSettings)
const localSettings = ref<NodeReadSettings>({
  node_id: props.nodeId,
  cache_results: true,
  pos_x: 0,
  pos_y: 0,
  is_setup: false,
  description: '',
  received_table: {
    name: '',
    file_type: 'csv',
    table_settings: {
      file_type: 'csv',
      delimiter: ',',
      has_headers: true,
      starting_from_line: 0,
      encoding: 'utf-8'
    }
  },
  file_name: ''
})

// Get table settings helper
const tableSettings = computed(() => localSettings.value.received_table?.table_settings)

// Load settings on mount
onMounted(() => {
  loadSettings(props.settings)
})

function loadSettings(settings: NodeSettings) {
  const s = settings as any

  // Copy base properties
  localSettings.value.node_id = s.node_id ?? props.nodeId
  localSettings.value.is_setup = s.is_setup ?? false
  localSettings.value.description = s.description ?? ''
  localSettings.value.pos_x = s.pos_x ?? 0
  localSettings.value.pos_y = s.pos_y ?? 0
  localSettings.value.cache_results = s.cache_results ?? true

  // Handle new schema (received_table)
  if (s.received_table) {
    localSettings.value.received_table = s.received_table
    localSettings.value.file_name = s.file_name ?? s.received_table.name ?? ''
  }
  // Handle legacy schema (direct properties)
  else if (s.file_name !== undefined || s.has_headers !== undefined) {
    localSettings.value.file_name = s.file_name ?? ''
    localSettings.value.received_table = {
      name: s.file_name ?? '',
      file_type: 'csv',
      table_settings: {
        file_type: 'csv',
        delimiter: s.delimiter ?? ',',
        has_headers: s.has_headers ?? true,
        starting_from_line: s.skip_rows ?? 0
      }
    }
  }

  fileName.value = localSettings.value.file_name ?? ''
  console.log('[ReadCsvSettings] Loaded settings:', localSettings.value)
}

watch(() => props.settings, (newSettings) => {
  loadSettings(newSettings)
}, { deep: true })

async function handleFileSelect(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]

  if (!file) return

  fileError.value = ''
  fileWarning.value = ''
  fileName.value = file.name

  // Validate file size
  if (file.size > FILE_SIZE_LIMIT) {
    fileError.value = `File too large (${formatFileSize(file.size)}). Maximum size is ${FILE_SIZE_LIMIT_MB}MB. Large files can freeze your browser.`
    // Reset input so user can select again
    input.value = ''
    return
  }

  if (file.size > FILE_SIZE_WARNING) {
    fileWarning.value = `Large file (${formatFileSize(file.size)}). Files over ${FILE_SIZE_WARNING_MB}MB may cause slow performance.`
  }

  try {
    const content = await file.text()
    flowStore.setFileContent(props.nodeId, content)

    // Update settings
    localSettings.value.file_name = file.name
    if (localSettings.value.received_table) {
      localSettings.value.received_table.name = file.name
    }
    localSettings.value.is_setup = true
    emitUpdate()
  } catch (err) {
    fileError.value = 'Failed to read file'
    console.error(err)
  }
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB'
}

function updateTableSetting(key: string, value: any) {
  if (localSettings.value.received_table?.table_settings) {
    (localSettings.value.received_table.table_settings as any)[key] = value
  }
  emitUpdate()

  // Re-infer schema if delimiter or has_headers changed and we have file content
  if (key === 'delimiter' || key === 'has_headers') {
    const content = flowStore.fileContents.get(props.nodeId)
    if (content) {
      // Re-set the file content to trigger schema re-inference with new settings
      // The settings need to be updated first, so we delay slightly
      setTimeout(() => {
        flowStore.setFileContent(props.nodeId, content)
      }, 0)
    }
  }
}

function emitUpdate() {
  emit('update:settings', { ...localSettings.value })
}
</script>

<style scoped>
.settings-form {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.form-group label {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

.file-input-wrapper {
  position: relative;
  border: 2px dashed var(--border-color);
  border-radius: var(--radius-md);
  padding: 20px;
  text-align: center;
  cursor: pointer;
  transition: border-color 0.2s, background 0.2s;
}

.file-input-wrapper:hover {
  border-color: var(--accent-color);
  background: var(--accent-light);
}

.file-input {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  opacity: 0;
  cursor: pointer;
}

.file-name {
  color: var(--text-primary);
  font-weight: 500;
}

.file-placeholder {
  color: var(--text-secondary);
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
}

.checkbox-label input {
  width: 16px;
  height: 16px;
}

.input, .select {
  width: 100%;
  padding: 8px 12px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
}

.input:focus, .select:focus {
  outline: none;
  border-color: var(--accent-color);
}

.warning-message {
  color: #f57c00;
  font-size: 12px;
  padding: 8px;
  background: rgba(255, 152, 0, 0.1);
  border-radius: var(--radius-sm);
}

.error-message {
  color: var(--error-color);
  font-size: 12px;
  padding: 8px;
  background: rgba(244, 67, 54, 0.1);
  border-radius: var(--radius-sm);
}
</style>
