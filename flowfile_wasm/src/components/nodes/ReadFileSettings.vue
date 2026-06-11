<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Data File</label>
      <div class="file-input-wrapper">
        <input
          type="file"
          accept=".csv,.txt,.xlsx,.xlsm,.parquet,.pq"
          @change="handleFileSelect"
          class="file-input"
        />
        <span v-if="fileName" class="file-name">{{ fileName }}</span>
        <span v-else class="file-placeholder">Choose a CSV, Excel or Parquet file...</span>
      </div>
    </div>

    <div class="form-group">
      <label>Or load from a URL</label>
      <div class="url-row">
        <input
          v-model="urlInput"
          type="text"
          class="input"
          placeholder="https://example.com/data.parquet"
          :disabled="urlLoading"
          @keydown.enter.prevent="loadFromUrl()"
        />
        <button class="url-btn" :disabled="urlLoading || !urlInput.trim()" @click="loadFromUrl()">
          {{ urlLoading ? 'Loading…' : 'Load' }}
        </button>
      </div>
      <button
        v-if="sourceUrl && !urlLoading"
        class="url-refresh"
        @click="loadFromUrl(sourceUrl)"
      >
        ⟳ Refresh from URL
      </button>
    </div>

    <template v-if="fileType === 'excel'">
      <div class="form-group">
        <label>Worksheet</label>
        <select
          :value="excelSettings?.sheet_name ?? ''"
          :disabled="sheetsLoading"
          @change="updateTableSetting('sheet_name', ($event.target as HTMLSelectElement).value || null)"
          class="select"
        >
          <option value="">First sheet</option>
          <option v-for="sheet in sheets" :key="sheet" :value="sheet">{{ sheet }}</option>
        </select>
        <span v-if="sheetsLoading" class="hint">Loading worksheets… (downloads openpyxl on first use)</span>
        <span v-else-if="!pyodideStore.isReady" class="hint">Worksheets will load when the engine is ready</span>
      </div>

      <div class="form-group">
        <label class="checkbox-label">
          <input
            type="checkbox"
            :checked="excelSettings?.has_headers ?? true"
            @change="updateTableSetting('has_headers', ($event.target as HTMLInputElement).checked)"
          />
          <span>Has Headers</span>
        </label>
      </div>
    </template>

    <template v-else-if="fileType === 'parquet'">
      <div class="form-group">
        <span class="hint">Parquet has no options — the schema resolves when the node runs.</span>
      </div>
    </template>

    <template v-else>
      <div class="form-group">
        <label class="checkbox-label">
          <input
            type="checkbox"
            :checked="csvSettings?.has_headers ?? true"
            @change="updateTableSetting('has_headers', ($event.target as HTMLInputElement).checked)"
          />
          <span>Has Headers</span>
        </label>
      </div>

      <div class="form-group">
        <label>Delimiter</label>
        <select
          :value="csvSettings?.delimiter ?? ','"
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
          :value="csvSettings?.starting_from_line ?? 0"
          @change="updateTableSetting('starting_from_line', parseInt(($event.target as HTMLInputElement).value) || 0)"
          min="0"
          class="input"
        />
      </div>
    </template>

    <div v-if="fileWarning" class="warning-message">{{ fileWarning }}</div>
    <div v-if="fileError" class="error-message">{{ fileError }}</div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import { usePyodideStore } from '../../stores/pyodide-store'
import { binaryContent, detectFormat } from '../../types/file-content'
import { checkFileSize } from '../../utils/file-size-limits'
import { fetchRemoteFile } from '../../utils/remote-file'
import type { InputCsvTable, InputExcelTable, NodeReadSettings, NodeSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeSettings): void
}>()

const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()
const fileName = ref('')
const fileError = ref('')
const fileWarning = ref('')
const sheets = ref<string[]>([])
const sheetsLoading = ref(false)
const urlInput = ref('')
const urlLoading = ref(false)

/** The node's source URL when the loaded file came from one (drives the refresh button). */
const sourceUrl = computed(() => {
  const path = localSettings.value.received_file?.path ?? ''
  return path.startsWith('http://') || path.startsWith('https://') ? path : ''
})

const localSettings = ref<NodeReadSettings>({
  node_id: props.nodeId,
  cache_results: true,
  pos_x: 0,
  pos_y: 0,
  is_setup: false,
  description: '',
  received_file: {
    name: '',
    path: '',  // Required by flowfile_core
    file_type: 'csv',
    table_settings: {
      file_type: 'csv',
      delimiter: ',',
      has_headers: true,
      encoding: 'utf-8',
      starting_from_line: 0
    }
  },
  file_name: ''
})

const fileType = computed(() => localSettings.value.received_file?.file_type ?? 'csv')
const csvSettings = computed(() =>
  fileType.value === 'csv' ? (localSettings.value.received_file?.table_settings as InputCsvTable) : undefined
)
const excelSettings = computed(() =>
  fileType.value === 'excel' ? (localSettings.value.received_file?.table_settings as InputExcelTable) : undefined
)

onMounted(() => {
  loadSettings(props.settings)
  if (fileType.value === 'excel') loadSheets()
})

function loadSettings(settings: NodeSettings) {
  const s = settings as any

  localSettings.value.node_id = s.node_id ?? props.nodeId
  localSettings.value.is_setup = s.is_setup ?? false
  localSettings.value.description = s.description ?? ''
  localSettings.value.pos_x = s.pos_x ?? 0
  localSettings.value.pos_y = s.pos_y ?? 0
  localSettings.value.cache_results = s.cache_results ?? true

  const receivedFile = s.received_file || s.received_table
  if (receivedFile) {
    localSettings.value.received_file = receivedFile
    localSettings.value.file_name = s.file_name ?? receivedFile.name ?? ''
  }
  else if (s.file_name !== undefined || s.has_headers !== undefined) {
    localSettings.value.file_name = s.file_name ?? ''
    localSettings.value.received_file = {
      name: s.file_name ?? '',
      path: s.file_name ?? '',  // Required by flowfile_core
      file_type: 'csv',
      table_settings: {
        file_type: 'csv',
        delimiter: s.delimiter ?? ',',
        has_headers: s.has_headers ?? true,
        encoding: 'utf-8',
        starting_from_line: s.skip_rows ?? 0
      }
    }
  }

  fileName.value = localSettings.value.file_name ?? ''
}

watch(() => props.settings, (newSettings) => {
  loadSettings(newSettings)
}, { deep: true })

// Worksheets can only load once the engine is up; re-try when it becomes ready.
watch(() => pyodideStore.isReady, (ready) => {
  if (ready && fileType.value === 'excel' && sheets.value.length === 0) loadSheets()
})

async function loadSheets() {
  if (!pyodideStore.isReady) return
  sheetsLoading.value = true
  try {
    sheets.value = await flowStore.listExcelSheets(props.nodeId)
  } catch (err) {
    fileError.value = err instanceof Error ? err.message : String(err)
  } finally {
    sheetsLoading.value = false
  }
}

async function handleFileSelect(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]

  if (!file) return

  fileError.value = ''
  fileWarning.value = ''
  fileName.value = file.name

  const format = detectFormat(file.name)
  if (format === 'arrow-ipc') {
    fileError.value = 'Arrow files are not supported by the Read File node — use CSV, Excel or Parquet'
    input.value = ''
    return
  }
  const sizeCheck = checkFileSize(format, file.size, file.name)
  if (!sizeCheck.ok) {
    fileError.value = sizeCheck.error
    input.value = ''
    return
  }
  if (sizeCheck.warning) fileWarning.value = sizeCheck.warning

  try {
    if (format === 'excel' || format === 'parquet') {
      const content = binaryContent(new Uint8Array(await file.arrayBuffer()), format)
      flowStore.setFileContent(props.nodeId, content)
      applyPickedFile(file.name, format)
      if (format === 'excel') {
        sheets.value = []
        loadSheets()
      }
    } else {
      const content = await file.text()
      flowStore.setFileContent(props.nodeId, content)
      applyPickedFile(file.name, 'csv')
    }
    emitUpdate()
  } catch (err) {
    fileError.value = 'Failed to read file'
    console.error(err)
  }
}

async function loadFromUrl(url?: string) {
  const target = (url ?? urlInput.value).trim()
  if (!target) return

  fileError.value = ''
  fileWarning.value = ''
  urlLoading.value = true
  try {
    const remote = await fetchRemoteFile(target)
    if (remote.warning) fileWarning.value = remote.warning

    flowStore.setFileContent(props.nodeId, remote.content)
    applyPickedFile(remote.fileName, remote.format)
    // Keep the URL as the path so the flow remembers its source (and the
    // desktop app can re-read the same location)
    if (localSettings.value.received_file) {
      localSettings.value.received_file.path = target
    }
    fileName.value = remote.fileName
    if (remote.format === 'excel') {
      sheets.value = []
      loadSheets()
    }
    emitUpdate()
  } catch (err) {
    fileError.value = err instanceof Error ? err.message : String(err)
  } finally {
    urlLoading.value = false
  }
}

function applyPickedFile(name: string, type: 'csv' | 'excel' | 'parquet') {
  localSettings.value.file_name = name
  const rf = localSettings.value.received_file
  if (!rf) return
  rf.name = name
  rf.path = name  // Required by flowfile_core
  if (rf.file_type !== type) {
    rf.file_type = type
    rf.table_settings =
      type === 'excel'
        ? {
            file_type: 'excel',
            sheet_name: null,
            has_headers: true,
            start_row: 0,
            start_column: 0,
            end_row: 0,
            end_column: 0,
            type_inference: true
          }
        : type === 'parquet'
          ? { file_type: 'parquet' }
          : { file_type: 'csv', delimiter: ',', has_headers: true, encoding: 'utf-8', starting_from_line: 0 }
  }
  localSettings.value.is_setup = true
}

function updateTableSetting(key: string, value: any) {
  if (localSettings.value.received_file?.table_settings) {
    (localSettings.value.received_file.table_settings as any)[key] = value
  }
  emitUpdate()

  if (key === 'delimiter' || key === 'has_headers') {
    const content = flowStore.getTextContent(props.nodeId)
    if (content) {
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

.hint {
  font-size: 12px;
  color: var(--text-secondary);
}

.url-row {
  display: flex;
  gap: 8px;
}

.url-row .input {
  flex: 1;
}

.url-btn {
  padding: 8px 14px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
  cursor: pointer;
}

.url-btn:disabled {
  opacity: 0.6;
  cursor: default;
}

.url-refresh {
  align-self: flex-start;
  margin-top: 4px;
  padding: 4px 8px;
  font-size: 12px;
  border: none;
  background: none;
  color: var(--accent-color);
  cursor: pointer;
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
