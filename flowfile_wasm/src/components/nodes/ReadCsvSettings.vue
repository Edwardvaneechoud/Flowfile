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
        <input type="checkbox" v-model="localSettings.has_headers" @change="emitUpdate" />
        <span>Has Headers</span>
      </label>
    </div>

    <div class="form-group">
      <label>Delimiter</label>
      <select v-model="localSettings.delimiter" @change="emitUpdate" class="select">
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
        v-model.number="localSettings.skip_rows"
        @change="emitUpdate"
        min="0"
        class="input"
      />
    </div>

    <div v-if="fileError" class="error-message">{{ fileError }}</div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { ReadCsvSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: ReadCsvSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: ReadCsvSettings): void
}>()

const flowStore = useFlowStore()
const fileName = ref('')
const fileError = ref('')
const localSettings = ref<ReadCsvSettings>({ ...props.settings })

// Load initial file name from settings
onMounted(() => {
  if (props.settings?.file_name) {
    fileName.value = props.settings.file_name
    console.log('[ReadCsvSettings] Loaded file name from settings:', fileName.value)
  }
})

watch(() => props.settings, (newSettings) => {
  localSettings.value = { ...newSettings }
  // Also update fileName when settings change
  if (newSettings?.file_name) {
    fileName.value = newSettings.file_name
  }
}, { deep: true })

async function handleFileSelect(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]

  if (!file) return

  fileError.value = ''
  fileName.value = file.name

  try {
    const content = await file.text()
    flowStore.setFileContent(props.nodeId, content)
    localSettings.value.file_name = file.name
    localSettings.value.is_setup = true
    emitUpdate()
  } catch (err) {
    fileError.value = 'Failed to read file'
    console.error(err)
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

.error-message {
  color: var(--error-color);
  font-size: 12px;
  padding: 8px;
  background: rgba(244, 67, 54, 0.1);
  border-radius: var(--radius-sm);
}
</style>
