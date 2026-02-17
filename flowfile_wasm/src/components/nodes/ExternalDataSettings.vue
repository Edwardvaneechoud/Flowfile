<template>
  <div class="settings-form">
    <div class="form-group">
      <label>Dataset</label>
      <select
        :value="selectedDataset"
        @change="handleDatasetChange(($event.target as HTMLSelectElement).value)"
        class="select"
      >
        <option value="">-- Select a dataset --</option>
        <option
          v-for="name in availableDatasets"
          :key="name"
          :value="name"
        >
          {{ name }}
        </option>
      </select>
    </div>

    <div v-if="!availableDatasets.length" class="info-message">
      <p>No external datasets available. The host application must provide datasets via the <code>inputData</code> prop.</p>
    </div>

    <div v-if="selectedDataset && !dataLoaded" class="warning-message">
      Dataset "{{ selectedDataset }}" is configured but no data is currently loaded.
      The host application needs to provide this dataset.
    </div>

    <div v-if="dataLoaded && schemaFields.length > 0" class="schema-preview">
      <label>Schema ({{ schemaFields.length }} columns)</label>
      <div class="schema-list">
        <div v-for="col in schemaFields" :key="col.name" class="schema-item">
          <span class="col-name">{{ col.name }}</span>
          <span class="col-type">{{ col.data_type }}</span>
        </div>
      </div>
    </div>

    <div v-if="rowCount !== null" class="stats">
      <div class="stat">
        <span class="stat-label">Rows</span>
        <span class="stat-value">{{ rowCount }}</span>
      </div>
      <div class="stat">
        <span class="stat-label">Columns</span>
        <span class="stat-value">{{ schemaFields.length }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeExternalDataSettings, NodeSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeSettings): void
}>()

const flowStore = useFlowStore()

const selectedDataset = ref('')

// Available datasets from the store
const availableDatasets = computed(() => flowStore.getExternalDatasetNames())

// Check if data is currently loaded for this node
const dataLoaded = computed(() => flowStore.fileContents.has(props.nodeId))

// Schema from node results
const schemaFields = computed(() => {
  const result = flowStore.getNodeResult(props.nodeId)
  return result?.schema ?? []
})

// Row count from results
const rowCount = computed(() => {
  const result = flowStore.getNodeResult(props.nodeId)
  return result?.data?.total_rows ?? null
})

onMounted(() => {
  const s = props.settings as NodeExternalDataSettings
  selectedDataset.value = s.dataset_name || ''

  // If a dataset is selected and data is available, load it
  if (selectedDataset.value) {
    loadDataset(selectedDataset.value)
  }
})

watch(() => props.settings, (newSettings) => {
  const s = newSettings as NodeExternalDataSettings
  selectedDataset.value = s.dataset_name || ''
}, { deep: true })

// When external datasets change (e.g., host app updates inputData), refresh
watch(() => flowStore.externalDatasets, () => {
  if (selectedDataset.value) {
    loadDataset(selectedDataset.value)
  }
}, { deep: true })

function handleDatasetChange(name: string) {
  selectedDataset.value = name

  const currentSettings = props.settings as NodeExternalDataSettings
  const updated: NodeExternalDataSettings = {
    ...currentSettings,
    dataset_name: name,
    is_setup: !!name
  }
  emit('update:settings', updated)

  if (name) {
    loadDataset(name)
  }
}

function loadDataset(name: string) {
  const content = flowStore.getExternalDatasetContent(name)
  if (content) {
    flowStore.setFileContent(props.nodeId, content)
  }
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

.form-group label,
.schema-preview label {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

.select {
  width: 100%;
  padding: 8px 12px;
  font-size: 13px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-secondary);
}

.select:focus {
  outline: none;
  border-color: var(--accent-color);
}

.info-message {
  padding: 12px;
  font-size: 12px;
  color: var(--color-text-secondary, #4a5568);
  background: var(--color-background-tertiary, #f7f7f8);
  border-radius: var(--radius-sm, 4px);
  border: 1px solid var(--color-border-light, #e2e8f0);
}

.info-message p {
  margin: 0;
}

.info-message code {
  background: var(--color-background-primary, #fff);
  padding: 1px 4px;
  border-radius: 3px;
  font-size: 11px;
}

.warning-message {
  color: #f57c00;
  font-size: 12px;
  padding: 8px;
  background: rgba(255, 152, 0, 0.1);
  border-radius: var(--radius-sm, 4px);
}

.stats {
  display: flex;
  gap: 16px;
}

.stat {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 12px;
  background: var(--color-background-tertiary, #f7f7f8);
  border-radius: var(--border-radius-md, 6px);
  border: 1px solid var(--color-border-light, #e2e8f0);
}

.stat-label {
  font-size: var(--font-size-sm, 11px);
  color: var(--color-text-secondary, #4a5568);
}

.stat-value {
  font-size: var(--font-size-xl, 18px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary, #1a202c);
}

.schema-preview label {
  margin-bottom: 8px;
  display: block;
}

.schema-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--color-border-light, #e2e8f0);
  border-radius: var(--border-radius-sm, 4px);
}

.schema-item {
  display: flex;
  justify-content: space-between;
  padding: 6px 10px;
  background: var(--color-background-primary, #fff);
}

.schema-item:nth-child(even) {
  background: var(--color-background-tertiary, #f7f7f8);
}

.col-name {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-primary, #1a202c);
}

.col-type {
  font-size: 11px;
  color: var(--color-text-secondary, #4a5568);
}
</style>
