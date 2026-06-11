<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Write to Catalog</div>

    <div v-if="!hasInputConnection" class="no-columns">
      No input connected. Connect an input node first.
    </div>

    <div v-else class="output-settings">
      <div class="form-group">
        <label class="filter-label">Catalog table name</label>
        <input
          type="text"
          :value="datasetName"
          list="catalog-table-names"
          @input="updateDatasetName(($event.target as HTMLInputElement).value)"
          class="input"
          placeholder="my_table"
        />
        <datalist id="catalog-table-names">
          <option v-for="name in existingNames" :key="name" :value="name" />
        </datalist>
        <span class="hint">
          The flow result is saved to the Catalog under this name when you run.
        </span>
      </div>

      <div v-if="willOverwrite" class="warning-message">
        A catalog table named "{{ trimmedName }}" already exists — it will be overwritten on run.
      </div>

      <!-- Schema preview -->
      <div v-if="schemaFields.length > 0" class="schema-preview">
        <label class="filter-label">Schema ({{ schemaFields.length }} columns)</label>
        <div class="schema-list">
          <div v-for="col in schemaFields" :key="col.name" class="schema-item">
            <span class="col-name">{{ col.name }}</span>
            <span class="col-type">{{ col.data_type }}</span>
          </div>
        </div>
      </div>

      <div v-if="resultReady" class="result-ready">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
          <polyline points="22 4 12 14.01 9 11.01"></polyline>
        </svg>
        <span>Saved to Catalog as "{{ trimmedName }}"</span>
      </div>
      <div v-else class="result-pending">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"></circle>
          <polyline points="12 6 12 12 16 14"></polyline>
        </svg>
        <span>{{ trimmedName ? 'Run the flow to write to the Catalog' : 'Name the table, then run the flow' }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeWriteToCatalogSettings, NodeSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeSettings): void
}>()

const flowStore = useFlowStore()

const datasetName = ref((props.settings as NodeWriteToCatalogSettings).dataset_name || '')

const trimmedName = computed(() => datasetName.value.trim())

const existingNames = computed(() => flowStore.getCatalogDatasetNames())

const willOverwrite = computed(
  () => !!trimmedName.value && existingNames.value.includes(trimmedName.value)
)

const hasInputConnection = computed(() => {
  const node = flowStore.getNode(props.nodeId)
  if (!node) return false
  return node.inputIds.length > 0 || node.leftInputId !== undefined
})

const schemaFields = computed(() => {
  const result = flowStore.getNodeResult(props.nodeId)
  return result?.schema ?? []
})

const resultReady = computed(() => {
  const result = flowStore.getNodeResult(props.nodeId)
  return result?.success === true
})

watch(() => props.settings, (s) => {
  datasetName.value = (s as NodeWriteToCatalogSettings).dataset_name || ''
}, { deep: true })

function updateDatasetName(value: string) {
  datasetName.value = value
  const updated: NodeWriteToCatalogSettings = {
    ...(props.settings as NodeWriteToCatalogSettings),
    dataset_name: value,
    is_setup: !!value.trim()
  }
  emit('update:settings', updated)
}
</script>

<style scoped>
.output-settings {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3, 12px);
  padding: var(--spacing-2, 8px) 0;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.hint {
  font-size: 11px;
  color: var(--color-text-secondary, #4a5568);
}

.warning-message {
  color: #f57c00;
  font-size: 12px;
  padding: 8px;
  background: rgba(255, 152, 0, 0.1);
  border-radius: var(--radius-sm, 4px);
}

.schema-preview label {
  font-size: 13px;
  font-weight: 500;
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

.result-ready {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  color: var(--color-success, #48bb78);
  font-size: var(--font-size-sm, 12px);
  padding: var(--spacing-3, 12px);
  background: var(--color-background-tertiary, #f7f7f8);
  border-radius: var(--radius-md, 6px);
}

.result-pending {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  color: var(--color-text-secondary, #4a5568);
  font-size: var(--font-size-sm, 12px);
  padding: var(--spacing-3, 12px);
  background: var(--color-background-tertiary, #f7f7f8);
  border-radius: var(--radius-md, 6px);
}
</style>
