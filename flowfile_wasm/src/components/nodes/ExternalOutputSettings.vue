<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">External Output</div>

    <div v-if="!hasInputConnection" class="no-columns">
      No input connected. Connect an input node first.
    </div>

    <div v-else class="output-settings">
      <div class="form-group">
        <label class="filter-label">Output Name</label>
        <input
          type="text"
          :value="outputName"
          @input="updateOutputName(($event.target as HTMLInputElement).value)"
          class="input"
          placeholder="result"
        />
        <span class="hint">Identifies this output in the host application's callback.</span>
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
        <span>Output ready — data sent to host callback</span>
      </div>
      <div v-else class="result-pending">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"></circle>
          <polyline points="12 6 12 12 16 14"></polyline>
        </svg>
        <span>Run the flow to produce output</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeExternalOutputSettings, NodeSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeSettings): void
}>()

const flowStore = useFlowStore()

const outputName = ref((props.settings as NodeExternalOutputSettings).output_name || 'result')

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
  outputName.value = (s as NodeExternalOutputSettings).output_name || 'result'
}, { deep: true })

function updateOutputName(value: string) {
  outputName.value = value
  const updated: NodeExternalOutputSettings = {
    ...(props.settings as NodeExternalOutputSettings),
    output_name: value,
    is_setup: true
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
