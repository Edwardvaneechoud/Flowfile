<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Count Records</div>

    <div v-if="!hasInputConnection" class="no-columns">
      No input connected. Connect an input node first.
    </div>

    <div v-else class="record-count-settings">
      <p class="hint">
        This node takes no settings. It replaces the incoming table with a single
        row holding the total number of records in a
        <code>number_of_records</code> column.
      </p>

      <div v-if="recordCount !== null" class="result-ready">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path>
          <polyline points="22 4 12 14.01 9 11.01"></polyline>
        </svg>
        <span>{{ recordCount.toLocaleString() }} records</span>
      </div>
      <div v-else class="result-pending">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"></circle>
          <polyline points="12 6 12 12 16 14"></polyline>
        </svg>
        <span>Run the flow to count the records</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeSettings } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeSettings
}>()

const flowStore = useFlowStore()

const hasInputConnection = computed(() => {
  const node = flowStore.getNode(props.nodeId)
  if (!node) return false
  return node.inputIds.length > 0 || node.leftInputId !== undefined
})

// Read the count off the node's preview once it has been materialized. The
// output is a single row with a lone number_of_records column, so the value is
// the first cell of the first row.
const recordCount = computed<number | null>(() => {
  const result = flowStore.getNodeResult(props.nodeId)
  const preview = result?.data
  if (!result?.success || !preview) return null
  const col = preview.columns.indexOf('number_of_records')
  const value = col >= 0 ? preview.data?.[0]?.[col] : undefined
  return typeof value === 'number' ? value : null
})
</script>

<style scoped>
.record-count-settings {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3, 12px);
  padding: var(--spacing-2, 8px) 0;
}

.hint {
  font-size: 12px;
  line-height: 1.5;
  color: var(--color-text-secondary, #4a5568);
  margin: 0;
}

.hint code {
  font-family: var(--font-mono, monospace);
  font-size: 11px;
  background: var(--color-background-tertiary, #f7f7f8);
  padding: 1px 4px;
  border-radius: var(--radius-sm, 4px);
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
