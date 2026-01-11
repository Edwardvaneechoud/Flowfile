<template>
  <div class="settings-form">
    <div class="preview-info">
      <div class="info-icon">👁️</div>
      <div class="info-text">
        <h4>Preview Node</h4>
        <p>This node displays the data from the connected input. No configuration needed.</p>
      </div>
    </div>

    <div v-if="result" class="stats">
      <div class="stat">
        <span class="stat-label">Rows</span>
        <span class="stat-value">{{ result.data?.total_rows ?? 0 }}</span>
      </div>
      <div class="stat">
        <span class="stat-label">Columns</span>
        <span class="stat-value">{{ result.data?.columns?.length ?? 0 }}</span>
      </div>
    </div>

    <div v-if="result?.schema" class="schema-preview">
      <label>Schema</label>
      <div class="schema-list">
        <div v-for="col in result.schema" :key="col.name" class="schema-item">
          <span class="col-name">{{ col.name }}</span>
          <span class="col-type">{{ col.data_type }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { BaseNodeSettings, NodeResult } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: BaseNodeSettings
}>()

const flowStore = useFlowStore()

const result = computed<NodeResult | undefined>(() => {
  return flowStore.getNodeResult(props.nodeId)
})
</script>

<style scoped>
.settings-form {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.preview-info {
  display: flex;
  gap: 16px;
  padding: 16px;
  background: var(--accent-light);
  border-radius: var(--radius-md);
}

.info-icon {
  font-size: 32px;
}

.info-text h4 {
  margin: 0 0 4px 0;
  font-size: 14px;
  font-weight: 600;
}

.info-text p {
  margin: 0;
  font-size: 13px;
  color: var(--text-secondary);
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
  padding: 16px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-md);
}

.stat-label {
  font-size: 12px;
  color: var(--text-secondary);
}

.stat-value {
  font-size: 24px;
  font-weight: 600;
  color: var(--text-primary);
}

.schema-preview label {
  display: block;
  font-size: 13px;
  font-weight: 500;
  margin-bottom: 8px;
}

.schema-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--border-light);
  border-radius: var(--radius-sm);
}

.schema-item {
  display: flex;
  justify-content: space-between;
  padding: 8px 12px;
}

.schema-item:nth-child(even) {
  background: var(--bg-tertiary);
}

.col-name {
  font-size: 13px;
  font-weight: 500;
}

.col-type {
  font-size: 12px;
  color: var(--text-secondary);
}
</style>
