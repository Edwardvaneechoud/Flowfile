<template>
  <div class="flow-node" :class="{ 'has-error': hasError, 'has-success': hasSuccess }">
    <!-- Status indicator -->
    <div class="status-indicator" :class="statusClass" :title="statusTooltip"></div>

    <!-- Node content -->
    <div class="node-content">
      <div class="node-icon">{{ data.icon }}</div>
      <div class="node-info">
        <div class="node-id">{{ data.id }}</div>
        <div class="node-label">{{ data.label }}</div>
      </div>
    </div>

    <!-- Input handles -->
    <Handle
      v-for="i in data.inputs"
      :key="`input-${i - 1}`"
      :id="`input-${i - 1}`"
      type="target"
      :position="Position.Left"
      :style="getHandleStyle(i - 1, data.inputs, 'input')"
    />

    <!-- Output handles -->
    <Handle
      v-for="i in data.outputs"
      :key="`output-${i - 1}`"
      :id="`output-${i - 1}`"
      type="source"
      :position="Position.Right"
      :style="getHandleStyle(i - 1, data.outputs, 'output')"
    />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { Handle, Position } from '@vue-flow/core'
import type { NodeResult } from '../../types'

interface NodeData {
  id: number
  type: string
  label: string
  icon: string
  inputs: number
  outputs: number
  result?: NodeResult
}

const props = defineProps<{
  data: NodeData
}>()

const hasError = computed(() => props.data.result?.success === false)
const hasSuccess = computed(() => props.data.result?.success === true)

const statusClass = computed(() => {
  if (!props.data.result) return 'unknown'
  return props.data.result.success ? 'success' : 'error'
})

const statusTooltip = computed(() => {
  if (!props.data.result) return 'Not executed'
  if (props.data.result.success) {
    const rows = props.data.result.data?.total_rows
    return rows !== undefined ? `Success: ${rows} rows` : 'Success'
  }
  return `Error: ${props.data.result.error}`
})

function getHandleStyle(index: number, total: number, _type: 'input' | 'output') {
  if (total === 1) {
    return { top: '50%' }
  }

  const spacing = 100 / (total + 1)
  const top = spacing * (index + 1)

  return { top: `${top}%` }
}
</script>

<style scoped>
.flow-node {
  background: var(--bg-secondary);
  border: 2px solid var(--border-color);
  border-radius: var(--radius-md);
  padding: 8px 12px;
  min-width: 120px;
  position: relative;
  transition: border-color 0.2s, box-shadow 0.2s;
}

.flow-node:hover {
  border-color: var(--accent-color);
}

.flow-node.has-error {
  border-color: var(--error-color);
}

.flow-node.has-success {
  border-color: var(--success-color);
}

.status-indicator {
  position: absolute;
  top: -4px;
  right: -4px;
  width: 12px;
  height: 12px;
  border-radius: 50%;
  border: 2px solid var(--bg-secondary);
}

.status-indicator.unknown {
  background: var(--text-muted);
}

.status-indicator.success {
  background: var(--success-color);
}

.status-indicator.error {
  background: var(--error-color);
}

.node-content {
  display: flex;
  align-items: center;
  gap: 8px;
}

.node-icon {
  font-size: 20px;
}

.node-info {
  display: flex;
  flex-direction: column;
}

.node-id {
  font-size: 10px;
  color: var(--text-muted);
}

.node-label {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
}

:deep(.vue-flow__handle) {
  width: 10px;
  height: 10px;
  background: var(--accent-color);
  border: 2px solid var(--bg-secondary);
}

:deep(.vue-flow__handle:hover) {
  background: var(--accent-hover);
}
</style>
