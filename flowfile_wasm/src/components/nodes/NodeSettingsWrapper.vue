<template>
  <div class="node-settings-wrapper">
    <!-- Node-specific settings slot -->
    <slot></slot>

    <!-- General Settings Section -->
    <div class="general-settings-section">
      <div class="section-header" @click="isExpanded = !isExpanded">
        <span class="section-title">General Settings</span>
        <span class="expand-icon">{{ isExpanded ? '−' : '+' }}</span>
      </div>

      <div v-show="isExpanded" class="section-content">
        <!-- Node Reference -->
        <div class="setting-group">
          <label class="setting-label">
            Node Reference
            <span class="setting-hint" title="A unique identifier used as the variable name in code generation. Must be lowercase with no spaces.">?</span>
          </label>
          <input
            type="text"
            :value="localReference"
            @input="handleReferenceInput(($event.target as HTMLInputElement).value)"
            @blur="handleReferenceBlur"
            class="input"
            :class="{ 'input-error': referenceError }"
            :placeholder="defaultReference"
          />
          <div v-if="referenceError" class="validation-error">
            {{ referenceError }}
          </div>
          <div v-else class="setting-description">
            Used as variable name in generated code (e.g., {{ localReference || defaultReference }})
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { NodeBase } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: NodeBase
}>()

const flowStore = useFlowStore()

const isExpanded = ref(false)
const localReference = ref('')
const referenceError = ref<string | null>(null)

const defaultReference = computed(() => `df_${props.nodeId}`)

// Initialize from node or settings
onMounted(() => {
  const node = flowStore.nodes.get(props.nodeId)
  localReference.value = node?.node_reference || props.settings?.node_reference || ''
})

// Watch for external changes
watch(() => props.settings?.node_reference, (newRef) => {
  const node = flowStore.nodes.get(props.nodeId)
  localReference.value = node?.node_reference || newRef || ''
})

function handleReferenceInput(value: string) {
  localReference.value = value

  // Validate locally first
  const result = flowStore.validateNodeReference(props.nodeId, value)
  referenceError.value = result.error
}

function handleReferenceBlur() {
  const value = localReference.value.trim()

  // Validate
  const result = flowStore.validateNodeReference(props.nodeId, value)
  if (!result.valid) {
    referenceError.value = result.error
    return
  }

  // Save if valid
  referenceError.value = null
  flowStore.updateNodeReference(props.nodeId, value || undefined)
}
</script>

<style scoped>
.node-settings-wrapper {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.general-settings-section {
  margin-top: 16px;
  border-top: 1px solid var(--border-color, #e0e0e0);
  padding-top: 8px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 0;
  cursor: pointer;
  user-select: none;
}

.section-header:hover {
  opacity: 0.8;
}

.section-title {
  font-size: 12px;
  font-weight: 600;
  color: var(--text-secondary, #666);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.expand-icon {
  font-size: 16px;
  font-weight: bold;
  color: var(--text-secondary, #666);
}

.section-content {
  padding: 8px 0;
}

.setting-group {
  margin-bottom: 12px;
}

.setting-label {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  font-weight: 500;
  color: var(--text-primary, #333);
  margin-bottom: 4px;
}

.setting-hint {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 14px;
  height: 14px;
  border-radius: 50%;
  background: var(--bg-secondary, #f0f0f0);
  color: var(--text-secondary, #666);
  font-size: 10px;
  cursor: help;
}

.setting-description {
  font-size: 11px;
  color: var(--text-secondary, #888);
  margin-top: 4px;
}

.validation-error {
  font-size: 11px;
  color: var(--error-color, #dc3545);
  margin-top: 4px;
}

.input-error {
  border-color: var(--error-color, #dc3545) !important;
}

/* Dark mode support */
@media (prefers-color-scheme: dark) {
  .general-settings-section {
    border-top-color: #444;
  }

  .section-title,
  .expand-icon {
    color: #aaa;
  }

  .setting-label {
    color: #ddd;
  }

  .setting-hint {
    background: #444;
    color: #aaa;
  }

  .setting-description {
    color: #888;
  }
}
</style>
