<template>
  <div class="node-settings-wrapper">
    <slot></slot>

    <!-- Both gates: the host offers the capability, the user asked to learn. -->
    <NodeExplainer v-if="teachingMode && learning.enabled" :node-id="nodeId" />

    <div class="general-settings-section">
      <button type="button" class="section-header" :aria-expanded="isExpanded" @click="isExpanded = !isExpanded">
        <svg class="section-chevron" :class="{ open: isExpanded }" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="9 18 15 12 9 6"></polyline></svg>
        <span class="section-title">General settings</span>
      </button>

      <div v-show="isExpanded" class="section-content">
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
import { useLearningStore } from '../../stores/learning-store'
import NodeExplainer from './NodeExplainer.vue'
import type { NodeBase } from '../../types'

const props = withDefaults(
  defineProps<{
    nodeId: number
    settings: NodeBase
    /** Show the plain-Python "how would I write this myself?" panel. */
    teachingMode?: boolean
  }>(),
  { teachingMode: true }
)

const flowStore = useFlowStore()
const learning = useLearningStore()

const isExpanded = ref(false)
const localReference = ref('')
const referenceError = ref<string | null>(null)

const defaultReference = computed(() => `df_${props.nodeId}`)

onMounted(() => {
  const node = flowStore.nodes.get(props.nodeId)
  localReference.value = node?.node_reference || props.settings?.node_reference || ''
})

watch(() => props.settings?.node_reference, (newRef) => {
  const node = flowStore.nodes.get(props.nodeId)
  localReference.value = node?.node_reference || newRef || ''
})

function handleReferenceInput(value: string) {
  localReference.value = value

  const result = flowStore.validateNodeReference(props.nodeId, value)
  referenceError.value = result.error
}

function handleReferenceBlur() {
  const value = localReference.value.trim()

  const result = flowStore.validateNodeReference(props.nodeId, value)
  if (!result.valid) {
    referenceError.value = result.error
    return
  }

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
}

.section-header {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%;
  height: 32px;
  padding: 0 var(--spacing-4, 16px);
  border: none;
  border-radius: var(--radius-sm, 4px);
  background: var(--color-background-muted, #fafafa);
  color: var(--color-text-primary, #333);
  cursor: pointer;
  user-select: none;
  text-align: left;
}

.section-header:hover {
  background: var(--color-background-secondary, #f0f2f5);
}

.section-header:focus {
  outline: none;
}

.section-header:focus-visible {
  outline: 2px solid var(--color-accent, #4a90d9);
  outline-offset: 1px;
}

.section-title {
  font-size: 12px;
  font-weight: 500;
}

.section-chevron {
  flex: 0 0 auto;
  color: var(--color-text-secondary, #888);
  transition: transform 0.15s ease;
}

.section-chevron.open {
  transform: rotate(90deg);
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
