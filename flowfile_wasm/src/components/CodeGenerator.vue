<template>
  <div v-if="isVisible" class="code-generator-overlay" @click.self="closePanel">
    <div class="code-generator-panel">
      <div class="code-header">
        <h3>Generated Python Code</h3>
        <div class="header-actions">
          <button class="icon-button refresh-button" :disabled="loading" @click="refreshCode" title="Refresh code">
            <svg
              v-if="!loading"
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
            >
              <path d="M23 4v6h-6"></path>
              <path d="M1 20v-6h6"></path>
              <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
            </svg>
            <span v-if="loading" class="spinner"></span>
          </button>
          <button class="icon-button export-button" @click="exportCode" title="Export as .py file">
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
            >
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
              <polyline points="7 10 12 15 17 10"></polyline>
              <line x1="12" y1="15" x2="12" y2="3"></line>
            </svg>
          </button>
          <button class="icon-button close-button" @click="closePanel" title="Close">
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
            >
              <line x1="18" y1="6" x2="6" y2="18"></line>
              <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
          </button>
        </div>
      </div>

      <div v-if="error" class="error-message">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"></circle>
          <line x1="12" y1="8" x2="12" y2="12"></line>
          <line x1="12" y1="16" x2="12.01" y2="16"></line>
        </svg>
        <span>{{ error }}</span>
      </div>

      <div class="code-editor-container">
        <Codemirror
          v-model="code"
          :extensions="extensions"
          :disabled="true"
          :style="{ height: '100%', fontSize: '13px' }"
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView } from '@codemirror/view'
import { useFlowStore } from '../stores/flow-store'
import { useCodeGeneration } from '../composables/useCodeGeneration'

const props = defineProps<{
  isVisible: boolean
}>()

const emit = defineEmits<{
  close: []
}>()

const flowStore = useFlowStore()
const { generateCode } = useCodeGeneration()

const code = ref('')
const loading = ref(false)
const error = ref<string | null>(null)

const extensions = [
  python(),
  oneDark,
  EditorView.theme({
    '&': { height: '100%' },
    '.cm-scroller': { overflow: 'auto' },
    '.cm-content': { padding: '16px' },
    '.cm-focused': { outline: 'none' },
  }),
  EditorView.lineWrapping
]

const generateCodeFromFlow = () => {
  loading.value = true
  error.value = null

  try {
    const generatedCode = generateCode({
      nodes: flowStore.nodes,
      edges: flowStore.edges,
      flowName: 'WASM Flow'
    })
    code.value = generatedCode
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Failed to generate code'
    code.value = '# Failed to generate code. Please check your flow configuration.'
  } finally {
    loading.value = false
  }
}

const refreshCode = () => {
  generateCodeFromFlow()
}

const exportCode = () => {
  const blob = new Blob([code.value], { type: 'text/plain' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'flowfile_pipeline.py'
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

const closePanel = () => {
  emit('close')
}

// Watch for visibility changes and regenerate code
watch(() => props.isVisible, (isVisible) => {
  if (isVisible) {
    generateCodeFromFlow()
  }
})
</script>

<style scoped>
.code-generator-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  backdrop-filter: blur(2px);
}

.code-generator-panel {
  background: var(--color-bg-primary, #1e1e1e);
  border-radius: 8px;
  width: 90%;
  max-width: 1200px;
  height: 85vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
  overflow: hidden;
}

.code-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  border-bottom: 1px solid var(--color-border, #333);
  background: var(--color-bg-secondary, #252525);
}

.code-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: var(--color-text-primary, #e0e0e0);
}

.header-actions {
  display: flex;
  gap: 8px;
  align-items: center;
}

.icon-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  background: var(--color-bg-tertiary, #2d2d2d);
  color: var(--color-text-primary, #e0e0e0);
  border: 1px solid var(--color-border, #404040);
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.icon-button:hover:not(:disabled) {
  background: var(--color-accent, #0066cc);
  border-color: var(--color-accent, #0066cc);
}

.icon-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.close-button:hover {
  background: #dc3545 !important;
  border-color: #dc3545 !important;
}

.error-message {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 20px;
  background: rgba(220, 53, 69, 0.1);
  border-bottom: 1px solid rgba(220, 53, 69, 0.3);
  color: #ff6b6b;
  font-size: 14px;
}

.error-message svg {
  flex-shrink: 0;
}

.code-editor-container {
  flex: 1;
  overflow: hidden;
  background: #1e1e1e;
}

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
  border: 2px solid currentColor;
  border-radius: 50%;
  border-top-color: transparent;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

/* Dark theme overrides */
:root {
  --color-bg-primary: #1e1e1e;
  --color-bg-secondary: #252525;
  --color-bg-tertiary: #2d2d2d;
  --color-border: #404040;
  --color-text-primary: #e0e0e0;
  --color-accent: #0066cc;
}
</style>
