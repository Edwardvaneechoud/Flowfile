<template>
  <div
    class="flowfile-editor-root"
    :data-theme="effectiveTheme"
    :style="rootStyle"
  >
    <!-- Loading state overlay -->
    <div v-if="!pyodideReady && showLoadingOverlay" class="flowfile-loading-overlay">
      <div class="flowfile-loading-content">
        <div class="spinner"></div>
        <span class="flowfile-loading-text">{{ loadingStatusText }}</span>
      </div>
    </div>

    <!-- The actual Canvas editor -->
    <Canvas
      v-if="mounted"
      :toolbar-config="toolbarConfigMerged"
      :node-categories-config="props.nodeCategories"
      :readonly="props.readonly"
      @execution-complete="onExecutionComplete"
      @output="onOutput"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import Canvas from '../components/Canvas.vue'
import { usePyodideStore } from '../stores/pyodide-store'
import { useFlowStore } from '../stores/flow-store'
import { useThemeStore } from '../stores/theme-store'
import { storeToRefs } from 'pinia'
import type {
  FlowfileEditorProps,
  ToolbarConfig,
  FlowfileEditorAPI,
  InputDataMap,
  OutputData,
  EditorError
} from './types'
import type { FlowfileData, NodeResult } from '../types'

const props = withDefaults(defineProps<FlowfileEditorProps>(), {
  readonly: false,
  height: '100%',
  width: '100%'
})

const emit = defineEmits<{
  (e: 'ready'): void
  (e: 'flow-change', data: FlowfileData): void
  (e: 'execution-complete', results: Map<number, NodeResult>): void
  (e: 'output', data: OutputData): void
  (e: 'error', error: EditorError): void
  (e: 'loading-status', status: string): void
}>()

const mounted = ref(false)

// Initialize stores
const pyodideStore = usePyodideStore()
const flowStore = useFlowStore()
const themeStore = useThemeStore()

const { isReady: pyodideReady } = storeToRefs(pyodideStore)

// Loading status from pyodide store
const loadingStatusText = computed(() => {
  return pyodideStore.loadingStatus || 'Initializing Pyodide...'
})

const showLoadingOverlay = computed(() => {
  return props.pyodide?.autoInit !== false
})

// Theme handling
const effectiveTheme = computed(() => {
  if (props.theme?.mode && props.theme.mode !== 'system') {
    return props.theme.mode
  }
  return themeStore.effectiveTheme
})

const rootStyle = computed(() => ({
  height: props.height,
  width: props.width,
  position: 'relative' as const,
  overflow: 'hidden' as const,
  display: 'flex' as const,
  flexDirection: 'column' as const
}))

// Merge toolbar config with defaults for embedded mode
const toolbarConfigMerged = computed<ToolbarConfig>(() => ({
  showRun: true,
  showSaveLoad: true,
  showClear: true,
  showCodeGen: true,
  showDemo: false,  // Default off for embedded
  ...props.toolbar
}))

// Watch for initialFlow prop changes
watch(() => props.initialFlow, (flow) => {
  if (flow && pyodideReady.value) {
    flowStore.importFromFlowfile(flow)
  }
}, { deep: true })

// Watch for inputData prop changes — push to store as external datasets
watch(() => props.inputData, (data) => {
  if (data) {
    pushExternalDatasets(data)
  }
}, { deep: true })

// Watch for pyodide ready
watch(pyodideReady, (ready) => {
  if (ready) {
    emit('ready')
    // Load initial flow if provided
    if (props.initialFlow) {
      flowStore.importFromFlowfile(props.initialFlow)
    }
    // Push external datasets if provided (re-apply in case flow was just imported)
    if (props.inputData) {
      pushExternalDatasets(props.inputData)
    }
  }
})

/**
 * Push input data to the flow store as external datasets.
 * External Data nodes select from these by name.
 */
function pushExternalDatasets(data: InputDataMap) {
  const datasets: Record<string, string> = {}
  for (const [name, config] of Object.entries(data)) {
    datasets[name] = typeof config === 'string' ? config : config.content
  }
  flowStore.setExternalDatasets(datasets)
}

function onExecutionComplete(results: Map<number, NodeResult>) {
  emit('execution-complete', results)
}

function onOutput(data: OutputData) {
  emit('output', data)
}

onMounted(async () => {
  mounted.value = true

  // Push external datasets immediately so they're available in the UI
  if (props.inputData) {
    pushExternalDatasets(props.inputData)
  }

  // Set embedded mode on theme store
  themeStore.setEmbedded(true)

  // Initialize theme
  if (props.theme?.mode) {
    themeStore.setTheme(props.theme.mode)
  } else {
    themeStore.initialize()
  }

  // Initialize Pyodide unless autoInit is disabled
  if (props.pyodide?.autoInit !== false) {
    emit('loading-status', 'Initializing...')
    try {
      await pyodideStore.initialize()
    } catch (err: any) {
      emit('error', { type: 'pyodide', message: err?.message || 'Failed to initialize Pyodide' })
    }
  }
})

onUnmounted(() => {
  // Restore non-embedded mode if needed
  themeStore.setEmbedded(false)
})

// Expose programmatic API
const api: FlowfileEditorAPI = {
  get isReady() { return pyodideStore.isReady },
  get isExecuting() { return flowStore.isExecuting },
  executeFlow: () => flowStore.executeFlow(),
  executeNode: (nodeId: number) => flowStore.executeNode(nodeId),
  exportFlow: () => flowStore.exportToFlowfile(),
  importFlow: (data: FlowfileData) => flowStore.importFromFlowfile(data),
  setInputData: (name: string, content: string) => {
    pushExternalDatasets({ [name]: content })
  },
  getNodeResult: (nodeId: number) => flowStore.getNodeResult(nodeId),
  clearFlow: () => flowStore.clearFlow(),
  initializePyodide: () => pyodideStore.initialize()
}

defineExpose(api)
</script>

<style>
/* Import scoped editor styles */
@import '../styles/editor.css';

.flowfile-loading-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-overlay, rgba(0, 0, 0, 0.5));
  z-index: 9999;
}

.flowfile-loading-content {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 16px;
  padding: 32px;
  background: var(--color-background-primary, #ffffff);
  border-radius: var(--border-radius-lg, 8px);
  box-shadow: var(--shadow-lg);
}

.flowfile-loading-text {
  font-size: var(--font-size-md, 13px);
  color: var(--color-text-secondary, #4a5568);
}
</style>
