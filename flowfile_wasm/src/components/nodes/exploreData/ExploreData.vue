<script lang="ts" setup>
import { computed, ref, onMounted, onBeforeUnmount, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useFlowStore } from '../../../stores/flow-store'
import { useThemeStore } from '../../../stores/theme-store'
import type { ExploreDataSettings, NodeResult } from '../../../types'
import VueGraphicWalker from './VueGraphicWalker.vue'
import type { IRow, IMutField, IChart, IGWProps } from './interfaces'

const props = defineProps<{
  nodeId: number
  settings: ExploreDataSettings
}>()

const flowStore = useFlowStore()
const { nodeResults } = storeToRefs(flowStore)
const themeStore = useThemeStore()

const vueGraphicWalkerRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null)
const isFullscreen = ref(false)
const statusMessage = ref<string | null>(null)
const statusKind = ref<'info' | 'error'>('info')

const result = computed<NodeResult | undefined>(() =>
  nodeResults.value.get(props.nodeId)
)

const gwInput = computed(() => result.value?.graphic_walker_input)
const rowInfo = computed(() => result.value?.row_info)

const fields = computed<IMutField[]>(() =>
  (gwInput.value?.dataModel.fields ?? []) as unknown as IMutField[]
)
const data = computed<IRow[]>(() =>
  (gwInput.value?.dataModel.data ?? []) as unknown as IRow[]
)
// specList comes from node settings rather than the result payload so it
// stays in sync with user edits across re-mounts (e.g. toggling fullscreen).
// Saves go to settings via updateNodeSettingsSilent, and this computed
// reacts to them immediately.
const specList = computed<IChart[]>(
  () => (props.settings.graphic_walker_input?.specList ?? []) as unknown as IChart[]
)

const appearance = computed<IGWProps['appearance']>(() => {
  const theme = themeStore.mode
  if (theme === 'system') return 'media'
  return theme // 'light' | 'dark'
})

const notExecuted = computed(() => result.value === undefined || result.value.success === undefined)
const executionError = computed(() => result.value?.success === false)
const hasData = computed(
  () => data.value.length > 0 && fields.value.length > 0
)

async function saveCurrentSpec(options: { silent?: boolean } = {}) {
  const walker = vueGraphicWalkerRef.value
  if (!walker) return

  try {
    const exported = await walker.exportCode()
    if (exported === null) {
      if (!options.silent) {
        statusKind.value = 'error'
        statusMessage.value = 'Failed to retrieve the current chart configuration.'
      }
      return
    }

    const current = props.settings.graphic_walker_input ?? {
      is_initial: true,
      dataModel: { fields: [], data: [] },
      specList: [],
    }

    flowStore.updateNodeSettingsSilent(props.nodeId, {
      graphic_walker_input: {
        ...current,
        specList: exported,
        // Don't persist data/fields in settings - they are regenerated
        // from the upstream node on every run.
        dataModel: { fields: [], data: [] },
        is_initial: exported.length === 0,
      },
    })

    if (!options.silent) {
      statusKind.value = 'info'
      statusMessage.value =
        exported.length === 0
          ? 'No charts to save yet. Build a chart first.'
          : `Saved ${exported.length} chart${exported.length === 1 ? '' : 's'}.`
      setTimeout(() => {
        if (
          statusMessage.value?.startsWith('Saved') ||
          statusMessage.value?.startsWith('No charts')
        ) {
          statusMessage.value = null
        }
      }, 2500)
    }
  } catch (err) {
    console.error('[ExploreData] Failed to save chart spec:', err)
    if (!options.silent) {
      statusKind.value = 'error'
      statusMessage.value = `Failed to save: ${err instanceof Error ? err.message : String(err)}`
    }
  }
}

// Register a hook that the flow store calls right before downloading the
// flow file, so the currently-displayed chart spec is flushed into node
// settings and included in the exported YAML.
let unregisterHook: (() => void) | null = null
onMounted(() => {
  unregisterHook = flowStore.registerBeforeExportHook(async () => {
    await saveCurrentSpec({ silent: true })
  })
})

onBeforeUnmount(() => {
  // Flush one last time on unmount (e.g. selecting a different node) so
  // work is not lost when switching panels.
  void saveCurrentSpec({ silent: true })
  unregisterHook?.()
  unregisterHook = null
  // Exit fullscreen on unmount so the overlay doesn't leak across nodes.
  isFullscreen.value = false
})

// Clear any lingering status when the node data changes.
watch(result, () => {
  statusMessage.value = null
})

// Escape key exits fullscreen
function onKeyDown(e: KeyboardEvent) {
  if (e.key === 'Escape' && isFullscreen.value) {
    isFullscreen.value = false
  }
}
onMounted(() => window.addEventListener('keydown', onKeyDown))
onBeforeUnmount(() => window.removeEventListener('keydown', onKeyDown))

async function toggleFullscreen() {
  // Save BEFORE swapping instances: toggling fullscreen unmounts one
  // VueGraphicWalker and mounts another, which resets the internal React
  // store. Persisting the spec first and letting the new instance mount
  // with :spec-list="specList" rehydrates the chart.
  await saveCurrentSpec({ silent: true })
  isFullscreen.value = !isFullscreen.value
}

defineExpose({
  saveCurrentSpec,
  toggleFullscreen,
})
</script>

<template>
  <div class="explore-data-container">
    <div v-if="notExecuted" class="fallback-message">
      Run the flow to load data for this node.
    </div>

    <div v-else-if="executionError" class="error-display">
      <p>Error: {{ result?.error }}</p>
    </div>

    <template v-else>
      <div v-if="rowInfo?.truncated" class="truncation-banner">
        Showing the first {{ rowInfo.loaded_rows.toLocaleString() }} of
        {{ rowInfo.total_rows?.toLocaleString() }} rows
        (Graphic Walker is capped at {{ rowInfo.max_rows.toLocaleString() }} rows in-browser).
      </div>

      <div v-if="hasData && !isFullscreen" class="graphic-walker-wrapper">
        <VueGraphicWalker
          ref="vueGraphicWalkerRef"
          :appearance="appearance"
          :data="data"
          :fields="fields"
          :spec-list="specList"
        />
      </div>
      <div v-else-if="!hasData" class="empty-data-message">
        Data loaded, but the dataset is empty or has no fields.
      </div>

      <div class="explore-toolbar">
        <button
          v-if="hasData"
          class="toolbar-btn primary"
          type="button"
          @click="toggleFullscreen"
          :title="isFullscreen ? 'Exit fullscreen (Esc)' : 'Open in fullscreen'"
        >
          <svg v-if="!isFullscreen" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 3h6v6"/><path d="M9 21H3v-6"/><path d="M21 3l-7 7"/><path d="M3 21l7-7"/></svg>
          <svg v-else xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg>
          <span>{{ isFullscreen ? 'Exit fullscreen' : 'Fullscreen' }}</span>
        </button>
        <button
          v-if="hasData"
          class="toolbar-btn"
          type="button"
          @click="() => saveCurrentSpec()"
          title="Save the current chart specifications to the node settings (also runs automatically on flow save)"
        >
          Save chart
        </button>
        <span
          v-if="statusMessage"
          class="status-message"
          :class="{ 'status-error': statusKind === 'error' }"
        >
          {{ statusMessage }}
        </span>
      </div>
    </template>

    <!-- Fullscreen overlay: teleport Graphic Walker outside the Draggable
         panel so it gets the entire viewport. The same vueGraphicWalkerRef
         is reused so exportCode() and spec state persist across the toggle. -->
    <Teleport to="body">
      <div v-if="isFullscreen && hasData" class="explore-fullscreen-overlay">
        <div class="explore-fullscreen-header">
          <span class="explore-fullscreen-title">Explore Data (fullscreen)</span>
          <div class="explore-fullscreen-actions">
            <button class="toolbar-btn" type="button" @click="() => saveCurrentSpec()">
              Save chart
            </button>
            <button
              class="toolbar-btn"
              type="button"
              @click="toggleFullscreen"
              title="Exit fullscreen (Esc)"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
              <span>Close</span>
            </button>
          </div>
        </div>
        <div class="explore-fullscreen-body">
          <VueGraphicWalker
            ref="vueGraphicWalkerRef"
            :appearance="appearance"
            :data="data"
            :fields="fields"
            :spec-list="specList"
          />
        </div>
      </div>
    </Teleport>
  </div>
</template>

<style scoped>
.explore-data-container {
  height: 100%;
  display: flex;
  flex-direction: column;
  background-color: var(--color-background, #ffffff);
}

.graphic-walker-wrapper {
  flex-grow: 1;
  min-height: 400px;
  overflow: hidden;
}

:deep(.gw-wrapper) {
  height: 100%;
}

.truncation-banner {
  padding: 0.5rem 0.75rem;
  font-size: 12px;
  background-color: #fff8e1;
  color: #8a6d00;
  border-bottom: 1px solid #f0e0a0;
}

.error-display {
  padding: 1rem;
  margin: 1rem;
  border-radius: 4px;
  background-color: #fdecea;
  color: #b71c1c;
  border: 1px solid #f5c6c0;
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 13px;
}

.empty-data-message,
.fallback-message {
  padding: 1.5rem;
  text-align: center;
  color: var(--text-color-secondary, #909399);
  font-size: 13px;
}

.explore-toolbar {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  border-top: 1px solid var(--border-color, #e5e7eb);
}

.toolbar-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  padding: 0.35rem 0.85rem;
  font-size: 12px;
  border: 1px solid var(--border-color, #d0d5dd);
  background-color: var(--color-background, #ffffff);
  color: inherit;
  border-radius: 4px;
  cursor: pointer;
  transition: background-color 0.15s ease;
}

.toolbar-btn:hover {
  background-color: var(--color-background-hover, #f3f4f6);
}

.toolbar-btn.primary {
  background-color: #2563eb;
  border-color: #2563eb;
  color: #ffffff;
}

.toolbar-btn.primary:hover {
  background-color: #1d4ed8;
}

.status-message {
  font-size: 12px;
  color: var(--text-color-secondary, #6b7280);
}

.status-message.status-error {
  color: #b91c1c;
}
</style>

<style>
/* Unscoped so the teleported overlay is styled despite leaving the
   component's scoped-CSS subtree. */
.explore-fullscreen-overlay {
  position: fixed;
  inset: 0;
  z-index: 9999;
  background-color: var(--color-background, #ffffff);
  display: flex;
  flex-direction: column;
}

.explore-fullscreen-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.5rem 1rem;
  border-bottom: 1px solid var(--border-color, #e5e7eb);
  background-color: var(--color-background, #ffffff);
}

.explore-fullscreen-title {
  font-size: 14px;
  font-weight: 600;
}

.explore-fullscreen-actions {
  display: flex;
  gap: 0.5rem;
}

.explore-fullscreen-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

.explore-fullscreen-body .gw-wrapper {
  height: 100%;
}
</style>
