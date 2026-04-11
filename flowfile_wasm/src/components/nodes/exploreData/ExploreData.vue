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
// specList reads from node settings so saves propagate reactively and survive
// re-mounts (e.g. toggling fullscreen, which unmounts the GW React root).
const specList = computed<IChart[]>(
  () => (props.settings.graphic_walker_input?.specList ?? []) as unknown as IChart[]
)

const appearance = computed<IGWProps['appearance']>(() => {
  const theme = themeStore.mode
  if (theme === 'system') return 'media'
  return theme // 'light' | 'dark'
})

const notExecuted = computed(
  () => result.value === undefined || result.value.success === undefined
)
const executionError = computed(() => result.value?.success === false)
const hasData = computed(() => data.value.length > 0 && fields.value.length > 0)

const savedChartCount = computed(() => specList.value.length)
const totalRows = computed(() => rowInfo.value?.total_rows ?? null)
const loadedRows = computed(() => rowInfo.value?.loaded_rows ?? data.value.length)
const columnCount = computed(() => fields.value.length)

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
        dataModel: { fields: [], data: [] },
        is_initial: exported.length === 0,
      },
    })

    if (!options.silent) {
      statusKind.value = 'info'
      statusMessage.value =
        exported.length === 0
          ? 'No charts to save yet.'
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
      statusMessage.value = `Failed to save: ${
        err instanceof Error ? err.message : String(err)
      }`
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
  void saveCurrentSpec({ silent: true })
  unregisterHook?.()
  unregisterHook = null
  isFullscreen.value = false
})

watch(result, () => {
  statusMessage.value = null
})

function onKeyDown(e: KeyboardEvent) {
  if (e.key === 'Escape' && isFullscreen.value) {
    void closeFullscreen()
  }
}
onMounted(() => window.addEventListener('keydown', onKeyDown))
onBeforeUnmount(() => window.removeEventListener('keydown', onKeyDown))

async function openFullscreen() {
  if (!hasData.value) return
  isFullscreen.value = true
}

async function closeFullscreen() {
  // Flush the live spec before unmounting the React root so work is not lost.
  await saveCurrentSpec({ silent: true })
  isFullscreen.value = false
}

defineExpose({
  saveCurrentSpec,
  openFullscreen,
  closeFullscreen,
})
</script>

<template>
  <div class="explore-data-container">
    <div class="explore-header">
      <h3 class="explore-title">Explore Data</h3>
      <p class="explore-hint">
        Interactively visualize the data flowing through this node with Graphic
        Walker. Charts open in fullscreen mode and are saved automatically.
      </p>
    </div>

    <div v-if="notExecuted" class="explore-state">
      <p class="state-msg">Run the flow to load data for this node.</p>
    </div>

    <div v-else-if="executionError" class="explore-state explore-error">
      <p class="state-label">Error</p>
      <p class="state-msg">{{ result?.error }}</p>
    </div>

    <template v-else>
      <div class="explore-stats">
        <div class="stat">
          <span class="stat-label">Rows</span>
          <span class="stat-value">
            {{ loadedRows.toLocaleString()
            }}<template v-if="rowInfo?.truncated && totalRows != null">
              / {{ totalRows.toLocaleString() }}
            </template>
          </span>
        </div>
        <div class="stat">
          <span class="stat-label">Columns</span>
          <span class="stat-value">{{ columnCount }}</span>
        </div>
        <div class="stat">
          <span class="stat-label">Saved charts</span>
          <span class="stat-value">{{ savedChartCount }}</span>
        </div>
      </div>

      <div v-if="rowInfo?.truncated" class="explore-banner">
        Graphic Walker is capped at
        {{ rowInfo.max_rows.toLocaleString() }} rows in-browser. Only the first
        {{ loadedRows.toLocaleString() }} rows will be visualized.
      </div>

      <div v-if="!hasData" class="explore-state">
        <p class="state-msg">Data loaded, but the dataset is empty or has no fields.</p>
      </div>

      <div v-else class="explore-actions">
        <button
          class="btn btn-primary"
          type="button"
          @click="openFullscreen"
          title="Open the Graphic Walker visualization in fullscreen"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
          >
            <path d="M15 3h6v6" />
            <path d="M9 21H3v-6" />
            <path d="M21 3l-7 7" />
            <path d="M3 21l7-7" />
          </svg>
          <span>Open in fullscreen</span>
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

    <!-- Fullscreen overlay: Graphic Walker only lives here, so the side
         panel stays compact and the chart always has room to breathe. -->
    <Teleport to="body">
      <div
        v-if="isFullscreen && hasData"
        class="explore-fullscreen-overlay"
        :data-theme="themeStore.effectiveTheme"
      >
        <div class="explore-fullscreen-header">
          <div class="explore-fullscreen-title-group">
            <span class="explore-fullscreen-title">Explore Data</span>
            <span class="explore-fullscreen-subtitle">
              {{ loadedRows.toLocaleString() }} rows · {{ columnCount }} columns
            </span>
          </div>
          <div class="explore-fullscreen-actions">
            <button
              class="btn btn-secondary btn-small"
              type="button"
              @click="closeFullscreen"
              title="Close fullscreen (Esc)"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="14"
                height="14"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
              >
                <line x1="18" y1="6" x2="6" y2="18" />
                <line x1="6" y1="6" x2="18" y2="18" />
              </svg>
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
  display: flex;
  flex-direction: column;
  gap: 1rem;
  padding: 1rem;
  color: var(--color-text-primary);
}

.explore-header {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.explore-title {
  margin: 0;
  font-size: var(--font-size-lg, 15px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
}

.explore-hint {
  margin: 0;
  font-size: var(--font-size-sm, 12px);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.explore-stats {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.5rem;
  padding: 0.75rem;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  background-color: var(--color-background-secondary);
}

.stat {
  display: flex;
  flex-direction: column;
  gap: 0.15rem;
  min-width: 0;
}

.stat-label {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-tertiary);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.stat-value {
  font-size: var(--font-size-md, 13px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.explore-banner {
  padding: 0.5rem 0.75rem;
  border-radius: 4px;
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs, 11px);
  line-height: 1.4;
  border: 1px solid var(--color-border-primary);
}

.explore-state {
  padding: 1rem;
  border-radius: 6px;
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  font-size: var(--font-size-sm, 12px);
  color: var(--color-text-secondary);
}

.explore-state.explore-error {
  background-color: var(--color-danger-light);
  border-color: var(--color-danger);
  color: var(--color-danger-dark);
}

.state-label {
  margin: 0 0 0.25rem 0;
  font-weight: var(--font-weight-semibold, 600);
  font-size: var(--font-size-sm, 12px);
}

.state-msg {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
  line-height: 1.5;
}

.explore-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.75rem;
}

.status-message {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.status-message.status-error {
  color: var(--color-danger);
}
</style>

<style>
/* Unscoped: the fullscreen overlay is teleported to <body>, so it is
   outside the .flowfile-editor-root subtree where the design-token CSS
   variables are defined. Explicitly declare both light and dark token
   values on the overlay element so .btn/.btn-primary/.btn-secondary
   (which all use var(--color-...)) resolve correctly. */
.explore-fullscreen-overlay {
  /* Light mode tokens (match editor.css defaults) */
  --color-background-primary: #ffffff;
  --color-background-secondary: #f8f9fa;
  --color-background-tertiary: #f1f3f5;
  --color-background-hover: #f0f7ff;
  --color-text-primary: #1a1a2e;
  --color-text-secondary: #4a5568;
  --color-text-tertiary: #718096;
  --color-border-primary: #e2e8f0;
  --color-accent: #0891b2;
  --color-accent-hover: #0e7490;
  --color-danger: #ef4444;
  --color-danger-light: #fee2e2;
  --color-danger-dark: #b91c1c;

  position: fixed;
  inset: 0;
  z-index: 9999;
  display: flex;
  flex-direction: column;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
  font-family:
    'Roboto', 'Source Sans Pro', -apple-system, BlinkMacSystemFont, 'Segoe UI',
    Helvetica, Arial, sans-serif;
  font-size: 14px;
}

.explore-fullscreen-overlay[data-theme='dark'] {
  --color-background-primary: #1a1a2e;
  --color-background-secondary: #16213e;
  --color-background-tertiary: #0f3460;
  --color-background-hover: #1e3a5f;
  --color-text-primary: #f1f5f9;
  --color-text-secondary: #cbd5e1;
  --color-text-tertiary: #94a3b8;
  --color-border-primary: #334155;
}

.explore-fullscreen-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--color-border-primary);
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
}

.explore-fullscreen-title-group {
  display: flex;
  align-items: baseline;
  gap: 0.75rem;
  min-width: 0;
}

.explore-fullscreen-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.explore-fullscreen-subtitle {
  font-size: 12px;
  color: var(--color-text-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.explore-fullscreen-actions {
  display: flex;
  gap: 0.5rem;
}

.explore-fullscreen-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  background-color: var(--color-background-primary);
}

.explore-fullscreen-body .gw-wrapper {
  height: 100%;
}
</style>
