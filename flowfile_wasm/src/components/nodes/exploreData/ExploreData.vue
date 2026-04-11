<script lang="ts" setup>
import { computed, ref, onBeforeUnmount, watch } from 'vue'
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
const specList = computed<IChart[]>(() =>
  (gwInput.value?.specList ?? []) as unknown as IChart[]
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
        if (statusMessage.value?.startsWith('Saved') || statusMessage.value?.startsWith('No charts')) {
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

// Auto-save on unmount (e.g. selecting a different node) so work is not lost.
onBeforeUnmount(() => {
  void saveCurrentSpec({ silent: true })
})

// Clear any lingering status when the node data changes.
watch(result, () => {
  statusMessage.value = null
})

defineExpose({
  saveCurrentSpec,
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

      <div v-if="hasData" class="graphic-walker-wrapper">
        <VueGraphicWalker
          ref="vueGraphicWalkerRef"
          :appearance="appearance"
          :data="data"
          :fields="fields"
          :spec-list="specList"
        />
      </div>
      <div v-else class="empty-data-message">
        Data loaded, but the dataset is empty or has no fields.
      </div>

      <div class="explore-toolbar">
        <button
          v-if="hasData"
          class="save-spec-btn"
          type="button"
          @click="() => saveCurrentSpec()"
          title="Save the currently-displayed chart specifications to the node settings"
        >
          Save chart spec
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
  gap: 0.75rem;
  padding: 0.5rem 0.75rem;
  border-top: 1px solid var(--border-color, #e5e7eb);
}

.save-spec-btn {
  padding: 0.35rem 0.85rem;
  font-size: 12px;
  border: 1px solid var(--border-color, #d0d5dd);
  background-color: var(--color-background, #ffffff);
  border-radius: 4px;
  cursor: pointer;
  transition: background-color 0.15s ease;
}

.save-spec-btn:hover {
  background-color: var(--color-background-hover, #f3f4f6);
}

.status-message {
  font-size: 12px;
  color: var(--text-color-secondary, #6b7280);
}

.status-message.status-error {
  color: #b91c1c;
}
</style>
