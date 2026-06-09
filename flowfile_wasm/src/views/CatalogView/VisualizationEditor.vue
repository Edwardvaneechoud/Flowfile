<script setup lang="ts">
import { computed, ref } from 'vue'
import VueGraphicWalker from '../../components/nodes/exploreData/VueGraphicWalker.vue'
import { useVisualsStore } from '../../stores/visuals-store'
import { useVisualData } from '../../composables/useVisualData'
import { useGraphicWalkerAppearance } from '../../composables/useGraphicWalkerAppearance'
import { captureThumbnail } from '../../composables/useChartThumbnail'
import type { IChart } from '../../components/nodes/exploreData/interfaces'
import type { SavedVisual, VizSourceDescriptor } from '../../types/visuals'

const props = defineProps<{
  source: VizSourceDescriptor
  viz?: SavedVisual | null
}>()
const emit = defineEmits<{ saved: [viz: SavedVisual]; cancel: [] }>()

const visualsStore = useVisualsStore()
const appearance = useGraphicWalkerAppearance()
const gwRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null)

const name = ref(props.viz?.name ?? 'Untitled chart')
const saving = ref(false)
const saveError = ref<string | null>(null)

const datasetName = computed(() => props.source.dataset_name)
const { fields, data, rowInfo, loading, error } = useVisualData(datasetName)

const initialSpecList = computed<IChart[] | undefined>(() =>
  props.viz?.spec && props.viz.spec.length
    ? (props.viz.spec as unknown as IChart[])
    : undefined,
)

const canSave = computed(() => name.value.trim().length > 0 && !loading.value && !saving.value)
const truncated = computed(() => !!rowInfo.value?.truncated)
const loadedRows = computed(() => Number(rowInfo.value?.loaded_rows ?? 0))
const maxRows = computed(() => Number(rowInfo.value?.max_rows ?? 0))

async function save() {
  if (!gwRef.value) return
  if (!name.value.trim()) {
    saveError.value = 'Enter a name to save the visualization.'
    return
  }
  saving.value = true
  saveError.value = null
  try {
    const charts = await gwRef.value.exportCode()
    if (!charts || !charts.length) {
      saveError.value = 'No chart to save — build one in the editor first.'
      return
    }
    const spec = charts as unknown as Record<string, any>[]
    const thumbnail = await captureThumbnail(gwRef)
    let saved: SavedVisual | undefined
    if (props.viz) {
      saved = visualsStore.update(props.viz.id, {
        name: name.value.trim(),
        spec,
        thumbnail_data_url: thumbnail ?? undefined,
      })
    } else {
      saved = visualsStore.create({
        name: name.value.trim(),
        spec,
        source_type: props.source.source_type,
        dataset_name: props.source.dataset_name,
        source_kind: props.source.source_kind,
        thumbnail_data_url: thumbnail ?? undefined,
      })
    }
    if (saved) emit('saved', saved)
  } catch (e) {
    saveError.value = e instanceof Error ? e.message : String(e)
  } finally {
    saving.value = false
  }
}
</script>

<template>
  <div class="viz-editor">
    <div class="viz-editor-toolbar">
      <div class="viz-name-field">
        <label class="viz-name-label" for="viz-name">Name</label>
        <input
          id="viz-name"
          v-model="name"
          class="viz-name-input"
          type="text"
          placeholder="e.g. Revenue by region"
          :disabled="saving"
        />
      </div>
      <div class="viz-editor-actions">
        <span class="viz-source-hint" :title="source.dataset_name">{{ source.dataset_name }}</span>
        <button class="btn-secondary" :disabled="saving" @click="emit('cancel')">Cancel</button>
        <button class="btn-primary" :disabled="!canSave" @click="save">
          {{ saving ? 'Saving…' : viz ? 'Save changes' : 'Save visualization' }}
        </button>
      </div>
    </div>

    <div v-if="truncated" class="viz-banner">
      Graphic Walker is capped at {{ maxRows.toLocaleString() }} rows in-browser. Charting the
      first {{ loadedRows.toLocaleString() }} rows.
    </div>
    <div v-if="saveError" class="viz-error">{{ saveError }}</div>

    <div class="viz-body">
      <div v-if="loading" class="viz-state">Loading data…</div>
      <div v-else-if="error" class="viz-state viz-state-error">{{ error }}</div>
      <VueGraphicWalker
        v-else
        ref="gwRef"
        :appearance="appearance"
        :data="data"
        :fields="fields"
        :spec-list="initialSpecList"
      />
    </div>
  </div>
</template>

<style scoped>
.viz-editor {
  display: flex;
  flex-direction: column;
  gap: 12px;
  height: 100%;
  min-height: 0;
  overflow: hidden;
}
.viz-editor-toolbar {
  display: flex;
  align-items: flex-end;
  gap: 12px;
  justify-content: space-between;
}
.viz-name-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1;
  max-width: 360px;
}
.viz-name-label {
  font-size: 12px;
  color: var(--color-text-secondary);
}
.viz-name-input {
  width: 100%;
  padding: 8px 12px;
  font-size: 14px;
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
}
.viz-name-input:focus {
  outline: none;
  border-color: var(--color-accent);
}
.viz-editor-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.viz-source-hint {
  font-size: 12px;
  color: var(--color-text-muted);
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.btn-secondary {
  padding: 8px 16px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
}
.btn-secondary:hover {
  background: var(--color-background-hover);
}
.btn-primary {
  padding: 8px 16px;
  background: var(--color-accent);
  color: #fff;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
}
.btn-primary:hover:not(:disabled) {
  background: var(--color-accent-hover, #1d4ed8);
}
.btn-primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.viz-banner {
  padding: 8px 12px;
  border-radius: 4px;
  background: var(--color-background-tertiary);
  border: 1px solid var(--color-border-primary);
  color: var(--color-text-secondary);
  font-size: 11px;
}
.viz-error {
  padding: 8px 12px;
  border-radius: 4px;
  background: var(--color-danger-light);
  border: 1px solid var(--color-danger);
  color: var(--color-danger-dark, var(--color-danger));
  font-size: 12px;
}
.viz-body {
  flex: 1 1 auto;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}
.viz-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--color-text-secondary);
  font-size: 13px;
}
.viz-state-error {
  color: var(--color-danger);
  white-space: pre-wrap;
}
</style>
