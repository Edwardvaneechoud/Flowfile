<script setup lang="ts">
import { computed, nextTick, ref } from 'vue'
import { marked } from 'marked'
import DOMPurify from 'dompurify'
import { useVisualsStore } from '../../stores/visuals-store'
import { useVisualData } from '../../composables/useVisualData'
import { applyDashboardFilters, filtersTargetingTile } from '../../composables/applyDashboardFilters'
import VueGraphicRenderer from '../../components/nodes/exploreData/VueGraphicRenderer.vue'
import type { DashboardFilter, DashboardTile as DashboardTileT } from '../../types/visuals'
import type { IChart } from '../../components/nodes/exploreData/interfaces'

const props = defineProps<{
  tile: DashboardTileT
  mode: 'edit' | 'view'
  appearance: 'light' | 'dark' | 'media'
  filters: DashboardFilter[]
}>()
const emit = defineEmits<{ remove: []; 'update:tile': [tile: DashboardTileT] }>()

const visualsStore = useVisualsStore()
const isText = computed(() => props.tile.type === 'text')

const viz = computed(() =>
  props.tile.viz_id ? (visualsStore.get(props.tile.viz_id) ?? null) : null,
)
const vizMissing = computed(() => !isText.value && props.tile.viz_id != null && !viz.value)

const { fields, data, loading, error } = useVisualData(() => viz.value?.dataset_name ?? null)

const chart = computed<IChart | null>(
  () => (viz.value?.spec?.[props.tile.chart_index] ?? null) as IChart | null,
)

const filteredData = computed(() =>
  applyDashboardFilters(
    data.value,
    filtersTargetingTile(props.filters, props.tile, viz.value?.dataset_name ?? null),
  ),
)

const headerTitle = computed(() =>
  viz.value?.name ?? (vizMissing.value ? 'Missing visual' : 'Untitled'),
)

// ---- text branch ----
const textEditing = ref(false)
const textDraft = ref(props.tile.text_md ?? '')
const textareaEl = ref<HTMLTextAreaElement | null>(null)

const renderedHtml = computed(() => {
  const md = props.tile.text_md ?? ''
  if (!md.trim()) return ''
  // marked v15 returns a string for a sync parse; cast across its overloads.
  const raw = marked.parse(md, { async: false }) as string
  return DOMPurify.sanitize(raw)
})

async function enterTextEdit() {
  if (props.mode !== 'edit' || !isText.value) return
  textDraft.value = props.tile.text_md ?? ''
  textEditing.value = true
  await nextTick()
  textareaEl.value?.focus()
}

function commitText() {
  if (textDraft.value !== (props.tile.text_md ?? '')) {
    emit('update:tile', { ...props.tile, text_md: textDraft.value })
  }
  textEditing.value = false
}
</script>

<template>
  <div class="tile" :class="{ 'tile-text': isText }">
    <div v-if="!isText" class="tile-header" :class="{ 'tile-handle': mode === 'edit' }">
      <span class="tile-title">{{ headerTitle }}</span>
      <button
        v-if="mode === 'edit'"
        class="tile-btn"
        title="Remove from dashboard"
        @click.stop="emit('remove')"
      >
        <i class="fa-solid fa-xmark"></i>
      </button>
    </div>

    <div class="tile-body" :class="{ 'tile-handle': isText && mode === 'edit' && !textEditing }">
      <template v-if="isText">
        <textarea
          v-if="mode === 'edit' && textEditing"
          ref="textareaEl"
          v-model="textDraft"
          class="text-editor"
          placeholder="Type a note…"
          @blur="commitText"
        />
        <div
          v-else-if="!renderedHtml"
          class="text-empty"
          :title="mode === 'edit' ? 'Double-click to edit' : undefined"
          @dblclick="enterTextEdit"
        >
          {{ mode === 'edit' ? 'Double-click to edit' : '' }}
        </div>
        <!-- eslint-disable-next-line vue/no-v-html -- renderedHtml is DOMPurify-sanitised marked output -->
        <div
          v-else
          class="text-rendered"
          :title="mode === 'edit' ? 'Double-click to edit' : undefined"
          @dblclick="enterTextEdit"
          v-html="renderedHtml"
        />
        <button
          v-if="mode === 'edit' && !textEditing"
          class="tile-btn tile-text-remove"
          title="Remove from dashboard"
          @click.stop="emit('remove')"
        >
          <i class="fa-solid fa-xmark"></i>
        </button>
      </template>

      <template v-else>
        <div v-if="loading" class="tile-state">Loading…</div>
        <div v-else-if="vizMissing" class="tile-state tile-missing">This visual was deleted.</div>
        <div v-else-if="error" class="tile-state tile-missing">{{ error }}</div>
        <div v-else-if="!chart" class="tile-state">No chart at this index.</div>
        <VueGraphicRenderer
          v-else
          :chart="chart as IChart"
          :fields="fields"
          :data="filteredData"
          :appearance="appearance"
        />
      </template>
    </div>
  </div>
</template>

<style scoped>
.tile {
  position: relative;
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--color-background-primary);
  overflow: hidden;
}
.tile-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 4px 8px;
  font-size: 12px;
  border-bottom: 1px solid var(--color-border-light);
  background: var(--color-background-secondary);
}
.tile-handle {
  cursor: move;
}
.tile-title {
  font-weight: 600;
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.tile-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
}
.tile-btn:hover {
  color: var(--color-danger);
  background: var(--color-background-hover);
}
.tile-btn i {
  font-size: 13px;
}
.tile-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  padding: 4px;
  position: relative;
}
.tile-text-remove {
  position: absolute;
  top: 4px;
  right: 4px;
  opacity: 0;
}
.tile:hover .tile-text-remove {
  opacity: 1;
}
.tile-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--color-text-secondary);
  font-size: 12px;
  text-align: center;
  padding: 8px;
}
.tile-missing {
  color: var(--color-warning, #d97706);
}
.text-editor {
  width: 100%;
  height: 100%;
  border: none;
  outline: none;
  resize: none;
  padding: 8px 12px;
  font-size: 13px;
  line-height: 1.5;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  box-sizing: border-box;
}
.text-empty {
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--color-text-muted);
  font-size: 13px;
  font-style: italic;
}
.text-rendered {
  height: 100%;
  overflow: auto;
  padding: 10px 14px;
  font-size: 14px;
  line-height: 1.5;
  word-break: break-word;
  color: var(--color-text-primary);
}
/* Markdown element styles (rendered via DOMPurify-sanitised marked output). */
.text-rendered :deep(h1) {
  font-size: 22px;
  font-weight: 600;
  margin: 0 0 8px;
}
.text-rendered :deep(h2) {
  font-size: 18px;
  font-weight: 600;
  margin: 12px 0 6px;
}
.text-rendered :deep(h3) {
  font-size: 16px;
  font-weight: 600;
  margin: 10px 0 4px;
}
.text-rendered :deep(h4),
.text-rendered :deep(h5),
.text-rendered :deep(h6) {
  font-size: 14px;
  font-weight: 600;
  margin: 8px 0 4px;
}
.text-rendered :deep(p) {
  margin: 6px 0;
}
.text-rendered :deep(ul),
.text-rendered :deep(ol) {
  padding-left: 22px;
  margin: 6px 0;
}
.text-rendered :deep(li) {
  margin: 2px 0;
}
.text-rendered :deep(code) {
  background: var(--color-background-secondary);
  padding: 1px 4px;
  border-radius: 3px;
  font-size: 12.5px;
  font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace;
}
.text-rendered :deep(pre) {
  background: var(--color-background-secondary);
  padding: 8px 12px;
  border-radius: 4px;
  overflow: auto;
  margin: 8px 0;
}
.text-rendered :deep(pre code) {
  background: transparent;
  padding: 0;
}
.text-rendered :deep(a) {
  color: var(--color-accent);
  text-decoration: none;
}
.text-rendered :deep(a:hover) {
  text-decoration: underline;
}
.text-rendered :deep(blockquote) {
  border-left: 3px solid var(--color-border-primary);
  padding-left: 12px;
  margin: 8px 0;
  color: var(--color-text-secondary);
}
.text-rendered :deep(table) {
  border-collapse: collapse;
  margin: 8px 0;
}
.text-rendered :deep(th),
.text-rendered :deep(td) {
  border: 1px solid var(--color-border-light);
  padding: 4px 8px;
  text-align: left;
}
.text-rendered :deep(th) {
  background: var(--color-background-secondary);
}
.text-rendered :deep(img) {
  max-width: 100%;
}
.text-rendered :deep(hr) {
  border: 0;
  border-top: 1px solid var(--color-border-light);
  margin: 12px 0;
}
</style>
