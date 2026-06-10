<template>
  <div class="catalog-view">
    <!-- Tab bar -->
    <div class="catalog-tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="catalog-tab"
        :class="{ active: activeTab === tab.key }"
        @click="setTab(tab.key)"
      >
        <i class="fa-solid" :class="tab.icon"></i>
        <span>{{ tab.label }}</span>
        <span v-if="tab.badge !== null" class="tab-badge">{{ tab.badge }}</span>
      </button>
      <div class="tab-spacer"></div>
      <button class="catalog-tab info-btn" title="Refresh" @click="refresh">
        <i class="fa-solid fa-arrows-rotate"></i>
      </button>
    </div>

    <!-- Catalog (flows + tables in one selection) / Favorites: sidebar + detail -->
    <div v-if="activeTab === 'catalog' || activeTab === 'favorites'" class="catalog-body">
      <aside class="catalog-sidebar">
        <div class="sidebar-filters">
          <button class="upload-btn" @click="triggerUpload">
            <i class="fa-solid fa-file-arrow-up"></i>
            <span>Upload table</span>
          </button>
          <input
            ref="uploadInput"
            type="file"
            accept=".csv,.txt"
            style="display: none"
            @change="handleUpload"
          />
          <input v-model="search" type="text" class="search-input" placeholder="Search catalog..." />
        </div>
        <div class="sidebar-scroll">
          <div v-for="group in displayGroups" :key="group.key" class="source-group">
            <div class="group-header">
              <span>{{ group.label }}</span>
              <span class="group-count">{{ group.items.length }}</span>
            </div>
            <ul v-if="group.items.length" class="listbox">
              <li
                v-for="item in group.items"
                :key="item.id"
                :class="{ 'is-selected': item.id === selectedId, unavailable: item.unavailable }"
                @click="selectedId = item.id"
              >
                <i class="fa-solid item-type-icon" :class="item.kind === 'flow' ? 'fa-diagram-project' : 'fa-table'"></i>
                <span class="item-name">{{ item.name }}</span>
                <span class="item-meta">{{ itemMeta(item) }}</span>
                <button
                  v-if="item.kind === 'catalog' && item.datasetName"
                  class="item-icon-btn item-delete"
                  title="Delete catalog table"
                  @click.stop="deleteCatalogTable(item)"
                >
                  <i class="fa-solid fa-trash"></i>
                </button>
                <button
                  v-else-if="item.kind === 'flow'"
                  class="item-icon-btn item-delete"
                  title="Delete flow"
                  @click.stop="deleteFlowItem(item)"
                >
                  <i class="fa-solid fa-trash"></i>
                </button>
                <button
                  class="item-icon-btn item-star"
                  :class="{ active: favoritesStore.isFavorite(item.id) }"
                  :title="favoritesStore.isFavorite(item.id) ? 'Remove favorite' : 'Add favorite'"
                  @click.stop="favoritesStore.toggle(item.id)"
                >
                  <i :class="favoritesStore.isFavorite(item.id) ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
                </button>
              </li>
            </ul>
            <p v-else class="group-empty">{{ group.emptyText }}</p>
          </div>
        </div>
      </aside>

      <section class="catalog-detail">
        <FlowDetailPanel
          v-if="selectedFlow"
          :flow-id="selectedFlow.flowId!"
          @deleted="selectedId = null"
          @select="selectedId = $event"
        />
        <CatalogDetailPanel v-else-if="selected" :item="selected" />
        <StatsPanel
          v-else
          :flows="flowItems.length"
          :tables="items.length"
          :runs="runHistoryStore.total"
          :success="runHistoryStore.successCount"
          :favorites="favoritesStore.count"
          @open-tab="setTab"
          @go-designer="router.push({ name: 'designer' })"
        />
      </section>
    </div>

    <!-- Visuals: charts + dashboards combined behind a sub-toggle -->
    <section v-else-if="activeTab === 'visuals'" class="catalog-panel-host">
      <VisualsPanel />
    </section>

    <!-- Run History: full width -->
    <section v-else class="catalog-body catalog-body--full">
      <RunHistoryTable :runs="runHistoryStore.runs" />
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useRoute, useRouter } from 'vue-router'
import { useFlowStore } from '../stores/flow-store'
import { useFavoritesStore } from '../stores/favorites-store'
import { useRunHistoryStore } from '../stores/run-history-store'
import { inferSchemaFromCsv } from '../stores/schema-inference'
import CatalogDetailPanel from '../components/catalog/CatalogDetailPanel.vue'
import StatsPanel from './CatalogView/StatsPanel.vue'
import RunHistoryTable from './CatalogView/RunHistoryTable.vue'
import FlowDetailPanel from './CatalogView/FlowDetailPanel.vue'
import VisualsPanel from './CatalogView/VisualsPanel.vue'
import { useSavedFlowsStore } from '../stores/saved-flows-store'
import { useVisualsStore } from '../stores/visuals-store'
import type { CatalogItem } from '../components/catalog/types'
import type { DataPreview } from '../types'

type TabKey = 'catalog' | 'favorites' | 'visuals' | 'runs'

const route = useRoute()
const router = useRouter()
const flowStore = useFlowStore()
const favoritesStore = useFavoritesStore()
const runHistoryStore = useRunHistoryStore()
const savedFlowsStore = useSavedFlowsStore()
const visualsStore = useVisualsStore()
const { externalDatasets, catalogDatasets } = storeToRefs(flowStore)

const search = ref('')
const selectedId = ref<string | null>(null)
const activeTab = ref<TabKey>('catalog')
const uploadInput = ref<HTMLInputElement | null>(null)

function byteSize(content: string): number {
  return new Blob([content]).size
}

function parseCsvLine(line: string): string[] {
  const out: string[] = []
  let cur = ''
  let inQuotes = false
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++ } else { inQuotes = false }
      } else cur += ch
    } else if (ch === '"') {
      inQuotes = true
    } else if (ch === ',') {
      out.push(cur); cur = ''
    } else {
      cur += ch
    }
  }
  out.push(cur)
  return out
}

function parseCsvHead(content: string, maxRows = 20): DataPreview | null {
  if (!content) return null
  const lines = content.split(/\r?\n/)
  const nonEmpty = lines.filter((l) => l.length > 0)
  if (nonEmpty.length === 0) return null
  const columns = parseCsvLine(nonEmpty[0])
  const data = nonEmpty.slice(1, maxRows + 1).map(parseCsvLine)
  return { columns, data, total_rows: Math.max(0, nonEmpty.length - 1) }
}

const items = computed<CatalogItem[]>(() => {
  const out: CatalogItem[] = []

  for (const [name, content] of externalDatasets.value) {
    const schema = inferSchemaFromCsv(content) ?? undefined
    out.push({
      id: `ext-${name}`,
      kind: 'external',
      name,
      subtitle: 'External dataset',
      datasetName: name,
      schema,
      columns: schema?.length ?? null,
      sizeBytes: byteSize(content),
      status: 'success',
      preview: parseCsvHead(content)
    })
  }

  // User-uploaded catalog tables (persisted in IndexedDB)
  for (const [name, content] of catalogDatasets.value) {
    const schema = inferSchemaFromCsv(content) ?? undefined
    out.push({
      id: `catalog-${name}`,
      kind: 'catalog',
      name,
      subtitle: 'Catalog table',
      datasetName: name,
      schema,
      columns: schema?.length ?? null,
      sizeBytes: byteSize(content),
      status: 'success',
      preview: parseCsvHead(content)
    })
  }

  // The catalog lists only persistent data sources (uploaded catalog tables +
  // host-provided external datasets). Flow-bound data — a read node's loaded
  // CSV and transient node outputs — is intentionally NOT listed here, so the
  // catalog never changes when a flow is opened, closed, or switched.

  return out
})

// Saved flows as catalog entries (kind 'flow'). Flows and tables live in one
// catalog, mirroring the full app's namespace tree (FLOWS / TABLES sections).
const flowItems = computed<CatalogItem[]>(() =>
  savedFlowsStore.flows.map((f) => ({
    id: `flow-${f.id}`,
    kind: 'flow',
    name: f.name,
    subtitle: f.description || 'Flow',
    flowId: f.id,
    nodeCount: f.nodeCount,
    createdAt: f.createdAt,
    updatedAt: f.updatedAt,
    description: f.description
  }))
)

const allItems = computed(() => [...flowItems.value, ...items.value])

const tabs = computed(() => [
  { key: 'catalog' as TabKey, label: 'Catalog', icon: 'fa-folder-tree', badge: flowItems.value.length + items.value.length },
  { key: 'favorites' as TabKey, label: 'Favorites', icon: 'fa-star', badge: favoritesStore.count },
  { key: 'visuals' as TabKey, label: 'Visuals', icon: 'fa-chart-pie', badge: visualsStore.count },
  { key: 'runs' as TabKey, label: 'Run History', icon: 'fa-clock-rotate-left', badge: runHistoryStore.total }
])

const displayGroups = computed(() => {
  const q = search.value.trim().toLowerCase()
  const onlyFavorites = activeTab.value === 'favorites'
  const match = (i: CatalogItem) =>
    (!onlyFavorites || favoritesStore.isFavorite(i.id)) &&
    (!q || i.name.toLowerCase().includes(q) || (i.subtitle?.toLowerCase().includes(q) ?? false))

  const flows = flowItems.value.filter(match)
  const byKind = (kind: CatalogItem['kind']) => items.value.filter((i) => i.kind === kind && match(i))

  return [
    { key: 'flows', label: 'Flows', items: flows, emptyText: onlyFavorites ? 'No favorited flows.' : 'Build a flow in the Designer and click Save.' },
    { key: 'catalog', label: 'Tables', items: byKind('catalog'), emptyText: onlyFavorites ? 'No favorited tables.' : 'Upload a CSV to add a catalog table.' },
    { key: 'external', label: 'External datasets', items: byKind('external'), emptyText: onlyFavorites ? 'No favorited datasets.' : 'No external datasets provided.' }
  ]
})

const selected = computed(() => allItems.value.find((i) => i.id === selectedId.value) ?? null)
const selectedFlow = computed(() => (selected.value?.kind === 'flow' ? selected.value : null))

function itemMeta(item: CatalogItem): string {
  if (item.kind === 'flow') return `${item.nodeCount ?? 0} node${item.nodeCount === 1 ? '' : 's'}`
  if (item.unavailable) return 'missing'
  if (item.rows != null) return `${item.rows.toLocaleString()} rows`
  if (item.columns != null) return `${item.columns} cols`
  return ''
}

function deleteFlowItem(item: CatalogItem) {
  if (!item.flowId) return
  if (!confirm(`Delete "${item.name}"? This cannot be undone.`)) return
  void savedFlowsStore.remove(item.flowId)
  if (favoritesStore.isFavorite(item.id)) favoritesStore.toggle(item.id)
  if (selectedId.value === item.id) selectedId.value = null
}

function triggerUpload() {
  uploadInput.value?.click()
}

async function handleUpload(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  input.value = '' // allow re-uploading the same filename
  if (!file) return
  let content: string
  try {
    content = await file.text()
  } catch (e) {
    console.error('[catalog] failed to read file', e)
    alert('Failed to read the file. Please pick a valid CSV.')
    return
  }
  // Use the filename (without extension) as the table name; dedupe by name.
  const name = file.name.replace(/\.[^.]+$/, '') || file.name
  try {
    await flowStore.addCatalogDataset(name, content)
    selectedId.value = `catalog-${name}`
  } catch (e) {
    // The table is shown (in-memory) but persistence failed — say so explicitly
    // rather than letting it silently disappear on the next refresh.
    console.error('[catalog] failed to persist catalog table', e)
    const reason = e instanceof Error ? e.message : String(e)
    selectedId.value = `catalog-${name}`
    alert(`Couldn't save "${name}" to the Catalog — ${reason}\n\nIt won't survive a page refresh.`)
  }
}

async function deleteCatalogTable(item: CatalogItem) {
  if (!item.datasetName) return
  if (!confirm(`Delete catalog table "${item.datasetName}"?`)) return
  await flowStore.removeCatalogDataset(item.datasetName)
  if (selectedId.value === item.id) selectedId.value = null
}

function setTab(tab: TabKey) {
  activeTab.value = tab
  selectedId.value = null
  if (route.query.tab !== tab) {
    router.replace({ query: { ...route.query, tab } })
  }
}

function syncTabFromQuery() {
  const q = route.query.tab
  const valid: TabKey[] = ['catalog', 'favorites', 'visuals', 'runs']
  activeTab.value = typeof q === 'string' && valid.includes(q as TabKey) ? (q as TabKey) : 'catalog'
}

watch(() => route.query.tab, syncTabFromQuery)

onMounted(async () => {
  syncTabFromQuery()
  runHistoryStore.refresh()
  visualsStore.refresh()
  await savedFlowsStore.refresh()
  // Prune favorites whose item no longer exists (flows + tables). Runs after the
  // flow list loads so persisted flow favorites aren't dropped.
  favoritesStore.clearMissing(new Set(allItems.value.map((i) => i.id)))
})

function refresh() {
  runHistoryStore.refresh()
  savedFlowsStore.refresh()
}
</script>

<style scoped>
.catalog-view {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--color-background-primary);
}

/* Tab bar */
.catalog-tabs {
  display: flex;
  gap: 2px;
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  flex-shrink: 0;
}

.catalog-tab {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-4);
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  border-radius: var(--border-radius-md);
  transition: all var(--transition-fast);
}
.catalog-tab i { font-size: 14px; }
.catalog-tab:hover { background: var(--color-background-hover); color: var(--color-text-primary); }
.catalog-tab.active { background: var(--color-background-primary); color: var(--color-primary); box-shadow: var(--shadow-xs); }

.tab-badge {
  background: var(--color-primary);
  color: var(--color-text-inverse);
  font-size: 11px;
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  min-width: 18px;
  text-align: center;
  line-height: 18px;
}

.tab-spacer { flex: 1; }
.info-btn { color: var(--color-text-muted); }
.info-btn:hover { color: var(--color-primary); }

/* Body */
.catalog-body { display: flex; flex: 1; overflow: hidden; }
.catalog-body--full { display: block; overflow-y: auto; padding: var(--spacing-5) var(--spacing-6); }
.catalog-panel-host { flex: 1; min-height: 0; overflow: hidden; }

.catalog-sidebar {
  width: 320px;
  min-width: 280px;
  display: flex;
  flex-direction: column;
  border-right: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.sidebar-filters {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  border-bottom: 1px solid var(--color-border-light);
}

.search-input {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  font-size: var(--font-size-sm);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
}
.search-input:focus { outline: none; border-color: var(--color-accent); box-shadow: 0 0 0 2px var(--color-focus-ring-accent); }

.sidebar-scroll { flex: 1; overflow-y: auto; padding: var(--spacing-3); }
.source-group { margin-bottom: var(--spacing-4); }

.group-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-1) var(--spacing-2);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-muted);
}
.group-count { font-weight: var(--font-weight-medium); }
.group-empty { margin: 0; padding: var(--spacing-2); font-size: var(--font-size-xs); color: var(--color-text-muted); font-style: italic; }

/* Reuse .listbox from main.css */
.listbox li { display: flex; align-items: center; gap: var(--spacing-2); }
.listbox li.unavailable .item-name { color: var(--color-text-muted); }

.item-status { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; background: var(--color-text-muted); }
.item-status--success { background: var(--color-success); }
.item-status--failure { background: var(--color-danger); }
.item-status--pending { background: var(--color-text-muted); }

.item-type-icon { font-size: 14px; width: 16px; text-align: center; color: var(--color-text-secondary); flex-shrink: 0; }
.item-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--color-text-primary); }
.item-meta { font-size: var(--font-size-xs); color: var(--color-text-muted); flex-shrink: 0; }

.item-icon-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  width: 22px;
  height: 22px;
  border: none;
  background: transparent;
  border-radius: var(--border-radius-sm);
  color: var(--color-text-muted);
  cursor: pointer;
  opacity: 0;
  transition: all var(--transition-fast);
}
.listbox li:hover .item-icon-btn, .item-star.active { opacity: 1; }
.item-star.active { color: var(--color-warning); }
.item-star:hover { color: var(--color-warning); }
.item-delete:hover { color: var(--color-danger); }
.item-icon-btn i { font-size: 14px; }

/* Upload table button */
.upload-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  width: 100%;
  height: 32px;
  border: 1px solid var(--color-accent);
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.upload-btn:hover { background: var(--color-accent); color: #fff; }
.upload-btn i { font-size: 15px; }

/* Detail */
.catalog-detail { flex: 1; overflow-y: auto; padding: var(--spacing-5) var(--spacing-6); }

@media (max-width: 768px) {
  .catalog-sidebar { width: 240px; min-width: 220px; }
}
</style>
