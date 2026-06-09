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
        <span class="material-icons">{{ tab.icon }}</span>
        <span>{{ tab.label }}</span>
        <span v-if="tab.badge !== null" class="tab-badge">{{ tab.badge }}</span>
      </button>
      <div class="tab-spacer"></div>
      <button class="catalog-tab info-btn" title="Refresh" @click="refresh">
        <span class="material-icons">refresh</span>
      </button>
    </div>

    <!-- Catalog / Favorites: sidebar + detail -->
    <div v-if="activeTab !== 'runs'" class="catalog-body">
      <aside class="catalog-sidebar">
        <div class="sidebar-filters">
          <input v-model="search" type="text" class="search-input" placeholder="Search tables..." />
          <label class="unavailable-toggle">
            <input v-model="showUnavailable" type="checkbox" />
            <span>Show unavailable</span>
          </label>
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
                <span class="item-status" :class="`item-status--${item.status || 'pending'}`"></span>
                <span class="item-name">{{ item.name }}</span>
                <span class="item-meta">{{ itemMeta(item) }}</span>
                <button
                  class="item-star"
                  :class="{ active: favoritesStore.isFavorite(item.id) }"
                  :title="favoritesStore.isFavorite(item.id) ? 'Remove favorite' : 'Add favorite'"
                  @click.stop="favoritesStore.toggle(item.id)"
                >
                  <span class="material-icons">{{ favoritesStore.isFavorite(item.id) ? 'star' : 'star_border' }}</span>
                </button>
              </li>
            </ul>
            <p v-else class="group-empty">{{ group.emptyText }}</p>
          </div>
        </div>
      </aside>

      <section class="catalog-detail">
        <CatalogDetailPanel v-if="selected" :item="selected" />
        <StatsPanel
          v-else
          :tables="items.length"
          :runs="runHistoryStore.total"
          :success="runHistoryStore.successCount"
          :favorites="favoritesStore.count"
          @open-tab="setTab"
          @go-designer="router.push({ name: 'designer' })"
        />
      </section>
    </div>

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
import type { CatalogItem, CatalogStatus } from '../components/catalog/types'
import type { DataPreview, NodeResult } from '../types'

type TabKey = 'catalog' | 'favorites' | 'runs'

const route = useRoute()
const router = useRouter()
const flowStore = useFlowStore()
const favoritesStore = useFavoritesStore()
const runHistoryStore = useRunHistoryStore()
// `getNode` (used in the items computed) establishes the reactive dep on nodes.
const { nodeResults, fileContents, externalDatasets } = storeToRefs(flowStore)

const search = ref('')
const showUnavailable = ref(true)
const selectedId = ref<string | null>(null)
const activeTab = ref<TabKey>('catalog')

const SOURCE_TYPES = new Set(['read', 'manual_input', 'external_data'])
const TYPE_LABELS: Record<string, string> = {
  read: 'Read CSV', manual_input: 'Manual Input', external_data: 'External Data',
  filter: 'Filter', select: 'Select', sort: 'Sort', polars_code: 'Polars Code',
  unique: 'Unique', head: 'Take Sample', join: 'Join', group_by: 'Group By',
  pivot: 'Pivot', unpivot: 'Unpivot', explore_data: 'Explore Data',
  output: 'Write Data', external_output: 'External Output'
}
const typeLabel = (t: string) => TYPE_LABELS[t] || t

function statusOf(result?: NodeResult): CatalogStatus {
  if (!result) return 'pending'
  if (result.success === true) return 'success'
  if (result.success === false) return 'failure'
  return 'pending'
}

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

function inputName(nodeId: number): string {
  const node = flowStore.getNode(nodeId)
  if (!node) return `Node #${nodeId}`
  const s = (node.settings ?? {}) as Record<string, any>
  if (node.type === 'read') return s.received_file?.name || s.file_name || node.description || `CSV #${nodeId}`
  if (node.type === 'external_data') return s.dataset_name || node.description || `External #${nodeId}`
  if (node.type === 'manual_input') return node.description || `Manual input #${nodeId}`
  return node.description || typeLabel(node.type)
}

const items = computed<CatalogItem[]>(() => {
  const out: CatalogItem[] = []

  for (const [nodeId, content] of fileContents.value) {
    const node = flowStore.getNode(nodeId)
    if (!node) continue
    const result = nodeResults.value.get(nodeId)
    out.push({
      id: `file-${nodeId}`,
      kind: 'file',
      name: inputName(nodeId),
      subtitle: typeLabel(node.type),
      nodeId,
      schema: result?.schema ?? inferSchemaFromCsv(content) ?? undefined,
      rows: result?.data?.total_rows ?? null,
      columns: result?.schema?.length ?? null,
      sizeBytes: byteSize(content),
      status: statusOf(result),
      preview: result?.data ?? parseCsvHead(content)
    })
  }

  for (const mf of flowStore.getMissingFileNodes()) {
    out.push({
      id: `missing-${mf.nodeId}`,
      kind: 'file',
      name: mf.fileName || `Node #${mf.nodeId}`,
      subtitle: 'File not loaded',
      nodeId: mf.nodeId,
      unavailable: true,
      status: 'failure'
    })
  }

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

  for (const [nodeId, result] of nodeResults.value) {
    const node = flowStore.getNode(nodeId)
    if (!node || SOURCE_TYPES.has(node.type)) continue
    out.push({
      id: `out-${nodeId}`,
      kind: 'output',
      name: node.description?.trim() || typeLabel(node.type),
      subtitle: `#${nodeId} · ${typeLabel(node.type)}`,
      nodeId,
      schema: result.schema,
      rows: result.data?.total_rows ?? result.download?.row_count ?? null,
      columns: result.schema?.length ?? null,
      status: statusOf(result),
      preview: result.data ?? null,
      download: result.download
    })
  }

  return out
})

const tabs = computed(() => [
  { key: 'catalog' as TabKey, label: 'Catalog', icon: 'account_tree', badge: items.value.length },
  { key: 'favorites' as TabKey, label: 'Favorites', icon: 'star', badge: favoritesStore.count },
  { key: 'runs' as TabKey, label: 'Run History', icon: 'history', badge: runHistoryStore.total }
])

const displayGroups = computed(() => {
  const q = search.value.trim().toLowerCase()
  const onlyFavorites = activeTab.value === 'favorites'
  const match = (i: CatalogItem) =>
    (showUnavailable.value || !i.unavailable) &&
    (!onlyFavorites || favoritesStore.isFavorite(i.id)) &&
    (!q || i.name.toLowerCase().includes(q) || (i.subtitle?.toLowerCase().includes(q) ?? false))

  const byKind = (kind: CatalogItem['kind']) => items.value.filter((i) => i.kind === kind && match(i))

  return [
    { key: 'file', label: 'Loaded files', items: byKind('file'), emptyText: onlyFavorites ? 'No favorited files.' : 'No CSV files loaded yet.' },
    { key: 'external', label: 'External datasets', items: byKind('external'), emptyText: onlyFavorites ? 'No favorited datasets.' : 'No external datasets provided.' },
    { key: 'output', label: 'Node outputs', items: byKind('output'), emptyText: onlyFavorites ? 'No favorited outputs.' : 'Run nodes to populate outputs.' }
  ]
})

const selected = computed(() => items.value.find((i) => i.id === selectedId.value) ?? null)

function itemMeta(item: CatalogItem): string {
  if (item.unavailable) return 'missing'
  if (item.rows != null) return `${item.rows.toLocaleString()} rows`
  if (item.columns != null) return `${item.columns} cols`
  return ''
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
  activeTab.value = q === 'runs' || q === 'favorites' ? q : 'catalog'
}

watch(() => route.query.tab, syncTabFromQuery)

onMounted(() => {
  syncTabFromQuery()
  runHistoryStore.refresh()
  // Prune favorites whose item no longer exists.
  favoritesStore.clearMissing(new Set(items.value.map((i) => i.id)))
})

function refresh() {
  runHistoryStore.refresh()
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
.catalog-tab .material-icons { font-size: 16px; }
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

.unavailable-toggle {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  cursor: pointer;
}
.unavailable-toggle input { accent-color: var(--color-accent); }

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

.item-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--color-text-primary); }
.item-meta { font-size: var(--font-size-xs); color: var(--color-text-muted); flex-shrink: 0; }

.item-star {
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
.listbox li:hover .item-star, .item-star.active { opacity: 1; }
.item-star.active { color: var(--color-warning); }
.item-star:hover { color: var(--color-warning); }
.item-star .material-icons { font-size: 16px; }

/* Detail */
.catalog-detail { flex: 1; overflow-y: auto; padding: var(--spacing-5) var(--spacing-6); }

@media (max-width: 768px) {
  .catalog-sidebar { width: 240px; min-width: 220px; }
}
</style>
