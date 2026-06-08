<template>
  <div class="catalog-view">
    <!-- Sidebar -->
    <aside class="catalog-sidebar">
      <div class="sidebar-header">
        <h3>Data Sources</h3>
        <span class="source-count">{{ items.length }}</span>
      </div>
      <div class="sidebar-filters">
        <input
          v-model="search"
          type="text"
          class="search-input"
          placeholder="Search data sources..."
        />
        <label class="unavailable-toggle">
          <input v-model="showUnavailable" type="checkbox" />
          <span>Show unavailable</span>
        </label>
      </div>

      <div class="sidebar-scroll">
        <div v-for="group in groups" :key="group.key" class="source-group">
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
            </li>
          </ul>
          <p v-else class="group-empty">{{ group.emptyText }}</p>
        </div>
      </div>
    </aside>

    <!-- Detail -->
    <section class="catalog-detail">
      <CatalogDetailPanel v-if="selected" :item="selected" />
      <div v-else class="detail-empty">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/>
        </svg>
        <p>Select a data source to view its schema and preview.</p>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { storeToRefs } from 'pinia'
import { useFlowStore } from '../stores/flow-store'
import { inferSchemaFromCsv } from '../stores/schema-inference'
import CatalogDetailPanel from '../components/catalog/CatalogDetailPanel.vue'
import type { CatalogItem, CatalogStatus } from '../components/catalog/types'
import type { DataPreview, NodeResult } from '../types'

const flowStore = useFlowStore()
// `getNode` (used in the items computed) establishes the reactive dep on nodes,
// so we only need to pull the collections we iterate directly here.
const { nodeResults, fileContents, externalDatasets } = storeToRefs(flowStore)

const search = ref('')
const showUnavailable = ref(true)
const selectedId = ref<string | null>(null)

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

// Quote-aware split for a single CSV line (handles "a,b" and escaped "").
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

// Lightweight head preview for raw CSV content (files / external datasets).
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

  // 1. Loaded CSV files (input nodes that have content in the browser)
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

  // Missing-file inputs (referenced but no content loaded) — shown as unavailable
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

  // 2. Host-injected external datasets
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

  // 3. Node outputs (executed transformation/output nodes — not the raw sources)
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

const groups = computed(() => {
  const q = search.value.trim().toLowerCase()
  const match = (i: CatalogItem) =>
    (showUnavailable.value || !i.unavailable) &&
    (!q || i.name.toLowerCase().includes(q) || (i.subtitle?.toLowerCase().includes(q) ?? false))

  const byKind = (kind: CatalogItem['kind']) => items.value.filter((i) => i.kind === kind && match(i))

  return [
    { key: 'file', label: 'Loaded files', items: byKind('file'), emptyText: 'No CSV files loaded yet.' },
    { key: 'external', label: 'External datasets', items: byKind('external'), emptyText: 'No external datasets provided.' },
    { key: 'output', label: 'Node outputs', items: byKind('output'), emptyText: 'Run nodes to populate outputs.' }
  ]
})

const selected = computed(() => items.value.find((i) => i.id === selectedId.value) ?? null)

function itemMeta(item: CatalogItem): string {
  if (item.unavailable) return 'missing'
  if (item.rows != null) return `${item.rows.toLocaleString()} rows`
  if (item.columns != null) return `${item.columns} cols`
  return ''
}
</script>

<style scoped>
.catalog-view {
  display: flex;
  height: 100%;
  background: var(--color-background-primary);
}

/* Sidebar */
.catalog-sidebar {
  width: 320px;
  min-width: 280px;
  display: flex;
  flex-direction: column;
  border-right: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.sidebar-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-3) var(--spacing-4);
  border-bottom: 1px solid var(--color-border-light);
}

.sidebar-header h3 {
  margin: 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.source-count {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  background: var(--color-background-tertiary);
  border-radius: var(--border-radius-full);
  padding: 1px 8px;
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

.search-input:focus {
  outline: none;
  border-color: var(--color-accent);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}

.unavailable-toggle {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  cursor: pointer;
}

.unavailable-toggle input { accent-color: var(--color-accent); }

.sidebar-scroll {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-3);
}

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

.group-empty {
  margin: 0;
  padding: var(--spacing-2);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  font-style: italic;
}

/* Reuse .listbox from main.css; tune the item rows here */
.listbox li {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.listbox li.unavailable .item-name { color: var(--color-text-muted); }

.item-status {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  background: var(--color-text-muted);
}

.item-status--success { background: var(--color-success); }
.item-status--failure { background: var(--color-danger); }
.item-status--pending { background: var(--color-text-muted); }

.item-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--color-text-primary);
}

.item-meta {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  flex-shrink: 0;
}

/* Detail */
.catalog-detail {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-5) var(--spacing-6);
}

.detail-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-3);
  height: 100%;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.detail-empty svg { width: 48px; height: 48px; opacity: 0.5; }

@media (max-width: 768px) {
  .catalog-sidebar { width: 240px; min-width: 220px; }
}
</style>
