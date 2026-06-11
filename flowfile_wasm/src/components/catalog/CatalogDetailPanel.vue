<template>
  <div class="catalog-detail-panel">
    <!-- Header -->
    <div class="detail-header">
      <div class="detail-title">
        <span class="detail-icon" :class="`detail-icon--${item.kind}`">
          <i v-if="item.kind === 'file'" class="fa-solid fa-file"></i>
          <i v-else-if="item.kind === 'external'" class="fa-solid fa-database"></i>
          <i v-else-if="item.kind === 'catalog'" class="fa-solid fa-table"></i>
          <i v-else class="fa-solid fa-chart-column"></i>
        </span>
        <h2 class="detail-name">{{ item.name }}</h2>
        <span class="kind-badge" :class="`kind-badge--${item.kind}`">{{ kindLabel }}</span>
      </div>
      <p v-if="item.subtitle" class="detail-subtitle">{{ item.subtitle }}</p>
    </div>

    <!-- Unavailable banner -->
    <div v-if="item.unavailable" class="missing-banner">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div>
        <strong>File not loaded</strong>
        <p>This input references a file whose contents aren't available in the browser. Re-select the file in the node.</p>
      </div>
    </div>

    <!-- Metadata grid -->
    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Status</span>
        <span class="meta-value">
          <span class="status-badge" :class="`status-badge--${item.status || 'pending'}`">
            <span class="status-dot"></span>{{ statusLabel }}
          </span>
        </span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Rows</span>
        <span class="meta-value">{{ item.rows != null ? formatNumber(item.rows) : '—' }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Columns</span>
        <span class="meta-value">{{ item.columns != null ? item.columns : (item.schema?.length ?? '—') }}</span>
      </div>
      <div v-if="item.sizeBytes != null" class="meta-card">
        <span class="meta-label">Size</span>
        <span class="meta-value">{{ formatBytes(item.sizeBytes) }}</span>
      </div>
      <div v-if="item.nodeId != null" class="meta-card">
        <span class="meta-label">Source node</span>
        <span class="meta-value">#{{ item.nodeId }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Type</span>
        <span class="meta-value">{{ kindLabel }}</span>
      </div>
    </div>

    <!-- Schema -->
    <div v-if="item.schema && item.schema.length" class="detail-section">
      <h3 class="section-title">Schema</h3>
      <div class="schema-table-wrapper">
        <table class="styled-table schema-table">
          <thead>
            <tr><th>Column</th><th>Type</th></tr>
          </thead>
          <tbody>
            <tr v-for="col in item.schema" :key="col.name">
              <td>{{ col.name }}</td>
              <td><code class="dtype">{{ col.data_type }}</code></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Data preview -->
    <div class="detail-section">
      <div class="section-head">
        <h3 class="section-title">Preview</h3>
        <button
          v-if="item.download"
          class="ghost-btn"
          @click="downloadOutput"
        >Download CSV</button>
      </div>

      <div v-if="item.preview && item.preview.columns.length" class="schema-table-wrapper preview-wrapper">
        <table class="styled-table preview-table">
          <thead>
            <tr><th v-for="col in item.preview.columns" :key="col">{{ col }}</th></tr>
          </thead>
          <tbody>
            <tr v-for="(row, ri) in previewRows" :key="ri">
              <td v-for="(cell, ci) in row" :key="ci">{{ formatCell(cell) }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-else class="preview-empty">
        <p>No preview available yet.</p>
        <button
          v-if="item.nodeId != null && !item.unavailable"
          class="ghost-btn"
          :disabled="isFetching"
          @click="fetchPreview"
        >{{ isFetching ? 'Fetching…' : 'Fetch preview' }}</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { CatalogItem } from './types'

const props = defineProps<{ item: CatalogItem }>()

const flowStore = useFlowStore()
const isFetching = ref(false)

const kindLabel = computed(() => {
  switch (props.item.kind) {
    case 'file': return 'Loaded file'
    case 'external': return 'External dataset'
    case 'catalog': return 'Catalog table'
    default: return 'Node output'
  }
})

const statusLabel = computed(() => {
  switch (props.item.status) {
    case 'success': return 'Ready'
    case 'failure': return props.item.unavailable ? 'Unavailable' : 'Failed'
    default: return 'Not run'
  }
})

// Cap the rendered preview rows so large datasets stay snappy.
const previewRows = computed(() => (props.item.preview?.data ?? []).slice(0, 50))

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function formatNumber(n: number): string {
  return n.toLocaleString()
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

async function fetchPreview() {
  if (props.item.nodeId == null || isFetching.value) return
  isFetching.value = true
  try {
    const result = await flowStore.executeNodeWithUpstream(props.item.nodeId)
    if (result.success) {
      await flowStore.fetchNodePreview(props.item.nodeId, { maxRows: 100 })
    }
  } finally {
    isFetching.value = false
  }
}

async function downloadOutput() {
  if (props.item.nodeId == null) return
  const dl = await flowStore.getDownloadContent(props.item.nodeId)
  if (!dl) return
  const blob = new Blob([dl.content], { type: dl.mimeType })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = dl.fileName
  a.click()
  URL.revokeObjectURL(url)
}
</script>

<style scoped>
.catalog-detail-panel {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-5);
}

.detail-header {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.detail-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.detail-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  color: var(--color-accent);
}

.detail-icon i { font-size: 15px; }

.detail-name {
  margin: 0;
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.kind-badge {
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  background: var(--color-background-tertiary);
  color: var(--color-text-secondary);
}

.kind-badge--output { background: var(--color-accent-subtle); color: var(--color-accent); }

.detail-subtitle {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.missing-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  background: var(--color-warning-light);
  border: 1px solid var(--color-warning);
  border-radius: var(--border-radius-md);
  color: var(--color-warning-dark);
}

.missing-banner i { font-size: 17px; flex-shrink: 0; margin-top: 2px; }
.missing-banner strong { font-size: var(--font-size-sm); }
.missing-banner p { margin: 2px 0 0; font-size: var(--font-size-xs); }

/* Metadata grid (mirrors flowfile_frontend CatalogView) */
.meta-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
  gap: var(--spacing-3);
}

.meta-card {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.meta-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.meta-value {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.status-dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.status-badge--success { background: color-mix(in srgb, var(--color-success) 14%, transparent); color: var(--color-success); }
.status-badge--failure { background: color-mix(in srgb, var(--color-danger) 14%, transparent); color: var(--color-danger); }
.status-badge--pending { background: var(--color-background-tertiary); color: var(--color-text-muted); }

.detail-section {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.section-title {
  margin: 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.schema-table-wrapper {
  max-height: 280px;
  overflow: auto;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.preview-wrapper { overflow: auto; }

/* Override the fixed 35/30/35 column split from main.css .styled-table */
.schema-table th,
.schema-table td { width: auto !important; }
.schema-table th:first-child,
.schema-table td:first-child { width: 60% !important; }

.preview-table { table-layout: auto; }
.preview-table th,
.preview-table td { width: auto !important; white-space: nowrap; }

.dtype {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-accent);
}

.preview-empty {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border: 1px dashed var(--color-border-primary);
  border-radius: var(--border-radius-md);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.preview-empty p { margin: 0; }

.ghost-btn {
  display: inline-flex;
  align-items: center;
  height: 28px;
  padding: 0 12px;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.ghost-btn:hover:not(:disabled) {
  background: var(--color-background-tertiary);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.ghost-btn:disabled { opacity: 0.6; cursor: not-allowed; }
</style>
