<template>
  <div class="table-detail">
    <!-- Header -->
    <div class="detail-header">
      <div class="header-main">
        <div class="header-title">
          <i class="fa-solid fa-table header-icon"></i>
          <h2>{{ table.name }}</h2>
        </div>
        <p v-if="table.description" class="description">{{ table.description }}</p>
      </div>
      <div class="header-actions">
        <button class="btn-danger-outline" title="Delete table" @click="$emit('deleteTable', table.id)">
          <i class="fa-solid fa-trash"></i>
          Delete
        </button>
      </div>
    </div>

    <!-- Metadata Grid -->
    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Rows</span>
        <span class="meta-value">{{ formatNumber(table.row_count) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Columns</span>
        <span class="meta-value">{{ table.column_count ?? "--" }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Size</span>
        <span class="meta-value">{{ formatSize(table.size_bytes) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Created</span>
        <span class="meta-value">{{ formatDate(table.created_at) }}</span>
      </div>
    </div>

    <!-- Schema -->
    <div v-if="table.schema_columns && table.schema_columns.length > 0" class="section">
      <h3>Schema</h3>
      <div class="schema-table-wrapper">
        <table class="schema-table">
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="col in table.schema_columns" :key="col.name">
              <td class="col-name">{{ col.name }}</td>
              <td class="col-type">{{ col.dtype }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Data Preview -->
    <div class="section">
      <div class="section-header">
        <h3>Data Preview</h3>
        <span v-if="preview" class="preview-info">
          Showing {{ preview.rows.length }} of {{ preview.total_rows }} rows
        </span>
      </div>
      <div v-if="loadingPreview" class="loading-state">Loading preview...</div>
      <div v-else-if="preview && preview.rows.length > 0" class="preview-table-wrapper">
        <table class="preview-table">
          <thead>
            <tr>
              <th v-for="(col, idx) in preview.columns" :key="idx">
                <div class="col-header">
                  <span class="col-header-name">{{ col }}</span>
                  <span class="col-header-type">{{ preview.dtypes[idx] }}</span>
                </div>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, rIdx) in preview.rows" :key="rIdx">
              <td v-for="(cell, cIdx) in row" :key="cIdx">{{ formatCell(cell) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div v-else class="empty-state">No data to preview.</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { CatalogTable, CatalogTablePreview } from "../../types";

defineProps<{
  table: CatalogTable;
  preview: CatalogTablePreview | null;
  loadingPreview: boolean;
}>();

defineEmits<{
  deleteTable: [id: number];
}>();

function formatNumber(n: number | null | undefined): string {
  if (n === null || n === undefined) return "--";
  return n.toLocaleString();
}

function formatSize(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return "--";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function formatDate(isoStr: string): string {
  const d = new Date(isoStr);
  return d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

function formatCell(value: any): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "number" && !Number.isInteger(value)) {
    return value.toFixed(4);
  }
  return String(value);
}
</script>

<style scoped>
.table-detail {
  max-width: 1000px;
}

/* ========== Header ========== */
.detail-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: var(--spacing-6);
}

.header-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.header-icon {
  font-size: 20px;
  color: var(--color-primary);
}

.header-title h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
}

.description {
  margin: var(--spacing-2) 0 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.header-actions {
  display: flex;
  gap: var(--spacing-2);
}

.btn-danger-outline {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-3);
  background: transparent;
  color: #ef4444;
  border: 1px solid #ef4444;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-xs);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.btn-danger-outline:hover {
  background: rgba(239, 68, 68, 0.1);
}

/* ========== Meta Grid ========== */
.meta-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-6);
}

.meta-card {
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.meta-label {
  display: block;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: var(--spacing-1);
}

.meta-value {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

/* ========== Sections ========== */
.section {
  margin-bottom: var(--spacing-6);
}

.section h3 {
  margin: 0 0 var(--spacing-3) 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: var(--spacing-3);
}

.section-header h3 {
  margin: 0;
}

.preview-info {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

/* ========== Schema Table ========== */
.schema-table-wrapper {
  max-height: 300px;
  overflow-y: auto;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.schema-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
}

.schema-table th {
  position: sticky;
  top: 0;
  background: var(--color-background-secondary);
  padding: var(--spacing-2) var(--spacing-3);
  text-align: left;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  border-bottom: 1px solid var(--color-border-light);
}

.schema-table td {
  padding: var(--spacing-1) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
}

.col-name {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.col-type {
  font-family: monospace;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

/* ========== Data Preview Table ========== */
.preview-table-wrapper {
  max-height: 500px;
  overflow: auto;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.preview-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
  white-space: nowrap;
}

.preview-table th {
  position: sticky;
  top: 0;
  background: var(--color-background-secondary);
  padding: var(--spacing-2) var(--spacing-3);
  text-align: left;
  border-bottom: 1px solid var(--color-border-light);
  z-index: 1;
}

.col-header {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.col-header-name {
  font-weight: var(--font-weight-medium);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.col-header-type {
  font-family: monospace;
  font-size: 10px;
  color: var(--color-text-muted);
}

.preview-table td {
  padding: var(--spacing-1) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  color: var(--color-text-primary);
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.preview-table tbody tr:hover {
  background: var(--color-background-hover);
}

/* ========== States ========== */
.loading-state,
.empty-state {
  padding: var(--spacing-6);
  text-align: center;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}
</style>
