<template>
  <div class="table-detail">
    <button class="back-btn" @click="emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>
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
        <button
          class="action-btn-lg"
          :class="{ active: table.is_favorite }"
          @click="emit('toggleTableFavorite', table.id)"
        >
          <i :class="table.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
          {{ table.is_favorite ? "Favorited" : "Favorite" }}
        </button>
        <button
          class="btn btn-danger btn-sm"
          title="Delete table"
          @click="emit('deleteTable', table.id)"
        >
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
      <div v-if="table.source_registration_id && table.source_registration_name" class="meta-card">
        <span class="meta-label">Produced by</span>
        <span
          class="meta-value meta-link"
          :title="table.source_registration_name"
          @click="emit('navigateToFlow', table.source_registration_id)"
        >
          <i class="fa-solid fa-diagram-project"></i>
          <span class="meta-link-text">{{ table.source_registration_name }}</span>
        </span>
      </div>
      <div
        v-if="table.read_by_flows && table.read_by_flows.length > 0"
        class="meta-card meta-card-clickable"
        @click="showReadByModal = true"
      >
        <span class="meta-label">Read by</span>
        <span class="meta-value">
          {{ table.read_by_flows.length }} flow{{ table.read_by_flows.length !== 1 ? "s" : "" }}
        </span>
      </div>
    </div>

    <!-- Read by Flows Modal -->
    <Teleport to="body">
      <div v-if="showReadByModal" class="modal-overlay" @click.self="showReadByModal = false">
        <div class="modal-container">
          <div class="modal-header">
            <h3 class="modal-title">Flows reading "{{ table.name }}"</h3>
            <button class="modal-close" @click="showReadByModal = false">
              <i class="fa-solid fa-xmark"></i>
            </button>
          </div>
          <div class="modal-content">
            <div
              v-for="flow in table.read_by_flows"
              :key="flow.id"
              class="read-by-item"
              @click="handleReadByClick(flow.id)"
            >
              <i class="fa-solid fa-diagram-project read-by-icon"></i>
              <span>{{ flow.name }}</span>
            </div>
          </div>
        </div>
      </div>
    </Teleport>

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
import { ref } from "vue";
import type { CatalogTable, CatalogTablePreview } from "../../types";
import { formatDate, formatNumber, formatSize } from "./catalog-formatters";

const showReadByModal = ref(false);

defineProps<{
  table: CatalogTable;
  preview: CatalogTablePreview | null;
  loadingPreview: boolean;
}>();

const emit = defineEmits<{
  close: [];
  deleteTable: [id: number];
  toggleTableFavorite: [id: number];
  navigateToFlow: [registrationId: number];
}>();

function handleReadByClick(flowId: number) {
  emit("navigateToFlow", flowId);
  showReadByModal.value = false;
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
  margin: 0 auto;
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

.action-btn-lg {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-3);
  background: transparent;
  color: var(--color-text-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-xs);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.action-btn-lg:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}
.action-btn-lg.active {
  color: var(--color-warning);
  border-color: var(--color-warning);
}

/* ========== Meta Grid (override minmax for table) ========== */
.meta-grid {
  grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
  margin-bottom: var(--spacing-6);
}

.meta-card {
  display: block;
}

.meta-label {
  display: block;
  margin-bottom: var(--spacing-1);
}

.meta-value {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
}

.meta-link {
  cursor: pointer;
  color: var(--color-primary);
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
  min-width: 0;
}

.meta-link-text {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.meta-link:hover {
  text-decoration: underline;
}

.meta-card-clickable {
  cursor: pointer;
  transition: all var(--transition-fast);
}

.meta-card-clickable:hover {
  border-color: var(--color-primary);
}

.read-by-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.read-by-item:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

.read-by-icon {
  color: var(--color-primary);
  font-size: var(--font-size-xs);
}

/* ========== Sections ========== */
.section {
  margin-bottom: var(--spacing-6);
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

.schema-table .col-type {
  font-family: var(--font-family-mono);
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
  font-family: var(--font-family-mono);
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
.loading-state {
  padding: var(--spacing-6);
  text-align: center;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

/* ========== Modal (read-by list items) ========== */
.modal-content {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}
</style>
