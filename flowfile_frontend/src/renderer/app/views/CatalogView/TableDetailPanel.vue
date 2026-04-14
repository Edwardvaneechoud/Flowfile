<template>
  <div class="table-detail">
    <button class="back-btn" @click="emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>
    <!-- Header -->
    <div class="detail-header">
      <div class="header-main">
        <div class="header-title">
          <i
            :class="
              table.table_type === 'virtual'
                ? 'fa-solid fa-bolt header-icon virtual-header-icon'
                : 'fa-solid fa-table header-icon'
            "
          ></i>
          <h2>{{ table.name }}</h2>
          <span v-if="table.table_type === 'virtual'" class="virtual-badge">Virtual</span>
        </div>
        <p v-if="table.description" class="description">{{ table.description }}</p>
      </div>
      <div class="header-actions">
        <button
          class="action-btn-lg"
          :disabled="!table.file_exists"
          @click="emit('queryTable', table.name)"
        >
          <i class="fa-solid fa-code"></i>
          Query table
        </button>
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

    <!-- File missing banner (physical tables only) -->
    <div v-if="table.table_type !== 'virtual' && !table.file_exists" class="missing-banner">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div>
        <strong>Table data not found</strong>
        <p>
          The data file for this table no longer exists on disk. Previews and queries will not work
          until the data is regenerated or the table is re-registered.
        </p>
        <button
          v-if="table.source_run_id"
          class="btn btn-sm btn-primary recovery-btn"
          @click="emit('recoverFromRun', table.source_run_id!)"
        >
          <i class="fa-solid fa-arrow-rotate-left"></i>
          Recover source flow from snapshot
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
      <div v-if="table.table_type !== 'virtual'" class="meta-card">
        <span class="meta-label">Size</span>
        <span class="meta-value">{{ formatSize(table.size_bytes) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Created</span>
        <span class="meta-value">{{ formatDate(table.created_at) }}</span>
      </div>
      <div
        v-if="
          table.table_type === 'virtual' &&
          table.producer_registration_id &&
          table.producer_registration_name
        "
        class="meta-card"
      >
        <span class="meta-label">Produced by</span>
        <span
          class="meta-value meta-link"
          :title="table.producer_registration_name"
          @click="emit('navigateToFlow', table.producer_registration_id)"
        >
          <i class="fa-solid fa-diagram-project"></i>
          <span class="meta-link-text">{{ table.producer_registration_name }}</span>
        </span>
      </div>
      <div
        v-else-if="table.source_registration_id && table.source_registration_name"
        class="meta-card"
      >
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
      <div v-if="table.table_type === 'virtual'" class="meta-card">
        <span class="meta-label">Optimization</span>
        <span class="meta-value" :class="table.is_optimized ? 'optimized-badge' : ''">
          {{ table.is_optimized ? "Optimized" : "Standard" }}
        </span>
      </div>
      <div
        v-if="table.laziness_blockers && table.laziness_blockers.length > 0"
        class="laziness-blockers"
      >
        <span class="blockers-label">Nodes preventing optimization:</span>
        <ul class="blocker-list">
          <li v-for="(reason, i) in table.laziness_blockers" :key="i">{{ reason }}</li>
        </ul>
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
      <div v-if="tableHistory" class="meta-card">
        <span class="meta-label">Version</span>
        <span class="meta-value">{{ tableHistory.current_version }}</span>
      </div>
    </div>

    <!-- Version History (physical tables only) -->
    <div v-if="table.table_type !== 'virtual' && hasHistory" class="section">
      <h3>Version History</h3>
      <div class="version-table-wrapper">
        <table class="styled-table version-table">
          <thead>
            <tr>
              <th>Version</th>
              <th>Timestamp</th>
              <th>Operation</th>
              <th>Parameters</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="entry in tableHistory!.history"
              :key="entry.version"
              class="version-row"
              :class="{
                'version-row-active': selectedVersion === entry.version,
                'version-row-current': entry.version === tableHistory!.current_version,
              }"
              @click="emit('selectVersion', entry.version)"
            >
              <td class="version-number">
                {{ entry.version }}
                <span v-if="entry.version === tableHistory!.current_version" class="version-badge"
                  >current</span
                >
              </td>
              <td>{{ formatTimestamp(entry.timestamp) }}</td>
              <td class="version-operation">{{ entry.operation ?? "--" }}</td>
              <td class="version-params">
                <span class="version-params-cell">
                  {{ truncateParams(formatParams(entry.parameters)) }}
                  <span
                    v-if="formatParams(entry.parameters).length > 60"
                    class="version-params-full"
                  >
                    {{ formatParams(entry.parameters) }}
                  </span>
                </span>
              </td>
              <td class="version-action">
                <span
                  v-if="entry.version !== tableHistory!.current_version"
                  class="version-view-link"
                  >view</span
                >
              </td>
            </tr>
          </tbody>
        </table>
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
        <table class="styled-table schema-table">
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
      <div v-if="isViewingHistorical" class="version-banner">
        <i class="fa-solid fa-clock-rotate-left"></i>
        Viewing version {{ selectedVersion }}
        <button class="version-banner-link" @click="emit('selectVersion', null)">
          Back to latest
        </button>
      </div>
      <div class="section-header">
        <h3>Data Preview</h3>
        <span v-if="preview" class="preview-info">
          Showing {{ preview.rows.length }} of {{ preview.total_rows }} rows
        </span>
      </div>
      <div v-if="loadingPreview" class="loading-state">Loading preview...</div>
      <div v-else-if="preview && preview.rows.length > 0" class="preview-table-wrapper">
        <table class="styled-table preview-table">
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
      <div v-else-if="table.table_type === 'virtual'" class="empty-state">
        This is a virtual table, no data preview available. Use the SQL editor or a flow to view the
        data.
      </div>
      <div v-else class="empty-state">No data to preview.</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import type { CatalogTable, CatalogTablePreview, DeltaTableHistory } from "../../types";
import { formatDate, formatNumber, formatSize } from "./catalog-formatters";

const showReadByModal = ref(false);

const props = defineProps<{
  table: CatalogTable;
  preview: CatalogTablePreview | null;
  loadingPreview: boolean;
  tableHistory: DeltaTableHistory | null;
  selectedVersion: number | null;
}>();

const emit = defineEmits([
  "close",
  "deleteTable",
  "toggleTableFavorite",
  "navigateToFlow",
  "selectVersion",
  "queryTable",
  "recoverFromRun",
]);

const hasHistory = computed(() => props.tableHistory && props.tableHistory.history.length > 0);

const isViewingHistorical = computed(
  () =>
    props.selectedVersion !== null &&
    props.tableHistory !== null &&
    props.selectedVersion !== props.tableHistory.current_version,
);

function formatTimestamp(ts: string | null): string {
  if (!ts) return "--";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return ts;
  return d.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function handleReadByClick(flowId: number) {
  emit("navigateToFlow", flowId);
  showReadByModal.value = false;
}

function formatParams(params: Record<string, any> | null): string {
  if (!params || Object.keys(params).length === 0) return "--";
  return Object.entries(params)
    .map(([k, v]) => `${k}: ${v}`)
    .join(", ");
}

function truncateParams(value: string): string {
  if (value.length <= 60) return value;
  return value.slice(0, 60) + "...";
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

.virtual-header-icon {
  color: var(--el-color-primary, var(--color-primary));
}

.virtual-badge {
  font-size: var(--font-size-xs, 11px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--el-color-primary, var(--color-primary));
  background: var(--el-color-primary-light-9, rgba(64, 158, 255, 0.1));
  padding: 2px 8px;
  border-radius: var(--border-radius-sm, 4px);
  line-height: 1.4;
}

.optimized-badge {
  color: var(--color-success, #67c23a);
  font-weight: var(--font-weight-semibold, 600);
}

.laziness-blockers {
  padding: 10px 12px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  margin-bottom: var(--spacing-3);
}

.blockers-label {
  font-weight: var(--font-weight-medium);
  color: var(--color-warning, #f59e0b);
}

.blocker-list {
  margin: 4px 0 0;
  padding-left: 18px;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.blocker-list li {
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-xs);
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

/* schema-table extends .styled-table */
.schema-table th {
  font-size: var(--font-size-xs);
  text-transform: uppercase;
  letter-spacing: 0.05em;
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

/* preview-table extends .styled-table */
.preview-table {
  table-layout: auto;
  white-space: nowrap;
}

.preview-table th,
.preview-table td {
  width: auto;
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
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* ========== Version History ========== */
.version-table-wrapper {
  max-height: 240px;
  overflow-y: auto;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

/* Override the 3-column width distribution from .styled-table */
.version-table th,
.version-table td {
  width: auto;
}

.version-table th:first-child,
.version-table td:first-child {
  width: 15%;
}

.version-table th:nth-child(2),
.version-table td:nth-child(2) {
  width: 25%;
}

.version-table th:nth-child(3),
.version-table td:nth-child(3) {
  width: 15%;
}

.version-table th:nth-child(4),
.version-table td:nth-child(4) {
  width: 35%;
}

.version-table th:last-child,
.version-table td:last-child {
  width: 10%;
}

.version-table th {
  font-size: var(--font-size-xs);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.version-row {
  cursor: pointer;
  transition: background var(--transition-fast);
}

.version-row:hover {
  background: var(--color-background-hover, rgba(0, 0, 0, 0.03));
}

.version-row-active {
  background: var(--color-primary-light, rgba(59, 130, 246, 0.08));
}

.version-row-current td {
  font-weight: var(--font-weight-medium);
}

.version-number {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-weight: var(--font-weight-medium);
}

.version-badge {
  font-size: 10px;
  font-weight: var(--font-weight-semibold);
  color: var(--color-success, #16a34a);
  background: var(--color-success-light, rgba(22, 163, 74, 0.1));
  padding: 1px 6px;
  border-radius: var(--border-radius-sm);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.version-operation {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.version-params {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  position: relative;
  max-width: 0;
}

.version-params-cell {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: default;
}

.version-params-full {
  display: none;
  position: absolute;
  left: 0;
  top: 0;
  z-index: 10;
  background: var(--color-background-primary, #fff);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  padding: var(--spacing-2) var(--spacing-3);
  white-space: pre-wrap;
  word-break: break-all;
  max-width: 400px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12);
}

.version-params-cell:hover .version-params-full {
  display: block;
}

.version-action {
  text-align: right;
}

.version-view-link {
  font-size: var(--font-size-xs);
  color: var(--color-primary);
  cursor: pointer;
}

.version-view-link:hover {
  text-decoration: underline;
}

/* ========== Version Banner ========== */
.version-banner {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  margin-bottom: var(--spacing-3);
  background: var(--color-warning-light, rgba(234, 179, 8, 0.1));
  border: 1px solid var(--color-warning, #eab308);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.version-banner i {
  color: var(--color-warning, #eab308);
}

.version-banner-link {
  margin-left: auto;
  background: none;
  border: none;
  color: var(--color-primary);
  font-size: var(--font-size-sm);
  cursor: pointer;
  padding: 0;
  text-decoration: underline;
}

.version-banner-link:hover {
  color: var(--color-primary-dark, #1d4ed8);
}

/* ========== Missing File Banner ========== */
.missing-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  margin-bottom: var(--spacing-4);
  background: color-mix(in srgb, var(--color-warning) 8%, transparent);
  border: 1px solid color-mix(in srgb, var(--color-warning) 30%, transparent);
  border-radius: var(--border-radius-md);
  color: var(--color-text-primary);
}

.missing-banner > i {
  color: var(--color-warning);
  font-size: var(--font-size-lg);
  margin-top: 2px;
  flex-shrink: 0;
}

.missing-banner strong {
  font-size: var(--font-size-sm);
  display: block;
  margin-bottom: var(--spacing-1);
}

.missing-banner p {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.recovery-btn {
  margin-top: var(--spacing-2);
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
}

.action-btn-lg:disabled {
  opacity: 0.4;
  cursor: not-allowed;
  pointer-events: none;
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
