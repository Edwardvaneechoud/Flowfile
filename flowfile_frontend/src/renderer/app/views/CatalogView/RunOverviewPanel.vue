<template>
  <div class="run-overview">
    <h2>Run History</h2>

    <!-- Summary cards -->
    <div class="summary-cards">
      <div class="summary-card">
        <i class="fa-solid fa-clock-rotate-left summary-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ catalogStore.runsTotal }}</span>
          <span class="summary-label">Total</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-circle-check summary-icon success-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ successCount }}</span>
          <span class="summary-label">Successful</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-circle-xmark summary-icon failure-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ failedCount }}</span>
          <span class="summary-label">Failed</span>
        </div>
      </div>
      <div class="summary-card">
        <i class="fa-solid fa-spinner summary-icon running-icon"></i>
        <div class="summary-info">
          <span class="summary-value">{{ runningCount }}</span>
          <span class="summary-label">Running</span>
        </div>
      </div>
    </div>

    <!-- Empty state -->
    <div v-if="catalogStore.runs.length === 0" class="empty-state">
      <i class="fa-solid fa-clock-rotate-left empty-icon"></i>
      <h3>No runs recorded yet</h3>
      <p>Run a registered flow to see its execution history here.</p>
    </div>

    <!-- Runs table -->
    <div v-else class="runs-table">
      <div class="table-header">
        <span class="col-status">Status</span>
        <span class="col-flow">Flow</span>
        <span class="col-trigger">Triggered By</span>
        <span class="col-started">Started</span>
        <span class="col-duration">Duration</span>
        <span class="col-nodes">Nodes</span>
        <span class="col-version">Version</span>
      </div>
      <div
        v-for="run in catalogStore.runs"
        :key="run.id"
        class="table-row"
        @click="$emit('viewRun', run.id)"
      >
        <div class="col-status">
          <span class="status-badge" :class="runStatusClass(run)">
            <i v-if="run.success === null" class="fa-solid fa-spinner fa-spin" />
            <i v-else-if="run.success" class="fa-solid fa-circle-check" />
            <i v-else class="fa-solid fa-circle-xmark" />
            {{ runStatusText(run) }}
          </span>
        </div>
        <div class="col-flow">
          <span
            class="flow-name flow-link"
            @click.stop="run.registration_id && $emit('viewFlow', run.registration_id)"
          >
            {{ run.flow_name }}
          </span>
        </div>
        <div class="col-trigger">
          <i :class="runTypeIcon(run.run_type)" class="trigger-icon" />
          {{ formatRunType(run.run_type) }}
        </div>
        <div class="col-started">
          {{ formatDate(run.started_at) }}
        </div>
        <div class="col-duration">
          {{ formatDuration(run.duration_seconds) }}
        </div>
        <div class="col-nodes">{{ run.nodes_completed }} / {{ run.number_of_nodes }}</div>
        <div class="col-version">
          <span v-if="run.has_snapshot" class="snapshot-link">
            <i class="fa-solid fa-code-branch" /> View
          </span>
          <span v-else class="no-snapshot">--</span>
        </div>
      </div>
    </div>

    <!-- Pagination -->
    <div v-if="catalogStore.runsTotal > catalogStore.runsPageSize" class="pagination-bar">
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage <= 1"
        @click="catalogStore.setRunsPage(1)"
      >
        <i class="fa-solid fa-angles-left" />
      </button>
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage <= 1"
        @click="catalogStore.setRunsPage(catalogStore.runsPage - 1)"
      >
        <i class="fa-solid fa-angle-left" />
      </button>
      <span class="page-info">
        Page {{ catalogStore.runsPage }} of {{ catalogStore.runsTotalPages }}
      </span>
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage >= catalogStore.runsTotalPages"
        @click="catalogStore.setRunsPage(catalogStore.runsPage + 1)"
      >
        <i class="fa-solid fa-angle-right" />
      </button>
      <button
        class="page-btn"
        :disabled="catalogStore.runsPage >= catalogStore.runsTotalPages"
        @click="catalogStore.setRunsPage(catalogStore.runsTotalPages)"
      >
        <i class="fa-solid fa-angles-right" />
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowRun } from "../../types";
import { formatDate, formatDuration, formatRunType, runTypeIcon } from "./catalog-formatters";

const catalogStore = useCatalogStore();

defineEmits<{
  viewRun: [runId: number];
  viewFlow: [registrationId: number];
}>();

const successCount = computed(() => catalogStore.runsTotalSuccess);
const failedCount = computed(() => catalogStore.runsTotalFailed);
const runningCount = computed(() => catalogStore.runsTotalRunning);

function runStatusClass(run: FlowRun): string {
  if (run.success === null) return "running";
  return run.success ? "success" : "failure";
}

function runStatusText(run: FlowRun): string {
  if (run.success === null) return "Running";
  return run.success ? "Success" : "Failed";
}
</script>

<style scoped>
.run-overview {
  max-width: 1000px;
}

.run-overview h2 {
  margin: 0 0 var(--spacing-5) 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

/* Summary cards */
.summary-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-6);
}

.summary-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.summary-icon {
  font-size: var(--font-size-xl);
  color: var(--color-primary);
}

.success-icon {
  color: #22c55e;
}

.failure-icon {
  color: #ef4444;
}

.running-icon {
  color: #3b82f6;
}

.summary-info {
  display: flex;
  flex-direction: column;
}

.summary-value {
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-bold);
  color: var(--color-text-primary);
  line-height: 1.2;
}

.summary-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

/* Empty state */
.empty-state {
  text-align: center;
  padding: var(--spacing-8) var(--spacing-4);
  color: var(--color-text-secondary);
}

.empty-icon {
  font-size: 48px;
  color: var(--color-primary);
  opacity: 0.5;
  margin-bottom: var(--spacing-4);
}

.empty-state h3 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
  color: var(--color-text-primary);
}

.empty-state p {
  margin: 0;
  font-size: var(--font-size-sm);
  max-width: 400px;
  margin-left: auto;
  margin-right: auto;
}

/* Runs table */
.runs-table {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.table-header {
  display: grid;
  grid-template-columns: 100px 1fr 120px 150px 100px 90px 80px;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.table-row {
  display: grid;
  grid-template-columns: 100px 1fr 120px 150px 100px 90px 80px;
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  align-items: center;
  cursor: pointer;
  transition: background var(--transition-fast);
}

.table-row:last-child {
  border-bottom: none;
}

.table-row:hover {
  background: var(--color-background-hover);
}

/* Status badges */
.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
}

.status-badge.success {
  color: #22c55e;
  background: rgba(34, 197, 94, 0.1);
}

.status-badge.failure {
  color: #ef4444;
  background: rgba(239, 68, 68, 0.1);
}

.status-badge.running {
  color: #3b82f6;
  background: rgba(59, 130, 246, 0.1);
}

/* Columns */
.col-flow {
  min-width: 0;
}

.flow-name {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.flow-link {
  cursor: pointer;
  transition: color var(--transition-fast);
}

.flow-link:hover {
  color: var(--color-primary);
}

.col-trigger {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.trigger-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.col-started,
.col-duration {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.col-nodes {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  font-family: monospace;
}

.snapshot-link {
  color: var(--color-primary);
  font-size: var(--font-size-xs);
}

.no-snapshot {
  color: var(--color-text-muted);
}

/* Pagination */
.pagination-bar {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  padding: var(--spacing-4) 0;
}

.page-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.page-btn:hover:not(:disabled) {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

.page-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.page-info {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  padding: 0 var(--spacing-2);
}
</style>
