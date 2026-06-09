<template>
  <div class="run-history">
    <h2 class="run-history-title">Run History</h2>

    <div class="summary-cards">
      <div class="summary-card">
        <span class="material-icons summary-icon">history</span>
        <div class="summary-info"><span class="summary-value">{{ runs.length }}</span><span class="summary-label">Total</span></div>
      </div>
      <div class="summary-card">
        <span class="material-icons summary-icon success">check_circle</span>
        <div class="summary-info"><span class="summary-value">{{ successCount }}</span><span class="summary-label">Successful</span></div>
      </div>
      <div class="summary-card">
        <span class="material-icons summary-icon failure">cancel</span>
        <div class="summary-info"><span class="summary-value">{{ failureCount }}</span><span class="summary-label">Failed</span></div>
      </div>
    </div>

    <div v-if="runs.length" class="runs-table">
      <div class="table-header">
        <span class="col-status">Status</span>
        <span class="col-flow">Flow</span>
        <span class="col-started">Started</span>
        <span class="col-duration">Duration</span>
        <span class="col-nodes">Nodes</span>
      </div>
      <div v-for="run in runs" :key="run.id" class="table-row">
        <div class="col-status">
          <span class="status-badge" :class="run.success ? 'is-success' : 'is-failure'">
            <span class="material-icons">{{ run.success ? 'check_circle' : 'cancel' }}</span>
            {{ run.success ? 'Success' : 'Failed' }}
          </span>
        </div>
        <div class="col-flow" :title="run.flowName">{{ run.flowName }}</div>
        <div class="col-started">{{ formatDate(run.startedAt) }}</div>
        <div class="col-duration">{{ formatDuration(run.durationMs) }}</div>
        <div class="col-nodes">{{ run.nodesCompleted }} / {{ run.nodesTotal }}</div>
      </div>
    </div>

    <div v-else class="runs-empty">
      <span class="material-icons">history</span>
      <p>No runs yet. Run a flow in the Designer and it'll show up here.</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { RunSummary } from '../../stores/run-history-store'

const props = defineProps<{ runs: RunSummary[] }>()

const successCount = computed(() => props.runs.filter((r) => r.success).length)
const failureCount = computed(() => props.runs.filter((r) => !r.success).length)

function formatDate(ts: number): string {
  return new Date(ts).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
  })
}
function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms} ms`
  return `${(ms / 1000).toFixed(1)}s`
}
</script>

<style scoped>
.run-history { display: flex; flex-direction: column; gap: var(--spacing-4); }

.run-history-title {
  margin: 0;
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.summary-cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: var(--spacing-3); }

.summary-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}
.summary-icon { font-size: var(--font-size-lg); color: var(--color-primary); }
.summary-icon.success { color: var(--color-success); }
.summary-icon.failure { color: var(--color-danger); }
.summary-info { display: flex; flex-direction: column; }
.summary-value { font-size: var(--font-size-lg); font-weight: var(--font-weight-bold); color: var(--color-text-primary); line-height: 1.2; }
.summary-label { font-size: var(--font-size-xs); color: var(--color-text-muted); }

.runs-table { border: 1px solid var(--color-border-light); border-radius: var(--border-radius-md); overflow: hidden; }

.table-header, .table-row {
  display: grid;
  grid-template-columns: 130px 1fr 150px 100px 90px;
  gap: var(--spacing-2);
  align-items: center;
  padding: var(--spacing-2) var(--spacing-3);
}

.table-header {
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.table-row {
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
}
.table-row:last-child { border-bottom: none; }
.table-row:hover { background: var(--color-background-hover); }

.col-flow { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: var(--font-weight-medium); color: var(--color-text-primary); }
.col-started, .col-duration, .col-nodes { color: var(--color-text-secondary); font-size: var(--font-size-xs); }
.col-nodes { font-family: var(--font-family-mono); }

.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}
.status-badge .material-icons { font-size: 13px; }
.status-badge.is-success { background: color-mix(in srgb, var(--color-success) 14%, transparent); color: var(--color-success); }
.status-badge.is-failure { background: color-mix(in srgb, var(--color-danger) 14%, transparent); color: var(--color-danger); }

.runs-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  padding: var(--spacing-10);
  color: var(--color-text-muted);
  border: 1px dashed var(--color-border-light);
  border-radius: var(--border-radius-md);
  text-align: center;
}
.runs-empty .material-icons { font-size: 32px; opacity: 0.6; }
.runs-empty p { margin: 0; font-size: var(--font-size-sm); }
</style>
