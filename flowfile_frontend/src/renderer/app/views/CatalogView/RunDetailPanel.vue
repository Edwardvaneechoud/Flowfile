<template>
  <div class="run-detail">
    <div class="detail-header">
      <button class="back-btn" @click="$emit('close')">
        <i class="fa-solid fa-arrow-left"></i> Back
      </button>
      <h2>Run #{{ run.id }}</h2>
    </div>

    <!-- Run Metadata -->
    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Flow</span>
        <span class="meta-value">{{ run.flow_name }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Status</span>
        <span class="meta-value" :class="statusClass">{{ statusText }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Started</span>
        <span class="meta-value">{{ formatDate(run.started_at) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Duration</span>
        <span class="meta-value">{{ formatDuration(run.duration_seconds) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Nodes</span>
        <span class="meta-value">{{ run.nodes_completed }} / {{ run.number_of_nodes }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Run Type</span>
        <span class="meta-value">{{ run.run_type }}</span>
      </div>
    </div>

    <!-- Node Results -->
    <div v-if="nodeResults.length > 0" class="section">
      <h3>Node Results</h3>
      <table class="results-table">
        <thead>
          <tr>
            <th>Node</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Error</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(nr, idx) in nodeResults" :key="idx">
            <td>{{ nr.node_name || `Node ${nr.node_id}` }}</td>
            <td>
              <span class="status-badge" :class="nr.success ? 'success' : (nr.success === false ? 'failure' : 'pending')">
                {{ nr.success ? 'OK' : (nr.success === false ? 'Failed' : 'Running') }}
              </span>
            </td>
            <td class="mono">{{ nr.run_time >= 0 ? `${nr.run_time}ms` : '--' }}</td>
            <td class="error-text">{{ nr.error || '--' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Flow Version Snapshot -->
    <div v-if="run.flow_snapshot" class="section">
      <h3>
        <i class="fa-solid fa-code-branch"></i>
        Flow Version at Run Time
      </h3>
      <div class="snapshot-viewer">
        <pre class="snapshot-code">{{ formattedSnapshot }}</pre>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { FlowRunDetail } from "../../types";

const props = defineProps<{
  run: FlowRunDetail;
}>();

defineEmits<{ close: [] }>();

interface NodeResultData {
  node_id: number;
  node_name: string | null;
  success: boolean | null;
  error: string;
  run_time: number;
  is_running: boolean;
}

const statusClass = computed(() => {
  if (props.run.success === null) return "text-pending";
  return props.run.success ? "text-success" : "text-danger";
});

const statusText = computed(() => {
  if (props.run.success === null) return "Running";
  return props.run.success ? "Success" : "Failed";
});

const nodeResults = computed<NodeResultData[]>(() => {
  if (!props.run.node_results_json) return [];
  try {
    return JSON.parse(props.run.node_results_json);
  } catch {
    return [];
  }
});

const formattedSnapshot = computed(() => {
  if (!props.run.flow_snapshot) return "";
  try {
    return JSON.stringify(JSON.parse(props.run.flow_snapshot), null, 2);
  } catch {
    return props.run.flow_snapshot;
  }
});

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString();
}

function formatDuration(seconds: number | null): string {
  if (seconds === null) return "--";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}
</script>

<style scoped>
.run-detail { max-width: 900px; }

.detail-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-5);
}

.detail-header h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
}

.back-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.back-btn:hover { border-color: var(--color-primary); color: var(--color-primary); }

/* Meta Grid */
.meta-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-5);
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

.text-success { color: #22c55e; }
.text-danger { color: #ef4444; }
.text-pending { color: #eab308; }

/* Section */
.section {
  margin-bottom: var(--spacing-5);
}

.section h3 {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-3) 0;
  color: var(--color-text-primary);
}

/* Results Table */
.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
}

.results-table th {
  text-align: left;
  padding: var(--spacing-2) var(--spacing-3);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  border-bottom: 1px solid var(--color-border-primary);
  font-size: var(--font-size-xs);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.results-table td {
  padding: var(--spacing-2) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  color: var(--color-text-primary);
}

.mono { font-family: monospace; font-size: var(--font-size-xs); }

.error-text {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.status-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.status-badge.success { background: rgba(34, 197, 94, 0.15); color: #22c55e; }
.status-badge.failure { background: rgba(239, 68, 68, 0.15); color: #ef4444; }
.status-badge.pending { background: rgba(234, 179, 8, 0.15); color: #eab308; }

/* Snapshot Viewer */
.snapshot-viewer {
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  max-height: 400px;
  overflow: auto;
}

.snapshot-code {
  padding: var(--spacing-3);
  margin: 0;
  font-size: var(--font-size-xs);
  font-family: monospace;
  white-space: pre-wrap;
  word-break: break-word;
  color: var(--color-text-primary);
}
</style>
