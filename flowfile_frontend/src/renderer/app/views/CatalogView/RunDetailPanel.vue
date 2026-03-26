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
        <span
          v-if="run.registration_id"
          class="meta-value flow-link"
          @click="$emit('viewFlow', run.registration_id)"
        >
          {{ run.flow_name }}
          <i class="fa-solid fa-arrow-right flow-link-icon"></i>
        </span>
        <span v-else class="meta-value">{{ run.flow_name }}</span>
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
        <span v-if="run.run_type === 'scheduled' && run.schedule_id" class="meta-value">
          <span class="flow-link" @click="$emit('viewScheduleRuns', run.schedule_id)">
            <i :class="runTypeIcon(run.run_type)" class="trigger-icon" />
            {{ formatRunType(run.run_type) }}
            <i class="fa-solid fa-arrow-right flow-link-icon"></i>
          </span>
          <div class="schedule-name-hint">
            {{
              getScheduleDisplayName(catalogStore.getScheduleById(run.schedule_id), run.schedule_id)
            }}
          </div>
        </span>
        <span v-else class="meta-value">
          <i :class="runTypeIcon(run.run_type)" class="trigger-icon" />
          {{ formatRunType(run.run_type) }}
        </span>
      </div>
    </div>

    <!-- Node Results -->
    <div v-if="nodeResults.length > 0" class="section">
      <h3>Node Results</h3>
      <div class="table-wrapper">
      <table class="styled-table results-table">
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
              <span
                class="status-badge"
                :class="nr.success ? 'success' : nr.success === false ? 'failure' : 'pending'"
              >
                {{ nr.success ? "OK" : nr.success === false ? "Failed" : "Running" }}
              </span>
            </td>
            <td class="mono">{{ nr.run_time >= 0 ? `${nr.run_time}ms` : "--" }}</td>
            <td class="error-text">{{ nr.error || "--" }}</td>
          </tr>
        </tbody>
      </table>
      </div>
    </div>

    <!-- Run Log (scheduled runs) -->
    <div v-if="run.has_log" class="section">
      <div class="snapshot-header">
        <h3>
          <i class="fa-solid fa-file-lines"></i>
          Run Log
        </h3>
        <button v-if="!logContent && !loadingLog" class="btn btn-primary btn-sm" @click="loadLog">
          <i class="fa-solid fa-eye"></i>
          View log
        </button>
      </div>
      <div v-if="loadingLog" class="log-viewer">
        <pre class="snapshot-code">Loading...</pre>
      </div>
      <div v-else-if="logContent !== null" class="log-viewer">
        <pre class="snapshot-code">{{ logContent }}</pre>
      </div>
    </div>

    <!-- Flow Version Snapshot -->
    <div v-if="run.flow_snapshot" class="section">
      <div class="snapshot-header">
        <h3>
          <i class="fa-solid fa-code-branch"></i>
          Flow Version at Run Time
        </h3>
        <button class="btn btn-primary btn-sm" @click="$emit('openSnapshot', run.id)">
          <i class="fa-solid fa-up-right-from-square"></i>
          Open this version
        </button>
      </div>
      <div class="snapshot-viewer">
        <pre class="snapshot-code">{{ formattedSnapshot }}</pre>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowRunDetail } from "../../types";
import {
  formatDate,
  formatDuration,
  formatRunType,
  getScheduleDisplayName,
  runTypeIcon,
} from "./catalog-formatters";

const catalogStore = useCatalogStore();

const props = defineProps<{
  run: FlowRunDetail;
}>();

defineEmits(["close", "openSnapshot", "viewFlow", "viewScheduleRuns"]);

const logContent = ref<string | null>(null);
const loadingLog = ref(false);

async function loadLog() {
  loadingLog.value = true;
  try {
    logContent.value = await CatalogApi.getRunLog(props.run.id);
  } catch {
    logContent.value = "Failed to load log file.";
  } finally {
    loadingLog.value = false;
  }
}

// Auto-load log when viewing a scheduled run
watch(
  () => props.run.id,
  () => {
    logContent.value = null;
    if (props.run.has_log) {
      loadLog();
    }
  },
  { immediate: true },
);

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
</script>

<style scoped>
.run-detail {
  max-width: 1000px;
  margin: 0 auto;
}

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

/* Meta Grid (override for run detail) */
.meta-grid {
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
}

.flow-link {
  color: var(--color-primary);
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  transition: opacity var(--transition-fast);
}

.flow-link:hover {
  opacity: 0.8;
}

.flow-link-icon {
  font-size: 10px;
  opacity: 0.6;
}

/* Schedule name hint */
.schedule-name-hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  margin-top: 2px;
}

.trigger-icon {
  font-size: var(--font-size-xs);
  margin-right: 4px;
}

/* Results Table - extends .styled-table */
.results-table th {
  font-size: var(--font-size-xs);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.results-table td {
  padding: var(--spacing-2) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  color: var(--color-text-primary);
}

.mono {
  font-family: monospace;
  font-size: var(--font-size-xs);
}

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

.status-badge.success {
  background: var(--color-success-light);
  color: var(--color-success);
}
.status-badge.failure {
  background: var(--color-danger-light);
  color: var(--color-danger);
}
.status-badge.pending {
  background: var(--color-warning-light);
  color: var(--color-warning);
}
/* Snapshot Header */
.snapshot-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-3);
}

.snapshot-header h3 {
  margin: 0;
}

/* Log Viewer */
.log-viewer {
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  max-height: 500px;
  overflow: auto;
}

.log-path {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  font-family: var(--font-family-mono);
}

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
  font-family: var(--font-family-mono);
  white-space: pre-wrap;
  word-break: break-word;
  color: var(--color-text-primary);
}
</style>
