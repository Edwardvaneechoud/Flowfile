<template>
  <div class="flow-detail">
    <!-- Header -->
    <div class="detail-header">
      <div class="header-main">
        <h2>{{ flow.name }}</h2>
        <p v-if="flow.description" class="description">{{ flow.description }}</p>
      </div>
      <div class="header-actions">
        <button
          v-if="flow.file_exists"
          class="action-btn-primary"
          @click="$emit('openFlow', flow.flow_path)"
        >
          <i class="fa-solid fa-up-right-from-square"></i>
          Open in Designer
        </button>
        <button
          class="action-btn-lg"
          :class="{ active: flow.is_favorite }"
          @click="$emit('toggleFavorite', flow.id)"
        >
          <i :class="flow.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
          {{ flow.is_favorite ? "Favorited" : "Favorite" }}
        </button>
        <button
          class="action-btn-lg"
          :class="{ active: flow.is_following }"
          @click="$emit('toggleFollow', flow.id)"
        >
          <i :class="flow.is_following ? 'fa-solid fa-bell' : 'fa-regular fa-bell'"></i>
          {{ flow.is_following ? "Following" : "Follow" }}
        </button>
      </div>
    </div>

    <!-- File missing banner -->
    <div v-if="!flow.file_exists" class="missing-banner">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div>
        <strong>Flow file not found</strong>
        <p>
          The file <code>{{ flow.flow_path }}</code> no longer exists on disk. The flow cannot be
          opened or executed, but its run history is still available below.
        </p>
      </div>
    </div>

    <!-- Metadata Grid -->
    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Total Runs</span>
        <span class="meta-value">{{ flow.run_count }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Last Run</span>
        <span class="meta-value">{{
          flow.last_run_at ? formatDate(flow.last_run_at) : "Never"
        }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Artifacts</span>
        <span class="meta-value">{{ flow.artifact_count ?? 0 }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Status</span>
        <span class="meta-value" :class="statusClass">{{ statusText }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Path</span>
        <span class="meta-value mono">{{ flow.flow_path }}</span>
      </div>
    </div>

    <!-- Run History Table -->
    <div class="section">
      <h3>Run History</h3>
      <div v-if="runs.length === 0" class="empty-runs">No runs recorded yet.</div>
      <table v-else class="runs-table">
        <thead>
          <tr>
            <th>Status</th>
            <th>Started</th>
            <th>Duration</th>
            <th>Nodes</th>
            <th>Version</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="run in runs" :key="run.id" class="run-row" @click="$emit('viewRun', run.id)">
            <td>
              <span class="status-badge" :class="runStatusClass(run)">
                {{ runStatusText(run) }}
              </span>
            </td>
            <td>{{ formatDate(run.started_at) }}</td>
            <td>{{ formatDuration(run.duration_seconds) }}</td>
            <td>{{ run.nodes_completed }} / {{ run.number_of_nodes }}</td>
            <td>
              <span v-if="run.has_snapshot" class="snapshot-link">
                <i class="fa-solid fa-code-branch"></i> View
              </span>
              <span v-else class="no-snapshot">--</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Artifacts Section -->
    <div class="section">
      <h3>Global Artifacts</h3>
      <div v-if="artifacts.length === 0" class="empty-runs">No artifacts published yet.</div>
      <table v-else class="runs-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Version</th>
            <th>Type</th>
            <th>Size</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="artifact in artifacts" :key="artifact.id" class="artifact-row">
            <td>
              <div class="artifact-name">
                <i class="fa-solid fa-cube artifact-icon"></i>
                {{ artifact.name }}
              </div>
              <div v-if="artifact.description" class="artifact-desc">
                {{ artifact.description }}
              </div>
            </td>
            <td>v{{ artifact.version }}</td>
            <td>
              <span class="type-badge">{{ formatType(artifact) }}</span>
            </td>
            <td>{{ formatSize(artifact.size_bytes) }}</td>
            <td>{{ artifact.created_at ? formatDate(artifact.created_at) : "--" }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { FlowRegistration, FlowRun, GlobalArtifact } from "../../types";

const props = defineProps<{
  flow: FlowRegistration;
  runs: FlowRun[];
  artifacts: GlobalArtifact[];
}>();

defineEmits<{
  viewRun: [runId: number];
  toggleFavorite: [flowId: number];
  toggleFollow: [flowId: number];
  openFlow: [flowPath: string];
}>();

const statusClass = computed(() => {
  if (props.flow.last_run_success === null) return "";
  return props.flow.last_run_success ? "text-success" : "text-danger";
});

const statusText = computed(() => {
  if (props.flow.last_run_success === null) return "No runs";
  return props.flow.last_run_success ? "Success" : "Failed";
});

function runStatusClass(run: FlowRun): string {
  if (run.success === null) return "pending";
  return run.success ? "success" : "failure";
}

function runStatusText(run: FlowRun): string {
  if (run.success === null) return "Running";
  return run.success ? "Success" : "Failed";
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatDuration(seconds: number | null): string {
  if (seconds === null) return "--";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function formatType(artifact: GlobalArtifact): string {
  if (artifact.python_type) {
    const parts = artifact.python_type.split(".");
    return parts[parts.length - 1];
  }
  return artifact.serialization_format ?? "unknown";
}

function formatSize(bytes: number | null): string {
  if (bytes === null) return "--";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
</script>

<style scoped>
.flow-detail {
  max-width: 900px;
}

.detail-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: var(--spacing-5);
}

.header-main h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.description {
  margin: var(--spacing-1) 0 0;
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
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.action-btn-lg:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}
.action-btn-lg.active {
  color: var(--color-primary);
  border-color: var(--color-primary);
}

.action-btn-primary {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-3);
  background: var(--color-primary);
  color: #fff;
  border: none;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: opacity var(--transition-fast);
}

.action-btn-primary:hover {
  opacity: 0.9;
}

/* ========== Meta Grid ========== */
.meta-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
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

.meta-value.mono {
  font-family: monospace;
  font-size: var(--font-size-xs);
  word-break: break-all;
}

.text-success {
  color: #22c55e;
}
.text-danger {
  color: #ef4444;
}

/* ========== Run History Table ========== */
.section h3 {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-3) 0;
}

.runs-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
}

.runs-table th {
  text-align: left;
  padding: var(--spacing-2) var(--spacing-3);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  border-bottom: 1px solid var(--color-border-primary);
  font-size: var(--font-size-xs);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.runs-table td {
  padding: var(--spacing-2) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  color: var(--color-text-primary);
}

.run-row {
  cursor: pointer;
  transition: background var(--transition-fast);
}

.run-row:hover {
  background: var(--color-background-hover);
}

.status-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.status-badge.success {
  background: rgba(34, 197, 94, 0.15);
  color: #22c55e;
}
.status-badge.failure {
  background: rgba(239, 68, 68, 0.15);
  color: #ef4444;
}
.status-badge.pending {
  background: rgba(234, 179, 8, 0.15);
  color: #eab308;
}

.snapshot-link {
  color: var(--color-primary);
  cursor: pointer;
  font-size: var(--font-size-xs);
}

.no-snapshot {
  color: var(--color-text-muted);
}
.empty-runs {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  padding: var(--spacing-4);
  text-align: center;
}

/* ========== Artifact Rows ========== */
.artifact-row td {
  vertical-align: top;
}
.artifact-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  font-weight: var(--font-weight-medium);
}
.artifact-icon {
  color: var(--color-primary);
  font-size: var(--font-size-xs);
}
.artifact-desc {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  margin-top: 2px;
}
.type-badge {
  display: inline-block;
  padding: 1px 6px;
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
  font-family: monospace;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  color: var(--color-text-secondary);
}

/* ========== Missing File Banner ========== */
.missing-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  margin-bottom: var(--spacing-4);
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: var(--border-radius-md);
  color: var(--color-text-primary);
}

.missing-banner > i {
  color: #f59e0b;
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

.missing-banner code {
  font-family: monospace;
  font-size: var(--font-size-xs);
  background: rgba(0, 0, 0, 0.06);
  padding: 1px 4px;
  border-radius: var(--border-radius-sm);
}
</style>
