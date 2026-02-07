<template>
  <div class="artifact-detail">
    <!-- Header -->
    <div class="detail-header">
      <div class="header-main">
        <div class="header-title">
          <i class="fa-solid fa-cube header-icon"></i>
          <h2>{{ artifact.name }}</h2>
          <span class="version-badge">v{{ artifact.version }}</span>
          <span class="status-badge" :class="artifact.status">{{ artifact.status }}</span>
        </div>
        <p v-if="artifact.description" class="description">{{ artifact.description }}</p>
      </div>
    </div>

    <!-- Metadata Grid -->
    <div class="meta-grid">
      <div class="meta-card">
        <span class="meta-label">Type</span>
        <span class="meta-value mono">{{ formatType(artifact) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Format</span>
        <span class="meta-value">{{ artifact.serialization_format ?? 'unknown' }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Size</span>
        <span class="meta-value">{{ formatSize(artifact.size_bytes) }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Created</span>
        <span class="meta-value">{{ artifact.created_at ? formatDate(artifact.created_at) : '--' }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Updated</span>
        <span class="meta-value">{{ artifact.updated_at ? formatDate(artifact.updated_at) : '--' }}</span>
      </div>
      <div class="meta-card">
        <span class="meta-label">Version</span>
        <span class="meta-value">{{ artifact.version }}</span>
      </div>
    </div>

    <!-- Technical Details -->
    <div class="section">
      <h3>Details</h3>
      <div class="detail-list">
        <div v-if="artifact.python_type" class="detail-row">
          <span class="detail-label">Python Type</span>
          <span class="detail-value mono">{{ artifact.python_type }}</span>
        </div>
        <div v-if="artifact.python_module" class="detail-row">
          <span class="detail-label">Module</span>
          <span class="detail-value mono">{{ artifact.python_module }}</span>
        </div>
        <div v-if="artifact.sha256" class="detail-row">
          <span class="detail-label">SHA-256</span>
          <span class="detail-value mono sha">{{ artifact.sha256 }}</span>
        </div>
        <div v-if="artifact.source_node_id" class="detail-row">
          <span class="detail-label">Source Node</span>
          <span class="detail-value">{{ artifact.source_node_id }}</span>
        </div>
      </div>
    </div>

    <!-- Tags -->
    <div v-if="artifact.tags && artifact.tags.length > 0" class="section">
      <h3>Tags</h3>
      <div class="tags-list">
        <span v-for="tag in artifact.tags" :key="tag" class="tag-badge">{{ tag }}</span>
      </div>
    </div>

    <!-- Usage Example -->
    <div class="section">
      <h3>Usage</h3>
      <div class="code-block">
        <code>obj = flowfile.get_global("{{ artifact.name }}")</code>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { GlobalArtifact } from "../../types";

defineProps<{
  artifact: GlobalArtifact;
}>();

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString(undefined, {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
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
.artifact-detail { max-width: 900px; }

.detail-header {
  margin-bottom: var(--spacing-5);
}

.header-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.header-icon {
  color: var(--color-primary);
  font-size: var(--font-size-xl);
}

.header-title h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.version-badge {
  font-size: var(--font-size-xs);
  font-family: monospace;
  color: var(--color-text-muted);
  background: var(--color-background-tertiary);
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  line-height: 20px;
}

.status-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.status-badge.active { background: rgba(34, 197, 94, 0.15); color: #22c55e; }
.status-badge.deleted { background: rgba(239, 68, 68, 0.15); color: #ef4444; }
.status-badge.pending { background: rgba(234, 179, 8, 0.15); color: #eab308; }

.description {
  margin: var(--spacing-2) 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
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
}

/* ========== Sections ========== */
.section {
  margin-bottom: var(--spacing-5);
}

.section h3 {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-3) 0;
}

/* ========== Detail List ========== */
.detail-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.detail-row {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.detail-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  min-width: 100px;
  flex-shrink: 0;
}

.detail-value {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.detail-value.mono {
  font-family: monospace;
  font-size: var(--font-size-xs);
  word-break: break-all;
}

.detail-value.sha {
  font-size: 11px;
  color: var(--color-text-muted);
}

/* ========== Tags ========== */
.tags-list {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
}

.tag-badge {
  font-size: var(--font-size-xs);
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  color: var(--color-text-secondary);
}

/* ========== Code Block ========== */
.code-block {
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  padding: var(--spacing-3);
}

.code-block code {
  font-family: monospace;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}
</style>
