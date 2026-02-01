<template>
  <div class="stats-panel">
    <h2>Catalog Overview</h2>

    <div v-if="stats" class="stats-grid">
      <div class="stat-card">
        <i class="fa-solid fa-folder-tree stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_namespaces }}</span>
          <span class="stat-label">Namespaces</span>
        </div>
      </div>
      <div class="stat-card">
        <i class="fa-solid fa-diagram-project stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_flows }}</span>
          <span class="stat-label">Registered Flows</span>
        </div>
      </div>
      <div class="stat-card">
        <i class="fa-solid fa-play stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_runs }}</span>
          <span class="stat-label">Total Runs</span>
        </div>
      </div>
      <div class="stat-card">
        <i class="fa-solid fa-star stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_favorites }}</span>
          <span class="stat-label">Favorites</span>
        </div>
      </div>
    </div>

    <!-- Recent Runs -->
    <div v-if="stats && stats.recent_runs.length > 0" class="section">
      <h3>Recent Runs</h3>
      <div class="recent-list">
        <div v-for="run in stats.recent_runs" :key="run.id" class="recent-item">
          <span class="status-dot" :class="run.success ? 'success' : (run.success === false ? 'failure' : 'pending')"></span>
          <span class="recent-name">{{ run.flow_name }}</span>
          <span class="recent-time">{{ formatDate(run.started_at) }}</span>
          <span class="recent-duration">{{ formatDuration(run.duration_seconds) }}</span>
        </div>
      </div>
    </div>

    <!-- Favorite Flows -->
    <div v-if="stats && stats.favorite_flows.length > 0" class="section">
      <h3>Favorite Flows</h3>
      <div class="fav-list">
        <div v-for="flow in stats.favorite_flows" :key="flow.id" class="fav-item">
          <i class="fa-solid fa-star fav-icon"></i>
          <span class="fav-name">{{ flow.name }}</span>
          <span class="fav-runs">{{ flow.run_count }} runs</span>
        </div>
      </div>
    </div>

    <!-- Empty state -->
    <div v-if="!stats || (stats.total_flows === 0 && stats.total_runs === 0)" class="welcome">
      <i class="fa-solid fa-folder-tree welcome-icon"></i>
      <h3>Welcome to Flow Catalog</h3>
      <p>Organize your flows into catalogs and schemas, track run history, and favorite the flows you use most.</p>
      <div class="welcome-steps">
        <div class="step">
          <span class="step-num">1</span>
          <span>Create a catalog to group related flows</span>
        </div>
        <div class="step">
          <span class="step-num">2</span>
          <span>Add schemas within catalogs for finer organization</span>
        </div>
        <div class="step">
          <span class="step-num">3</span>
          <span>Register your flow files to track and manage them</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { CatalogStats } from "../../types";

defineProps<{
  stats: CatalogStats | null;
}>();

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString(undefined, {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

function formatDuration(seconds: number | null): string {
  if (seconds === null) return "--";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}
</script>

<style scoped>
.stats-panel { max-width: 900px; }

.stats-panel h2 {
  margin: 0 0 var(--spacing-5) 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

/* Stats Grid */
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-6);
}

.stat-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.stat-icon {
  font-size: var(--font-size-xl);
  color: var(--color-primary);
}

.stat-info {
  display: flex;
  flex-direction: column;
}

.stat-value {
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-bold);
  color: var(--color-text-primary);
  line-height: 1.2;
}

.stat-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

/* Sections */
.section {
  margin-bottom: var(--spacing-5);
}

.section h3 {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-3) 0;
}

/* Recent Runs */
.recent-list { display: flex; flex-direction: column; gap: 2px; }

.recent-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.recent-item:hover { background: var(--color-background-hover); }

.status-dot {
  width: 8px; height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.status-dot.success { background: #22c55e; }
.status-dot.failure { background: #ef4444; }
.status-dot.pending { background: #eab308; }

.recent-name { flex: 1; color: var(--color-text-primary); }
.recent-time { color: var(--color-text-muted); font-size: var(--font-size-xs); }
.recent-duration { color: var(--color-text-secondary); font-family: monospace; font-size: var(--font-size-xs); min-width: 60px; text-align: right; }

/* Favorites */
.fav-list { display: flex; flex-direction: column; gap: 2px; }

.fav-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.fav-item:hover { background: var(--color-background-hover); }
.fav-icon { color: #f59e0b; font-size: 12px; }
.fav-name { flex: 1; color: var(--color-text-primary); }
.fav-runs { color: var(--color-text-muted); font-size: var(--font-size-xs); }

/* Welcome */
.welcome {
  text-align: center;
  padding: var(--spacing-8) var(--spacing-4);
  color: var(--color-text-secondary);
}

.welcome-icon {
  font-size: 48px;
  color: var(--color-primary);
  margin-bottom: var(--spacing-4);
  opacity: 0.6;
}

.welcome h3 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
  color: var(--color-text-primary);
}

.welcome p {
  margin: 0 0 var(--spacing-5);
  font-size: var(--font-size-sm);
  max-width: 500px;
  margin-left: auto;
  margin-right: auto;
}

.welcome-steps {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  max-width: 400px;
  margin: 0 auto;
  text-align: left;
}

.step {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  font-size: var(--font-size-sm);
}

.step-num {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border-radius: var(--border-radius-full);
  background: var(--color-primary);
  color: #fff;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-bold);
  flex-shrink: 0;
}
</style>
