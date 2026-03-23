<template>
  <div class="stats-panel">
    <h2>Catalog Overview</h2>

    <div v-if="stats" class="stats-grid">
      <div
        class="stat-card clickable"
        :class="{ 'stat-card--active': activeSection === 'flows' }"
        @click="toggleSection('flows')"
      >
        <i class="fa-solid fa-diagram-project stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_flows }}</span>
          <span class="stat-label">Registered Flows</span>
        </div>
      </div>
      <div
        class="stat-card clickable"
        :class="{ 'stat-card--active': activeSection === 'runs' }"
        @click="toggleSection('runs')"
      >
        <i class="fa-solid fa-play stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_runs }}</span>
          <span class="stat-label">Total Runs</span>
        </div>
      </div>
      <div
        class="stat-card clickable"
        :class="{ 'stat-card--active': activeSection === 'tables' }"
        @click="toggleSection('tables')"
      >
        <i class="fa-solid fa-table stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_tables }}</span>
          <span class="stat-label">Tables</span>
        </div>
      </div>
      <div
        class="stat-card clickable"
        :class="{ 'stat-card--active': activeSection === 'favorites' }"
        @click="toggleSection('favorites')"
      >
        <i class="fa-solid fa-star stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_favorites + stats.total_table_favorites }}</span>
          <span class="stat-label">Favorites</span>
        </div>
      </div>
      <div
        class="stat-card clickable"
        :class="{ 'stat-card--active': activeSection === 'schedules' }"
        @click="toggleSection('schedules')"
      >
        <i class="fa-solid fa-calendar-days stat-icon"></i>
        <div class="stat-info">
          <span class="stat-value">{{ stats.total_schedules }}</span>
          <span class="stat-label">Schedules</span>
        </div>
      </div>
    </div>

    <!-- Expanded list for clicked stat card -->
    <div v-if="activeSection === 'flows' && flows.length > 0" class="section">
      <h3>All Flows</h3>
      <div class="item-list">
        <div
          v-for="flow in flows"
          :key="flow.id"
          class="item-row clickable"
          @click="$emit('viewFlow', flow.id)"
        >
          <i class="fa-solid fa-diagram-project item-icon"></i>
          <span class="item-name">{{ flow.name }}</span>
          <span class="item-meta">{{ flow.run_count }} runs</span>
        </div>
      </div>
    </div>

    <div v-if="activeSection === 'tables' && tables.length > 0" class="section">
      <h3>All Tables</h3>
      <div class="item-list">
        <div
          v-for="table in tables"
          :key="table.id"
          class="item-row clickable"
          @click="$emit('viewTable', table.id)"
        >
          <i class="fa-solid fa-table item-icon"></i>
          <span class="item-name">{{ table.name }}</span>
          <span class="item-meta">{{ formatNumber(table.row_count) }} rows</span>
        </div>
      </div>
    </div>

    <div
      v-if="activeSection === 'favorites' && (favorites.length > 0 || favoriteTables.length > 0)"
      class="section"
    >
      <h3>Favorites</h3>
      <div class="item-list">
        <div
          v-for="flow in favorites"
          :key="'fav-f-' + flow.id"
          class="item-row clickable"
          @click="$emit('viewFlow', flow.id)"
        >
          <i class="fa-solid fa-star item-icon fav"></i>
          <span class="item-name">{{ flow.name }}</span>
          <span class="item-meta">{{ flow.run_count }} runs</span>
        </div>
        <div
          v-for="table in favoriteTables"
          :key="'fav-t-' + table.id"
          class="item-row clickable"
          @click="$emit('viewTable', table.id)"
        >
          <i class="fa-solid fa-star item-icon fav"></i>
          <i class="fa-solid fa-table item-icon"></i>
          <span class="item-name">{{ table.name }}</span>
          <span class="item-meta">{{ formatNumber(table.row_count) }} rows</span>
        </div>
      </div>
    </div>

    <div v-if="activeSection === 'runs' && runs.length > 0" class="section">
      <h3>Run History</h3>
      <div class="item-list">
        <div
          v-for="run in runs"
          :key="run.id"
          class="item-row clickable"
          @click="$emit('viewRun', run.id)"
        >
          <span
            class="status-dot"
            :class="run.success ? 'success' : run.success === false ? 'failure' : 'pending'"
          ></span>
          <span class="item-name">{{ run.flow_name }}</span>
          <span class="item-meta">{{ formatDate(run.started_at) }}</span>
          <span class="item-duration">{{ formatDuration(run.duration_seconds) }}</span>
        </div>
      </div>
    </div>

    <div v-if="activeSection === 'schedules' && catalogStore.schedules.length > 0" class="section">
      <h3>Schedules</h3>
      <div class="item-list">
        <div v-for="schedule in enrichedSchedules" :key="schedule.id" class="item-row">
          <span v-if="schedule.isRunning" class="status-dot running-dot" title="Running">
            <i class="fa-solid fa-spinner fa-spin" />
          </span>
          <span v-else class="status-dot" :class="schedule.enabled ? 'success' : 'pending'"></span>
          <i
            :class="
              schedule.schedule_type === 'interval' ? 'fa-solid fa-clock' : 'fa-solid fa-table'
            "
            class="item-icon"
          />
          <span class="item-name">{{ schedule.flowName }}</span>
          <span class="item-meta">{{ formatScheduleType(schedule) }}</span>
        </div>
      </div>
    </div>

    <!-- Favorite Flows (always visible) -->
    <div v-if="favorites.length > 0 && activeSection !== 'favorites'" class="section">
      <h3>Favorite Flows</h3>
      <div class="item-list">
        <div
          v-for="flow in favorites"
          :key="flow.id"
          class="item-row clickable"
          @click="$emit('viewFlow', flow.id)"
        >
          <i class="fa-solid fa-star item-icon fav"></i>
          <span class="item-name">{{ flow.name }}</span>
          <span class="item-meta">{{ flow.run_count }} runs</span>
        </div>
      </div>
    </div>

    <!-- Favorite Tables (always visible) -->
    <div v-if="favoriteTables.length > 0 && activeSection !== 'favorites'" class="section">
      <h3>Favorite Tables</h3>
      <div class="item-list">
        <div
          v-for="table in favoriteTables"
          :key="table.id"
          class="item-row clickable"
          @click="$emit('viewTable', table.id)"
        >
          <i class="fa-solid fa-star item-icon fav"></i>
          <span class="item-name">{{ table.name }}</span>
          <span class="item-meta">{{ formatNumber(table.row_count) }} rows</span>
        </div>
      </div>
    </div>

    <!-- Empty state -->
    <div v-if="!stats || (stats.total_flows === 0 && stats.total_runs === 0)" class="welcome">
      <i class="fa-solid fa-folder-tree welcome-icon"></i>
      <h3>Welcome to Catalog</h3>
      <p>
        Organize your flows into catalogs and schemas, track run history, and favorite the flows you
        use most.
      </p>
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
import { computed, ref } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import type { CatalogStats, CatalogTable, FlowRegistration, FlowRun } from "../../types";

type Section = "flows" | "tables" | "favorites" | "runs" | "schedules";

const catalogStore = useCatalogStore();

const props = defineProps<{
  stats: CatalogStats | null;
  flows: FlowRegistration[];
  tables: CatalogTable[];
  favorites: FlowRegistration[];
  runs: FlowRun[];
}>();

const favoriteTables = computed((): CatalogTable[] => {
  return props.stats?.favorite_tables ?? [];
});

defineEmits<{
  viewRun: [runId: number];
  viewFlow: [flowId: number];
  viewTable: [tableId: number];
}>();

const activeSection = ref<Section | null>(null);

function toggleSection(section: Section) {
  activeSection.value = activeSection.value === section ? null : section;
}

const activeRegistrationIds = computed(() => {
  return new Set(catalogStore.activeRuns.map((r) => r.registration_id).filter((id) => id !== null));
});

const enrichedSchedules = computed(() => {
  return catalogStore.schedules.map((s) => ({
    ...s,
    flowName:
      catalogStore.allFlows.find((f) => f.id === s.registration_id)?.name ??
      `Flow #${s.registration_id}`,
    isRunning: activeRegistrationIds.value.has(s.registration_id),
  }));
});

function formatScheduleType(schedule: {
  schedule_type: string;
  interval_seconds: number | null;
  trigger_table_id: number | null;
}): string {
  if (schedule.schedule_type === "interval" && schedule.interval_seconds) {
    const mins = Math.floor(schedule.interval_seconds / 60);
    if (mins < 60) return `Every ${mins}m`;
    const hrs = Math.floor(mins / 60);
    const remMins = mins % 60;
    return remMins > 0 ? `Every ${hrs}h ${remMins}m` : `Every ${hrs}h`;
  }
  if (schedule.schedule_type === "table_trigger") {
    return `Table trigger #${schedule.trigger_table_id}`;
  }
  return schedule.schedule_type;
}

function formatNumber(n: number | null): string {
  if (n === null) return "--";
  return n.toLocaleString();
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
</script>

<style scoped>
.stats-panel {
  max-width: 900px;
}

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
  transition: all var(--transition-fast);
}

.stat-card.clickable:hover {
  border-color: var(--color-primary);
}

.stat-card--active {
  border-color: var(--color-primary);
  background: var(--color-background-hover);
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

.clickable {
  cursor: pointer;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.status-dot.success {
  background: #22c55e;
}
.status-dot.failure {
  background: #ef4444;
}
.status-dot.pending {
  background: #eab308;
}
.running-dot {
  background: none !important;
  color: #3b82f6;
  font-size: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
}

/* Item Lists */
.item-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.item-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.item-row:hover {
  background: var(--color-background-hover);
}

.item-icon {
  color: var(--color-primary);
  font-size: var(--font-size-xs);
  width: 16px;
  text-align: center;
  flex-shrink: 0;
}

.item-icon.fav {
  color: #f59e0b;
}

.item-name {
  flex: 1;
  color: var(--color-text-primary);
}

.item-meta {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.item-duration {
  color: var(--color-text-secondary);
  font-family: monospace;
  font-size: var(--font-size-xs);
  min-width: 60px;
  text-align: right;
}

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
