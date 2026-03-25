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
          <span class="stat-label">
            Schedules
            <span
              class="scheduler-indicator"
              :class="catalogStore.schedulerStatus?.active ? 'indicator-green' : 'indicator-orange'"
              :title="
                catalogStore.schedulerStatus?.active ? 'Scheduler running' : 'Scheduler not running'
              "
            ></span>
          </span>
        </div>
      </div>
    </div>

    <!-- Expanded sections -->

    <!-- Flows list -->
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

    <!-- Tables list -->
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

    <!-- Favorites list -->
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

    <!-- Run History (full overview panel) -->
    <div v-if="activeSection === 'runs'" class="section">
      <RunOverviewPanel
        @view-run="$emit('viewRun', $event)"
        @view-flow="$emit('viewFlow', $event)"
      />
    </div>

    <!-- Schedules (full overview panel) -->
    <div v-if="activeSection === 'schedules'" class="section">
      <ScheduleOverviewPanel
        @create-schedule="$emit('createSchedule')"
        @toggle-schedule="(id: number, val: boolean) => $emit('toggleSchedule', id, val)"
        @delete-schedule="$emit('deleteSchedule', $event)"
        @run-now="$emit('runNow', $event)"
        @cancel-schedule-run="$emit('cancelScheduleRun', $event)"
        @view-flow="$emit('viewFlow', $event)"
      />
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

    <!-- Getting started / info section -->
    <div v-if="!activeSection" class="info-section">
      <h3>Getting Started</h3>
      <div class="info-cards">
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-folder-tree"></i>
            <h4>Organization</h4>
          </div>
          <p>
            Flows and tables are organized into <strong>catalogs</strong> and
            <strong>schemas</strong>. Use catalogs for broad groupings (e.g. by team or domain) and
            schemas for finer separation within them.
          </p>
        </div>
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-clock-rotate-left"></i>
            <h4>Run History</h4>
          </div>
          <p>
            Every registered flow tracks its executions automatically. View status, duration, and
            node-level progress for each run, or open a snapshot to inspect a past state.
          </p>
        </div>
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-table"></i>
            <h4>Tables &amp; Artifacts</h4>
          </div>
          <p>
            Register datasets as catalog tables to preview and track them centrally. Artifacts
            produced by flows are versioned and linked back to the run that created them.
          </p>
        </div>
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-calendar-days"></i>
            <h4>Schedules</h4>
          </div>
          <p>
            Automate flow execution with cron-based or table-trigger schedules. Monitor active runs
            and manage schedules from the Schedules tab.
          </p>
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
import type {
  CatalogStats,
  CatalogTable,
  FlowRegistration,
  FlowRun,
  FlowSchedule,
} from "../../types";
import RunOverviewPanel from "./RunOverviewPanel.vue";
import ScheduleOverviewPanel from "./ScheduleOverviewPanel.vue";

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
  createSchedule: [];
  toggleSchedule: [id: number, enabled: boolean];
  deleteSchedule: [id: number];
  runNow: [scheduleId: number];
  cancelScheduleRun: [schedule: FlowSchedule];
}>();

const activeSection = ref<Section | null>(null);

function toggleSection(section: Section) {
  activeSection.value = activeSection.value === section ? null : section;
}

function formatNumber(n: number | null): string {
  if (n === null) return "--";
  return n.toLocaleString();
}
</script>

<style scoped>
.stats-panel {
  max-width: 1000px;
  margin: 0 auto;
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
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.scheduler-indicator {
  display: inline-block;
  width: 6px;
  height: 6px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.scheduler-indicator.indicator-green {
  background: #22c55e;
}

.scheduler-indicator.indicator-orange {
  background: #f97316;
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

/* Info Section */
.info-section {
  margin-bottom: var(--spacing-5);
}

.info-section h3 {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-3) 0;
  color: var(--color-text-primary);
}

.info-cards {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: var(--spacing-3);
}

.info-card {
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.info-card-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
  color: var(--color-primary);
}

.info-card-header h4 {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.info-card p {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
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
