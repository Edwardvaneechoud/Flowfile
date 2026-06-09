<template>
  <div class="catalog-overview">
    <h2 class="overview-title">Catalog Overview</h2>

    <div class="stats-grid">
      <button class="stat-card" @click="emit('open-tab', 'catalog')">
        <i class="fa-solid fa-diagram-project stat-icon"></i>
        <span class="stat-info">
          <span class="stat-value">{{ flows }}</span>
          <span class="stat-label">Flows</span>
        </span>
      </button>
      <button class="stat-card" @click="emit('open-tab', 'catalog')">
        <i class="fa-solid fa-table stat-icon"></i>
        <span class="stat-info">
          <span class="stat-value">{{ tables }}</span>
          <span class="stat-label">Tables</span>
        </span>
      </button>
      <button class="stat-card" @click="emit('open-tab', 'runs')">
        <i class="fa-solid fa-play stat-icon"></i>
        <span class="stat-info">
          <span class="stat-value">{{ runs }}</span>
          <span class="stat-label">Total Runs</span>
        </span>
      </button>
      <button class="stat-card" @click="emit('open-tab', 'runs')">
        <i class="fa-solid fa-circle-check stat-icon success"></i>
        <span class="stat-info">
          <span class="stat-value">{{ success }}</span>
          <span class="stat-label">Successful</span>
        </span>
      </button>
      <button class="stat-card" @click="emit('open-tab', 'favorites')">
        <i class="fa-solid fa-star stat-icon"></i>
        <span class="stat-info">
          <span class="stat-value">{{ favorites }}</span>
          <span class="stat-label">Favorites</span>
        </span>
      </button>
    </div>

    <div class="info-section">
      <h3>Getting Started</h3>
      <div class="info-cards">
        <button class="info-card" @click="emit('open-tab', 'catalog')">
          <div class="info-card-header"><i class="fa-solid fa-folder-tree"></i><h4>Flows &amp; Tables</h4></div>
          <p>Your saved flows live alongside catalog tables and datasets — open, rename,
            duplicate, or preview them, all from one catalog.</p>
        </button>
        <button class="info-card" @click="emit('open-tab', 'runs')">
          <div class="info-card-header"><i class="fa-solid fa-clock-rotate-left"></i><h4>Run History</h4></div>
          <p>Every flow run is recorded with its status, duration, and node progress so
            you can see what happened across this session.</p>
        </button>
        <button class="info-card" @click="emit('open-tab', 'favorites')">
          <div class="info-card-header"><i class="fa-solid fa-star"></i><h4>Favorites</h4></div>
          <p>Star the tables you reach for most to pin them to the Favorites tab.</p>
        </button>
        <button class="info-card" @click="emit('go-designer')">
          <div class="info-card-header"><i class="fa-solid fa-diagram-project"></i><h4>Designer</h4></div>
          <p>Build and run flows on the canvas. Outputs you produce appear here as tables.</p>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{ flows: number; tables: number; runs: number; success: number; favorites: number }>()
const emit = defineEmits<{
  (e: 'open-tab', tab: 'catalog' | 'favorites' | 'runs'): void
  (e: 'go-designer'): void
}>()
</script>

<style scoped>
.catalog-overview { display: flex; flex-direction: column; gap: var(--spacing-5); }

.overview-title {
  margin: 0;
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: var(--spacing-3);
}

.stat-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  text-align: left;
  font-family: inherit;
  transition: border-color var(--transition-fast);
}
.stat-card:hover { border-color: var(--color-primary); }

.stat-icon { font-size: var(--font-size-2xl); color: var(--color-primary); }
.stat-icon.success { color: var(--color-success); }

.stat-info { display: flex; flex-direction: column; }
.stat-value { font-size: var(--font-size-xl); font-weight: var(--font-weight-bold); color: var(--color-text-primary); line-height: 1.2; }
.stat-label { font-size: var(--font-size-xs); color: var(--color-text-muted); }

.info-section h3 {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.info-cards { display: grid; grid-template-columns: repeat(2, 1fr); gap: var(--spacing-3); }

@media (max-width: 720px) { .info-cards { grid-template-columns: 1fr; } }

.info-card {
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  text-align: left;
  font-family: inherit;
  transition: border-color var(--transition-fast);
}
.info-card:hover { border-color: var(--color-primary); }

.info-card-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
  color: var(--color-primary);
}
.info-card-header i { font-size: 16px; }
.info-card-header h4 { margin: 0; font-size: var(--font-size-sm); font-weight: var(--font-weight-semibold); color: var(--color-text-primary); }
.info-card p { margin: 0; font-size: var(--font-size-sm); color: var(--color-text-secondary); line-height: 1.5; }
</style>
