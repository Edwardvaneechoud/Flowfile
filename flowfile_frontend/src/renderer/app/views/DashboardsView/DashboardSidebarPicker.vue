<template>
  <div class="picker">
    <div class="picker-tools">
      <h3>Insert</h3>
      <el-button
        size="small"
        plain
        class="picker-tool-btn"
        draggable="true"
        title="Click or drag onto the canvas"
        @click="emit('add-text')"
        @dragstart="onTextDragStart($event)"
      >
        <el-icon><EditPen /></el-icon>
        <span>Text block</span>
      </el-button>
    </div>
    <div class="picker-divider" />
    <div class="picker-header">
      <h3>Visualizations</h3>
      <p class="picker-sub">Click or drag onto the canvas.</p>
    </div>
    <el-input
      v-model="search"
      size="small"
      placeholder="Filter by name"
      clearable
      class="picker-search"
    >
      <template #prefix>
        <el-icon><Search /></el-icon>
      </template>
    </el-input>
    <el-select
      v-if="tableOptions.length"
      v-model="tableFilter"
      size="small"
      placeholder="All tables"
      clearable
      filterable
      class="picker-table-filter"
    >
      <el-option v-for="opt in tableOptions" :key="opt.id" :label="opt.label" :value="opt.id" />
    </el-select>

    <div v-if="catalogStore.loadingVisualizationLibrary" class="picker-state">
      <el-skeleton :rows="3" animated />
    </div>
    <div v-else-if="!filtered.length" class="picker-state">
      <el-empty
        :description="hasActiveFilter ? 'No visualizations match.' : 'No saved visualizations yet.'"
        :image-size="40"
      >
        <el-button v-if="!hasActiveFilter" size="small" type="primary" @click="emit('create')">
          <el-icon><Plus /></el-icon>
          <span>New visualization</span>
        </el-button>
      </el-empty>
    </div>
    <ul v-else class="picker-list">
      <li
        v-for="viz in filtered"
        :key="viz.id"
        class="picker-item"
        :class="{ 'picker-item-added': addedVizIds.has(viz.id) }"
        :title="
          addedVizIds.has(viz.id)
            ? 'Already on canvas — click or drag to add another tile'
            : 'Click or drag onto the canvas'
        "
        draggable="true"
        @dragstart="onVizDragStart($event, viz.id)"
        @click="onAdd(viz)"
      >
        <i
          :class="
            viz.source_type === 'sql'
              ? 'fa-solid fa-code picker-icon picker-icon-sql'
              : 'fa-solid fa-chart-column picker-icon'
          "
        ></i>
        <div class="picker-item-body">
          <div class="picker-item-name">{{ viz.name }}</div>
          <div class="picker-item-source">{{ sourceLabel(viz) }}</div>
        </div>
        <el-icon class="picker-add-icon"><Plus /></el-icon>
      </li>
    </ul>

    <div v-if="filtered.length || hasActiveFilter" class="picker-footer">
      <el-button size="small" plain class="picker-tool-btn" @click="emit('create')">
        <el-icon><Plus /></el-icon>
        <span>New visualization</span>
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { EditPen, Plus, Search } from "@element-plus/icons-vue";
import { useCatalogStore } from "../../stores/catalog-store";
import { useDashboardDragAndDrop } from "../../composables/useDashboardDragAndDrop";
import type { CatalogVisualization } from "../../types";

defineProps<{ addedVizIds: Set<number> }>();

const emit = defineEmits<{
  (e: "add", viz: CatalogVisualization): void;
  (e: "add-text"): void;
  (e: "create"): void;
}>();

const catalogStore = useCatalogStore();
const { onVizDragStart, onTextDragStart } = useDashboardDragAndDrop();
const search = ref("");
const tableFilter = ref<number | null>(null);

// Distinct catalog tables that back at least one saved visualization.
const tableOptions = computed(() => {
  const seen = new Map<number, string>();
  for (const v of catalogStore.visualizationLibrary) {
    if (v.source_type === "table" && v.catalog_table_id != null && !seen.has(v.catalog_table_id)) {
      seen.set(
        v.catalog_table_id,
        v.table_full_name ?? v.table_name ?? `Table #${v.catalog_table_id}`,
      );
    }
  }
  return [...seen.entries()]
    .map(([id, label]) => ({ id, label }))
    .sort((a, b) => a.label.localeCompare(b.label));
});

const hasActiveFilter = computed(
  () => search.value.trim().length > 0 || typeof tableFilter.value === "number",
);

const filtered = computed(() => {
  const q = search.value.trim().toLowerCase();
  const table = typeof tableFilter.value === "number" ? tableFilter.value : null;
  return catalogStore.visualizationLibrary.filter((v) => {
    if (table !== null && v.catalog_table_id !== table) return false;
    if (!q) return true;
    return (
      v.name.toLowerCase().includes(q) ||
      (v.table_full_name ?? "").toLowerCase().includes(q) ||
      (v.namespace_name ?? "").toLowerCase().includes(q)
    );
  });
});

const sourceLabel = (viz: CatalogVisualization): string => {
  if (viz.source_type === "sql") return "SQL query";
  return viz.table_full_name ?? viz.table_name ?? "table";
};

const onAdd = (viz: CatalogVisualization) => {
  emit("add", viz);
};

onMounted(() => {
  if (!catalogStore.visualizationLibrary.length) {
    catalogStore.loadVisualizationLibrary().catch(() => undefined);
  }
});
</script>

<style scoped>
.picker {
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: 12px;
  gap: 10px;
  overflow: hidden;
}
.picker-tools {
  display: flex;
  flex-direction: column;
  gap: 6px;
  flex-shrink: 0;
}
.picker-tools h3 {
  margin: 0;
  font-size: 13px;
  font-weight: 600;
}
.picker-tool-btn {
  justify-content: flex-start;
  width: 100%;
}
.picker-divider {
  height: 1px;
  background: var(--el-border-color-lighter);
  margin: 2px 0;
  flex-shrink: 0;
}
.picker-header {
  flex-shrink: 0;
}
.picker-header h3 {
  margin: 0 0 2px 0;
  font-size: 13px;
  font-weight: 600;
}
.picker-sub {
  margin: 0;
  font-size: 11px;
  color: var(--el-text-color-secondary);
}
.picker-search {
  flex-shrink: 0;
}
.picker-table-filter {
  flex-shrink: 0;
  width: 100%;
}
.picker-state {
  padding-top: 16px;
}
/* flex-grow:0 so the list takes only its content height — the footer then sits
 * directly under the last visual. flex-shrink:1 + min-height:0 + overflow lets
 * it scroll (footer pinned near the bottom) once it fills the sidebar. */
.picker-list {
  list-style: none;
  margin: 0;
  padding: 0;
  overflow-y: auto;
  flex: 0 1 auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.picker-footer {
  flex-shrink: 0;
  padding-top: 8px;
  border-top: 1px solid var(--el-border-color-lighter);
}
.picker-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  cursor: grab;
  background: var(--el-bg-color);
  transition: background 0.1s;
}
.picker-item:active {
  cursor: grabbing;
}
.picker-item:hover {
  background: var(--el-fill-color-light);
  border-color: var(--el-color-primary-light-5);
}
.picker-item-added {
  background: var(--el-fill-color-lighter);
}
.picker-icon {
  color: var(--el-color-primary);
  font-size: 13px;
}
.picker-icon-sql {
  color: var(--el-color-warning);
}
.picker-item-body {
  flex: 1;
  min-width: 0;
}
.picker-item-name {
  font-size: 13px;
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.picker-item-source {
  font-size: 11px;
  color: var(--el-text-color-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.picker-add-icon {
  color: var(--el-text-color-secondary);
}
.picker-item:hover .picker-add-icon {
  color: var(--el-color-primary);
}
</style>
