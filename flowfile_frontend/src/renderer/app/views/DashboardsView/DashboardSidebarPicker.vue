<template>
  <div class="picker">
    <div class="picker-tools">
      <h3>Insert</h3>
      <el-button size="small" plain class="picker-tool-btn" @click="emit('add-text')">
        <el-icon><EditPen /></el-icon>
        <span>Text block</span>
      </el-button>
    </div>
    <div class="picker-divider" />
    <div class="picker-header">
      <h3>Visualizations</h3>
      <p class="picker-sub">Click to add to the canvas.</p>
    </div>
    <el-input v-model="search" size="small" placeholder="Filter" clearable class="picker-search">
      <template #prefix>
        <el-icon><Search /></el-icon>
      </template>
    </el-input>

    <div v-if="catalogStore.loadingVisualizationLibrary" class="picker-state">
      <el-skeleton :rows="3" animated />
    </div>
    <div v-else-if="!filtered.length" class="picker-state">
      <el-empty
        :description="search ? 'No visualizations match.' : 'No saved visualizations yet.'"
        :image-size="40"
      />
    </div>
    <ul v-else class="picker-list">
      <li
        v-for="viz in filtered"
        :key="viz.id"
        class="picker-item"
        :class="{ 'picker-item-added': addedVizIds.has(viz.id) }"
        :title="addedVizIds.has(viz.id) ? 'Already on canvas — adds another tile' : 'Add to canvas'"
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
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { EditPen, Plus, Search } from "@element-plus/icons-vue";
import { useCatalogStore } from "../../stores/catalog-store";
import type { CatalogVisualization } from "../../types";

defineProps<{ addedVizIds: Set<number> }>();

const emit = defineEmits<{
  (e: "add", viz: CatalogVisualization): void;
  (e: "add-text"): void;
}>();

const catalogStore = useCatalogStore();
const search = ref("");

const filtered = computed(() => {
  const lib = catalogStore.visualizationLibrary;
  const q = search.value.trim().toLowerCase();
  if (!q) return lib;
  return lib.filter(
    (v) =>
      v.name.toLowerCase().includes(q) ||
      (v.table_full_name ?? "").toLowerCase().includes(q) ||
      (v.namespace_name ?? "").toLowerCase().includes(q),
  );
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
.picker-state {
  padding-top: 16px;
}
.picker-list {
  list-style: none;
  margin: 0;
  padding: 0;
  overflow-y: auto;
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.picker-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  cursor: pointer;
  background: var(--el-bg-color);
  transition: background 0.1s;
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
