<template>
  <div class="viz-library">
    <div class="viz-library-header">
      <div>
        <h2>Visualizations</h2>
        <p class="viz-library-sub">
          Saved Graphic Walker charts across the catalog. Click a chart to open the source table.
        </p>
      </div>
      <el-input
        v-model="search"
        size="small"
        placeholder="Filter by name or table"
        class="viz-library-search"
        clearable
      >
        <template #prefix>
          <el-icon><Search /></el-icon>
        </template>
      </el-input>
    </div>

    <div v-if="loading" class="viz-library-state">
      <el-skeleton :rows="6" animated />
    </div>

    <div v-else-if="!groups.length" class="viz-library-state">
      <el-empty
        description="No saved visualizations yet. Open a table or run a SQL query to create one."
      />
    </div>

    <div v-else class="viz-library-groups">
      <div v-for="group in groups" :key="group.tableId" class="viz-library-group">
        <div class="viz-library-group-header" @click="emit('viewTable', group.tableId)">
          <i
            :class="
              group.tableType === 'virtual'
                ? 'fa-solid fa-bolt group-icon virtual'
                : 'fa-solid fa-table group-icon'
            "
          ></i>
          <span class="group-name">{{ group.tableFullName }}</span>
          <span class="group-count">{{ group.items.length }}</span>
        </div>
        <div class="viz-library-group-items">
          <div
            v-for="item in group.items"
            :key="item.id"
            class="viz-library-item"
            role="button"
            tabindex="0"
            @click="emit('viewTable', item.catalog_table_id)"
            @keydown.enter="emit('viewTable', item.catalog_table_id)"
          >
            <div class="viz-library-item-name">{{ item.name }}</div>
            <div class="viz-library-item-meta">
              <span v-if="item.chart_type">{{ item.chart_type }}</span>
              <span class="dot">·</span>
              <span>{{ formatDate(item.updated_at) }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { Search } from "@element-plus/icons-vue";
import { useCatalogStore } from "../../stores/catalog-store";
import { formatDate } from "./catalog-formatters";
import type { VisualizationLibraryItem } from "../../types";

const emit = defineEmits<{
  (e: "viewTable", tableId: number): void;
}>();

const store = useCatalogStore();
const search = ref("");

const loading = computed(
  () => store.loadingVisualizationLibrary && !store.visualizationLibrary.length,
);

const filtered = computed<VisualizationLibraryItem[]>(() => {
  const q = search.value.trim().toLowerCase();
  if (!q) return store.visualizationLibrary;
  return store.visualizationLibrary.filter(
    (it) =>
      it.name.toLowerCase().includes(q) ||
      it.table_full_name.toLowerCase().includes(q) ||
      (it.chart_type ?? "").toLowerCase().includes(q),
  );
});

interface Group {
  tableId: number;
  tableFullName: string;
  tableType: string;
  items: VisualizationLibraryItem[];
}

const groups = computed<Group[]>(() => {
  const byTable = new Map<number, Group>();
  for (const item of filtered.value) {
    const existing = byTable.get(item.catalog_table_id);
    if (existing) {
      existing.items.push(item);
    } else {
      byTable.set(item.catalog_table_id, {
        tableId: item.catalog_table_id,
        tableFullName: item.table_full_name,
        tableType: item.table_type,
        items: [item],
      });
    }
  }
  return Array.from(byTable.values()).sort((a, b) =>
    a.tableFullName.localeCompare(b.tableFullName),
  );
});

onMounted(() => {
  store.loadVisualizationLibrary();
});
</script>

<style scoped>
.viz-library {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 16px 24px;
  height: 100%;
  overflow-y: auto;
}
.viz-library-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}
.viz-library-header h2 {
  margin: 0 0 4px;
  font-size: 18px;
}
.viz-library-sub {
  margin: 0;
  color: var(--el-text-color-secondary);
  font-size: 13px;
}
.viz-library-search {
  max-width: 320px;
}
.viz-library-state {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px 0;
}
.viz-library-groups {
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.viz-library-group {
  border: 1px solid var(--el-border-color-light);
  border-radius: 8px;
  background: var(--el-bg-color);
  overflow: hidden;
}
.viz-library-group-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  background: var(--el-fill-color-lighter);
  cursor: pointer;
  font-weight: 600;
}
.viz-library-group-header:hover {
  background: var(--el-fill-color);
}
.group-icon {
  color: var(--el-text-color-regular);
}
.group-icon.virtual {
  color: var(--el-color-warning);
}
.group-name {
  flex: 1;
  font-size: 14px;
}
.group-count {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  background: var(--el-bg-color);
  border-radius: 999px;
  padding: 1px 8px;
  border: 1px solid var(--el-border-color-lighter);
}
.viz-library-group-items {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 1px;
  background: var(--el-border-color-lighter);
}
.viz-library-item {
  background: var(--el-bg-color);
  padding: 10px 12px;
  cursor: pointer;
  transition: background 0.15s;
}
.viz-library-item:hover,
.viz-library-item:focus-visible {
  background: var(--el-fill-color-lighter);
  outline: none;
}
.viz-library-item-name {
  font-weight: 500;
  font-size: 13px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viz-library-item-meta {
  display: flex;
  gap: 6px;
  margin-top: 4px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}
.viz-library-item-meta .dot {
  color: var(--el-text-color-disabled);
}
</style>
