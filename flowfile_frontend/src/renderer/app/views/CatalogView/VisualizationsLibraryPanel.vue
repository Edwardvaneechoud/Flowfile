<template>
  <div class="viz-library">
    <div class="viz-library-header">
      <div>
        <h2>Visualizations</h2>
        <p class="viz-library-sub">
          Saved Graphic Walker charts across the catalog. Click any chart to open it.
        </p>
      </div>
      <el-input
        v-model="search"
        size="small"
        placeholder="Filter by name or source"
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

    <div v-else-if="!filtered.length" class="viz-library-state">
      <el-empty
        description="No saved visualizations yet. Open a table or run a SQL query to create one."
      />
    </div>

    <div v-else class="viz-library-grid">
      <div
        v-for="item in filtered"
        :key="item.id"
        class="viz-library-card"
        role="button"
        tabindex="0"
        @click="openViz(item)"
        @keydown.enter="openViz(item)"
      >
        <div class="viz-library-card-header">
          <i
            :class="
              item.source_type === 'sql'
                ? 'fa-solid fa-code source-icon sql'
                : item.table_type === 'virtual'
                  ? 'fa-solid fa-bolt source-icon virtual'
                  : 'fa-solid fa-table source-icon'
            "
          ></i>
          <div class="viz-library-card-title">
            <div class="viz-name">{{ item.name }}</div>
            <div class="viz-source">{{ sourceLabel(item) }}</div>
          </div>
          <el-dropdown trigger="click" @click.stop>
            <el-icon class="viz-card-menu" @click.stop><MoreFilled /></el-icon>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item @click="openViz(item)">
                  <el-icon><Edit /></el-icon> Open
                </el-dropdown-item>
                <el-dropdown-item
                  v-if="item.catalog_table_id"
                  @click="emit('viewTable', item.catalog_table_id!)"
                >
                  <el-icon><FolderOpened /></el-icon> Source table
                </el-dropdown-item>
                <el-dropdown-item divided @click="onDelete(item)">
                  <el-icon><Delete /></el-icon> Delete
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
        <div class="viz-library-card-meta">
          <span v-if="item.chart_type">{{ item.chart_type }}</span>
          <span v-if="item.chart_type" class="dot">·</span>
          <span>Updated {{ formatDate(item.updated_at) }}</span>
        </div>
      </div>
    </div>

    <el-dialog
      v-model="viewerOpen"
      :title="active?.name ?? ''"
      width="92vw"
      destroy-on-close
      append-to-body
    >
      <VisualizationViewer
        v-if="viewerOpen && active"
        :viz-id="active.id"
        :appearance="appearance"
        @close="closeViewer"
        @deleted="onDeletedFromViewer"
      />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { Delete, Edit, FolderOpened, MoreFilled, Search } from "@element-plus/icons-vue";
import { useCatalogStore } from "../../stores/catalog-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import { formatDate } from "./catalog-formatters";
import type { VisualizationLibraryItem } from "../../types";
import VisualizationViewer from "./VisualizationViewer.vue";

const emit = defineEmits<{
  (e: "viewTable", tableId: number): void;
}>();

const store = useCatalogStore();
const appearance = useGraphicWalkerAppearance();

const search = ref("");
const viewerOpen = ref(false);
const active = ref<VisualizationLibraryItem | null>(null);

const loading = computed(
  () => store.loadingVisualizationLibrary && !store.visualizationLibrary.length,
);

function sourceLabel(item: VisualizationLibraryItem): string {
  if (item.source_type === "sql") {
    return item.namespace_name
      ? `SQL · ${item.namespace_name}`
      : "SQL query";
  }
  return item.table_full_name ?? "Catalog table";
}

const filtered = computed<VisualizationLibraryItem[]>(() => {
  const q = search.value.trim().toLowerCase();
  if (!q) return store.visualizationLibrary;
  return store.visualizationLibrary.filter((it) => {
    const haystack = [
      it.name,
      it.table_full_name ?? "",
      it.namespace_name ?? "",
      it.chart_type ?? "",
      it.source_type,
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(q);
  });
});

function openViz(item: VisualizationLibraryItem) {
  active.value = item;
  viewerOpen.value = true;
}

function closeViewer() {
  viewerOpen.value = false;
  active.value = null;
  store.loadVisualizationLibrary().catch(() => {});
}

async function onDelete(item: VisualizationLibraryItem) {
  try {
    await ElMessageBox.confirm(
      `Delete visualization "${item.name}"?`,
      "Confirm delete",
      { type: "warning" },
    );
  } catch {
    return;
  }
  try {
    await store.deleteVisualization(item.id);
    ElMessage.success(`Deleted "${item.name}"`);
  } catch (err: any) {
    ElMessage.error(err?.response?.data?.detail ?? err?.message ?? String(err));
  }
}

function onDeletedFromViewer() {
  closeViewer();
}

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
.viz-library-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 12px;
}
.viz-library-card {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 12px 14px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 8px;
  background: var(--el-bg-color);
  cursor: pointer;
  transition:
    background 0.15s,
    border-color 0.15s,
    box-shadow 0.15s;
}
.viz-library-card:hover,
.viz-library-card:focus-visible {
  background: var(--el-fill-color-lighter);
  border-color: var(--el-color-primary-light-5);
  box-shadow: 0 1px 6px rgba(0, 0, 0, 0.06);
  outline: none;
}
.viz-library-card-header {
  display: flex;
  align-items: flex-start;
  gap: 10px;
}
.source-icon {
  flex-shrink: 0;
  margin-top: 2px;
  color: var(--el-text-color-regular);
}
.source-icon.virtual {
  color: var(--el-color-warning);
}
.source-icon.sql {
  color: var(--el-color-primary);
}
.viz-library-card-title {
  flex: 1;
  min-width: 0;
}
.viz-name {
  font-weight: 600;
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viz-source {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  margin-top: 2px;
}
.viz-card-menu {
  cursor: pointer;
  color: var(--el-text-color-secondary);
}
.viz-library-card-meta {
  display: flex;
  gap: 6px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}
.viz-library-card-meta .dot {
  color: var(--el-text-color-disabled);
}
</style>
