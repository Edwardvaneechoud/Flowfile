<template>
  <div class="dashboard-viewer">
    <div class="viewer-toolbar">
      <div class="viewer-toolbar-left">
        <el-button text @click="onBack">
          <el-icon><ArrowLeft /></el-icon> Back
        </el-button>
        <h2 v-if="store.current">{{ store.current.name }}</h2>
        <span v-if="store.current?.namespace_name" class="viewer-ns">{{
          store.current.namespace_name
        }}</span>
      </div>
      <div class="viewer-toolbar-right">
        <el-button v-if="store.current" :loading="refreshing" @click="onRefresh">
          <el-icon><Refresh /></el-icon> Refresh
        </el-button>
        <el-button v-if="store.current" type="primary" @click="onEdit">
          <el-icon><Edit /></el-icon> Edit
        </el-button>
      </div>
    </div>

    <div v-if="store.loadingCurrent" class="viewer-state">
      <el-skeleton :rows="6" animated />
    </div>
    <div v-else-if="!store.current" class="viewer-state">
      <el-empty description="Dashboard not found." />
    </div>
    <template v-else>
      <DashboardFilterBar
        :filters="liveFilters"
        mode="view"
        :datasources-in-use="datasourcesInUse"
        :tiles-by-datasource="tilesByDatasource"
        :tile-label="tileLabel"
        :get-column-stats="getColumnStats"
        :stats-refresh-nonce="statsRefreshNonce"
        @update:filters="onFiltersChange"
      />
      <DashboardCanvas
        :layout="liveLayout"
        mode="view"
        :appearance="appearance"
        :viz-refresh-nonces="vizRefreshNonces"
        :tile-datasource="tileDatasource"
        @edit-viz="onEditVizFromTile"
      />
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { ArrowLeft, Edit, Refresh } from "@element-plus/icons-vue";
import { useDashboardsStore } from "../../stores/dashboards-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import { useDashboardDatasources } from "../../composables/useDashboardDatasources";
import { useDashboardRefresh } from "../../composables/useDashboardRefresh";
import DashboardCanvas from "./DashboardCanvas.vue";
import DashboardFilterBar from "./DashboardFilterBar.vue";
import { EMPTY_DASHBOARD_LAYOUT, type DashboardFilter, type DashboardLayout } from "../../types";

const props = defineProps<{ id: string | number }>();
const router = useRouter();
const store = useDashboardsStore();
const appearance = useGraphicWalkerAppearance();

const dashboardId = computed(() => Number(props.id));

// Filter changes in view mode are local-only (not persisted).
const liveFilters = ref<DashboardFilter[]>([]);
const liveLayout = computed<DashboardLayout>(() => {
  if (!store.current) {
    return { ...EMPTY_DASHBOARD_LAYOUT, tiles: [], filters: [] };
  }
  return { ...store.current.layout, filters: liveFilters.value };
});
const { datasourcesInUse, tilesByDatasource, tileDatasource, tileLabel, getColumnStats, refresh } =
  useDashboardDatasources(liveLayout);

const vizRefreshNonces = ref<Record<number, number>>({});
const { refreshing, statsRefreshNonce, refreshAll } = useDashboardRefresh({
  layout: liveLayout,
  vizRefreshNonces,
  refreshDatasources: refresh,
});

const onRefresh = async () => {
  try {
    await refreshAll();
  } catch {
    ElMessage.error("Failed to refresh dashboard data");
  }
};

watch(
  () => store.current?.layout.filters,
  (filters) => {
    liveFilters.value = filters ? [...filters] : [];
  },
  { immediate: true },
);

const onFiltersChange = (next: DashboardFilter[]) => {
  liveFilters.value = next;
};

const load = async () => {
  try {
    await store.loadDashboard(dashboardId.value);
  } catch {
    ElMessage.error(store.error ?? "Failed to load dashboard");
  }
};

onMounted(load);

const onBack = () => {
  router.push({ name: "catalog", query: { tab: "visuals", kind: "dashboards" } });
};

const onEdit = () => {
  router.push({ name: "dashboard-edit", params: { id: dashboardId.value } });
};

const onEditVizFromTile = (vizId: number | null) => {
  if (vizId == null) return;
  router.push({
    name: "dashboard-edit",
    params: { id: dashboardId.value },
    query: { editViz: String(vizId) },
  });
};
</script>

<style scoped>
.dashboard-viewer {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}
.viewer-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 16px;
  border-bottom: 1px solid var(--el-border-color-lighter);
  background: var(--el-bg-color);
}
.viewer-toolbar-left {
  display: flex;
  align-items: center;
  gap: 12px;
}
.viewer-toolbar-right {
  display: flex;
  align-items: center;
  gap: 8px;
}
.viewer-toolbar-left h2 {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
}
.viewer-ns {
  font-size: 11px;
  color: var(--el-text-color-secondary);
  text-transform: uppercase;
}
.viewer-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
</style>
