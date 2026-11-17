<template>
  <div class="dashboard-viewer">
    <div class="viewer-toolbar">
      <div class="viewer-toolbar-left">
        <button class="back-btn" @click="onBack">All dashboards</button>
        <h2 v-if="store.current">{{ store.current.name }}</h2>
        <span v-if="store.current?.namespace_name" class="viewer-ns">{{
          store.current.namespace_name
        }}</span>
      </div>
      <div class="viewer-toolbar-right">
        <el-button
          v-if="filtersChanged && !stripFilterCount"
          text
          title="Back to the filters this dashboard was saved with"
          @click="onResetFilters"
        >
          Reset
        </el-button>
        <el-button v-if="store.current" :loading="refreshing" @click="onRefresh">
          <el-icon><Refresh /></el-icon> Refresh
        </el-button>
        <el-button v-if="store.current" type="primary" @click="onEdit">
          <el-icon><Edit /></el-icon> Edit
        </el-button>
      </div>
    </div>

    <div v-if="store.current && (store.current.description || stamps.length)" class="viewer-meta">
      <p v-if="store.current.description" class="viewer-description">
        {{ store.current.description }}
      </p>
      <span v-for="stamp in stamps" :key="stamp.text" class="viewer-stamp" :title="stamp.title">
        {{ stamp.text }}
      </span>
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
        :placed-filter-ids="placedIds"
        mode="view"
        :datasources-in-use="datasourcesInUse"
        :tiles-by-datasource="tilesByDatasource"
        :viz-tile-ids="vizTileIds"
        :tile-label="tileLabel"
        :tile-fields="tileFields"
        :load-tile-fields="loadTileFields"
        :get-column-stats="getColumnStats"
        :search-values="searchFilterValues"
        :stats-refresh-nonce="statsRefreshNonce"
        :can-reset="filtersChanged"
        @update:filters="onFiltersChange"
        @reset="onResetFilters"
      />
      <DashboardCanvas
        :layout="liveLayout"
        mode="view"
        :appearance="appearance"
        :viz-refresh-nonces="vizRefreshNonces"
        :data-refresh-nonce="statsRefreshNonce"
        :tile-datasource="tileDatasource"
        :get-column-stats="getColumnStats"
        :search-values="searchFilterValues"
        @edit-viz="onEditVizFromTile"
        @update:filter="onFilterChange"
      />
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { Edit, Refresh } from "@element-plus/icons-vue";
import { useDashboardsStore } from "../../stores/dashboards-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import { useDashboardDatasources } from "../../composables/useDashboardDatasources";
import { useDashboardRefresh } from "../../composables/useDashboardRefresh";
import { provideDashboardVizCache } from "../../composables/useDashboardVizCache";
import { provideDashboardColors } from "../../composables/useDashboardColors";
import { asUtc } from "../../utils/catalogCdc";
import { timeAgo } from "../../utils/time";
import DashboardCanvas from "./DashboardCanvas.vue";
import DashboardFilterBar from "./DashboardFilterBar.vue";
import { decodeFilterState, encodeFilterState, filtersDiffer } from "./filterUrlState";
import { placedFilterIds } from "./filterPlacement";
import { EMPTY_DASHBOARD_LAYOUT, type DashboardFilter, type DashboardLayout } from "../../types";

const props = defineProps<{ id: string | number }>();
const router = useRouter();
const route = useRoute();
const store = useDashboardsStore();
const appearance = useGraphicWalkerAppearance();

const dashboardId = computed(() => Number(props.id));
const vizCache = provideDashboardVizCache();
provideDashboardColors(() => store.current?.layout.color_assignments);

// A viewer's filter changes are not saved with the dashboard; they live in the URL instead.
const liveFilters = ref<DashboardFilter[]>([]);
const savedFilters = computed<DashboardFilter[]>(() => store.current?.layout.filters ?? []);
const filtersChanged = computed(() => filtersDiffer(liveFilters.value, savedFilters.value));
const liveLayout = computed<DashboardLayout>(() => {
  if (!store.current) {
    return { ...EMPTY_DASHBOARD_LAYOUT, tiles: [], filters: [] };
  }
  return { ...store.current.layout, filters: liveFilters.value };
});
const {
  datasourcesInUse,
  tilesByDatasource,
  vizTileIds,
  tileDatasource,
  tileLabel,
  tileFields,
  loadTileFields,
  getColumnStats,
  searchFilterValues,
  refresh,
} = useDashboardDatasources(liveLayout, vizCache);

const vizRefreshNonces = ref<Record<number, number>>({});
const { refreshing, statsRefreshNonce, refreshAll } = useDashboardRefresh({
  layout: liveLayout,
  vizRefreshNonces,
  refreshDatasources: refresh,
});

const refreshedAt = ref<Date | null>(null);

const onRefresh = async () => {
  try {
    await refreshAll();
    refreshedAt.value = new Date();
  } catch {
    ElMessage.error("Failed to refresh dashboard data");
  }
};

/** When the newest table behind the dashboard last changed; null for SQL-only dashboards. */
const dataUpdatedAt = computed<string | null>(() => {
  const times = datasourcesInUse.value
    .map((d) => d.updated_at)
    .filter((t): t is string => !!t)
    .map(asUtc)
    .sort();
  return times.length ? times[times.length - 1] : null;
});

const stamps = computed<{ text: string; title: string }[]>(() => {
  const out: { text: string; title: string }[] = [];
  if (dataUpdatedAt.value) {
    out.push({
      text: `Data updated ${timeAgo(dataUpdatedAt.value)}`,
      title: `The newest table this dashboard reads changed on ${new Date(dataUpdatedAt.value).toLocaleString()}`,
    });
  }
  if (refreshedAt.value) {
    out.push({
      text: `Refreshed ${refreshedAt.value.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`,
      title: "When this page last asked for the data",
    });
  }
  return out;
});

watch(
  () => store.current?.layout.filters,
  (filters) => {
    liveFilters.value = decodeFilterState(route.query.f, filters ?? []);
  },
  { immediate: true },
);

let urlTimer: ReturnType<typeof setTimeout> | null = null;

// Debounced, so typing a number does not write the URL on every keystroke.
const syncUrl = () => {
  if (urlTimer) clearTimeout(urlTimer);
  urlTimer = setTimeout(() => {
    urlTimer = null;
    const query = { ...route.query };
    const encoded = encodeFilterState(liveFilters.value, savedFilters.value);
    if (encoded) query.f = encoded;
    else delete query.f;
    void router.replace({ query });
  }, 300);
};

onBeforeUnmount(() => {
  if (urlTimer) clearTimeout(urlTimer);
});

const onFiltersChange = (next: DashboardFilter[]) => {
  liveFilters.value = next;
  syncUrl();
};

const onFilterChange = (next: DashboardFilter) =>
  onFiltersChange(liveFilters.value.map((f) => (f.id === next.id ? next : f)));

const placedIds = computed(() => placedFilterIds(liveLayout.value));
// With every filter on the canvas the strip takes no space, so Reset moves to the toolbar.
const stripFilterCount = computed(
  () => liveFilters.value.filter((f) => !placedIds.value.has(f.id)).length,
);

const onResetFilters = () => {
  liveFilters.value = savedFilters.value.map((f) => ({ ...f }));
  syncUrl();
};

const load = async () => {
  try {
    await store.loadDashboard(dashboardId.value);
    refreshedAt.value = new Date();
  } catch {
    ElMessage.error(store.error ?? "Failed to load dashboard");
  }
};

onMounted(load);
// A link to another dashboard (the global search) reuses this page.
watch(dashboardId, (id, previous) => {
  if (id !== previous) void load();
});

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
/* Padding stacks on .catalog-detail's so the header lines up with the library page. */
.dashboard-viewer {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
  padding: var(--spacing-3) var(--spacing-6) 0;
}
.viewer-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 0 var(--spacing-4);
}
.viewer-toolbar .back-btn {
  margin-bottom: 0;
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
.viewer-meta {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 4px 16px;
  margin: calc(-1 * var(--spacing-2)) 0 var(--spacing-3);
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.viewer-description {
  flex: 1 1 320px;
  margin: 0;
  font-size: 13px;
  color: var(--el-text-color-regular);
}
.viewer-stamp {
  white-space: nowrap;
}
.viewer-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
</style>
