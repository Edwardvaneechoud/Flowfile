<template>
  <div class="dashboard-viewer" :class="{ 'is-presenting': presenting }">
    <div class="viewer-toolbar">
      <div class="viewer-toolbar-left">
        <button v-if="!presenting" class="back-btn" @click="onBack">All dashboards</button>
        <h2 v-if="store.current">{{ store.current.name }}</h2>
        <span v-if="!presenting && store.current?.namespace_name" class="viewer-ns">{{
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
        <el-dropdown
          v-if="store.current"
          trigger="click"
          placement="bottom-end"
          @command="onAutoRefreshCommand"
        >
          <el-button
            data-testid="dashboard-auto-refresh"
            :title="autoRefreshTitle"
            :class="{ 'auto-refresh-on': effectiveInterval != null }"
          >
            <el-icon><Timer /></el-icon> {{ autoRefreshText }}
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item
                v-for="choice in AUTO_REFRESH_CHOICES"
                :key="String(choice)"
                :command="choice == null ? 'off' : String(choice)"
                :class="{ 'auto-refresh-current': !paused && choice === selectedInterval }"
              >
                {{ choice == null ? "Off" : `Every ${autoRefreshLabel(choice)}` }}
                <span v-if="choice === savedInterval" class="auto-refresh-saved">saved</span>
              </el-dropdown-item>
              <el-dropdown-item
                v-if="selectedInterval != null"
                divided
                :command="paused ? 'resume' : 'pause'"
              >
                {{ paused ? "Resume" : "Pause" }}
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button v-if="store.current" :loading="refreshing" @click="onRefresh">
          <el-icon><Refresh /></el-icon> Refresh
        </el-button>
        <el-button
          v-if="store.current && !presenting"
          data-testid="dashboard-present"
          title="Show the dashboard without the app around it"
          @click="onPresent"
        >
          <el-icon><FullScreen /></el-icon> Present
        </el-button>
        <el-button v-if="store.current && canEdit && !presenting" type="primary" @click="onEdit">
          <el-icon><Edit /></el-icon> Edit
        </el-button>
        <el-button
          v-if="presenting"
          data-testid="dashboard-exit-present"
          title="Leave present mode (Esc)"
          @click="onExitPresent"
        >
          <el-icon><Close /></el-icon> Exit
        </el-button>
      </div>
    </div>

    <div
      v-if="store.current && ((store.current.description && !presenting) || stamps.length)"
      class="viewer-meta"
    >
      <p v-if="store.current.description && !presenting" class="viewer-description">
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
      <DashboardTabBar
        :layout="liveLayout"
        :active-tab-id="activeTabId"
        mode="view"
        @select="onSelectTab"
      />
      <DashboardCanvas
        :key="activeTabId ?? 'canvas'"
        :layout="tabLayout"
        mode="view"
        :appearance="appearance"
        :viz-refresh-nonces="vizRefreshNonces"
        :data-refresh-nonce="statsRefreshNonce"
        :tile-datasource="tileDatasource"
        :get-column-stats="getColumnStats"
        :search-values="searchFilterValues"
        :can-edit-charts="canEdit && !presenting"
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
import { Close, Edit, FullScreen, Refresh, Timer } from "@element-plus/icons-vue";
import { useDashboardsStore } from "../../stores/dashboards-store";
import { usePresentationStore } from "../../stores/presentation-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import { useDashboardDatasources } from "../../composables/useDashboardDatasources";
import { useDashboardRefresh } from "../../composables/useDashboardRefresh";
import { provideDashboardVizCache } from "../../composables/useDashboardVizCache";
import { provideDashboardColors } from "../../composables/useDashboardColors";
import { asUtc } from "../../utils/catalogCdc";
import { timeAgo } from "../../utils/time";
import DashboardCanvas from "./DashboardCanvas.vue";
import DashboardFilterBar from "./DashboardFilterBar.vue";
import DashboardTabBar from "./DashboardTabBar.vue";
import { resolveTabId, tabCanvasLayout } from "./dashboardTabs";
import { decodeFilterState, encodeFilterState, filtersDiffer } from "./filterUrlState";
import { placedFilterIds } from "./filterPlacement";
import {
  AUTO_REFRESH_CHOICES,
  autoRefreshLabel,
  createAutoRefresher,
  savedAutoRefresh,
} from "./autoRefresh";
import { mayEdit } from "./dashboardAccess";
import {
  exitNativeFullscreen,
  isPresentQuery,
  requestNativeFullscreen,
  viewerQuery,
} from "./presentMode";
import { DEVICE_WIDTH_VIEWPORT, setViewportMeta } from "./viewportMeta";
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
// The page's own state, written to ?tab= with ?f= by writeUrl; an unknown id shows the first tab.
const chosenTabId = ref<string | null>(
  typeof route.query.tab === "string" ? route.query.tab : null,
);
// A link or history step to another ?tab= is followed; a write that lost the key is not.
watch(
  () => route.query.tab,
  (tab) => {
    if (typeof tab === "string") chosenTabId.value = tab;
  },
);
const activeTabId = computed(() => resolveTabId(liveLayout.value, chosenTabId.value));
// Only the active tab's tiles mount, so only they query and refresh.
const tabLayout = computed(() => tabCanvasLayout(liveLayout.value, activeTabId.value));
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
  layout: tabLayout,
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

// The author's interval is the default; a viewer's pick or pause lasts this session only.
const savedInterval = computed(() => savedAutoRefresh(store.current?.layout.auto_refresh_seconds));
const chosenInterval = ref<number | null | undefined>(undefined);
const paused = ref(false);
const selectedInterval = computed(() =>
  chosenInterval.value === undefined ? savedInterval.value : chosenInterval.value,
);
const effectiveInterval = computed(() => (paused.value ? null : selectedInterval.value));

const autoRefreshText = computed(() => {
  if (selectedInterval.value == null) return "Auto-refresh off";
  return paused.value ? "Auto-refresh paused" : `Every ${autoRefreshLabel(selectedInterval.value)}`;
});
const autoRefreshTitle = computed(() =>
  effectiveInterval.value == null
    ? "Refresh the data on a timer while this page is open"
    : `Refreshes the data every ${autoRefreshLabel(effectiveInterval.value)} while this page is open`,
);

// Quiet: a failed tick leaves its errors on the tiles instead of a toast every interval.
const refresher = createAutoRefresher({
  refresh: async () => {
    await refreshAll();
    refreshedAt.value = new Date();
  },
  isBusy: () => refreshing.value,
  isHidden: () => document.hidden,
});
watch(effectiveInterval, (seconds) => refresher.setInterval(seconds), { immediate: true });

const onAutoRefreshCommand = (command: string) => {
  if (command === "pause") paused.value = true;
  else if (command === "resume") paused.value = false;
  else {
    chosenInterval.value = command === "off" ? null : Number(command);
    paused.value = false;
  }
};

const onVisibilityChange = () => refresher.visibilityChanged();

const presentation = usePresentationStore();
const presenting = computed(() => presentation.presenting);
const canEdit = computed(() => mayEdit(store.current?.access));
// Only a fullscreen this page asked for is left again with present mode.
let nativeFullscreen = false;

// Page state written to the URL by writeUrl; ?present=1 on load or in a link opens a wall screen.
const presentOn = ref(isPresentQuery(route.query));
watch(
  () => isPresentQuery(route.query),
  (on) => {
    if (on) presentOn.value = true;
  },
);
watch(
  presentOn,
  (on) => {
    if (on) presentation.enter();
    else presentation.exit();
  },
  { immediate: true },
);

const onPresent = () => {
  // Asked first, inside the click: browsers only allow fullscreen from a user gesture.
  void requestNativeFullscreen(document.documentElement).then((ok) => {
    nativeFullscreen = ok;
  });
  presentOn.value = true;
  writeUrl();
};

const onExitPresent = () => {
  if (nativeFullscreen) {
    nativeFullscreen = false;
    void exitNativeFullscreen();
  }
  presentOn.value = false;
  writeUrl();
};

const onFullscreenChange = () => {
  // The browser's own Esc leaves fullscreen without a keydown reaching the page.
  if (nativeFullscreen && !document.fullscreenElement) onExitPresent();
};

// A popper on its way out is closed: a select list left mid-transition by Present stays in it.
const overlayOpen = (): boolean =>
  Array.from(
    document.querySelectorAll<HTMLElement>(".el-overlay, .el-dropdown__popper, .el-select__popper"),
  ).some(
    (el) =>
      el.style.display !== "none" &&
      el.getClientRects().length > 0 &&
      !/-leave-(active|to)\b/.test(el.className),
  );

const onPresentKeydown = (event: KeyboardEvent) => {
  if (event.key !== "Escape" || !presentation.presenting || event.defaultPrevented) return;
  if (overlayOpen()) return;
  onExitPresent();
};

// Only this page asks a phone for its own width; leaving puts back what the app had.
let restoreViewport: (() => void) | null = null;

onMounted(() => {
  restoreViewport = setViewportMeta(DEVICE_WIDTH_VIEWPORT);
  document.addEventListener("visibilitychange", onVisibilityChange);
  document.addEventListener("fullscreenchange", onFullscreenChange);
  window.addEventListener("keydown", onPresentKeydown);
});

onBeforeUnmount(() => {
  restoreViewport?.();
  refresher.stop();
  document.removeEventListener("visibilitychange", onVisibilityChange);
  document.removeEventListener("fullscreenchange", onFullscreenChange);
  window.removeEventListener("keydown", onPresentKeydown);
  presentation.exit();
  if (nativeFullscreen) void exitNativeFullscreen();
});

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

// Every key from the page's state: navigations resolve after async guards, so a route.query read
// while another write is in flight can be stale.
function writeUrl() {
  if (urlTimer) clearTimeout(urlTimer);
  urlTimer = null;
  const query = viewerQuery(route.query, {
    filters: encodeFilterState(liveFilters.value, savedFilters.value),
    tab: chosenTabId.value,
    present: presentOn.value,
  });
  void router.replace({ query });
}

// Debounced, so typing a number does not write the URL on every keystroke.
const syncUrl = () => {
  if (urlTimer) clearTimeout(urlTimer);
  urlTimer = setTimeout(writeUrl, 300);
};

const onSelectTab = (tabId: string) => {
  chosenTabId.value = tabId;
  writeUrl();
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
  if (id === previous) return;
  chosenInterval.value = undefined;
  paused.value = false;
  chosenTabId.value = typeof route.query.tab === "string" ? route.query.tab : null;
  presentOn.value = isPresentQuery(route.query);
  void load();
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
.dashboard-viewer.is-presenting {
  padding: var(--spacing-3) var(--spacing-4) 0;
}
/* On a phone the pane's own padding already insets the page; the tiles need the width. */
@media (max-width: 640px) {
  .dashboard-viewer,
  .dashboard-viewer.is-presenting {
    padding: var(--spacing-2) var(--spacing-2) 0;
  }
}
.viewer-toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 0 0 var(--spacing-4);
}
.viewer-toolbar .back-btn {
  margin-bottom: 0;
}
.viewer-toolbar-left {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}
.viewer-toolbar-right {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}
/* el-button adds a left margin to every sibling button; the gap spaces them. */
.viewer-toolbar-right :deep(.el-button + .el-button) {
  margin-left: 0;
}
.auto-refresh-on {
  color: var(--el-color-primary);
}
.auto-refresh-saved {
  margin-left: 8px;
  font-size: 11px;
  color: var(--el-text-color-secondary);
}
:global(.el-dropdown-menu__item.auto-refresh-current) {
  color: var(--el-color-primary);
  font-weight: 600;
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
