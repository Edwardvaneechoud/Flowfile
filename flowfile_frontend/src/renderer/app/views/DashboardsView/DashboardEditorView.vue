<template>
  <div class="dashboard-editor">
    <div class="editor-toolbar">
      <div class="editor-toolbar-left">
        <button class="back-btn" @click="onBack">
          <i class="fa-solid fa-arrow-left"></i> Back
        </button>
        <el-input
          v-if="store.current"
          v-model="nameDraft"
          size="small"
          placeholder="Dashboard name"
          class="editor-name"
          maxlength="120"
        />
        <el-input
          v-if="store.current"
          v-model="descriptionDraft"
          size="small"
          placeholder="Description (optional)"
          class="editor-description"
          maxlength="300"
        />
        <SaveStatusIndicator :state="autosaveState" />
      </div>
      <div class="editor-toolbar-right">
        <template v-if="store.current">
          <el-tooltip :content="UNDO_HINT" placement="bottom" :show-after="300">
            <span class="history-btn-wrap">
              <el-button
                class="history-btn"
                aria-label="Undo"
                data-testid="dashboard-undo"
                :disabled="!canUndo || reverting"
                @click="onUndo"
              >
                <span class="material-icons">undo</span>
              </el-button>
            </span>
          </el-tooltip>
          <el-tooltip :content="REDO_HINT" placement="bottom" :show-after="300">
            <span class="history-btn-wrap">
              <el-button
                class="history-btn"
                aria-label="Redo"
                data-testid="dashboard-redo"
                :disabled="!canRedo || reverting"
                @click="onRedo"
              >
                <span class="material-icons">redo</span>
              </el-button>
            </span>
          </el-tooltip>
        </template>
        <el-select
          v-if="store.current"
          v-model="autoRefreshSetting"
          class="editor-auto-refresh"
          data-testid="dashboard-auto-refresh-setting"
          title="How often a viewer's page refreshes the data; a viewer can change or pause it"
        >
          <template #prefix>
            <el-icon><Timer /></el-icon>
          </template>
          <el-option
            v-for="choice in AUTO_REFRESH_CHOICES"
            :key="String(choice)"
            :label="choice == null ? 'Auto-refresh off' : `Every ${autoRefreshLabel(choice)}`"
            :value="choice ?? 0"
          />
        </el-select>
        <el-button
          v-if="canBuildWithAi"
          data-testid="dashboard-ai-open"
          title="Ask a question over your tables; the AI builds a page that answers it"
          @click="aiBuilderOpen = true"
        >
          <span class="material-icons ai-build-icon" aria-hidden="true">auto_awesome</span>
          Build with AI
        </el-button>
        <el-button v-if="store.current" :loading="refreshing" @click="onRefresh">
          <el-icon><Refresh /></el-icon> Refresh
        </el-button>
        <el-button :disabled="!canRevert || reverting" :loading="reverting" @click="onRevert">
          Revert
        </el-button>
      </div>
    </div>

    <AutosaveConflictBanner
      :state="autosaveState"
      resource-label="dashboard"
      :busy="conflictBusy"
      @reload="onConflictReload"
      @overwrite="onConflictOverwrite"
    />

    <el-alert v-if="restoreAvailable" type="info" :closable="false" show-icon class="draft-banner">
      <template #title>You have an unfinished dashboard from last time.</template>
      <div class="draft-banner-actions">
        <el-button size="small" type="primary" @click="onRestoreDraft">Continue draft</el-button>
        <el-button size="small" @click="onDiscardDraft">Start fresh</el-button>
      </div>
    </el-alert>

    <div class="editor-body">
      <aside class="editor-sidebar">
        <DashboardSidebarPicker
          :added-viz-ids="addedVizIds"
          @add="onAddTile"
          @add-text="onAddTextTile"
          @add-separator="onAddSeparatorTile"
          @add-kpi="onAddKpiTile"
          @add-table="onAddTableTile"
          @add-image="onAddImageTile"
          @add-filter="onAddFilterTile"
          @create="sourcePickerOpen = true"
        />
      </aside>
      <main class="editor-canvas">
        <div v-if="store.loadingCurrent" class="editor-state">
          <el-skeleton :rows="6" animated />
        </div>
        <div v-else-if="!store.current" class="editor-state">
          <el-empty description="Dashboard not found." />
        </div>
        <template v-else>
          <DashboardFilterBar
            ref="filterBarRef"
            :filters="store.current.layout.filters"
            :placed-filter-ids="placedIds"
            mode="edit"
            :datasources-in-use="datasourcesInUse"
            :tiles-by-datasource="tilesByDatasource"
            :viz-tile-ids="vizTileIds"
            :tile-label="tileLabelOnTab"
            :tile-fields="tileFields"
            :load-tile-fields="loadTileFields"
            :get-column-stats="getColumnStats"
            :search-values="searchFilterValues"
            :stats-refresh-nonce="statsRefreshNonce"
            :datasource-of="datasourceOf"
            :load-datasource="loadDatasource"
            :no-tile-hints="noTileHints"
            @update:filters="onFiltersChange"
            @place="onPlaceFilter"
          />
          <DashboardTabBar
            :layout="store.current.layout"
            :active-tab-id="currentTabId"
            mode="edit"
            @select="activeTabId = $event"
            @update:layout="onLayoutChange"
          />
          <DashboardCanvas
            :key="currentTabId ?? 'canvas'"
            :layout="tabLayout"
            mode="edit"
            :appearance="appearance"
            :viz-refresh-nonces="vizRefreshNonces"
            :data-refresh-nonce="statsRefreshNonce"
            :tile-datasource="tileDatasource"
            :get-column-stats="getColumnStats"
            :search-values="searchFilterValues"
            :move-targets="moveTargets"
            :no-tile-hints="noTileHints"
            @update:layout="onCanvasLayoutChange"
            @edit-viz="onEditViz"
            @keep-chart="onKeepChart"
            @add-viz-at="onAddVizAt"
            @add-text-at="onAddTextAt"
            @add-separator-at="onAddSeparatorAt"
            @add-kpi-at="onAddKpiAt"
            @add-table-at="onAddTableAt"
            @add-image-at="onAddImageAt"
            @place-filter-at="onPlaceFilterAt"
            @add-filter-at="onAddFilterAt"
            @update:filter="onFilterChange"
            @edit-filter="(id: string) => filterBarRef?.openEdit(id)"
            @delete-filter="onDeleteFilter"
            @move-tile-to-tab="onMoveTileToTab"
          />
        </template>
      </main>
    </div>

    <el-dialog
      v-model="vizDialogOpen"
      :title="editingVizId ? `Editing visualization #${editingVizId}` : ''"
      width="92vw"
      destroy-on-close
      append-to-body
      :before-close="onVizDialogBeforeClose"
      @close="onCloseVizDialog"
    >
      <VisualizationViewer
        v-if="vizDialogOpen && editingVizId"
        ref="vizViewerRef"
        :viz-id="editingVizId"
        :appearance="appearance"
        @updated="onVizUpdated"
      />
    </el-dialog>

    <VisualizationSourcePicker v-model="sourcePickerOpen" @picked="onSourcePicked" />

    <DashboardAiBuilder v-if="canBuildWithAi" v-model="aiBuilderOpen" :place="placeAiPage" />

    <el-dialog
      v-model="vizCreatorOpen"
      title="New visualization"
      width="92vw"
      destroy-on-close
      append-to-body
      :close-on-click-modal="false"
      :before-close="onVizCreatorBeforeClose"
    >
      <VisualizationEditor
        v-if="vizCreatorOpen && pendingSource"
        ref="vizCreatorRef"
        :source="pendingSource"
        :taken-names="pendingTakenNames"
        :owner-dashboard-id="ensureDashboardCreated"
        :appearance="appearance"
        @saved="onVizCreated"
        @cancel="vizCreatorOpen = false"
      />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { onBeforeRouteLeave, useRoute, useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { Refresh, Timer } from "@element-plus/icons-vue";
import { CatalogApi } from "../../api/catalog.api";
import { useDashboardsStore } from "../../stores/dashboards-store";
import { useAuthStore } from "../../stores/auth-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import { useDashboardDatasources } from "../../composables/useDashboardDatasources";
import { useDashboardRefresh } from "../../composables/useDashboardRefresh";
import { provideDashboardVizCache } from "../../composables/useDashboardVizCache";
import { provideDashboardColors } from "../../composables/useDashboardColors";
import { useAutosave } from "../../composables/useAutosave";
import { classifyAutosaveError } from "../../composables/autosaveEngine";
import { createLocalDraft, type LocalDraft } from "../../composables/localDraft";
import { createDashboardCreation, newDashboardName, type CreationPhase } from "./dashboardCreation";
import { catalogSaveErrorMessage } from "../../composables/saveError";
import { toPlainJson, toPlainShared } from "../../utils/structuredClone";
import SaveStatusIndicator from "../../components/common/SaveStatusIndicator.vue";
import AutosaveConflictBanner from "../../components/common/AutosaveConflictBanner.vue";
import DashboardCanvas from "./DashboardCanvas.vue";
import DashboardFilterBar from "./DashboardFilterBar.vue";
import DashboardSidebarPicker from "./DashboardSidebarPicker.vue";
import DashboardTabBar from "./DashboardTabBar.vue";
import VisualizationViewer from "../CatalogView/VisualizationViewer.vue";
import VisualizationSourcePicker from "../CatalogView/VisualizationSourcePicker.vue";
import VisualizationEditor from "../CatalogView/VisualizationEditor.vue";
import { dashboardChartNames, distinctName, nameFromAiTitle } from "../CatalogView/chartNaming";
import { chartTypeOf } from "../CatalogView/vizAutosave";
import { useCatalogStore } from "../../stores/catalog-store";
import { useAiDashboardStore } from "../../stores/ai-dashboard-store";
import { useAiStore } from "../../stores/ai-store";
import DashboardAiBuilder, { type PlacedPage } from "./DashboardAiBuilder.vue";
import {
  landsOnTab,
  layoutPage,
  pageFilters,
  placeOnDashboard,
  type BuiltPage,
  type PageNote,
  type PlacedChart,
} from "./aiDashboard";
import { mayEdit } from "./dashboardAccess";
import { historyShortcutFor } from "../../composables/useFlowHotkeys";
import { IS_MAC } from "../../utils/shortcuts";
import { upgradeLayoutGrid } from "./gridVersion";
import { sameAssignments, withoutColorAssignments } from "./colorAssignment";
import {
  EditHistory,
  dashboardChange,
  sameDashboardSnapshot,
  type DashboardSnapshot,
  type RecordOptions,
} from "./editHistory";
import {
  addPlacedFilter,
  deleteFilter,
  filtersWithoutSourceTiles,
  placeFilter,
  placedFilterIds,
  pruneFilterTargets,
  withFilters,
} from "./filterPlacement";
import { firstFreeRow } from "./gridDrop";
import {
  healTabs,
  layoutTabs,
  mergeTabCanvas,
  moveTileToTab,
  resolveTabId,
  tabCanvasLayout,
  tabOfTile,
  tabTitleOfTile,
} from "./dashboardTabs";
import { KPI_DEFAULT_SIZE } from "./kpi";
import { IMAGE_DEFAULT_SIZE } from "./imageTile";
import { provideDashboardImageBudget } from "../../composables/useDashboardImageBudget";
import { AUTO_REFRESH_CHOICES, autoRefreshLabel, savedAutoRefresh } from "./autoRefresh";
import { TABLE_DEFAULT_SIZE } from "./tableTile";
import {
  EMPTY_DASHBOARD_LAYOUT,
  type CatalogVisualization,
  type Dashboard,
  type DashboardCreatePayload,
  type DashboardFilter,
  type DashboardLayout,
  type DashboardTile,
  type DashboardUpdatePayload,
  type SeparatorOrientation,
  type VizSourceDescriptor,
} from "../../types";

const props = defineProps<{ id?: string | number }>();
const router = useRouter();
const route = useRoute();
const store = useDashboardsStore();
const catalogStore = useCatalogStore();
const appearance = useGraphicWalkerAppearance();

// Opened from "New dashboard": not on the server until its first edit creates it.
const openedNew = props.id === undefined;
const creationPhase = ref<CreationPhase>(openedNew ? "pending" : "created");
const creation = createDashboardCreation<DashboardCreatePayload, Dashboard>({
  id: openedNew ? null : Number(props.id),
  send: (payload) => store.autosaveCreateDashboard(payload),
  onChange: (phase) => {
    creationPhase.value = phase;
  },
});
const dashboardId = (): number => (props.id !== undefined ? Number(props.id) : creation.id()!);
// Set while a navigation away waits in the leave guard, so a create landing then keeps the URL.
let leaving = false;
const nameDraft = ref("");
const descriptionDraft = ref("");
const descriptionValue = (): string | null => descriptionDraft.value.trim() || null;

const serializeDashboard = (dashName: string, layout: unknown, description: string | null) =>
  JSON.stringify({ name: dashName, description, layout });

// Revert anchor: the state captured when the editor opened this dashboard.
const openSnapshot = ref<{
  name: string;
  description: string | null;
  layout: DashboardLayout;
} | null>(null);
const openSerialized = ref("");
// CAS token echoed into every PUT; refreshed from each response.
const expectedLayoutVersion = ref<number | null>(null);
const sessionChanged = ref(false);
const reverting = ref(false);
const conflictBusy = ref(false);

const { engine, state: autosaveState } = useAutosave<DashboardUpdatePayload>({
  getSnapshot: () => {
    if (!store.current) return null;
    const trimmed = nameDraft.value.trim();
    if (!trimmed) return null;
    const layout = toPlainJson(store.current.layout) as DashboardLayout;
    const description = descriptionValue();
    return {
      payload: { name: trimmed, description, layout },
      serialized: serializeDashboard(trimmed, layout, description),
    };
  },
  save: async (payload) => {
    if (!creation.isCreated()) {
      const created = await creation.create({
        name: payload.name ?? "",
        description: payload.description ?? null,
        namespace_id: store.current?.namespace_id ?? null,
        layout: payload.layout,
      });
      adoptCreated(created, payload);
      return;
    }
    const body: DashboardUpdatePayload = { ...payload };
    if (expectedLayoutVersion.value != null) {
      body.expected_layout_version = expectedLayoutVersion.value;
    }
    const updated = await store.autosaveDashboard(dashboardId(), body);
    expectedLayoutVersion.value = updated.layout_version;
    sessionChanged.value =
      serializeDashboard(payload.name ?? "", payload.layout, payload.description ?? null) !==
      openSerialized.value;
  },
});

// The same instance carries on under the edit route; the leave guard lets this replace through.
const showCreatedInUrl = () => {
  const id = creation.id();
  if (leaving || id === null || route.name !== "dashboard-new") return;
  void router.replace({ name: "dashboard-edit", params: { id } });
};

const adoptCreated = (created: Dashboard, payload: DashboardUpdatePayload) => {
  expectedLayoutVersion.value = created.layout_version ?? 1;
  sessionChanged.value =
    serializeDashboard(payload.name ?? "", payload.layout, payload.description ?? null) !==
    openSerialized.value;
  showCreatedInUrl();
  // Edits made while the create was out go now, not a debounce later.
  void engine.flush();
};

/**
 * The dashboard's id, creating the dashboard first when it is not on the server yet, as its first
 * edit would. A chart made from here names it as the dashboard it belongs to.
 */
const ensureDashboardCreated = async (): Promise<number> => {
  if (creation.isCreated()) return dashboardId();
  const name = nameDraft.value.trim();
  if (!store.current || !name) throw new Error("Give the dashboard a name first.");
  const payload = {
    name,
    description: descriptionValue(),
    layout: toPlainJson(store.current.layout) as DashboardLayout,
  };
  // A create already out is the autosave's, and the autosave adopts its result.
  const ours = creationPhase.value !== "creating";
  const created = await creation.create({
    ...payload,
    namespace_id: store.current.namespace_id ?? null,
  });
  if (ours) adoptCreated(created, payload);
  return created.id;
};

const canRevert = computed(() => {
  const st = autosaveState.value.status;
  if (st === "conflict" || st === "blocked") return false;
  return !!openSnapshot.value && (sessionChanged.value || st !== "idle");
});

// A local draft the earlier create-first flow left: read once, offered, never written again.
const auth = useAuthStore();
const restoreAvailable = ref(false);
interface NewDashboardDraft {
  name: string;
  description?: string;
  layout: DashboardLayout;
}
let oldDraft: LocalDraft<NewDashboardDraft> | null = null;
let pendingDraft: NewDashboardDraft | null = null;

const offerOldDraft = () => {
  const userKey = auth.currentUser?.id ?? auth.currentUser?.username ?? "anon";
  oldDraft = createLocalDraft<NewDashboardDraft>({
    key: `dashboards.draft.new.${userKey}`,
    version: 1,
    serialize: () => null,
  });
  const existing = oldDraft.peek();
  if (existing) {
    pendingDraft = existing.data;
    restoreAvailable.value = true;
  }
};

const settleOldDraft = () => {
  oldDraft?.discard();
  pendingDraft = null;
  restoreAvailable.value = false;
};

// Loaded like an edit, so it is created like one and Undo takes it back to the blank state.
const onRestoreDraft = () => {
  const restored = pendingDraft;
  settleOldDraft();
  if (!restored || !store.current) return;
  nameDraft.value = restored.name;
  descriptionDraft.value = restored.description ?? "";
  store.setLayout(pruneFilterTargets(healTabs(upgradeLayoutGrid(restored.layout))));
  markChanged();
};

const onDiscardDraft = () => settleOldDraft();

const UNDO_HINT = IS_MAC ? "Undo (⌘Z)" : "Undo (Ctrl+Z)";
const REDO_HINT = IS_MAC ? "Redo (⇧⌘Z)" : "Redo (Ctrl+Shift+Z or Ctrl+Y)";

// Session-only: reset when a dashboard opens, a draft is restored or a conflict is settled.
const history = new EditHistory<DashboardSnapshot>({ equals: sameDashboardSnapshot });
const canUndo = ref(false);
const canRedo = ref(false);

const syncHistoryFlags = () => {
  canUndo.value = history.canUndo;
  canRedo.value = history.canRedo;
};

const currentSnapshot = (): DashboardSnapshot | null =>
  store.current
    ? {
        name: nameDraft.value,
        description: descriptionDraft.value,
        // Shared strings: a hundred snapshots of an image's data URL hold one copy of its bytes.
        layout: withoutColorAssignments(toPlainShared(store.current.layout)),
      }
    : null;

const resetHistory = () => {
  history.reset(currentSnapshot(), Date.now());
  syncHistoryFlags();
};

const recordEdit = (options?: Pick<RecordOptions, "key" | "absorb">) => {
  const snapshot = currentSnapshot();
  if (!snapshot) return;
  history.record(snapshot, {
    ...(options ?? dashboardChange(history.present, snapshot)),
    now: Date.now(),
  });
  syncHistoryFlags();
};

const dashboardColors = provideDashboardColors(() => store.current?.layout.color_assignments);

// Values the tiles gave a colour since the last edit are stored with this one, never on their own.
const adoptColorAssignments = () => {
  if (!store.current) return;
  const assignments = dashboardColors.sync();
  if (!sameAssignments(store.current.layout.color_assignments, assignments)) {
    store.setLayout({ ...store.current.layout, color_assignments: assignments });
  }
};

const markChanged = () => {
  adoptColorAssignments();
  recordEdit();
  // Editing past the offer starts fresh.
  if (restoreAvailable.value) settleOldDraft();
  engine.notifyChange();
  // The first edit creates the dashboard at once; later ones autosave as usual.
  if (creationPhase.value === "pending") void engine.flush();
};

const startAutosaveSession = (dashboard: Dashboard) => {
  const layout = toPlainJson(dashboard.layout) as DashboardLayout;
  const description = dashboard.description ?? null;
  openSnapshot.value = { name: dashboard.name, description, layout };
  openSerialized.value = serializeDashboard(dashboard.name, layout, description);
  expectedLayoutVersion.value = dashboard.layout_version ?? 1;
  sessionChanged.value = false;
  engine.reset(openSerialized.value);
};

const layoutRef = computed<DashboardLayout>(() => store.current?.layout ?? EMPTY_DASHBOARD_LAYOUT);
const vizCache = provideDashboardVizCache();
const {
  datasourcesInUse,
  tilesByDatasource,
  vizTileIds,
  tileDatasource,
  tileLabel,
  tileFields,
  loadTileFields,
  datasourceOf,
  loadDatasource,
  getColumnStats,
  searchFilterValues,
  refresh,
} = useDashboardDatasources(layoutRef, vizCache);

const noTileHints = computed(() =>
  filtersWithoutSourceTiles(
    layoutRef.value.filters,
    vizTileIds.value.map(tileDatasource),
    (key) => datasourceOf(key)?.name ?? key,
  ),
);

const addedVizIds = computed(() => {
  if (!store.current) return new Set<number>();
  return new Set(
    store.current.layout.tiles
      .filter((t) => t.type === "viz")
      .map((t) => t.viz_id)
      .filter((v): v is number => v != null),
  );
});

const generateTileId = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `tile-${Math.random().toString(36).slice(2, 10)}`;

const gridCols = () => store.current?.layout.grid.cols ?? EMPTY_DASHBOARD_LAYOUT.grid.cols;

// View state, not an edit: switching tabs records nothing.
const activeTabId = ref<string | null>(null);
const currentTabId = computed(() => resolveTabId(layoutRef.value, activeTabId.value));
// What the canvas draws and edits: the active tab's tiles; the whole layout without tabs.
const tabLayout = computed(() => tabCanvasLayout(layoutRef.value, currentTabId.value));
const moveTargets = computed(() =>
  layoutTabs(layoutRef.value).filter((t) => t.id !== currentTabId.value),
);

// Every edit passes here, so a removed tile leaves no filter target behind and a stale one heals.
const onLayoutChange = (next: DashboardLayout) => {
  if (!store.current) return;
  store.setLayout(pruneFilterTargets(healTabs(next)));
  markChanged();
};

const onCanvasLayoutChange = (next: DashboardLayout) =>
  onLayoutChange(mergeTabCanvas(layoutRef.value, currentTabId.value, next));

// New tiles land on the active tab; a negative y (grid not measurable) takes its next free row.
const addToActiveTab = (build: (y: number) => DashboardTile, y = -1) => {
  if (!store.current) return;
  const tab = tabLayout.value;
  onCanvasLayoutChange({
    ...tab,
    tiles: [...tab.tiles, build(y < 0 ? firstFreeRow(tab.tiles) : y)],
  });
};

const onMoveTileToTab = ({ tileId, tabId }: { tileId: string; tabId: string }) =>
  onLayoutChange(moveTileToTab(layoutRef.value, tileId, tabId));

const tileLabelOnTab = (tileId: string): string => {
  const tab = tabTitleOfTile(layoutRef.value, tileId);
  return tab ? `${tileLabel(tileId)} · ${tab}` : tileLabel(tileId);
};

// 0 stands for off in the select, which cannot hold null as an option's value.
const autoRefreshSetting = computed<number>({
  get: () => savedAutoRefresh(store.current?.layout.auto_refresh_seconds) ?? 0,
  set: (seconds) => {
    if (!store.current) return;
    onLayoutChange({ ...store.current.layout, auto_refresh_seconds: seconds || null });
  },
});

// A filter removed from the strip or its dialog takes its canvas tile with it.
const onFiltersChange = (next: DashboardFilter[]) => {
  if (!store.current) return;
  onLayoutChange(withFilters(store.current.layout, next));
};

const filterBarRef = ref<InstanceType<typeof DashboardFilterBar> | null>(null);
const placedIds = computed(() =>
  store.current ? placedFilterIds(store.current.layout) : new Set<string>(),
);

const onFilterChange = (next: DashboardFilter) => {
  if (!store.current) return;
  onFiltersChange(store.current.layout.filters.map((f) => (f.id === next.id ? next : f)));
};

// Checked on the whole layout: a filter placed on another tab is not placed again.
const onPlaceFilterAt = ({ filterId, x, y }: { filterId: string; x: number; y: number }) => {
  if (!store.current || placedIds.value.has(filterId)) return;
  const tab = tabLayout.value;
  const next = placeFilter(tab, filterId, { x, y }, generateTileId());
  if (next !== tab) onCanvasLayoutChange(next);
};

const onPlaceFilter = (filterId: string) => onPlaceFilterAt({ filterId, x: 0, y: -1 });

// A filter needs a source and a column first; nothing changes until the dialog confirms.
const onAddFilterAt = async ({ x, y }: { x: number; y: number }) => {
  if (!store.current || !filterBarRef.value) return;
  const filter = await filterBarRef.value.openAddForCanvas();
  if (!filter || !store.current) return;
  onCanvasLayoutChange(addPlacedFilter(tabLayout.value, filter, { x, y }, generateTileId()));
};

const onAddFilterTile = () => onAddFilterAt({ x: 0, y: -1 });

const onDeleteFilter = (filterId: string) => {
  if (!store.current) return;
  onLayoutChange(deleteFilter(store.current.layout, filterId));
};

// Same path as an edit (pruning, then markChanged), so autosave and the draft store it.
const applySnapshot = (snapshot: DashboardSnapshot) => {
  nameDraft.value = snapshot.name;
  descriptionDraft.value = snapshot.description;
  store.setLayout(pruneFilterTargets(toPlainShared(snapshot.layout)));
  const applied = currentSnapshot();
  if (applied) history.replacePresent(applied);
  markChanged();
};

const stepHistory = (direction: "undo" | "redo") => {
  if (!store.current || reverting.value) return;
  const target = direction === "undo" ? history.undo(Date.now()) : history.redo(Date.now());
  if (target) applySnapshot(target);
  syncHistoryFlags();
};

const onUndo = () => stepHistory("undo");
const onRedo = () => stepHistory("redo");

const dialogOpen = (): boolean =>
  vizDialogOpen.value ||
  vizCreatorOpen.value ||
  sourcePickerOpen.value ||
  Array.from(document.querySelectorAll<HTMLElement>(".el-overlay")).some(
    (el) => el.style.display !== "none" && el.getClientRects().length > 0,
  );

const handleHistoryKeydown = (event: KeyboardEvent) => {
  const action = historyShortcutFor(event, IS_MAC);
  if (!action || dialogOpen()) return;
  event.preventDefault();
  stepHistory(action);
};

const editingVizId = ref<number | null>(null);
const vizDialogOpen = ref(false);
const vizRefreshNonces = ref<Record<number, number>>({});
const vizViewerRef = ref<InstanceType<typeof VisualizationViewer> | null>(null);
const vizPendingRefresh = ref(false);

// Refresh-all shares the nonce map with the single-viz bump in onCloseVizDialog.
const { refreshing, statsRefreshNonce, refreshAll } = useDashboardRefresh({
  layout: layoutRef,
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

// Remember the change and refresh tiles once at close, not per autosave tick.
const onVizUpdated = () => {
  vizPendingRefresh.value = true;
};

// Flush while GW is still mounted; a failed flush gets a lose-changes confirm.
const onVizDialogBeforeClose = async (done: () => void) => {
  const clean = (await vizViewerRef.value?.flushAutosave()) ?? true;
  if (clean) {
    done();
    return;
  }
  try {
    await ElMessageBox.confirm(
      "Some chart changes couldn't be saved. Close anyway and lose them?",
      "Unsaved changes",
      { confirmButtonText: "Close", cancelButtonText: "Keep editing", type: "warning" },
    );
    done();
  } catch {
    // stay open
  }
};

const onEditViz = (vizId: number | null) => {
  if (vizId == null) return;
  editingVizId.value = vizId;
  vizDialogOpen.value = true;
};

const refreshVizTiles = (id: number) => {
  vizCache.invalidate(id);
  vizRefreshNonces.value = {
    ...vizRefreshNonces.value,
    [id]: (vizRefreshNonces.value[id] ?? 0) + 1,
  };
};

const onCloseVizDialog = () => {
  // Refresh tiles bound to this viz only when the viewer reported a save.
  if (editingVizId.value != null && vizPendingRefresh.value) refreshVizTiles(editingVizId.value);
  vizPendingRefresh.value = false;
  editingVizId.value = null;
};

// A chart this dashboard owns becomes a chart of its own: in the Charts list, and kept when the
// dashboard is deleted or stops using it.
const onKeepChart = async (vizId: number | null) => {
  if (vizId == null) return;
  try {
    const kept = await catalogStore.detachVisualization(vizId);
    refreshVizTiles(vizId);
    ElMessage.success(`"${kept.name}" is now in Charts`);
  } catch (error) {
    ElMessage.error(catalogSaveErrorMessage(error, "The chart could not be kept."));
  }
};

const buildVizTile = (vizId: number, x: number, y: number): DashboardTile => ({
  id: generateTileId(),
  type: "viz",
  viz_id: vizId,
  chart_index: 0,
  x,
  y,
  w: gridCols() / 2,
  h: 6,
});

const onAddTile = (viz: CatalogVisualization) =>
  addToActiveTab((row) => buildVizTile(viz.id, 0, row));

// Drag-drop from the sidebar lands a tile at the dropped cell.
const onAddVizAt = ({ vizId, x, y }: { vizId: number; x: number; y: number }) =>
  addToActiveTab((row) => buildVizTile(vizId, x, row), y);

const sourcePickerOpen = ref(false);
const vizCreatorOpen = ref(false);
const pendingSource = ref<VizSourceDescriptor | null>(null);
const vizCreatorRef = ref<InstanceType<typeof VisualizationEditor> | null>(null);

const onVizCreatorBeforeClose = async (done: () => void) => {
  const editor = vizCreatorRef.value;
  if (!editor?.isDirty) {
    done();
    return;
  }
  await editor.flushDraft();
  try {
    await ElMessageBox.confirm(
      "Close the chart builder? Your work is kept as a draft on this device.",
      "Close chart builder",
      { confirmButtonText: "Close", cancelButtonText: "Keep editing", type: "warning" },
    );
    done();
  } catch {
    // stay open
  }
};

const pendingTakenNames = ref<string[]>([]);

const onSourcePicked = (source: VizSourceDescriptor) => {
  const library = new Map(catalogStore.visualizationLibrary.map((v) => [v.id, v.name]));
  pendingTakenNames.value = dashboardChartNames(layoutRef.value.tiles, (id) => library.get(id));
  pendingSource.value = source;
  vizCreatorOpen.value = true;
};

// The new viz is saved to the catalog (createVisualization refreshes the
// library, so the sidebar picks it up) and dropped straight onto the canvas.
const onVizCreated = (viz: CatalogVisualization) => {
  vizCreatorOpen.value = false;
  pendingSource.value = null;
  onAddTile(viz);
};

const buildTextTile = (x: number, y: number): DashboardTile => ({
  id: generateTileId(),
  type: "text",
  viz_id: null,
  chart_index: 0,
  text_md: "## New section\n\nDescribe what's below.",
  x,
  y,
  w: gridCols(),
  h: 3,
});

const onAddTextTile = () => addToActiveTab((row) => buildTextTile(0, row));

const aiStore = useAiStore();
const aiDashboardStore = useAiDashboardStore();
const aiBuilderOpen = ref(false);
// The name a new dashboard opened with; null for one opened from the library.
let untitledName: string | null = null;
const canBuildWithAi = computed(
  () => Boolean(store.current) && aiDashboardStore.available && mayEdit(store.current?.access),
);

// Saves the charts the AI's page kept as this dashboard's own, then puts the whole page on the
// dashboard in one layout change, so one undo step. A chart that cannot be saved is left off and
// reported.
const placeAiPage = async (page: BuiltPage): Promise<PlacedPage> => {
  if (!store.current) throw new Error("No dashboard is open.");
  const ownerId = await ensureDashboardCreated();
  const library = new Map(catalogStore.visualizationLibrary.map((v) => [v.id, v.name]));
  const taken = dashboardChartNames(layoutRef.value.tiles, (id) => library.get(id));
  const saved: PlacedChart[] = [];
  const failed: PageNote[] = [];
  for (const chart of page.charts) {
    const spec = [toPlainJson(chart.chart)] as unknown as Record<string, any>[];
    try {
      const viz = await catalogStore.createVisualization({
        name: nameFromAiTitle(chart.title, taken) ?? distinctName("Chart", taken),
        spec,
        chart_type: chartTypeOf(spec),
        source_type: "table",
        catalog_table_id: chart.tableId,
        dashboard_id: ownerId,
      });
      taken.push(viz.name);
      saved.push({ ...chart, vizId: viz.id });
    } catch (error) {
      failed.push({
        title: chart.title,
        reason: catalogSaveErrorMessage(error, "The chart could not be saved."),
      });
    }
  }
  if (page.charts.length && !saved.length) throw new Error(failed[0].reason);
  try {
    const tiles = layoutPage(page, saved, gridCols());
    const filters = pageFilters(
      page.filters ?? [],
      tiles,
      saved,
      layoutRef.value.filters,
      landsOnTab(layoutRef.value),
    );
    const placed = placeOnDashboard(layoutRef.value, tiles, page.title, filters);
    // A new dashboard still on its opening name takes the page's title, in the same undo step.
    if (placed.tabId === null && nameDraft.value === untitledName) {
      nameDraft.value = distinctName(
        page.title.slice(0, 120),
        store.library.map((d) => d.name),
      );
    }
    onLayoutChange(placed.layout);
    // Saved at once, so the charts just made are on a saved page.
    void engine.flush();
    if (placed.tabId) activeTabId.value = placed.tabId;
    return {
      charts: saved.length,
      kpis: page.kpis.length,
      filters: filters.map((f) => f.label || f.field_name),
      failed,
    };
  } catch (error) {
    // The page is not on the dashboard, so its charts would be on none.
    await Promise.allSettled(saved.map((c) => catalogStore.deleteVisualization(c.vizId)));
    throw error;
  }
};

// ?ai=1 (the library's "Build with AI") opens the builder once; the param is stripped on consume.
const consumeAiQuery = async () => {
  if (route.query.ai !== "1" || typeof route.name !== "string") return;
  aiBuilderOpen.value = true;
  const query = { ...route.query };
  delete query.ai;
  await router.replace({ name: route.name, params: { ...route.params }, query });
};

const onAddTextAt = ({ x, y }: { x: number; y: number }) =>
  addToActiveTab((row) => buildTextTile(x, row), y);

const buildKpiTile = (x: number, y: number): DashboardTile => ({
  id: generateTileId(),
  type: "kpi",
  viz_id: null,
  chart_index: 0,
  kpi: null,
  x,
  y,
  ...KPI_DEFAULT_SIZE,
});

const onAddKpiTile = () => addToActiveTab((row) => buildKpiTile(0, row));

const onAddKpiAt = ({ x, y }: { x: number; y: number }) =>
  addToActiveTab((row) => buildKpiTile(x, row), y);

const buildTableTile = (x: number, y: number): DashboardTile => ({
  id: generateTileId(),
  type: "table",
  viz_id: null,
  table_id: null,
  chart_index: 0,
  table: null,
  x,
  y,
  ...TABLE_DEFAULT_SIZE,
});

const onAddTableTile = () => addToActiveTab((row) => buildTableTile(0, row));

provideDashboardImageBudget(() => store.current?.layout.tiles ?? []);

// Empty from the Insert list; filled when a file from the computer was dropped on the canvas.
const buildImageTile = (x: number, y: number, dataUrl: string | null): DashboardTile => ({
  id: generateTileId(),
  type: "image",
  viz_id: null,
  chart_index: 0,
  image: dataUrl ? { data_url: dataUrl, alt: null, fit: "contain" } : null,
  x,
  y,
  ...IMAGE_DEFAULT_SIZE,
});

const onAddImageTile = () => addToActiveTab((row) => buildImageTile(0, row, null));

const onAddImageAt = ({ x, y, dataUrl }: { x: number; y: number; dataUrl: string | null }) =>
  addToActiveTab((row) => buildImageTile(x, row, dataUrl), y);

const onAddTableAt = ({ x, y }: { x: number; y: number }) =>
  addToActiveTab((row) => buildTableTile(x, row), y);

const buildSeparatorTile = (
  x: number,
  y: number,
  orientation: SeparatorOrientation,
): DashboardTile => ({
  id: generateTileId(),
  type: "separator",
  viz_id: null,
  chart_index: 0,
  orientation,
  x,
  y,
  w: orientation === "vertical" ? 2 : gridCols(),
  h: orientation === "vertical" ? 6 : 1,
});

const onAddSeparatorTile = (orientation: SeparatorOrientation) =>
  addToActiveTab((row) => buildSeparatorTile(0, row, orientation));

const onAddSeparatorAt = ({
  x,
  y,
  orientation,
}: {
  x: number;
  y: number;
  orientation: SeparatorOrientation;
}) => addToActiveTab((row) => buildSeparatorTile(x, row, orientation), y);

const initialise = async () => {
  if (openedNew) {
    const blank = store.newBlankDashboard(newDashboardName(store.library.map((d) => d.name)));
    nameDraft.value = blank.name;
    untitledName = blank.name;
    descriptionDraft.value = "";
    // The blank state is the baseline: nothing is sent until it changes.
    startAutosaveSession(blank);
    offerOldDraft();
    resetHistory();
    return;
  }
  try {
    await store.loadDashboard(Number(props.id));
    if (store.current) {
      nameDraft.value = store.current.name;
      descriptionDraft.value = store.current.description ?? "";
      startAutosaveSession(store.current);
      resetHistory();
    }
  } catch {
    ElMessage.error(store.error ?? "Failed to load dashboard");
  }
};

// Typing back to the saved value is no autosave change, but the history still follows it.
watch(nameDraft, (v) => {
  if (!store.current) return;
  if (v !== store.current.name) markChanged();
  else recordEdit();
});

watch(descriptionDraft, () => {
  if (!store.current) return;
  if (descriptionValue() !== (store.current.description ?? null)) markChanged();
  else recordEdit();
});

// Honour ?editViz=<id> from the route by auto-opening the chart edit dialog
// once the dashboard finishes loading. The param is stripped on consume so a
// reload or back-nav doesn't keep reopening it.
const consumeEditVizQuery = async () => {
  const raw = route.query.editViz;
  const id = typeof raw === "string" ? Number(raw) : NaN;
  if (!Number.isFinite(id) || id <= 0) return;
  // Snapshot route identity before any awaits — initialise() above may finish
  // after the user has navigated, in which case route.* now points elsewhere.
  // Also avoids the unsafe `as string` cast on route.name (which can be a
  // symbol or undefined for catch-all routes).
  const targetRouteName = route.name;
  if (typeof targetRouteName !== "string") return;
  const targetParams = { ...route.params };

  const tiles = store.current?.layout.tiles ?? [];
  const targetTile = tiles.find((t) => t.type === "viz" && t.viz_id === id);
  if (targetTile && store.current) {
    activeTabId.value = tabOfTile(store.current.layout, targetTile);
    onEditViz(id);
  }
  // Strip the query whether or not the tile was found, so a reload or
  // back-nav doesn't keep reopening (or chasing a missing) tile.
  await router.replace({
    name: targetRouteName,
    params: targetParams,
    query: {},
  });
};

const handleBeforeUnload = (event: BeforeUnloadEvent) => {
  if (engine.isDirty()) {
    event.preventDefault();
    event.returnValue = "";
  }
};

onMounted(async () => {
  window.addEventListener("beforeunload", handleBeforeUnload);
  window.addEventListener("keydown", handleHistoryKeydown);
  await initialise();
  await consumeEditVizQuery();
  aiStore.loadProviders().catch(() => undefined);
  await consumeAiQuery();
});

onBeforeUnmount(() => {
  window.removeEventListener("beforeunload", handleBeforeUnload);
  window.removeEventListener("keydown", handleHistoryKeydown);
  oldDraft?.dispose();
  store.reset();
});

// Flush-first leave guard: a confirm only appears when a flush could not land.
// Leaving ends the undo history, so a chart this dashboard owns and no longer uses cannot come
// back. A draft still on offer may use such a chart, so nothing is removed while one is.
const pruneOwnCharts = async () => {
  const id = props.id !== undefined ? Number(props.id) : creation.id();
  if (id === null || restoreAvailable.value || !mayEdit(store.current?.access)) return;
  try {
    await catalogStore.pruneDashboardCharts(id);
  } catch {
    // The next visit to this editor prunes.
  }
};

onBeforeRouteLeave(async (to) => {
  const createdId = creation.id();
  if (createdId !== null && to.name === "dashboard-edit" && Number(to.params.id) === createdId) {
    return true;
  }
  leaving = true;
  const viewerClean = vizDialogOpen.value
    ? ((await vizViewerRef.value?.flushAutosave()) ?? true)
    : true;
  const clean = (await engine.flush()) && viewerClean;
  if (clean) {
    await pruneOwnCharts();
    return true;
  }
  try {
    await ElMessageBox.confirm(
      "Some changes couldn't be saved. Leave anyway and lose them?",
      "Unsaved changes",
      { confirmButtonText: "Leave", cancelButtonText: "Stay", type: "warning" },
    );
    return true;
  } catch {
    leaving = false;
    showCreatedInUrl();
    return false;
  }
});

const onBack = () => {
  // The route guard flushes; no confirm here.
  const id = props.id !== undefined ? Number(props.id) : creation.id();
  if (id === null) {
    router.push({ name: "catalog", query: { tab: "visuals", kind: "dashboards" } });
    return;
  }
  router.push({ name: "dashboard-view", params: { id } });
};

const onRevert = async () => {
  if (!openSnapshot.value || !store.current) return;
  try {
    await ElMessageBox.confirm(
      "Take this dashboard back to how it was when you opened it? Changes from this session will be undone.",
      "Revert dashboard",
      { confirmButtonText: "Revert", cancelButtonText: "Keep editing", type: "warning" },
    );
  } catch {
    return;
  }
  reverting.value = true;
  try {
    // Drain any in-flight save (no new write) so the CAS token is current.
    engine.suspend();
    await engine.flush();
    nameDraft.value = openSnapshot.value.name;
    descriptionDraft.value = openSnapshot.value.description ?? "";
    store.setLayout(toPlainJson(openSnapshot.value.layout) as DashboardLayout);
    // One undoable step, recorded before the PUT so the drafts' watchers find it already taken.
    recordEdit({ key: null });
    if (!creation.isCreated()) {
      // Nothing reached the server: the blank state it opened with is the baseline again.
      engine.reset(openSerialized.value);
      sessionChanged.value = false;
      return;
    }
    const body: DashboardUpdatePayload = {
      name: openSnapshot.value.name,
      description: openSnapshot.value.description,
      layout: openSnapshot.value.layout,
    };
    if (expectedLayoutVersion.value != null) {
      body.expected_layout_version = expectedLayoutVersion.value;
    }
    const updated = await store.autosaveDashboard(dashboardId(), body);
    expectedLayoutVersion.value = updated.layout_version;
    engine.markSaved(openSerialized.value);
    sessionChanged.value = false;
    // Edits made while the revert PUT was in flight must not vanish silently.
    const nowSerialized = serializeDashboard(
      nameDraft.value.trim(),
      toPlainJson(store.current.layout),
      descriptionValue(),
    );
    if (nowSerialized !== openSerialized.value) engine.notifyChange();
  } catch (err) {
    if (classifyAutosaveError(err) === "conflict") {
      // The revert itself hit a stale write: enter the normal conflict flow.
      engine.enterConflict(catalogSaveErrorMessage(err, "Changed somewhere else"));
    } else {
      ElMessage.error(catalogSaveErrorMessage(err, "Failed to revert dashboard"));
      // Local state is already the revert target; let autosave retry it.
      engine.notifyChange();
    }
  } finally {
    engine.resume();
    reverting.value = false;
  }
};

// Adopt the server's version wholesale; the revert anchor stays at original open.
const resyncFromServer = async () => {
  await store.loadDashboard(dashboardId());
  if (!store.current) return;
  nameDraft.value = store.current.name;
  descriptionDraft.value = store.current.description ?? "";
  expectedLayoutVersion.value = store.current.layout_version ?? 1;
  // The history's base is gone with the version it was built on.
  resetHistory();
  engine.resolveConflictReloaded(
    serializeDashboard(
      store.current.name,
      toPlainJson(store.current.layout),
      store.current.description ?? null,
    ),
  );
};

const onConflictReload = async () => {
  conflictBusy.value = true;
  try {
    await resyncFromServer();
  } catch (err) {
    ElMessage.error(catalogSaveErrorMessage(err, "Failed to load the latest version"));
  } finally {
    conflictBusy.value = false;
  }
};

const onConflictOverwrite = async () => {
  if (!store.current) return;
  conflictBusy.value = true;
  try {
    // Token-only re-GET; store.loadDashboard would clobber the state being kept.
    const fresh = await CatalogApi.getDashboard(dashboardId());
    const trimmed = nameDraft.value.trim() || fresh.name;
    const layout = toPlainJson(store.current.layout) as DashboardLayout;
    const description = descriptionValue();
    const updated = await store.autosaveDashboard(dashboardId(), {
      name: trimmed,
      description,
      layout,
      expected_layout_version: fresh.layout_version,
    });
    expectedLayoutVersion.value = updated.layout_version;
    const adopted = serializeDashboard(trimmed, layout, description);
    sessionChanged.value = adopted !== openSerialized.value;
    engine.markSaved(adopted);
    resetHistory();
    // Edits made while the overwrite PUT was in flight must not vanish silently.
    const nowSerialized = serializeDashboard(
      nameDraft.value.trim(),
      toPlainJson(store.current.layout),
      descriptionValue(),
    );
    if (nowSerialized !== adopted) engine.notifyChange();
  } catch (err) {
    ElMessage.error(catalogSaveErrorMessage(err, "Failed to save your version"));
  } finally {
    conflictBusy.value = false;
  }
};
</script>

<style scoped>
.ai-build-icon {
  margin-right: 4px;
  font-size: 15px;
  color: var(--color-accent);
}
/* Padding stacks on .catalog-detail's so the header lines up with the library page. */
.dashboard-editor {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
  padding: var(--spacing-3) var(--spacing-6) 0;
}
.editor-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 0 var(--spacing-4);
  gap: 12px;
}
.editor-toolbar .back-btn {
  margin-bottom: 0;
}
.editor-toolbar-left {
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 1;
  min-width: 0;
}
.editor-name {
  max-width: 320px;
}
.editor-description {
  max-width: 360px;
}
.draft-banner {
  margin: 0 0 var(--spacing-3);
  width: auto;
}
.draft-banner-actions {
  display: flex;
  gap: 8px;
  margin-top: 4px;
}
.editor-toolbar-right {
  display: flex;
  align-items: center;
  gap: 8px;
}
.editor-auto-refresh {
  width: 170px;
}
.history-btn-wrap {
  display: inline-flex;
}
.history-btn {
  padding: 0 10px;
}
.history-btn .material-icons {
  font-size: 18px;
}
.editor-body {
  flex: 1;
  display: flex;
  min-height: 0;
}
.editor-sidebar {
  width: 280px;
  flex-shrink: 0;
  border-right: 1px solid var(--el-border-color-lighter);
  overflow: hidden;
  display: flex;
  flex-direction: column;
}
.editor-canvas {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}
.editor-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 24px;
}
</style>
