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
        <SaveStatusIndicator v-if="!isNew" :state="autosaveState" />
        <SaveStatusIndicator v-else :state="autosaveState" mode="draft" :draft-saved="draftDirty" />
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
        <el-button v-if="store.current" :loading="refreshing" @click="onRefresh">
          <el-icon><Refresh /></el-icon> Refresh
        </el-button>
        <el-button
          v-if="!isNew"
          :disabled="!canRevert || reverting"
          :loading="reverting"
          @click="onRevert"
        >
          Revert
        </el-button>
        <el-button
          v-else
          type="primary"
          :loading="store.saving"
          :disabled="!nameDraft.trim()"
          @click="onCreate"
        >
          Create dashboard
        </el-button>
      </div>
    </div>

    <AutosaveConflictBanner
      v-if="!isNew"
      :state="autosaveState"
      resource-label="dashboard"
      :busy="conflictBusy"
      @reload="onConflictReload"
      @overwrite="onConflictOverwrite"
    />

    <el-alert
      v-if="isNew && restoreAvailable"
      type="info"
      :closable="false"
      show-icon
      class="draft-banner"
    >
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
            :tile-label="tileLabel"
            :tile-fields="tileFields"
            :load-tile-fields="loadTileFields"
            :get-column-stats="getColumnStats"
            :search-values="searchFilterValues"
            :stats-refresh-nonce="statsRefreshNonce"
            @update:filters="onFiltersChange"
            @place="onPlaceFilter"
          />
          <DashboardCanvas
            :layout="store.current.layout"
            mode="edit"
            :appearance="appearance"
            :viz-refresh-nonces="vizRefreshNonces"
            :data-refresh-nonce="statsRefreshNonce"
            :tile-datasource="tileDatasource"
            :get-column-stats="getColumnStats"
            :search-values="searchFilterValues"
            @update:layout="onLayoutChange"
            @edit-viz="onEditViz"
            @add-viz-at="onAddVizAt"
            @add-text-at="onAddTextAt"
            @add-separator-at="onAddSeparatorAt"
            @add-kpi-at="onAddKpiAt"
            @add-table-at="onAddTableAt"
            @place-filter-at="onPlaceFilterAt"
            @update:filter="onFilterChange"
            @edit-filter="(id: string) => filterBarRef?.openEdit(id)"
            @delete-filter="onDeleteFilter"
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
import { Refresh } from "@element-plus/icons-vue";
import { CatalogApi } from "../../api/catalog.api";
import { useDashboardsStore } from "../../stores/dashboards-store";
import { useAuthStore } from "../../stores/auth-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import { useDashboardDatasources } from "../../composables/useDashboardDatasources";
import { useDashboardRefresh } from "../../composables/useDashboardRefresh";
import { provideDashboardVizCache } from "../../composables/useDashboardVizCache";
import { useAutosave } from "../../composables/useAutosave";
import { classifyAutosaveError } from "../../composables/autosaveEngine";
import { createLocalDraft, type LocalDraft } from "../../composables/localDraft";
import { catalogSaveErrorMessage } from "../../composables/saveError";
import { toPlainJson } from "../../utils/structuredClone";
import SaveStatusIndicator from "../../components/common/SaveStatusIndicator.vue";
import AutosaveConflictBanner from "../../components/common/AutosaveConflictBanner.vue";
import DashboardCanvas from "./DashboardCanvas.vue";
import DashboardFilterBar from "./DashboardFilterBar.vue";
import DashboardSidebarPicker from "./DashboardSidebarPicker.vue";
import VisualizationViewer from "../CatalogView/VisualizationViewer.vue";
import VisualizationSourcePicker from "../CatalogView/VisualizationSourcePicker.vue";
import VisualizationEditor from "../CatalogView/VisualizationEditor.vue";
import { dashboardChartNames } from "../CatalogView/chartNaming";
import { useCatalogStore } from "../../stores/catalog-store";
import { historyShortcutFor } from "../../composables/useFlowHotkeys";
import { IS_MAC } from "../../utils/shortcuts";
import { upgradeLayoutGrid } from "./gridVersion";
import {
  EditHistory,
  dashboardChange,
  sameDashboardSnapshot,
  type DashboardSnapshot,
  type RecordOptions,
} from "./editHistory";
import {
  deleteFilter,
  placeFilter,
  placedFilterIds,
  pruneFilterTargets,
  withFilters,
} from "./filterPlacement";
import { KPI_DEFAULT_SIZE } from "./kpi";
import { TABLE_DEFAULT_SIZE } from "./tableTile";
import {
  EMPTY_DASHBOARD_LAYOUT,
  type CatalogVisualization,
  type Dashboard,
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

const isNew = computed(() => props.id === undefined);
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
    if (isNew.value || !store.current) return null;
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
    const body: DashboardUpdatePayload = { ...payload };
    if (expectedLayoutVersion.value != null) {
      body.expected_layout_version = expectedLayoutVersion.value;
    }
    const updated = await store.autosaveDashboard(Number(props.id), body);
    expectedLayoutVersion.value = updated.layout_version;
    sessionChanged.value =
      serializeDashboard(payload.name ?? "", payload.layout, payload.description ?? null) !==
      openSerialized.value;
  },
});

const canRevert = computed(() => {
  const st = autosaveState.value.status;
  if (st === "conflict" || st === "blocked") return false;
  return !!openSnapshot.value && (sessionChanged.value || st !== "idle");
});

// New-dashboard mode: user-scoped localStorage draft instead of server autosave.
const auth = useAuthStore();
const draftDirty = ref(false);
const restoreAvailable = ref(false);
interface NewDashboardDraft {
  name: string;
  description?: string;
  layout: DashboardLayout;
}
let draft: LocalDraft<NewDashboardDraft> | null = null;
let pendingDraft: NewDashboardDraft | null = null;

const setupDraft = () => {
  const userKey = auth.currentUser?.id ?? auth.currentUser?.username ?? "anon";
  draft = createLocalDraft({
    key: `dashboards.draft.new.${userKey}`,
    version: 1,
    serialize: () =>
      draftDirty.value && store.current
        ? {
            name: nameDraft.value,
            description: descriptionDraft.value,
            layout: toPlainJson(store.current.layout) as DashboardLayout,
          }
        : null,
  });
  const existing = draft.peek();
  if (existing) {
    pendingDraft = existing.data;
    restoreAvailable.value = true;
  }
};

const onRestoreDraft = () => {
  if (!pendingDraft) return;
  nameDraft.value = pendingDraft.name;
  descriptionDraft.value = pendingDraft.description ?? "";
  store.setLayout(pruneFilterTargets(upgradeLayoutGrid(pendingDraft.layout)));
  draftDirty.value = true;
  restoreAvailable.value = false;
  resetHistory();
};

const onDiscardDraft = () => {
  draft?.discard();
  pendingDraft = null;
  restoreAvailable.value = false;
};

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
        layout: toPlainJson(store.current.layout) as DashboardLayout,
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

const markChanged = () => {
  recordEdit();
  if (isNew.value) {
    // First edit claims the single draft slot; the restore offer is now stale.
    if (restoreAvailable.value) {
      restoreAvailable.value = false;
      pendingDraft = null;
    }
    draftDirty.value = true;
    draft?.scheduleWrite();
  } else {
    engine.notifyChange();
  }
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
  getColumnStats,
  searchFilterValues,
  refresh,
} = useDashboardDatasources(layoutRef, vizCache);

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

const findFreeRow = (layout: DashboardLayout): number => {
  if (!layout.tiles.length) return 0;
  return Math.max(...layout.tiles.map((t) => t.y + t.h));
};

// Every edit passes here, so a removed tile leaves no filter target behind and a stale one heals.
const onLayoutChange = (next: DashboardLayout) => {
  if (!store.current) return;
  store.setLayout(pruneFilterTargets(next));
  markChanged();
};

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

const onPlaceFilterAt = ({ filterId, x, y }: { filterId: string; x: number; y: number }) => {
  if (!store.current) return;
  const next = placeFilter(store.current.layout, filterId, { x, y }, generateTileId());
  if (next !== store.current.layout) onLayoutChange(next);
};

const onPlaceFilter = (filterId: string) => onPlaceFilterAt({ filterId, x: 0, y: -1 });

const onDeleteFilter = (filterId: string) => {
  if (!store.current) return;
  onLayoutChange(deleteFilter(store.current.layout, filterId));
};

// Same path as an edit (pruning, then markChanged), so autosave and the draft store it.
const applySnapshot = (snapshot: DashboardSnapshot) => {
  nameDraft.value = snapshot.name;
  descriptionDraft.value = snapshot.description;
  store.setLayout(pruneFilterTargets(toPlainJson(snapshot.layout)));
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

const onCloseVizDialog = () => {
  // Refresh tiles bound to this viz only when the viewer reported a save.
  if (editingVizId.value != null && vizPendingRefresh.value) {
    const id = editingVizId.value;
    vizCache.invalidate(id);
    vizRefreshNonces.value = {
      ...vizRefreshNonces.value,
      [id]: (vizRefreshNonces.value[id] ?? 0) + 1,
    };
  }
  vizPendingRefresh.value = false;
  editingVizId.value = null;
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

const onAddTile = (viz: CatalogVisualization) => {
  if (!store.current) return;
  const layout = store.current.layout;
  onLayoutChange({
    ...layout,
    tiles: [...layout.tiles, buildVizTile(viz.id, 0, findFreeRow(layout))],
  });
};

// Drag-drop from the sidebar lands a tile at the dropped cell; a negative y
// (grid not measurable) falls back to the next free row, like a click-add.
const onAddVizAt = ({ vizId, x, y }: { vizId: number; x: number; y: number }) => {
  if (!store.current) return;
  const layout = store.current.layout;
  const row = y < 0 ? findFreeRow(layout) : y;
  onLayoutChange({ ...layout, tiles: [...layout.tiles, buildVizTile(vizId, x, row)] });
};

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

const onAddTextTile = () => {
  if (!store.current) return;
  const layout = store.current.layout;
  onLayoutChange({ ...layout, tiles: [...layout.tiles, buildTextTile(0, findFreeRow(layout))] });
};

const onAddTextAt = ({ x, y }: { x: number; y: number }) => {
  if (!store.current) return;
  const layout = store.current.layout;
  const row = y < 0 ? findFreeRow(layout) : y;
  onLayoutChange({ ...layout, tiles: [...layout.tiles, buildTextTile(x, row)] });
};

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

const onAddKpiTile = () => {
  if (!store.current) return;
  const layout = store.current.layout;
  onLayoutChange({ ...layout, tiles: [...layout.tiles, buildKpiTile(0, findFreeRow(layout))] });
};

const onAddKpiAt = ({ x, y }: { x: number; y: number }) => {
  if (!store.current) return;
  const layout = store.current.layout;
  const row = y < 0 ? findFreeRow(layout) : y;
  onLayoutChange({ ...layout, tiles: [...layout.tiles, buildKpiTile(x, row)] });
};

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

const onAddTableTile = () => {
  if (!store.current) return;
  const layout = store.current.layout;
  onLayoutChange({ ...layout, tiles: [...layout.tiles, buildTableTile(0, findFreeRow(layout))] });
};

const onAddTableAt = ({ x, y }: { x: number; y: number }) => {
  if (!store.current) return;
  const layout = store.current.layout;
  const row = y < 0 ? findFreeRow(layout) : y;
  onLayoutChange({ ...layout, tiles: [...layout.tiles, buildTableTile(x, row)] });
};

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

const onAddSeparatorTile = (orientation: SeparatorOrientation) => {
  if (!store.current) return;
  const layout = store.current.layout;
  onLayoutChange({
    ...layout,
    tiles: [...layout.tiles, buildSeparatorTile(0, findFreeRow(layout), orientation)],
  });
};

const onAddSeparatorAt = ({
  x,
  y,
  orientation,
}: {
  x: number;
  y: number;
  orientation: SeparatorOrientation;
}) => {
  if (!store.current) return;
  const layout = store.current.layout;
  const row = y < 0 ? findFreeRow(layout) : y;
  onLayoutChange({
    ...layout,
    tiles: [...layout.tiles, buildSeparatorTile(x, row, orientation)],
  });
};

const initialise = async () => {
  if (isNew.value) {
    const blank = store.newBlankDashboard();
    nameDraft.value = blank.name;
    descriptionDraft.value = "";
    draftDirty.value = false;
    setupDraft();
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
  if (targetTile) {
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
  if (isNew.value) {
    // Dirty sessions only: a clean write would delete a still-offered prior draft.
    if (draftDirty.value) draft?.writeNow();
    return;
  }
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
});

onBeforeUnmount(() => {
  window.removeEventListener("beforeunload", handleBeforeUnload);
  window.removeEventListener("keydown", handleHistoryKeydown);
  if (draftDirty.value) draft?.writeNow();
  draft?.dispose();
  store.reset();
});

// Flush-first leave guard: a confirm only appears when a flush could not land.
onBeforeRouteLeave(async () => {
  if (isNew.value) {
    if (!draftDirty.value) return true;
    draft?.writeNow();
    try {
      await ElMessageBox.confirm(
        "Leave without creating this dashboard? Your draft stays on this device.",
        "Leave draft",
        { confirmButtonText: "Leave", cancelButtonText: "Keep editing", type: "warning" },
      );
      return true;
    } catch {
      return false;
    }
  }
  const viewerClean = vizDialogOpen.value
    ? ((await vizViewerRef.value?.flushAutosave()) ?? true)
    : true;
  const clean = (await engine.flush()) && viewerClean;
  if (clean) return true;
  try {
    await ElMessageBox.confirm(
      "Some changes couldn't be saved. Leave anyway and lose them?",
      "Unsaved changes",
      { confirmButtonText: "Leave", cancelButtonText: "Stay", type: "warning" },
    );
    return true;
  } catch {
    return false;
  }
});

const onCreate = async () => {
  if (!store.current) return;
  const trimmed = nameDraft.value.trim();
  if (!trimmed) {
    ElMessage.warning("Name is required");
    return;
  }
  try {
    const created = await store.createDashboard({
      name: trimmed,
      description: descriptionValue(),
      namespace_id: store.current.namespace_id,
      layout: store.current.layout,
    });
    draftDirty.value = false;
    draft?.discard();
    ElMessage.success(`Created "${created.name}"`);
    await router.replace({ name: "dashboard-edit", params: { id: created.id } });
    // Both routes share this instance; onMounted won't re-run after replace.
    startAutosaveSession(created);
  } catch (err) {
    ElMessage.error(catalogSaveErrorMessage(err, store.error ?? "Failed to save"));
  }
};

const onBack = () => {
  // The route guard flushes (existing) or keeps the draft (new); no confirm here.
  if (isNew.value || props.id === undefined) {
    router.push({ name: "catalog", query: { tab: "visuals", kind: "dashboards" } });
    return;
  }
  router.push({ name: "dashboard-view", params: { id: props.id } });
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
    const body: DashboardUpdatePayload = {
      name: openSnapshot.value.name,
      description: openSnapshot.value.description,
      layout: openSnapshot.value.layout,
    };
    if (expectedLayoutVersion.value != null) {
      body.expected_layout_version = expectedLayoutVersion.value;
    }
    const updated = await store.autosaveDashboard(Number(props.id), body);
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
  await store.loadDashboard(Number(props.id));
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
    const fresh = await CatalogApi.getDashboard(Number(props.id));
    const trimmed = nameDraft.value.trim() || fresh.name;
    const layout = toPlainJson(store.current.layout) as DashboardLayout;
    const description = descriptionValue();
    const updated = await store.autosaveDashboard(Number(props.id), {
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
}
.editor-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 24px;
}
</style>
