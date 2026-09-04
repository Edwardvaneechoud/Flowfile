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
        <SaveStatusIndicator v-if="!isNew" :state="autosaveState" />
        <SaveStatusIndicator v-else :state="autosaveState" mode="draft" :draft-saved="draftDirty" />
      </div>
      <div class="editor-toolbar-right">
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
            :filters="store.current.layout.filters"
            mode="edit"
            :datasources-in-use="datasourcesInUse"
            :tiles-by-datasource="tilesByDatasource"
            :viz-tile-ids="vizTileIds"
            :tile-label="tileLabel"
            :tile-fields="tileFields"
            :load-tile-fields="loadTileFields"
            :get-column-stats="getColumnStats"
            :stats-refresh-nonce="statsRefreshNonce"
            @update:filters="onFiltersChange"
          />
          <DashboardCanvas
            :layout="store.current.layout"
            mode="edit"
            :appearance="appearance"
            :viz-refresh-nonces="vizRefreshNonces"
            :tile-datasource="tileDatasource"
            @update:layout="onLayoutChange"
            @edit-viz="onEditViz"
            @add-viz-at="onAddVizAt"
            @add-text-at="onAddTextAt"
            @add-separator-at="onAddSeparatorAt"
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
import { upgradeLayoutGrid } from "./gridVersion";
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
const appearance = useGraphicWalkerAppearance();

const isNew = computed(() => props.id === undefined);
const nameDraft = ref("");

const serializeDashboard = (dashName: string, layout: unknown) =>
  JSON.stringify({ name: dashName, layout });

// Revert anchor: the state captured when the editor opened this dashboard.
const openSnapshot = ref<{ name: string; layout: DashboardLayout } | null>(null);
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
    return {
      payload: { name: trimmed, layout },
      serialized: serializeDashboard(trimmed, layout),
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
      serializeDashboard(payload.name ?? "", payload.layout) !== openSerialized.value;
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
let draft: LocalDraft<{ name: string; layout: DashboardLayout }> | null = null;
let pendingDraft: { name: string; layout: DashboardLayout } | null = null;

const setupDraft = () => {
  const userKey = auth.currentUser?.id ?? auth.currentUser?.username ?? "anon";
  draft = createLocalDraft({
    key: `dashboards.draft.new.${userKey}`,
    version: 1,
    serialize: () =>
      draftDirty.value && store.current
        ? {
            name: nameDraft.value,
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
  store.setLayout(upgradeLayoutGrid(pendingDraft.layout));
  draftDirty.value = true;
  restoreAvailable.value = false;
};

const onDiscardDraft = () => {
  draft?.discard();
  pendingDraft = null;
  restoreAvailable.value = false;
};

const markChanged = () => {
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
  openSnapshot.value = { name: dashboard.name, layout };
  openSerialized.value = serializeDashboard(dashboard.name, layout);
  expectedLayoutVersion.value = dashboard.layout_version ?? 1;
  sessionChanged.value = false;
  engine.reset(openSerialized.value);
};

const layoutRef = computed<DashboardLayout>(() => store.current?.layout ?? EMPTY_DASHBOARD_LAYOUT);
const {
  datasourcesInUse,
  tilesByDatasource,
  vizTileIds,
  tileDatasource,
  tileLabel,
  tileFields,
  loadTileFields,
  getColumnStats,
  refresh,
} = useDashboardDatasources(layoutRef);

const addedVizIds = computed(() => {
  if (!store.current) return new Set<number>();
  return new Set(
    store.current.layout.tiles.map((t) => t.viz_id).filter((v): v is number => v != null),
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

const onLayoutChange = (next: DashboardLayout) => {
  if (!store.current) return;
  store.setLayout(next);
  markChanged();
};

const onFiltersChange = (next: DashboardFilter[]) => {
  if (!store.current) return;
  store.setLayout({ ...store.current.layout, filters: next });
  markChanged();
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

const onSourcePicked = (source: VizSourceDescriptor) => {
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
    draftDirty.value = false;
    setupDraft();
    return;
  }
  try {
    await store.loadDashboard(Number(props.id));
    if (store.current) {
      nameDraft.value = store.current.name;
      startAutosaveSession(store.current);
    }
  } catch {
    ElMessage.error(store.error ?? "Failed to load dashboard");
  }
};

watch(nameDraft, (v) => {
  if (store.current && v !== store.current.name) markChanged();
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
  const targetTile = tiles.find((t) => t.viz_id === id);
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
  await initialise();
  await consumeEditVizQuery();
});

onBeforeUnmount(() => {
  window.removeEventListener("beforeunload", handleBeforeUnload);
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
      description: store.current.description,
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
    store.setLayout(toPlainJson(openSnapshot.value.layout) as DashboardLayout);
    const body: DashboardUpdatePayload = {
      name: openSnapshot.value.name,
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
  expectedLayoutVersion.value = store.current.layout_version ?? 1;
  engine.resolveConflictReloaded(
    serializeDashboard(store.current.name, toPlainJson(store.current.layout)),
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
    const updated = await store.autosaveDashboard(Number(props.id), {
      name: trimmed,
      layout,
      expected_layout_version: fresh.layout_version,
    });
    expectedLayoutVersion.value = updated.layout_version;
    const adopted = serializeDashboard(trimmed, layout);
    sessionChanged.value = adopted !== openSerialized.value;
    engine.markSaved(adopted);
    // Edits made while the overwrite PUT was in flight must not vanish silently.
    const nowSerialized = serializeDashboard(
      nameDraft.value.trim(),
      toPlainJson(store.current.layout),
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
