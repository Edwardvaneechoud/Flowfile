<template>
  <div class="dashboard-editor">
    <div class="editor-toolbar">
      <div class="editor-toolbar-left">
        <el-button text @click="onCancel">
          <el-icon><ArrowLeft /></el-icon> Back
        </el-button>
        <el-input
          v-if="store.current"
          v-model="nameDraft"
          size="small"
          placeholder="Dashboard name"
          class="editor-name"
          maxlength="120"
        />
        <span v-if="dirty" class="editor-dirty">unsaved changes</span>
      </div>
      <div class="editor-toolbar-right">
        <el-button :disabled="!dirty || store.saving" @click="onCancel">Discard</el-button>
        <el-button
          type="primary"
          :loading="store.saving"
          :disabled="!nameDraft.trim()"
          @click="onSave"
        >
          Save
        </el-button>
      </div>
    </div>

    <div class="editor-body">
      <aside class="editor-sidebar">
        <DashboardSidebarPicker
          :added-viz-ids="addedVizIds"
          @add="onAddTile"
          @add-text="onAddTextTile"
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
            :tile-label="tileLabel"
            :get-column-stats="getColumnStats"
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
      @close="onCloseVizDialog"
    >
      <VisualizationViewer
        v-if="vizDialogOpen && editingVizId"
        :viz-id="editingVizId"
        :appearance="appearance"
        @close="vizDialogOpen = false"
        @deleted="onVizDeleted"
      />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { ArrowLeft } from "@element-plus/icons-vue";
import { useDashboardsStore } from "../../stores/dashboards-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import { useDashboardDatasources } from "../../composables/useDashboardDatasources";
import DashboardCanvas from "./DashboardCanvas.vue";
import DashboardFilterBar from "./DashboardFilterBar.vue";
import DashboardSidebarPicker from "./DashboardSidebarPicker.vue";
import VisualizationViewer from "../CatalogView/VisualizationViewer.vue";
import {
  EMPTY_DASHBOARD_LAYOUT,
  type CatalogVisualization,
  type DashboardFilter,
  type DashboardLayout,
  type DashboardTile,
} from "../../types";

const props = defineProps<{ id?: string | number }>();
const router = useRouter();
const route = useRoute();
const store = useDashboardsStore();
const appearance = useGraphicWalkerAppearance();

const isNew = computed(() => props.id === undefined);
const nameDraft = ref("");
const dirty = ref(false);

const layoutRef = computed<DashboardLayout>(() => store.current?.layout ?? EMPTY_DASHBOARD_LAYOUT);
const { datasourcesInUse, tilesByDatasource, tileDatasource, tileLabel, getColumnStats } =
  useDashboardDatasources(layoutRef);

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

const findFreeRow = (layout: DashboardLayout): number => {
  if (!layout.tiles.length) return 0;
  return Math.max(...layout.tiles.map((t) => t.y + t.h));
};

const onLayoutChange = (next: DashboardLayout) => {
  if (!store.current) return;
  store.setLayout(next);
  dirty.value = true;
};

const onFiltersChange = (next: DashboardFilter[]) => {
  if (!store.current) return;
  store.setLayout({ ...store.current.layout, filters: next });
  dirty.value = true;
};

const editingVizId = ref<number | null>(null);
const vizDialogOpen = ref(false);
const vizRefreshNonces = ref<Record<number, number>>({});

const onEditViz = (vizId: number | null) => {
  if (vizId == null) return;
  editingVizId.value = vizId;
  vizDialogOpen.value = true;
};

const onCloseVizDialog = () => {
  // Bump the nonce so every tile bound to this viz_id remounts and re-fetches.
  // We can't tell from here whether the user actually saved (the viewer doesn't
  // emit a "saved" event), so bump unconditionally — a no-op refresh is cheap.
  if (editingVizId.value != null) {
    const id = editingVizId.value;
    vizRefreshNonces.value = {
      ...vizRefreshNonces.value,
      [id]: (vizRefreshNonces.value[id] ?? 0) + 1,
    };
  }
  editingVizId.value = null;
};

const onVizDeleted = (vizId: number) => {
  vizDialogOpen.value = false;
  editingVizId.value = null;
  // Drop every tile that pointed at the deleted viz.
  if (!store.current) return;
  const next = {
    ...store.current.layout,
    tiles: store.current.layout.tiles.filter((t) => t.viz_id !== vizId),
  };
  onLayoutChange(next);
  ElMessage.info("Removed deleted visualization from dashboard");
};

const onAddTile = (viz: CatalogVisualization) => {
  if (!store.current) return;
  const layout = store.current.layout;
  const tile: DashboardTile = {
    id: generateTileId(),
    type: "viz",
    viz_id: viz.id,
    chart_index: 0,
    x: 0,
    y: findFreeRow(layout),
    w: 6,
    h: 6,
  };
  onLayoutChange({ ...layout, tiles: [...layout.tiles, tile] });
};

const onAddTextTile = () => {
  if (!store.current) return;
  const layout = store.current.layout;
  const tile: DashboardTile = {
    id: generateTileId(),
    type: "text",
    viz_id: null,
    chart_index: 0,
    text_md: "## New section\n\nDescribe what's below.",
    x: 0,
    y: findFreeRow(layout),
    w: 12,
    h: 3,
  };
  onLayoutChange({ ...layout, tiles: [...layout.tiles, tile] });
};

const initialise = async () => {
  if (isNew.value) {
    const blank = store.newBlankDashboard();
    nameDraft.value = blank.name;
    dirty.value = false;
    return;
  }
  try {
    await store.loadDashboard(Number(props.id));
    if (store.current) nameDraft.value = store.current.name;
    dirty.value = false;
  } catch {
    ElMessage.error(store.error ?? "Failed to load dashboard");
  }
};

watch(nameDraft, (v) => {
  if (store.current && v !== store.current.name) dirty.value = true;
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

onMounted(async () => {
  await initialise();
  await consumeEditVizQuery();
});
onBeforeUnmount(() => store.reset());

const onSave = async () => {
  if (!store.current) return;
  const trimmed = nameDraft.value.trim();
  if (!trimmed) {
    ElMessage.warning("Name is required");
    return;
  }
  try {
    if (isNew.value) {
      const created = await store.createDashboard({
        name: trimmed,
        description: store.current.description,
        namespace_id: store.current.namespace_id,
        layout: store.current.layout,
      });
      dirty.value = false;
      ElMessage.success(`Created "${created.name}"`);
      router.replace({ name: "dashboard-edit", params: { id: created.id } });
    } else {
      const updated = await store.updateDashboard(Number(props.id), {
        name: trimmed,
        layout: store.current.layout,
      });
      dirty.value = false;
      ElMessage.success(`Saved "${updated.name}"`);
    }
  } catch {
    ElMessage.error(store.error ?? "Failed to save");
  }
};

const onCancel = async () => {
  if (dirty.value) {
    try {
      await ElMessageBox.confirm("Discard unsaved changes?", "Discard", {
        type: "warning",
        confirmButtonText: "Discard",
        cancelButtonText: "Stay",
      });
    } catch {
      return;
    }
  }
  // Brand-new dashboard with no persisted id has no view route to return to.
  if (isNew.value || props.id === undefined) {
    router.push({ name: "catalog", query: { tab: "visuals", kind: "dashboards" } });
    return;
  }
  router.push({ name: "dashboard-view", params: { id: props.id } });
};
</script>

<style scoped>
.dashboard-editor {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}
.editor-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 16px;
  border-bottom: 1px solid var(--el-border-color-lighter);
  background: var(--el-bg-color);
  gap: 12px;
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
.editor-dirty {
  font-size: 11px;
  color: var(--el-color-warning);
  text-transform: uppercase;
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
