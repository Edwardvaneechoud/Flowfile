<template>
  <div class="catalog-view">
    <!-- Tab Bar -->
    <div class="catalog-tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="catalog-tab"
        :class="{ active: catalogStore.activeTab === tab.key }"
        @click="handleTabClick(tab.key)"
      >
        <i :class="tab.icon"></i>
        <span>{{ tab.label }}</span>
        <span v-if="tab.badge !== null" class="tab-badge">{{ tab.badge }}</span>
      </button>
      <div class="tab-spacer"></div>
      <el-tooltip content="Refresh" placement="bottom" :show-after="400">
        <button class="catalog-tab info-btn" :disabled="refreshing" @click="refreshAll">
          <i class="fa-solid fa-arrows-rotate" :class="{ 'fa-spin': refreshing }"></i>
        </button>
      </el-tooltip>
      <el-tooltip content="About the Catalog" placement="bottom" :show-after="400">
        <button class="catalog-tab info-btn" @click="showInfoModal = true">
          <i class="fa-solid fa-circle-info"></i>
        </button>
      </el-tooltip>
    </div>

    <!-- Main Content -->
    <div class="catalog-content">
      <!-- Sidebar: Always shows catalog tree -->
      <div class="catalog-sidebar">
        <div class="sidebar-header">
          <h3>Catalogs</h3>
          <div class="sidebar-header-actions">
            <el-tooltip content="Register table" placement="bottom" :show-after="400">
              <button class="btn btn-ghost btn-icon btn-sm" @click="openRegisterTableGlobal">
                <i class="fa-solid fa-table"></i>
              </button>
            </el-tooltip>
            <el-tooltip content="Create virtual table" placement="bottom" :show-after="400">
              <button class="btn btn-ghost btn-icon btn-sm" @click="showCreateVirtualTable = true">
                <i class="fa-solid fa-bolt"></i>
              </button>
            </el-tooltip>
            <el-tooltip content="Register flow" placement="bottom" :show-after="400">
              <button class="btn btn-ghost btn-icon btn-sm" @click="openRegisterFlowGlobal">
                <i class="fa-solid fa-file-circle-plus"></i>
              </button>
            </el-tooltip>
            <el-tooltip content="Create Kafka Sync" placement="bottom" :show-after="400">
              <button class="btn btn-ghost btn-icon btn-sm" @click="showCreateSync = true">
                <i class="fa-solid fa-rotate"></i>
              </button>
            </el-tooltip>
            <el-tooltip content="New catalog" placement="bottom" :show-after="400">
              <button class="btn btn-ghost btn-icon btn-sm" @click="showCreateNamespace = true">
                <i class="fa-solid fa-plus"></i>
              </button>
            </el-tooltip>
          </div>
        </div>

        <!-- Catalog Tree -->
        <div class="tree-container">
          <div class="sidebar-filters">
            <input v-model="searchQuery" class="search-input" placeholder="Search..." />
            <label class="unavailable-toggle">
              <input v-model="showUnavailable" type="checkbox" />
              <span>Show unavailable</span>
            </label>
          </div>
          <div v-if="catalogStore.loading" class="loading-state">Loading...</div>
          <div v-else-if="catalogStore.tree.length === 0" class="empty-state">
            <p>No catalogs yet.</p>
            <button class="btn-primary btn-sm" @click="showCreateNamespace = true">
              Create your first catalog
            </button>
          </div>
          <div v-else>
            <CatalogTreeNode
              v-for="node in catalogStore.tree"
              :key="node.id"
              :node="node"
              :selected-flow-id="catalogStore.selectedFlowId"
              :selected-artifact-id="catalogStore.selectedArtifactId"
              :selected-table-id="catalogStore.selectedTableId"
              :search-query="searchQuery"
              :show-unavailable="showUnavailable"
              @select-flow="selectFlow($event)"
              @select-artifact="selectArtifact($event)"
              @select-table="selectTable($event)"
              @table-context-menu="onTableContextMenu($event)"
              @artifact-context-menu="onArtifactContextMenu($event)"
              @flow-context-menu="onFlowContextMenu($event)"
              @select-visualization="openVisualization($event)"
              @toggle-favorite="catalogStore.toggleFavorite($event)"
              @toggle-table-favorite="catalogStore.toggleTableFavorite($event)"
              @register-flow="openRegisterFlow($event)"
              @register-table="openRegisterTable($event)"
              @create-schema="openCreateSchema($event)"
              @delete-table="handleDeleteTable($event)"
              @delete-flow="handleDeleteFlow($event)"
            />
          </div>
        </div>
      </div>

      <!-- Detail Panel -->
      <div class="catalog-detail">
        <!-- Run detail view -->
        <RunDetailPanel
          v-if="catalogStore.selectedRunDetail"
          :run="catalogStore.selectedRunDetail"
          @close="handleCloseDetail"
          @open-snapshot="openRunSnapshot($event)"
          @view-flow="navigateToFlow($event)"
          @view-schedule-runs="navigateToScheduleRuns"
        />
        <!-- Schedule detail view -->
        <ScheduleDetailPanel
          v-else-if="catalogStore.selectedSchedule"
          :schedule="catalogStore.selectedSchedule"
          @close="handleCloseDetail"
          @view-run="handleViewRun"
          @view-flow="navigateToFlow"
          @view-schedule-runs="navigateToScheduleRuns"
          @toggle-schedule="handleToggleScheduleFromDetail"
          @delete-schedule="handleDeleteScheduleFromDetail"
          @run-now="handleRunNowFromDetail"
          @cancel-schedule-run="handleCancelScheduleRun"
          @open-snapshot="openRunSnapshot($event)"
        />
        <!-- Artifact detail view -->
        <ArtifactDetailPanel
          v-else-if="catalogStore.selectedArtifact"
          :artifact="catalogStore.selectedArtifact"
          :versions="selectedArtifactVersions"
          @close="handleCloseDetail"
          @navigate-to-flow="navigateToFlow($event)"
          @delete-artifact="handleDeleteArtifact($event)"
        />
        <!-- Table detail view -->
        <TableDetailPanel
          v-else-if="catalogStore.selectedTable"
          :table="catalogStore.selectedTable"
          :preview="catalogStore.tablePreview"
          :loading-preview="catalogStore.loadingTablePreview"
          :table-history="catalogStore.tableHistory"
          :selected-version="catalogStore.selectedVersion"
          @close="handleCloseDetail"
          @delete-table="handleDeleteTable($event)"
          @toggle-table-favorite="catalogStore.toggleTableFavorite($event)"
          @navigate-to-flow="navigateToFlow($event)"
          @select-version="catalogStore.selectVersion($event)"
          @query-table="handleQueryTable($event)"
          @recover-from-run="openRunSnapshot($event)"
        />
        <!-- Flow detail view -->
        <FlowDetailPanel
          v-else-if="catalogStore.selectedFlow"
          :flow="catalogStore.selectedFlow"
          :artifacts="catalogStore.flowArtifacts"
          :focus-runs="flowDetailFocusRuns"
          @runs-focused="flowDetailFocusRuns = false"
          @close="handleCloseDetail"
          @view-run="handleViewRun"
          @view-flow="navigateToFlow($event)"
          @view-schedule-runs="navigateToScheduleRuns"
          @toggle-favorite="catalogStore.toggleFavorite($event)"
          @toggle-follow="catalogStore.toggleFollow($event)"
          @open-flow="openFlowInDesigner($event)"
          @select-table="selectTable($event)"
          @delete-flow="handleDeleteFlow($event)"
          @rename-flow="handleRenameFlow"
          @add-schedule="handleAddFlowSchedule"
          @run-flow="handleRunFlow"
          @cancel-flow-run="handleCancelFlowRun"
          @select-schedule="selectSchedule"
          @recover-from-snapshot="openRunSnapshot($event)"
          @open-snapshot="openRunSnapshot($event)"
        />
        <!-- Run history overview -->
        <RunOverviewPanel
          v-else-if="catalogStore.activeTab === 'runs'"
          @view-run="handleViewRun"
          @view-flow="navigateToFlow($event)"
          @view-schedule-runs="navigateToScheduleRuns"
          @open-snapshot="openRunSnapshot($event)"
        />
        <!-- Schedule overview -->
        <ScheduleOverviewPanel
          v-else-if="catalogStore.activeTab === 'schedules'"
          @create-schedule="showCreateSchedule = true"
          @toggle-schedule="handleToggleSchedule"
          @delete-schedule="handleDeleteSchedule"
          @run-now="handleRunNow"
          @cancel-schedule-run="handleCancelScheduleRun"
          @view-flow="navigateToFlow"
          @select-schedule="selectSchedule"
        />
        <!-- Favorites list -->
        <div v-else-if="catalogStore.activeTab === 'favorites'" class="favorites-panel">
          <h2>Favorites</h2>
          <EmptyState
            v-if="catalogStore.favorites.length === 0"
            icon="fa-solid fa-star"
            title="No favorites yet"
            description="Star flows from the catalog tree to see them here."
          />
          <template v-else>
            <div class="favorites-filter-bar">
              <el-input
                v-model="favoritesSearch"
                placeholder="Search favorites"
                clearable
                size="small"
                style="width: 240px"
              />
            </div>
            <div class="favorites-list">
              <FlowListItem
                v-for="flow in filteredFavorites"
                :key="flow.id"
                :flow="flow"
                :selected="catalogStore.selectedFlowId === flow.id"
                @select="selectFlow(flow.id)"
                @toggle-favorite="catalogStore.toggleFavorite(flow.id)"
              />
            </div>
          </template>
        </div>
        <!-- SQL Editor -->
        <SqlEditorPanel
          v-else-if="catalogStore.activeTab === 'sql'"
          :initial-query="sqlInitialQuery"
        />
        <!-- Visuals (charts + dashboards) -->
        <VisualsPanel
          v-else-if="catalogStore.activeTab === 'visuals'"
          @view-table="selectTable($event)"
        />
        <!-- Published APIs -->
        <ApisPanel
          v-else-if="catalogStore.activeTab === 'apis'"
          @view-flow="navigateToFlow($event)"
        />
        <!-- Stats overview -->
        <StatsPanel
          v-else
          :stats="catalogStore.stats"
          :flows="catalogStore.allFlows"
          :tables="catalogStore.allTables"
          :favorites="catalogStore.favorites"
          :runs="catalogStore.runs"
          @view-run="handleViewRun"
          @view-flow="navigateToFlow($event)"
          @view-table="selectTable($event)"
          @create-schedule="showCreateSchedule = true"
          @toggle-schedule="handleToggleSchedule"
          @delete-schedule="handleDeleteSchedule"
          @run-now="handleRunNow"
          @cancel-schedule-run="handleCancelScheduleRun"
          @open-tab="handleTabClick($event)"
        />
      </div>
    </div>

    <!-- Modals -->
    <CreateNamespaceModal
      :visible="showCreateNamespace"
      :parent-id="createSchemaParentId"
      @close="
        showCreateNamespace = false;
        createSchemaParentId = null;
      "
    />

    <RegisterFlowModal
      :visible="showRegisterFlow"
      :namespace-id="registerFlowNamespaceId"
      :default-namespace-id="defaultNamespaceId"
      @close="showRegisterFlow = false"
    />

    <RegisterTableModal
      :visible="showRegisterTable"
      :namespace-id="registerTableNamespaceId"
      :default-namespace-id="defaultNamespaceId"
      @close="showRegisterTable = false"
    />

    <CreateVirtualTableModal
      :visible="showCreateVirtualTable"
      :namespace-id="registerTableNamespaceId"
      :default-namespace-id="defaultNamespaceId"
      @close="showCreateVirtualTable = false"
    />

    <CreateScheduleModal
      :visible="showCreateSchedule"
      :flows="catalogStore.allFlows"
      :tables="catalogStore.allTables"
      :preselected-flow-id="preselectedFlowId"
      @close="
        showCreateSchedule = false;
        preselectedFlowId = null;
      "
      @create="handleCreateSchedule"
    />

    <CreateSyncModal
      :visible="showCreateSync"
      @close="showCreateSync = false"
      @created="handleSyncCreated"
    />

    <!-- Saved-viz viewer (opened from the catalog tree) -->
    <el-dialog
      v-model="vizViewerOpen"
      title="Visualization"
      width="92vw"
      destroy-on-close
      append-to-body
      @close="closeVizViewer"
    >
      <VisualizationViewer
        v-if="vizViewerOpen && activeVizId !== null"
        :viz-id="activeVizId"
        :appearance="vizViewerAppearance"
        @close="closeVizViewer"
      />
    </el-dialog>

    <!-- New visualization editor (opened from a table's right-click menu) -->
    <el-dialog
      v-model="vizEditorOpen"
      title="New visualization"
      width="92vw"
      destroy-on-close
      append-to-body
    >
      <VisualizationEditor
        v-if="vizEditorOpen && vizEditorSource"
        :source="vizEditorSource"
        :viz="null"
        :appearance="vizViewerAppearance"
        @saved="onVizCreated"
        @cancel="vizEditorOpen = false"
      />
    </el-dialog>

    <!-- Right-click context menus for catalog tables and flows -->
    <Teleport to="body">
      <ContextMenu
        v-if="tableMenu"
        :position="{ x: tableMenu.x, y: tableMenu.y }"
        :options="tableMenuOptions"
        @select="onTableMenuSelect"
        @close="tableMenu = null"
      />
      <ContextMenu
        v-if="flowMenu"
        :position="{ x: flowMenu.x, y: flowMenu.y }"
        :options="flowMenuOptions"
        @select="onFlowMenuSelect"
        @close="flowMenu = null"
      />
      <ContextMenu
        v-if="artifactMenu"
        :position="{ x: artifactMenu.x, y: artifactMenu.y }"
        :options="artifactMenuOptions"
        @select="onArtifactMenuSelect"
        @close="artifactMenu = null"
      />
    </Teleport>

    <!-- Info Modal -->
    <el-dialog
      v-model="showInfoModal"
      title="About the Catalog"
      width="600px"
      :append-to-body="true"
    >
      <div class="info-modal-content">
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-folder-tree"></i>
            <h4>Organization</h4>
          </div>
          <p>
            Flows and tables are organized into <strong>catalogs</strong> and
            <strong>schemas</strong>. Use catalogs for broad groupings (e.g. by team or domain) and
            schemas for finer separation within them.
          </p>
        </div>
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-clock-rotate-left"></i>
            <h4>Run history</h4>
          </div>
          <p>
            Every registered flow tracks its executions automatically. View status, duration, and
            node-level progress for each run, or open a snapshot to inspect a past state.
          </p>
        </div>
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-table"></i>
            <h4>Tables &amp; artifacts</h4>
          </div>
          <p>
            Register datasets as catalog tables to preview and track them centrally. Artifacts
            produced by flows are versioned and linked back to the run that created them.
          </p>
        </div>
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-bolt"></i>
            <h4>Virtual Flow Tables</h4>
          </div>
          <p>
            Virtual flow tables do not store data on disk. When queried, they execute a registered
            flow on-demand to produce results. They appear with a
            <strong>bolt</strong> icon and a <strong>virtual</strong> badge in the catalog tree.
            Optimized virtual tables can push query predicates into the flow for faster execution.
          </p>
        </div>
        <div class="info-card">
          <div class="info-card-header">
            <i class="fa-solid fa-calendar-days"></i>
            <h4>Schedules</h4>
          </div>
          <p>
            Automate flow execution with cron-based or table-trigger schedules. Monitor active runs
            and manage schedules from the Schedules tab.
          </p>
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
// TODO(refactor): God component (~1700 LOC). Plan to extract:
//   - useModalState composable: 8 dialog refs at lines 472-490
//   - 19 async handlers: move into catalog-store.ts as Pinia actions (~lines 657-927)
//   - useRouteSync composable: applyRouteToStore (~lines 930-993)
import { computed, h, onMounted, onUnmounted, ref, watch } from "vue";
import { ElCheckbox, ElMessage, ElMessageBox } from "element-plus";
import { useRoute, useRouter } from "vue-router";
import { useCatalogStore } from "../../stores/catalog-store";
import { useFlowStore } from "../../stores/flow-store";
import { CatalogApi } from "../../api/catalog.api";
import { FlowApi } from "../../api/flow.api";
import { NodeApi } from "../../api/node.api";
import { EmptyState } from "../../components/common";
import ContextMenu from "../../components/common/ContextMenu/ContextMenu.vue";
import CatalogTreeNode from "./CatalogTreeNode.vue";
import FlowListItem from "./FlowListItem.vue";
import FlowDetailPanel from "./FlowDetailPanel.vue";
import ArtifactDetailPanel from "./ArtifactDetailPanel.vue";
import TableDetailPanel from "./TableDetailPanel.vue";
import RunDetailPanel from "./RunDetailPanel.vue";
import StatsPanel from "./StatsPanel.vue";
import CreateNamespaceModal from "./CreateNamespaceModal.vue";
import RegisterFlowModal from "./RegisterFlowModal.vue";
import RegisterTableModal from "./RegisterTableModal.vue";
import RunOverviewPanel from "./RunOverviewPanel.vue";
import ScheduleOverviewPanel from "./ScheduleOverviewPanel.vue";
import ScheduleDetailPanel from "./ScheduleDetailPanel.vue";
import CreateScheduleModal from "./CreateScheduleModal.vue";
import CreateSyncModal from "./CreateSyncModal.vue";
import CreateVirtualTableModal from "./CreateVirtualTableModal.vue";
import SqlEditorPanel from "./SqlEditorPanel.vue";
import VisualsPanel from "./VisualsPanel.vue";
import ApisPanel from "./ApisPanel.vue";
import VisualizationViewer from "./VisualizationViewer.vue";
import VisualizationEditor from "./VisualizationEditor.vue";
import { catalogTabs } from "./catalogTabs";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import type {
  CatalogTab,
  CatalogTable,
  FlowRegistration,
  FlowSchedule,
  FlowScheduleCreate,
  GlobalArtifact,
  NamespaceTree,
  VizSourceDescriptor,
} from "../../types";
import type { ContextMenuOption } from "../../components/common/ContextMenu/types";

const router = useRouter();
const route = useRoute();

const catalogStore = useCatalogStore();
const flowStore = useFlowStore();

const tabs = computed(() =>
  catalogTabs.map((tab) => ({
    key: tab.key as CatalogTab,
    label: tab.label,
    icon: tab.icon,
    badge:
      tab.key === "favorites"
        ? (catalogStore.stats?.total_favorites ?? null)
        : tab.key === "schedules"
          ? (catalogStore.stats?.total_schedules ?? null)
          : null,
  })),
);

// Search and filter state
const searchQuery = ref("");
const showUnavailable = ref(false);

// Client-side filter for the Favorites tab (fully-loaded list).
const favoritesSearch = ref("");
const filteredFavorites = computed(() => {
  const q = favoritesSearch.value.trim().toLowerCase();
  if (!q) return catalogStore.favorites;
  return catalogStore.favorites.filter((f) => (f.name ?? "").toLowerCase().includes(q));
});

// Modal state
const showInfoModal = ref(false);
const showCreateNamespace = ref(false);
const createSchemaParentId = ref<number | null>(null);
const showRegisterFlow = ref(false);
const registerFlowNamespaceId = ref<number | null>(null);
const showRegisterTable = ref(false);
const registerTableNamespaceId = ref<number | null>(null);
const showCreateSchedule = ref(false);
const preselectedFlowId = ref<number | null>(null);
const showCreateSync = ref(false);

// Saved-viz viewer modal (opened from the catalog tree's Visualizations section).
const vizViewerOpen = ref(false);
const activeVizId = ref<number | null>(null);
const vizViewerAppearance = useGraphicWalkerAppearance();

// New-visualization editor (opened from a table's right-click menu)
const vizEditorOpen = ref(false);
const vizEditorSource = ref<VizSourceDescriptor | null>(null);

// Right-click context menu for catalog tables
const tableMenu = ref<{ table: CatalogTable; x: number; y: number } | null>(null);
const tableMenuOptions: ContextMenuOption[] = [
  { label: "View table", action: "view" },
  { label: "Use in flow", action: "read" },
  { label: "Create visual", action: "visuals" },
];

// Right-click context menu for catalog models (artifacts)
const artifactMenu = ref<{ artifact: GlobalArtifact; x: number; y: number } | null>(null);
const artifactMenuOptions = computed<ContextMenuOption[]>(() => [
  { label: "View model", action: "view" },
  {
    label: "Read in flow",
    action: "read",
    disabled: artifactMenu.value?.artifact.blob_exists === false,
  },
  { label: "Delete model", action: "delete", danger: true },
]);

// Right-click context menu for catalog flows
const flowMenu = ref<{ flow: FlowRegistration; x: number; y: number } | null>(null);
const flowMenuOptions = computed<ContextMenuOption[]>(() => [
  { label: "Open flow in editor", action: "open", disabled: !flowMenu.value?.flow.file_exists },
  { label: "Flow overview", action: "view" },
  { label: "Run history", action: "runs" },
]);
// One-shot signal: scroll the flow detail panel to its Run History section.
const flowDetailFocusRuns = ref(false);

const showCreateVirtualTable = ref(false);
const sqlInitialQuery = ref<string | undefined>(undefined);

// Default namespace ID (loaded once on mount)
const defaultNamespaceId = ref<number | null>(null);

// Polling
let pollInterval: ReturnType<typeof setInterval> | null = null;
const refreshing = ref(false);

async function pollActiveRuns() {
  const hadActiveRuns = catalogStore.activeRuns.length > 0;
  await Promise.all([catalogStore.loadActiveRuns(), catalogStore.loadSchedulerStatus()]);
  const hasActiveRuns = catalogStore.activeRuns.length > 0;

  // When runs finish, refresh related data
  if (hadActiveRuns && !hasActiveRuns) {
    const loads: Promise<void>[] = [
      catalogStore.loadRuns(catalogStore.selectedFlowId),
      catalogStore.loadSchedules(),
      catalogStore.loadStats(),
    ];
    if (catalogStore.selectedScheduleId !== null) {
      loads.push(catalogStore.loadScheduleRuns(catalogStore.selectedScheduleId));
      loads.push(catalogStore.loadScheduleDetail(catalogStore.selectedScheduleId));
    }
    await Promise.all(loads);
  }
}

async function refreshAll() {
  refreshing.value = true;
  try {
    await Promise.all([
      catalogStore.loadActiveRuns(),
      catalogStore.loadRuns(catalogStore.selectedFlowId),
      catalogStore.loadSchedules(),
      catalogStore.loadStats(),
      catalogStore.loadTree(),
      catalogStore.loadAllFlows(),
      catalogStore.loadAllTables(),
      catalogStore.loadSchedulerStatus(),
    ]);
  } finally {
    refreshing.value = false;
  }
}

// --- Route-based navigation ---

function handleTabClick(tab: CatalogTab) {
  router.push({ name: "catalog", query: { tab } });
}

function selectFlow(flowId: number) {
  router.push({
    name: "catalog",
    query: { tab: catalogStore.activeTab, flowId: String(flowId) },
  });
}

function selectTable(tableId: number) {
  router.push({
    name: "catalog",
    query: { tab: catalogStore.activeTab, tableId: String(tableId) },
  });
}

function openVisualization(vizId: number) {
  activeVizId.value = vizId;
  vizViewerOpen.value = true;
}

function closeVizViewer() {
  vizViewerOpen.value = false;
  activeVizId.value = null;
  // Keep the namespace tree in sync if the user renamed/deleted from the viewer.
  catalogStore.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
}

function onTableContextMenu(payload: { table: CatalogTable; x: number; y: number }) {
  tableMenu.value = payload;
}

function onTableMenuSelect(action: string) {
  const table = tableMenu.value?.table;
  if (!table) return;
  if (action === "view") selectTable(table.id);
  else if (action === "read") createReadInFlow(table);
  else if (action === "visuals") openCreateViz(table);
}

function onArtifactContextMenu(payload: { artifact: GlobalArtifact; x: number; y: number }) {
  artifactMenu.value = payload;
}

function onArtifactMenuSelect(action: string) {
  const artifact = artifactMenu.value?.artifact;
  if (!artifact) return;
  if (action === "view") selectArtifact(artifact.id);
  else if (action === "read") createReadInFlowFromArtifact(artifact);
  else if (action === "delete") handleDeleteArtifact(artifact);
}

function onFlowContextMenu(payload: { flow: FlowRegistration; x: number; y: number }) {
  flowMenu.value = payload;
}

function onFlowMenuSelect(action: string) {
  const flow = flowMenu.value?.flow;
  if (!flow) return;
  if (action === "view") selectFlow(flow.id);
  else if (action === "open") openFlowInDesigner(flow.flow_path);
  else if (action === "runs") {
    flowDetailFocusRuns.value = true;
    selectFlow(flow.id);
  }
}

// Spin up a fresh flow whose only node is a catalog reader pre-configured for the
// given table, then open it in the designer. Mirrors the setting_input the
// CatalogReader node persists for a configured table (is_setup: true).
async function createReadInFlow(table: CatalogTable) {
  try {
    const flowId = await FlowApi.createFlow(null, null);
    const nodeId = 1;
    const posX = 200;
    const posY = 200;
    await FlowApi.insertNode(flowId, nodeId, "catalog_reader", posX, posY);
    await NodeApi.updateSettingsDirectly("catalog_reader", {
      catalog_table_id: table.id,
      catalog_table_name: table.name,
      catalog_full_table_name: null,
      catalog_namespace_id: table.namespace_id,
      delta_version: null,
      sql_query: null,
      flow_id: flowId,
      node_id: nodeId,
      cache_results: false,
      pos_x: posX,
      pos_y: posY,
      is_setup: true,
      description: "",
    });
    flowStore.setFlowId(flowId);
    router.push({ name: "designer" });
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to create read flow");
  }
}

// Spin up a fresh flow whose only node is a Python Script that loads the given
// model from the catalog, then open it in the designer. Mirrors createReadInFlow
// (tables) but targets the kernel-backed python_script node where flowfile_ctx lives.
async function createReadInFlowFromArtifact(artifact: GlobalArtifact) {
  try {
    const flowId = await FlowApi.createFlow(null, null);
    const nodeId = 1;
    const posX = 200;
    const posY = 200;
    const id = artifact.namespace_id;
    let nsArg = "";
    if (id !== null && id !== undefined) {
      const nsName = catalogStore.getNamespaceName(id);
      nsArg = nsName ? `, namespace=${JSON.stringify(nsName)}` : `, namespace_id=${id}`;
    }
    // Omit version so the flow always reads the latest model (picks up retrains).
    const code = `obj = flowfile_ctx.get_global("${artifact.name}"${nsArg})\nobj`;
    await FlowApi.insertNode(flowId, nodeId, "python_script", posX, posY);
    await NodeApi.updateSettingsDirectly("python_script", {
      flow_id: flowId,
      node_id: nodeId,
      cache_results: false,
      pos_x: posX,
      pos_y: posY,
      is_setup: true,
      description: "",
      depending_on_ids: [],
      output_names: ["main"],
      python_script_input: { code, kernel_id: null },
    });
    flowStore.setFlowId(flowId);
    router.push({ name: "designer" });
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to create read flow");
  }
}

function openCreateViz(table: CatalogTable) {
  vizEditorSource.value = { source_type: "table", table_id: table.id };
  vizEditorOpen.value = true;
}

function onVizCreated() {
  vizEditorOpen.value = false;
  vizEditorSource.value = null;
  catalogStore.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
}

function selectArtifact(artifactId: number) {
  router.push({
    name: "catalog",
    query: { tab: catalogStore.activeTab, artifactId: String(artifactId) },
  });
}

function handleViewRun(runId: number) {
  const q: Record<string, string> = { tab: catalogStore.activeTab };
  if (catalogStore.selectedScheduleId !== null) {
    q.scheduleId = String(catalogStore.selectedScheduleId);
  } else if (catalogStore.selectedFlowId !== null) {
    q.flowId = String(catalogStore.selectedFlowId);
  }
  q.runId = String(runId);
  router.push({ name: "catalog", query: q });
}

function handleCloseDetail() {
  router.back();
}

/** Collect all versions of the selected artifact from the tree. */
function collectArtifactVersions(
  nodes: NamespaceTree[],
  name: string,
  nsId: number | null,
): GlobalArtifact[] {
  const result: GlobalArtifact[] = [];
  for (const node of nodes) {
    for (const a of node.artifacts ?? []) {
      if (a.name === name && a.namespace_id === nsId) result.push(a);
    }
    result.push(...collectArtifactVersions(node.children, name, nsId));
  }
  return result;
}

const selectedArtifactVersions = computed((): GlobalArtifact[] => {
  const a = catalogStore.selectedArtifact;
  if (!a) return [];
  const versions = collectArtifactVersions(catalogStore.tree, a.name, a.namespace_id);
  return versions.sort((x, y) => y.version - x.version);
});

function openCreateSchema(parentId: number) {
  createSchemaParentId.value = parentId;
  showCreateNamespace.value = true;
}

function openRegisterFlow(namespaceId: number) {
  registerFlowNamespaceId.value = namespaceId;
  showRegisterFlow.value = true;
}

function openRegisterTable(namespaceId: number) {
  registerTableNamespaceId.value = namespaceId;
  showRegisterTable.value = true;
}

/** Open register table modal from the sidebar header (no pre-selected namespace). */
function openRegisterTableGlobal() {
  const schemaNamespaces: { id: number }[] = [];
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      schemaNamespaces.push({ id: schema.id });
    }
  }
  registerTableNamespaceId.value = defaultNamespaceId.value ?? schemaNamespaces[0]?.id ?? null;
  showRegisterTable.value = true;
}

function openRegisterFlowGlobal() {
  registerFlowNamespaceId.value = defaultNamespaceId.value ?? null;
  showRegisterFlow.value = true;
}

function quoteSqlName(name: string): string {
  return /^[a-zA-Z_]\w*$/.test(name) ? name : `"${name}"`;
}

function handleQueryTable(tableName: string) {
  sqlInitialQuery.value = `SELECT * FROM ${quoteSqlName(tableName)}`;
  // Clear selected table so the SQL editor panel shows
  catalogStore.clearTableSelection();
  router.push({ name: "catalog", query: { tab: "sql" } });
}

// Confirm dialog with an "also delete from disk" checkbox (default on). Returns
// the checkbox state on confirm, or null when the user cancels.
async function confirmDeleteWithFile(opts: {
  title: string;
  question: string;
  fileLabel: string;
}): Promise<{ deleteFile: boolean } | null> {
  const deleteFile = ref(true);
  try {
    await ElMessageBox.confirm(
      () =>
        h("div", [
          h("p", { style: "margin: 0 0 12px" }, opts.question),
          h(
            ElCheckbox,
            {
              modelValue: deleteFile.value,
              "onUpdate:modelValue": (v: boolean | string | number) =>
                (deleteFile.value = Boolean(v)),
            },
            () => opts.fileLabel,
          ),
        ]),
      opts.title,
      { confirmButtonText: "Delete", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return null; // User cancelled
  }
  return { deleteFile: deleteFile.value };
}

async function handleDeleteTable(tableId: number) {
  const result = await confirmDeleteWithFile({
    title: "Delete Table",
    question: "Delete this table from the catalog?",
    fileLabel: "Also delete the table data from disk",
  });
  if (!result) return;
  try {
    await CatalogApi.deleteTable(tableId, result.deleteFile);
    catalogStore.clearTableSelection();
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllTables(),
      catalogStore.loadStats(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to delete table");
  }
}

// Delete a model (all versions). Backend hard-deletes the blobs; mirrors the
// table delete flow (confirm -> API -> clear selection -> refresh).
async function handleDeleteArtifact(artifact: GlobalArtifact) {
  try {
    await ElMessageBox.confirm(
      `Are you sure you want to delete the model "${artifact.name}"? All versions and their data will be removed.`,
      "Delete Model",
      {
        confirmButtonText: "Delete",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );
  } catch {
    return; // User cancelled
  }
  try {
    await CatalogApi.deleteArtifact(artifact.name, artifact.namespace_id);
    catalogStore.clearArtifactSelection();
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to delete model");
  }
}

async function handleDeleteFlow(flowId: number) {
  const result = await confirmDeleteWithFile({
    title: "Delete Flow",
    question: "Delete this flow from the catalog? Run history will also be removed.",
    fileLabel: "Also delete the flow file from disk",
  });
  if (!result) return;
  try {
    await CatalogApi.deleteFlow(flowId, result.deleteFile);
    catalogStore.selectedFlowId = null;
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllFlows(),
      catalogStore.loadFavorites(),
      catalogStore.loadFollowing(),
      catalogStore.loadStats(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to delete flow");
  }
}

async function handleRenameFlow(flowId: number, newName: string) {
  try {
    await CatalogApi.updateFlow(flowId, { name: newName });
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllFlows(),
      catalogStore.loadFavorites(),
      catalogStore.loadFollowing(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to rename flow");
  }
}

async function openRunSnapshot(runId: number) {
  try {
    const flowId = await CatalogApi.openRunSnapshot(runId);
    flowStore.setFlowId(flowId);
    router.push({ name: "designer" });
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to open flow snapshot");
  }
}

async function openFlowInDesigner(flowPath: string) {
  try {
    const flowId = await FlowApi.importFlow(flowPath);
    flowStore.setFlowId(flowId);
    router.push({ name: "designer" });
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to open flow");
  }
}

function navigateToFlow(registrationId: number) {
  router.push({
    name: "catalog",
    query: { tab: "catalog", flowId: String(registrationId) },
  });
}

function selectSchedule(scheduleId: number) {
  router.push({
    name: "catalog",
    query: { tab: catalogStore.activeTab, scheduleId: String(scheduleId) },
  });
}

function navigateToScheduleRuns(scheduleId: number) {
  selectSchedule(scheduleId);
}

function handleAddFlowSchedule(flowId: number) {
  preselectedFlowId.value = flowId;
  showCreateSchedule.value = true;
}

function handleSyncCreated(flowRegistrationId: number) {
  showCreateSync.value = false;
  preselectedFlowId.value = flowRegistrationId;
  showCreateSchedule.value = true;
}

async function handleCreateSchedule(body: FlowScheduleCreate) {
  try {
    await CatalogApi.createSchedule(body);
    showCreateSchedule.value = false;
    preselectedFlowId.value = null;
    const loads: Promise<void>[] = [catalogStore.loadSchedules(), catalogStore.loadStats()];
    if (catalogStore.selectedFlowId) {
      loads.push(catalogStore.loadFlowSchedules(catalogStore.selectedFlowId));
    }
    await Promise.all(loads);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to create schedule");
  }
}

async function handleRunNow(scheduleId: number) {
  try {
    await CatalogApi.triggerScheduleNow(scheduleId);
    await Promise.all([
      catalogStore.loadActiveRuns(),
      catalogStore.loadRuns(catalogStore.selectedFlowId),
      catalogStore.loadSchedules(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to trigger run");
  }
}

async function handleToggleSchedule(id: number, enabled: boolean) {
  try {
    await CatalogApi.updateSchedule(id, { enabled });
    await catalogStore.loadSchedules();
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to update schedule");
  }
}

async function handleDeleteSchedule(id: number) {
  try {
    await ElMessageBox.confirm(
      "Are you sure you want to delete this schedule?",
      "Delete Schedule",
      {
        confirmButtonText: "Delete",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );
  } catch {
    return; // User cancelled
  }
  try {
    await CatalogApi.deleteSchedule(id);
    await Promise.all([catalogStore.loadSchedules(), catalogStore.loadStats()]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to delete schedule");
  }
}

async function handleCancelScheduleRun(schedule: FlowSchedule) {
  try {
    const activeRuns = catalogStore.activeRuns.filter(
      (r) => r.registration_id === schedule.registration_id,
    );
    for (const run of activeRuns) {
      await catalogStore.cancelRun(run.id);
    }
    await Promise.all([
      catalogStore.loadActiveRuns(),
      catalogStore.loadSchedules(),
      catalogStore.loadRuns(catalogStore.selectedFlowId),
      catalogStore.loadStats(),
    ]);
    ElMessage.success("Run cancelled");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to cancel run");
  }
}

async function handleToggleScheduleFromDetail(id: number, enabled: boolean) {
  try {
    await CatalogApi.updateSchedule(id, { enabled });
    await Promise.all([catalogStore.loadScheduleDetail(id), catalogStore.loadSchedules()]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to update schedule");
  }
}

async function handleDeleteScheduleFromDetail(id: number) {
  try {
    await ElMessageBox.confirm(
      "Are you sure you want to delete this schedule?",
      "Delete Schedule",
      {
        confirmButtonText: "Delete",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );
  } catch {
    return; // User cancelled
  }
  try {
    await CatalogApi.deleteSchedule(id);
    catalogStore.clearScheduleSelection();
    router.push({ name: "catalog", query: { tab: catalogStore.activeTab } });
    await Promise.all([catalogStore.loadSchedules(), catalogStore.loadStats()]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to delete schedule");
  }
}

async function handleRunNowFromDetail(scheduleId: number) {
  try {
    await CatalogApi.triggerScheduleNow(scheduleId);
    await Promise.all([
      catalogStore.loadActiveRuns(),
      catalogStore.loadScheduleRuns(scheduleId),
      catalogStore.loadScheduleDetail(scheduleId),
      catalogStore.loadSchedules(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to trigger run");
  }
}

async function handleRunFlow(flowId: number) {
  try {
    await CatalogApi.runFlow(flowId);
    await Promise.all([catalogStore.loadActiveRuns(), catalogStore.loadRuns(flowId)]);
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to trigger run");
  }
}

async function handleCancelFlowRun(flowId: number) {
  try {
    const activeRuns = catalogStore.activeRuns.filter((r) => r.registration_id === flowId);
    for (const run of activeRuns) {
      await catalogStore.cancelRun(run.id);
    }
    await Promise.all([
      catalogStore.loadActiveRuns(),
      catalogStore.loadRuns(flowId),
      catalogStore.loadStats(),
    ]);
    ElMessage.success("Run cancelled");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to cancel run");
  }
}

// --- Route → Store sync ---

function applyRouteToStore() {
  const q = route.query;
  const tab = (q.tab as CatalogTab) || "runs";
  const flowId = q.flowId ? Number(q.flowId) : null;
  const runId = q.runId ? Number(q.runId) : null;
  const artifactId = q.artifactId ? Number(q.artifactId) : null;
  const tableId = q.tableId ? Number(q.tableId) : null;
  const scheduleId = q.scheduleId ? Number(q.scheduleId) : null;

  // Update tab (set directly to avoid setActiveTab clearing selections)
  if (catalogStore.activeTab !== tab) {
    catalogStore.activeTab = tab;
    if (tab === "favorites") catalogStore.loadFavorites();
    else if (tab === "runs" && !flowId) catalogStore.loadRuns();
    else if (tab === "schedules") catalogStore.loadSchedules();
    else if (tab === "catalog") catalogStore.loadTree();
  }

  // Handle selections (mutually exclusive: table, artifact, schedule, or flow+run)
  if (tableId !== null) {
    if (tableId !== catalogStore.selectedTableId) catalogStore.selectTable(tableId);
    if (catalogStore.selectedArtifact) catalogStore.clearArtifactSelection();
    if (catalogStore.selectedSchedule) catalogStore.clearScheduleSelection();
  } else if (scheduleId !== null) {
    if (scheduleId !== catalogStore.selectedScheduleId) catalogStore.selectSchedule(scheduleId);
    if (catalogStore.selectedArtifact) catalogStore.clearArtifactSelection();
    if (catalogStore.selectedTable) catalogStore.clearTableSelection();
  } else if (artifactId !== null) {
    if (artifactId !== catalogStore.selectedArtifactId) {
      catalogStore.selectedFlowId = null;
      catalogStore.clearTableSelection();
      catalogStore.clearScheduleSelection();
      catalogStore.selectArtifact(artifactId);
    }
  } else {
    // Clear table, artifact, and schedule if not in URL
    if (catalogStore.selectedTable) catalogStore.clearTableSelection();
    if (catalogStore.selectedArtifact) catalogStore.clearArtifactSelection();
    if (catalogStore.selectedSchedule) catalogStore.clearScheduleSelection();

    // Flow selection (set manually to avoid selectFlow clearing run detail)
    if (flowId !== null) {
      if (flowId !== catalogStore.selectedFlowId) {
        catalogStore.selectedFlowId = flowId;
        catalogStore.clearTableSelection();
        catalogStore.clearArtifactSelection();
        catalogStore.clearScheduleSelection();
        catalogStore.loadRuns(flowId);
        catalogStore.loadFlowArtifacts(flowId);
        catalogStore.loadFlowSchedules(flowId);
      }
    } else if (catalogStore.selectedFlowId !== null) {
      catalogStore.selectedFlowId = null;
    }
  }

  // Run detail can coexist with any selection (schedule, flow, etc.)
  if (runId !== null) {
    if (runId !== catalogStore.selectedRunId) catalogStore.loadRunDetail(runId);
  } else if (catalogStore.selectedRunDetail) {
    catalogStore.selectedRunId = null;
    catalogStore.selectedRunDetail = null;
  }
}

// Watch route changes (handles browser back/forward)
watch(() => route.query, applyRouteToStore);

onMounted(async () => {
  await catalogStore.initialize();
  applyRouteToStore(); // Restore state from URL after data is loaded
  try {
    defaultNamespaceId.value = await CatalogApi.getDefaultNamespaceId();
  } catch {
    // Not critical — leave null
  }
  pollInterval = setInterval(pollActiveRuns, 20_000);
});

onUnmounted(() => {
  if (pollInterval !== null) {
    clearInterval(pollInterval);
    pollInterval = null;
  }
});
</script>

<!-- Shared catalog styles: inherited by all child components via .catalog-view ancestor -->
<style>
/* ========== Back Button ========== */
.catalog-view .back-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  cursor: pointer;
  transition: all var(--transition-fast);
  margin-bottom: var(--spacing-3);
}

.catalog-view .back-btn:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

/* ========== Meta Grid ========== */
.catalog-view .meta-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-5);
}

.catalog-view .meta-card {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.catalog-view .meta-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.catalog-view .meta-value {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.catalog-view .meta-value.mono {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  word-break: break-all;
}

/* ========== Sections ========== */
.catalog-view .section {
  margin-bottom: var(--spacing-5);
}

.catalog-view .section h3 {
  display: flex;
  align-items: center;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  margin: 0 0 var(--spacing-3) 0;
  color: var(--color-text-primary);
}

.catalog-view .section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: var(--spacing-2);
}

.catalog-view .section-header h3 {
  margin: 0;
}

.catalog-view .section-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  margin-right: var(--spacing-1);
}

/* ========== Summary Cards ========== */
.catalog-view .summary-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-4);
}

.catalog-view .summary-cards-3 {
  grid-template-columns: repeat(3, 1fr);
}

.catalog-view .summary-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  border: 1px solid var(--color-border-light);
}

.catalog-view .summary-icon {
  font-size: var(--font-size-lg);
  color: var(--color-primary);
}

.catalog-view .success-icon {
  color: var(--color-success);
}

.catalog-view .failure-icon {
  color: var(--color-danger);
}

.catalog-view .running-icon {
  color: var(--color-info);
}

.catalog-view .enabled-icon {
  color: var(--color-success);
}

.catalog-view .summary-info {
  display: flex;
  flex-direction: column;
}

.catalog-view .summary-value {
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-bold);
  color: var(--color-text-primary);
  line-height: 1.2;
}

.catalog-view .summary-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

/* ========== Status Badges ========== */
.catalog-view .status-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
}

.catalog-view .status-badge.success,
.catalog-view .status-badge.active {
  background: color-mix(in srgb, var(--color-success) 12%, transparent);
  color: var(--color-success);
}

.catalog-view .status-badge.failure {
  background: color-mix(in srgb, var(--color-danger) 12%, transparent);
  color: var(--color-danger);
}

.catalog-view .status-badge.pending {
  background: color-mix(in srgb, var(--color-warning) 12%, transparent);
  color: var(--color-warning);
}

.catalog-view .status-badge.running {
  background: color-mix(in srgb, var(--color-info) 12%, transparent);
  color: var(--color-info);
}

.catalog-view .status-badge.enabled {
  background: color-mix(in srgb, var(--color-success) 12%, transparent);
  color: var(--color-success);
}

.catalog-view .status-badge.paused {
  background: rgba(156, 163, 175, 0.15);
  color: var(--color-text-muted);
}

.catalog-view .status-badge.deleted {
  background: color-mix(in srgb, var(--color-danger) 12%, transparent);
  color: var(--color-danger);
}

.catalog-view .status-badge.unavailable {
  background: color-mix(in srgb, var(--color-warning) 12%, transparent);
  color: var(--color-warning);
}

/* ========== Overview Table ========== */
.catalog-view .overview-table,
.catalog-view .runs-table,
.catalog-view .schedules-table {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.catalog-view .table-header {
  display: grid;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}

.catalog-view .table-row {
  display: grid;
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  align-items: center;
  cursor: pointer;
  transition: background var(--transition-fast);
}

.catalog-view .table-row:last-child {
  border-bottom: none;
}

.catalog-view .table-row:hover {
  background: var(--color-background-hover);
}

.catalog-view .table-row.row-disabled {
  opacity: 0.6;
}

/* ========== Common Column Styles ========== */
.catalog-view .col-trigger {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.catalog-view .trigger-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.catalog-view .type-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.catalog-view .col-started,
.catalog-view .col-duration,
.catalog-view .col-last {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
}

.catalog-view .col-nodes {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  font-family: var(--font-family-mono);
}

.catalog-view .col-type {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.catalog-view .col-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
}

.catalog-view .col-flow {
  min-width: 0;
}

/* ========== Flow Name / Link / Snapshot ========== */
.catalog-view .flow-name {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.catalog-view .flow-link {
  cursor: pointer;
  transition: color var(--transition-fast);
}

.catalog-view .flow-link:hover {
  color: var(--color-primary);
}

.catalog-view .snapshot-link {
  color: var(--color-primary);
  cursor: pointer;
  font-size: var(--font-size-xs);
}

.catalog-view .no-snapshot {
  color: var(--color-text-muted);
}

/* ========== Description Editing ========== */
.catalog-view .col-description {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  min-width: 0;
}

.catalog-view .col-description .btn-icon-inline {
  opacity: 0;
  transition: opacity var(--transition-fast);
  flex-shrink: 0;
}

.catalog-view .table-row:hover .col-description .btn-icon-inline {
  opacity: 1;
}

.catalog-view .btn-icon-inline {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-md);
  transition: all var(--transition-fast);
  flex-shrink: 0;
}

.catalog-view .btn-icon-inline:hover {
  background: var(--color-background-hover);
  color: var(--color-primary);
}

.catalog-view .description-text {
  cursor: pointer;
  transition: color var(--transition-fast);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.catalog-view .description-text:hover {
  color: var(--color-text-primary);
}

.catalog-view .description-text.placeholder {
  font-style: italic;
  opacity: 0.6;
}

.catalog-view .edit-description-input {
  width: 100%;
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-xs);
  outline: none;
}

/* ========== Pagination ========== */
.catalog-view .pagination-bar {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
  padding: var(--spacing-4) 0;
}

.catalog-view .page-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.catalog-view .page-btn:hover:not(:disabled) {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

.catalog-view .page-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.catalog-view .page-info {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  padding: 0 var(--spacing-2);
}

/* ========== Empty State (catalog variant) ========== */
.catalog-view .empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-5) var(--spacing-4);
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  text-align: center;
}

.catalog-view .empty-state-icon,
.catalog-view .empty-icon {
  font-size: var(--font-size-xl);
  opacity: 0.4;
}

.catalog-view .empty-state h3 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
  color: var(--color-text-primary);
}

.catalog-view .empty-state p {
  margin: 0;
  font-size: var(--font-size-sm);
}

/* ========== Text Status Colors ========== */
.catalog-view .text-success {
  color: var(--color-success);
}

.catalog-view .text-danger {
  color: var(--color-danger);
}

.catalog-view .text-pending {
  color: var(--color-warning);
}

/* ========== Mono ========== */
.catalog-view .mono {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
}

/* ========== Info Card (CatalogView + StatsPanel) ========== */
.catalog-view .info-card {
  padding: var(--spacing-4);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.catalog-view .info-card-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
  color: var(--color-primary);
}

.catalog-view .info-card-header h4 {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.catalog-view .info-card p {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
}
</style>

<style scoped>
.catalog-view {
  display: flex;
  flex-direction: column;
  height: 100%;
  background-color: var(--color-background-primary);
  font-family: var(--font-family-base);
}

/* ========== Tab Bar ========== */
.catalog-tabs {
  display: flex;
  gap: 2px;
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.catalog-tab {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-4);
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  border-radius: var(--border-radius-md);
  transition: all var(--transition-fast);
}

.catalog-tab:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.catalog-tab.active {
  background: var(--color-background-primary);
  color: var(--color-primary);
  box-shadow: var(--shadow-xs);
}

.tab-badge {
  background: var(--color-primary);
  color: var(--color-text-inverse);
  font-size: 11px;
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  min-width: 18px;
  text-align: center;
  line-height: 18px;
}

.tab-spacer {
  flex: 1;
}

.info-btn {
  color: var(--color-text-muted);
}

.info-btn:hover {
  color: var(--color-primary);
}

/* ========== Content Layout ========== */
.catalog-content {
  display: flex;
  flex: 1;
  overflow: hidden;
}

.catalog-sidebar {
  width: 340px;
  min-width: 280px;
  border-right: 1px solid var(--color-border-primary);
  display: flex;
  flex-direction: column;
  overflow-y: auto;
}

.sidebar-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-3) var(--spacing-4);
  border-bottom: 1px solid var(--color-border-light);
}

.sidebar-header h3 {
  margin: 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.sidebar-header-actions {
  display: flex;
  gap: 4px;
}

.catalog-detail {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-4) var(--spacing-6);
}

/* ========== Sidebar Filters ========== */
.sidebar-filters {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
}

.search-input {
  flex: 1;
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-xs);
  min-width: 0;
}

.search-input:focus {
  outline: none;
  border-color: var(--color-primary);
}

.unavailable-toggle {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  white-space: nowrap;
  cursor: pointer;
}

.unavailable-toggle input {
  margin: 0;
}

/* ========== Tree / List Containers ========== */
.tree-container,
.list-container {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-2);
}

/* ========== States ========== */
.loading-state {
  padding: var(--spacing-6);
  text-align: center;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.empty-state .muted {
  margin-top: var(--spacing-1);
  font-size: var(--font-size-xs);
}

/* ========== Favorites Panel ========== */
.favorites-panel {
  max-width: 1000px;
  margin: 0 auto;
}

.favorites-panel h2 {
  margin: 0 0 var(--spacing-5) 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.favorites-panel .empty-icon {
  font-size: 48px;
  color: var(--color-primary);
  opacity: 0.5;
  margin-bottom: var(--spacing-4);
}

.favorites-panel .empty-state h3 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
  color: var(--color-text-primary);
}

.favorites-panel .empty-state p {
  margin: 0;
  font-size: var(--font-size-sm);
}

.favorites-filter-bar {
  margin-bottom: var(--spacing-3);
}

.favorites-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

/* ========== Info Modal ========== */
.info-modal-content {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}
</style>
