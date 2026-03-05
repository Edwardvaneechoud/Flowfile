<template>
  <div class="catalog-view">
    <!-- Tab Bar -->
    <div class="catalog-tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="catalog-tab"
        :class="{ active: catalogStore.activeTab === tab.key }"
        @click="catalogStore.setActiveTab(tab.key)"
      >
        <i :class="tab.icon"></i>
        <span>{{ tab.label }}</span>
        <span v-if="tab.badge !== null" class="tab-badge">{{ tab.badge }}</span>
      </button>
    </div>

    <!-- Main Content -->
    <div class="catalog-content">
      <!-- Sidebar: Tree / List -->
      <div class="catalog-sidebar">
        <div class="sidebar-header">
          <h3>{{ sidebarTitle }}</h3>
          <div v-if="catalogStore.activeTab === 'catalog'" class="sidebar-header-actions">
            <button class="btn-icon" title="Register table" @click="openRegisterTableGlobal">
              <i class="fa-solid fa-table"></i>
            </button>
            <button class="btn-icon" title="New catalog" @click="showCreateNamespace = true">
              <i class="fa-solid fa-plus"></i>
            </button>
          </div>
        </div>

        <!-- Catalog Tree -->
        <div v-if="catalogStore.activeTab === 'catalog'" class="tree-container">
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
              @toggle-favorite="catalogStore.toggleFavorite($event)"
              @toggle-follow="catalogStore.toggleFollow($event)"
              @register-flow="openRegisterFlow($event)"
              @register-table="openRegisterTable($event)"
              @create-schema="openCreateSchema($event)"
            />
          </div>
        </div>

        <!-- Favorites List -->
        <div v-else-if="catalogStore.activeTab === 'favorites'" class="list-container">
          <div v-if="catalogStore.favorites.length === 0" class="empty-state">
            <p>No favorites yet.</p>
            <p class="muted">Star flows from the Catalog tab to see them here.</p>
          </div>
          <FlowListItem
            v-for="flow in catalogStore.favorites"
            :key="flow.id"
            :flow="flow"
            :selected="catalogStore.selectedFlowId === flow.id"
            @select="catalogStore.selectFlow(flow.id)"
            @toggle-favorite="catalogStore.toggleFavorite(flow.id)"
            @toggle-follow="catalogStore.toggleFollow(flow.id)"
          />
        </div>

        <!-- Following List -->
        <div v-else-if="catalogStore.activeTab === 'following'" class="list-container">
          <div v-if="catalogStore.following.length === 0" class="empty-state">
            <p>No followed flows.</p>
            <p class="muted">Follow flows you want to keep track of.</p>
          </div>
          <FlowListItem
            v-for="flow in catalogStore.following"
            :key="flow.id"
            :flow="flow"
            :selected="catalogStore.selectedFlowId === flow.id"
            @select="catalogStore.selectFlow(flow.id)"
            @toggle-favorite="catalogStore.toggleFavorite(flow.id)"
            @toggle-follow="catalogStore.toggleFollow(flow.id)"
          />
        </div>

        <!-- All Runs List -->
        <div v-else-if="catalogStore.activeTab === 'runs'" class="list-container">
          <RunListItem
            v-for="run in catalogStore.runs"
            :key="run.id"
            :run="run"
            :selected="catalogStore.selectedRunId === run.id"
            @select="catalogStore.loadRunDetail(run.id)"
          />
          <div v-if="catalogStore.runs.length === 0" class="empty-state">
            <p>No runs recorded yet.</p>
            <p class="muted">Run a flow to see its history here.</p>
          </div>
        </div>
      </div>

      <!-- Detail Panel -->
      <div class="catalog-detail">
        <!-- Run detail view -->
        <RunDetailPanel
          v-if="catalogStore.selectedRunDetail"
          :run="catalogStore.selectedRunDetail"
          @close="
            catalogStore.selectedRunId = null;
            catalogStore.selectedRunDetail = null;
          "
          @open-snapshot="openRunSnapshot($event)"
          @view-flow="navigateToFlow($event)"
        />
        <!-- Artifact detail view -->
        <ArtifactDetailPanel
          v-else-if="catalogStore.selectedArtifact"
          :artifact="catalogStore.selectedArtifact"
          :versions="selectedArtifactVersions"
          @navigate-to-flow="navigateToFlow($event)"
        />
        <!-- Table detail view -->
        <TableDetailPanel
          v-else-if="catalogStore.selectedTable"
          :table="catalogStore.selectedTable"
          :preview="catalogStore.tablePreview"
          :loading-preview="catalogStore.loadingTablePreview"
          @delete-table="handleDeleteTable($event)"
          @navigate-to-flow="navigateToFlow($event)"
        />
        <!-- Flow detail view -->
        <FlowDetailPanel
          v-else-if="catalogStore.selectedFlow"
          :flow="catalogStore.selectedFlow"
          :runs="catalogStore.flowRuns"
          :artifacts="catalogStore.flowArtifacts"
          @view-run="catalogStore.loadRunDetail($event)"
          @toggle-favorite="catalogStore.toggleFavorite($event)"
          @toggle-follow="catalogStore.toggleFollow($event)"
          @open-flow="openFlowInDesigner($event)"
          @select-table="selectTable($event)"
        />
        <!-- Stats overview -->
        <StatsPanel
          v-else
          :stats="catalogStore.stats"
          @view-run="catalogStore.loadRunDetail($event)"
          @view-flow="navigateToFlow($event)"
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
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { useCatalogStore } from "../../stores/catalog-store";
import { useFlowStore } from "../../stores/flow-store";
import { CatalogApi } from "../../api/catalog.api";
import { FlowApi } from "../../api/flow.api";
import CatalogTreeNode from "./CatalogTreeNode.vue";
import FlowListItem from "./FlowListItem.vue";
import FlowDetailPanel from "./FlowDetailPanel.vue";
import ArtifactDetailPanel from "./ArtifactDetailPanel.vue";
import TableDetailPanel from "./TableDetailPanel.vue";
import RunListItem from "./RunListItem.vue";
import RunDetailPanel from "./RunDetailPanel.vue";
import StatsPanel from "./StatsPanel.vue";
import CreateNamespaceModal from "./CreateNamespaceModal.vue";
import RegisterFlowModal from "./RegisterFlowModal.vue";
import RegisterTableModal from "./RegisterTableModal.vue";
import type { CatalogTab, GlobalArtifact, NamespaceTree } from "../../types";

const router = useRouter();

const catalogStore = useCatalogStore();
const flowStore = useFlowStore();

const tabs = computed(() => [
  {
    key: "catalog" as CatalogTab,
    label: "Catalog",
    icon: "fa-solid fa-folder-tree",
    badge: null,
  },
  {
    key: "favorites" as CatalogTab,
    label: "Favorites",
    icon: "fa-solid fa-star",
    badge: catalogStore.stats?.total_favorites ?? null,
  },
  {
    key: "following" as CatalogTab,
    label: "Following",
    icon: "fa-solid fa-bell",
    badge: null,
  },
  {
    key: "runs" as CatalogTab,
    label: "Run History",
    icon: "fa-solid fa-clock-rotate-left",
    badge: null,
  },
]);

const sidebarTitle = computed(() => {
  switch (catalogStore.activeTab) {
    case "catalog":
      return "Catalogs";
    case "favorites":
      return "Favorites";
    case "following":
      return "Following";
    case "runs":
      return "Run History";
    default:
      return "";
  }
});

// Search and filter state
const searchQuery = ref("");
const showUnavailable = ref(false);

// Modal state
const showCreateNamespace = ref(false);
const createSchemaParentId = ref<number | null>(null);
const showRegisterFlow = ref(false);
const registerFlowNamespaceId = ref<number | null>(null);
const showRegisterTable = ref(false);
const registerTableNamespaceId = ref<number | null>(null);

// Default namespace ID (loaded once on mount)
const defaultNamespaceId = ref<number | null>(null);

function selectFlow(flowId: number) {
  catalogStore.clearArtifactSelection();
  catalogStore.selectFlow(flowId);
}

function selectTable(tableId: number) {
  catalogStore.clearArtifactSelection();
  catalogStore.selectTable(tableId);
}

function selectArtifact(artifactId: number) {
  catalogStore.selectedFlowId = null;
  catalogStore.clearTableSelection();
  catalogStore.selectArtifact(artifactId);
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

async function handleDeleteTable(tableId: number) {
  if (
    !confirm("Are you sure you want to delete this table? The materialized data will be removed.")
  ) {
    return;
  }
  try {
    await CatalogApi.deleteTable(tableId);
    catalogStore.clearTableSelection();
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllTables(),
      catalogStore.loadStats(),
    ]);
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to delete table");
  }
}

async function openRunSnapshot(runId: number) {
  try {
    const flowId = await CatalogApi.openRunSnapshot(runId);
    flowStore.setFlowId(flowId);
    router.push({ name: "designer" });
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to open flow snapshot");
  }
}

async function openFlowInDesigner(flowPath: string) {
  try {
    const flowId = await FlowApi.importFlow(flowPath);
    flowStore.setFlowId(flowId);
    router.push({ name: "designer" });
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to open flow");
  }
}

function navigateToFlow(registrationId: number) {
  catalogStore.selectedRunId = null;
  catalogStore.selectedRunDetail = null;
  catalogStore.selectFlow(registrationId);
  catalogStore.setActiveTab("catalog");
}

onMounted(async () => {
  await catalogStore.initialize();
  try {
    defaultNamespaceId.value = await CatalogApi.getDefaultNamespaceId();
  } catch {
    // Not critical — leave null
  }
});
</script>

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
  color: #fff;
  font-size: 11px;
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  min-width: 18px;
  text-align: center;
  line-height: 18px;
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
.loading-state,
.empty-state {
  padding: var(--spacing-6);
  text-align: center;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.empty-state .muted {
  margin-top: var(--spacing-1);
  font-size: var(--font-size-xs);
}

/* ========== Buttons ========== */
.btn-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.btn-icon:hover {
  background: var(--color-background-hover);
  color: var(--color-primary);
  border-color: var(--color-primary);
}

.btn-primary {
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-primary);
  color: #fff;
  border: none;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: opacity var(--transition-fast);
}

.btn-primary:hover {
  opacity: 0.9;
}
.btn-primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.btn-sm {
  padding: var(--spacing-1) var(--spacing-3);
  font-size: var(--font-size-xs);
}
</style>
