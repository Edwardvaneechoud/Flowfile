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
          <button
            v-if="catalogStore.activeTab === 'catalog'"
            class="btn-icon"
            title="New catalog"
            @click="showCreateNamespace = true"
          >
            <i class="fa-solid fa-plus"></i>
          </button>
        </div>

        <!-- Catalog Tree -->
        <div v-if="catalogStore.activeTab === 'catalog'" class="tree-container">
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
              @select-flow="catalogStore.selectFlow($event)"
              @toggle-favorite="catalogStore.toggleFavorite($event)"
              @toggle-follow="catalogStore.toggleFollow($event)"
              @register-flow="openRegisterFlow($event)"
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
            <p class="muted">Follow flows to get notified about new runs.</p>
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
          @close="catalogStore.selectedRunId = null; catalogStore.selectedRunDetail = null"
          @open-snapshot="openRunSnapshot($event)"
          @view-flow="navigateToFlow($event)"
        />
        <!-- Flow detail view -->
        <FlowDetailPanel
          v-else-if="catalogStore.selectedFlow"
          :flow="catalogStore.selectedFlow"
          :runs="catalogStore.flowRuns"
          @view-run="catalogStore.loadRunDetail($event)"
          @toggle-favorite="catalogStore.toggleFavorite($event)"
          @toggle-follow="catalogStore.toggleFollow($event)"
          @open-flow="openFlowInDesigner($event)"
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

    <!-- Create Namespace Modal -->
    <div v-if="showCreateNamespace" class="modal-overlay" @click.self="showCreateNamespace = false">
      <div class="modal-card">
        <h3>{{ createSchemaParentId ? 'Create Schema' : 'Create Catalog' }}</h3>
        <input
          v-model="newNamespaceName"
          class="input-field"
          :placeholder="createSchemaParentId ? 'Schema name' : 'Catalog name'"
          @keyup.enter="createNamespace"
        />
        <input
          v-model="newNamespaceDesc"
          class="input-field"
          placeholder="Description (optional)"
        />
        <div class="modal-actions">
          <button class="btn-secondary" @click="showCreateNamespace = false; createSchemaParentId = null">
            Cancel
          </button>
          <button class="btn-primary" :disabled="!newNamespaceName.trim()" @click="createNamespace">
            Create
          </button>
        </div>
      </div>
    </div>

    <!-- Register Flow Modal -->
    <div v-if="showRegisterFlow" class="modal-overlay" @click.self="showRegisterFlow = false">
      <div class="modal-card modal-card-lg">
        <h3>Register Flow</h3>
        <input v-model="newFlowName" class="input-field" placeholder="Flow name" />
        <input v-model="newFlowDesc" class="input-field" placeholder="Description (optional)" />
        <div class="file-browser-section">
          <label class="field-label">Flow file</label>
          <div v-if="newFlowPath" class="selected-file-badge">
            <i class="fa-solid fa-file"></i>
            <span>{{ newFlowPath }}</span>
            <button class="clear-file-btn" @click="newFlowPath = ''" title="Clear">
              <i class="fa-solid fa-xmark"></i>
            </button>
          </div>
          <div class="file-browser-container">
            <FileBrowser
              :allowed-file-types="['yaml', 'yml', 'flowfile']"
              mode="open"
              context="flows"
              :is-visible="showRegisterFlow"
              @file-selected="handleFlowFileSelected"
            />
          </div>
        </div>
        <div class="modal-actions">
          <button class="btn-secondary" @click="showRegisterFlow = false">Cancel</button>
          <button
            class="btn-primary"
            :disabled="!newFlowName.trim() || !newFlowPath.trim()"
            @click="registerFlow"
          >
            Register
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { useCatalogStore } from "../../stores/catalog-store";
import { CatalogApi } from "../../api/catalog.api";
import { FlowApi } from "../../api/flow.api";
import CatalogTreeNode from "./CatalogTreeNode.vue";
import FlowListItem from "./FlowListItem.vue";
import FlowDetailPanel from "./FlowDetailPanel.vue";
import RunListItem from "./RunListItem.vue";
import RunDetailPanel from "./RunDetailPanel.vue";
import StatsPanel from "./StatsPanel.vue";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";
import type { CatalogTab } from "../../types";

const router = useRouter();

const catalogStore = useCatalogStore();

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
    case "catalog": return "Catalogs";
    case "favorites": return "Favorites";
    case "following": return "Following";
    case "runs": return "Run History";
    default: return "";
  }
});

// Create namespace state
const showCreateNamespace = ref(false);
const createSchemaParentId = ref<number | null>(null);
const newNamespaceName = ref("");
const newNamespaceDesc = ref("");

// Register flow state
const showRegisterFlow = ref(false);
const registerFlowNamespaceId = ref<number | null>(null);
const newFlowName = ref("");
const newFlowPath = ref("");
const newFlowDesc = ref("");

function openCreateSchema(parentId: number) {
  createSchemaParentId.value = parentId;
  newNamespaceName.value = "";
  newNamespaceDesc.value = "";
  showCreateNamespace.value = true;
}

// Default namespace ID (loaded once on mount)
const defaultNamespaceId = ref<number | null>(null);

function openRegisterFlow(namespaceId: number) {
  registerFlowNamespaceId.value = namespaceId;
  newFlowName.value = "";
  newFlowPath.value = "";
  newFlowDesc.value = "";
  showRegisterFlow.value = true;
}

function handleFlowFileSelected(fileInfo: { name: string; path: string }) {
  newFlowPath.value = fileInfo.path;
  if (!newFlowName.value.trim()) {
    // Auto-fill name from filename (without extension)
    const baseName = fileInfo.name.replace(/\.(yaml|yml|flowfile)$/i, "");
    newFlowName.value = baseName;
  }
}

async function openRunSnapshot(runId: number) {
  try {
    const flowId = await CatalogApi.openRunSnapshot(runId);
    router.push({ name: "designer" });
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to open flow snapshot");
  }
}

async function openFlowInDesigner(flowPath: string) {
  try {
    await FlowApi.importFlow(flowPath);
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

async function createNamespace() {
  if (!newNamespaceName.value.trim()) return;
  try {
    await CatalogApi.createNamespace({
      name: newNamespaceName.value.trim(),
      parent_id: createSchemaParentId.value,
      description: newNamespaceDesc.value.trim() || null,
    });
    showCreateNamespace.value = false;
    createSchemaParentId.value = null;
    newNamespaceName.value = "";
    newNamespaceDesc.value = "";
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to create namespace");
  }
}

async function registerFlow() {
  if (!newFlowName.value.trim() || !newFlowPath.value.trim()) return;
  try {
    const nsId = registerFlowNamespaceId.value ?? defaultNamespaceId.value;
    await CatalogApi.registerFlow({
      name: newFlowName.value.trim(),
      flow_path: newFlowPath.value.trim(),
      description: newFlowDesc.value.trim() || null,
      namespace_id: nsId,
    });
    showRegisterFlow.value = false;
    newFlowName.value = "";
    newFlowPath.value = "";
    newFlowDesc.value = "";
    await Promise.all([catalogStore.loadTree(), catalogStore.loadAllFlows(), catalogStore.loadStats()]);
  } catch (e: any) {
    alert(e?.response?.data?.detail ?? "Failed to register flow");
  }
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

.catalog-detail {
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-4) var(--spacing-6);
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

.btn-primary:hover { opacity: 0.9; }
.btn-primary:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-sm { padding: var(--spacing-1) var(--spacing-3); font-size: var(--font-size-xs); }

.btn-secondary {
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.btn-secondary:hover { background: var(--color-background-hover); }

/* ========== Modal ========== */
.modal-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-card {
  background: var(--color-background-primary);
  border-radius: var(--border-radius-lg);
  padding: var(--spacing-6);
  width: 400px;
  max-width: 90vw;
  box-shadow: var(--shadow-lg);
}

.modal-card-lg {
  width: 700px;
  max-height: 85vh;
  overflow-y: auto;
}

.modal-card h3 {
  margin: 0 0 var(--spacing-4) 0;
  font-size: var(--font-size-lg);
}

.input-field {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  margin-bottom: var(--spacing-3);
  box-sizing: border-box;
}

.input-field:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  margin-top: var(--spacing-2);
}

/* ========== File Browser in Register Modal ========== */
.file-browser-section {
  margin-bottom: var(--spacing-3);
}

.field-label {
  display: block;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: var(--spacing-2);
}

.selected-file-badge {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: rgba(59, 130, 246, 0.08);
  border: 1px solid rgba(59, 130, 246, 0.25);
  border-radius: var(--border-radius-md);
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-primary);
}

.selected-file-badge span {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: monospace;
  font-size: var(--font-size-xs);
}

.clear-file-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: 10px;
}

.clear-file-btn:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}

.file-browser-container {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  height: 350px;
  overflow: hidden;
}
</style>
