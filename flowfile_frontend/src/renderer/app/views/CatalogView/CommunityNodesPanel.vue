<template>
  <div class="community-panel">
    <div class="panel-header">
      <h2>Community Nodes</h2>
      <div class="header-actions">
        <span v-if="store.stale" class="status-badge status-badge--warning stale-badge">
          <i class="fa-solid fa-clock-rotate-left"></i> Offline · cached
        </span>
        <el-button size="small" :loading="store.loading" @click="refresh">
          <i class="fa-solid fa-arrows-rotate" /> Refresh
        </el-button>
      </div>
    </div>
    <p class="panel-hint">
      Install nodes shared by the community. Every node is reviewed and scanned, but runs as code on
      your machine — read what it can do before you install.
    </p>

    <!-- Filters -->
    <div class="filter-bar">
      <el-input
        v-model="store.filters.search"
        placeholder="Search nodes"
        clearable
        size="small"
        class="search-input"
      >
        <template #prefix><i class="fa-solid fa-magnifying-glass" /></template>
      </el-input>
      <el-select
        v-model="store.filters.category"
        placeholder="All categories"
        clearable
        size="small"
        class="category-select"
      >
        <el-option v-for="cat in store.categories" :key="cat" :label="cat" :value="cat" />
      </el-select>
      <el-select
        v-if="store.hasPopularity"
        v-model="store.filters.sort"
        size="small"
        class="sort-select"
      >
        <el-option label="Most popular" value="popularity" />
        <el-option label="Name" value="name" />
        <el-option label="Recently updated" value="updated" />
      </el-select>
    </div>

    <!-- States -->
    <EmptyState
      v-if="store.loading && store.nodes.length === 0"
      icon="fa-solid fa-spinner fa-spin"
      title="Loading community nodes…"
    />
    <EmptyState
      v-else-if="store.error && store.nodes.length === 0"
      icon="fa-solid fa-plug-circle-xmark"
      title="Community registry unavailable"
      :description="errorMessage"
    >
      <template #actions>
        <button class="btn btn-primary btn-sm" @click="refresh">
          <i class="fa-solid fa-arrows-rotate" /> Retry
        </button>
      </template>
    </EmptyState>
    <EmptyState
      v-else-if="store.filteredNodes.length === 0"
      icon="fa-solid fa-store-slash"
      title="No nodes match your filters"
      description="Try a different search or category."
    >
      <template #actions>
        <button class="btn btn-secondary btn-sm" @click="store.clearFilters()">
          Clear filters
        </button>
      </template>
    </EmptyState>

    <!-- Card grid -->
    <div v-else class="nodes-grid">
      <div
        v-for="node in store.filteredNodes"
        :key="node.id"
        class="node-card"
        :data-testid="`community-card-${node.id}`"
        @click="openDetail(node.id)"
      >
        <div v-if="shotUrls[node.id]" class="card-shot">
          <img :src="shotUrls[node.id]" alt="" />
        </div>
        <div class="node-card-header">
          <img :src="iconUrls[node.id] || defaultIcon" alt="" class="node-icon" />
          <span class="node-name">{{ node.node_name }}</span>
          <span
            v-if="node.environment === 'kernel'"
            class="chip chip-kernel"
            title="Runs in an isolated Docker kernel"
            >Kernel</span
          >
        </div>
        <div class="node-card-body">
          <span v-if="node.category" class="node-category">{{ node.category }}</span>
          <p class="node-description">{{ node.description || "No description" }}</p>
          <div class="node-footer-line">
            <span v-if="node.author.github">@{{ node.author.github }}</span>
            <span class="dot">·</span>
            <span>v{{ node.version }}</span>
            <template v-if="node.popularity">
              <span class="dot">·</span>
              <span class="thumbs"
                ><i class="fa-solid fa-thumbs-up" /> {{ node.popularity.thumbs_up }}</span
              >
            </template>
          </div>
        </div>
        <div class="node-card-actions" @click.stop>
          <!-- not installed -->
          <button
            v-if="node.install_state === 'not_installed'"
            class="btn btn-primary btn-sm"
            :disabled="store.isInstalling(node.id)"
            @click="openConsent(node)"
          >
            <i class="fa-solid fa-download" /> Install
          </button>

          <!-- update available -->
          <template v-else-if="node.install_state === 'update_available'">
            <span class="status-badge status-badge--info">Update available</span>
            <button
              class="btn btn-primary btn-sm"
              :disabled="store.isInstalling(node.id)"
              @click="openConsent(node)"
            >
              Update
            </button>
          </template>

          <!-- installed -->
          <template v-else-if="node.install_state === 'installed'">
            <span class="status-badge status-badge--success">Installed</span>
            <button
              class="btn btn-ghost btn-sm"
              :disabled="store.isInstalling(node.id)"
              @click="confirmUninstall(node)"
            >
              Uninstall
            </button>
          </template>

          <!-- modified locally -->
          <template v-else-if="node.install_state === 'modified_locally'">
            <el-tooltip
              content="This node's file was edited after install; it no longer matches the registry version."
              placement="top"
            >
              <span class="status-badge status-badge--warning">Modified locally</span>
            </el-tooltip>
            <button
              class="btn btn-ghost btn-sm"
              :disabled="store.isInstalling(node.id)"
              @click="confirmUninstall(node)"
            >
              Uninstall
            </button>
          </template>

          <!-- incompatible -->
          <el-tooltip
            v-else-if="node.install_state === 'incompatible'"
            :content="`Requires Flowfile ${node.min_flowfile_version} or newer.`"
            placement="top"
          >
            <span class="btn btn-sm btn-disabled" aria-disabled="true">
              <i class="fa-solid fa-ban" /> Incompatible
            </span>
          </el-tooltip>
        </div>
      </div>
    </div>

    <CommunityConsentDialog
      v-model="consentOpen"
      :node="consentNode"
      :installing="consentNode ? store.isInstalling(consentNode.id) : false"
      @confirm="onConsentConfirm"
    />

    <CommunityNodeDetailModal
      v-model="detailOpen"
      :node-id="detailNodeId"
      :busy="detailNodeId ? store.isInstalling(detailNodeId) : false"
      @install="onDetailInstall"
      @uninstall="onDetailUninstall"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from "vue";
import {
  ElButton,
  ElInput,
  ElMessage,
  ElMessageBox,
  ElOption,
  ElSelect,
  ElTooltip,
} from "element-plus";
import { EmptyState } from "../../components/common";
import { mediaFileName, type CommunityNodeSummary } from "../../api/communityNodes";
import { describeError, useCommunityNodesStore } from "../../stores/community-nodes-store";
import { fetchMediaObjectUrl } from "../../composables/useCommunityMedia";
import { getDefaultIconUrl } from "../../features/designer/utils";
import CommunityConsentDialog from "./CommunityConsentDialog.vue";
import CommunityNodeDetailModal from "./CommunityNodeDetailModal.vue";

const store = useCommunityNodesStore();
const defaultIcon = getDefaultIconUrl();

const iconUrls = reactive<Record<string, string>>({});
const shotUrls = reactive<Record<string, string>>({});

const consentOpen = ref(false);
const consentNode = ref<CommunityNodeSummary | null>(null);
const detailOpen = ref(false);
const detailNodeId = ref<string | null>(null);

const errorMessage = computed(() => (store.error ? describeError(store.error) : ""));

async function loadMedia() {
  const jobs: Promise<void>[] = [];
  for (const node of store.nodes) {
    if (node.artifacts.icon && !iconUrls[node.id]) {
      jobs.push(
        fetchMediaObjectUrl(node.id, mediaFileName(node.artifacts.icon.path)).then((url) => {
          if (url) iconUrls[node.id] = url;
        }),
      );
    }
    const firstShot = node.artifacts.screenshots[0];
    if (firstShot && !shotUrls[node.id]) {
      jobs.push(
        fetchMediaObjectUrl(node.id, mediaFileName(firstShot.path)).then((url) => {
          if (url) shotUrls[node.id] = url;
        }),
      );
    }
  }
  await Promise.all(jobs);
}

async function refresh() {
  await store.loadIndex(true);
  await loadMedia();
}

function openDetail(nodeId: string) {
  detailNodeId.value = nodeId;
  detailOpen.value = true;
}

function openConsent(node: CommunityNodeSummary) {
  consentNode.value = node;
  consentOpen.value = true;
}

async function onConsentConfirm(acknowledgedCapabilities: string[]) {
  const node = consentNode.value;
  if (!node) return;
  try {
    const res = await store.install(node, acknowledgedCapabilities);
    consentOpen.value = false;
    detailOpen.value = false;
    await loadMedia();
    if (res.load_error) {
      ElMessage.warning(`Installed ${node.node_name}, but it failed to load: ${res.load_error}`);
    } else {
      ElMessage.success(`Installed ${node.node_name}. It's now available in the flow editor.`);
    }
  } catch (e) {
    ElMessage.error(describeError(e));
  }
}

function onDetailInstall(node: CommunityNodeSummary) {
  openConsent(node);
}

async function onDetailUninstall(nodeId: string) {
  const node = store.nodes.find((n) => n.id === nodeId);
  if (node) await confirmUninstall(node);
}

async function confirmUninstall(node: CommunityNodeSummary) {
  try {
    await ElMessageBox.confirm(
      `Remove "${node.node_name}"? Its file is deleted from disk; flows that use it will show a missing-node error.`,
      "Uninstall community node",
      { confirmButtonText: "Uninstall", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  try {
    await store.uninstall(node.id);
    detailOpen.value = false;
    ElMessage.success(`Uninstalled ${node.node_name}`);
  } catch (e) {
    ElMessage.error(describeError(e));
  }
}

watch(() => store.nodes, loadMedia);

onMounted(async () => {
  if (!store.loaded) await store.loadIndex(false);
  await loadMedia();
});
</script>

<style scoped>
.community-panel {
  padding: 16px;
}
.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.panel-header h2 {
  font-size: 18px;
  margin: 0;
}
.header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.stale-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
}
.panel-hint {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 4px 0 12px;
}
.filter-bar {
  display: flex;
  gap: 8px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}
.search-input {
  width: 260px;
}
.category-select {
  width: 180px;
}
.sort-select {
  width: 170px;
}
.nodes-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 1rem;
}
.node-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  overflow: hidden;
  display: flex;
  flex-direction: column;
  cursor: pointer;
  transition:
    border-color var(--transition-fast),
    box-shadow var(--transition-fast);
}
.node-card:hover {
  border-color: var(--color-accent);
  box-shadow: var(--shadow-sm);
}
.card-shot {
  height: 120px;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}
.card-shot img {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}
.node-card-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.75rem 1rem;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}
.node-icon {
  width: 22px;
  height: 22px;
  object-fit: contain;
}
.node-name {
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-base);
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.chip {
  display: inline-flex;
  align-items: center;
  font-size: var(--font-size-2xs);
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
}
.chip-kernel {
  background: var(--color-warning-light);
  color: var(--color-warning);
}
.node-card-body {
  padding: 0.75rem 1rem;
  flex: 1;
}
.node-category {
  display: inline-block;
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 0.125rem 0.5rem;
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-full);
  margin-bottom: 0.5rem;
}
.node-description {
  margin: 0 0 0.5rem;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.4;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.node-footer-line {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  display: flex;
  align-items: center;
  gap: 0.25rem;
  flex-wrap: wrap;
}
.dot {
  opacity: 0.5;
}
.thumbs i {
  color: var(--color-accent);
}
.node-card-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  border-top: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}
.btn-disabled {
  opacity: 0.55;
  cursor: not-allowed;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  color: var(--color-text-muted);
}
</style>
