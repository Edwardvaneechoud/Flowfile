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

    <!-- Registry alerts for installed nodes: blocked/yanked nodes are delisted
         from the browse grid, so this banner is the only place they surface. -->
    <div v-for="alert in store.alerts" :key="alert.node_id" class="registry-alert">
      <i class="fa-solid fa-triangle-exclamation" />
      <span class="registry-alert__text">
        <strong>{{ alert.node_id }}</strong>
        {{
          alert.kind === "blocked"
            ? "was blocked by the registry after you installed it"
            : "had its installed version withdrawn by the registry"
        }}<template v-if="alert.reason">: {{ alert.reason }}</template
        >. It stays on this machine and keeps running until you uninstall it.
      </span>
      <el-button
        type="danger"
        plain
        size="small"
        :disabled="store.isInstalling(alert.node_id)"
        @click="uninstallAlerted(alert)"
      >
        Uninstall
      </el-button>
    </div>

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
      <article
        v-for="node in store.filteredNodes"
        :key="node.id"
        class="node-card"
        :data-testid="`community-card-${node.id}`"
        @click="openDetail(node.id)"
      >
        <div class="node-card__cover">
          <img v-if="shotUrls[node.id]" :src="shotUrls[node.id]" alt="" class="node-card__shot" />
          <div v-else class="node-card__cover-empty">
            <img :src="iconUrls[node.id] || defaultIcon" alt="" class="node-card__cover-glyph" />
          </div>
          <span
            v-if="node.environment === 'kernel'"
            class="chip node-card__kernel"
            title="Runs in an isolated Docker kernel"
            >Kernel</span
          >
        </div>
        <div class="node-card__body">
          <div class="node-card__title">
            <img :src="iconUrls[node.id] || defaultIcon" alt="" class="node-card__icon" />
            <span class="node-card__name">{{ node.node_name }}</span>
          </div>
          <span v-if="node.category" class="node-card__category">{{ node.category }}</span>
          <p class="node-card__desc">{{ node.description || "No description" }}</p>
          <div class="node-card__meta">
            <span v-if="node.author.github">@{{ node.author.github }}</span>
            <span v-if="node.author.github" class="dot">·</span>
            <span>v{{ node.version }}</span>
            <template v-if="node.popularity">
              <span class="dot">·</span>
              <button
                v-if="node.popularity.discussion_url"
                class="node-card__upvotes node-card__upvotes--link"
                title="Upvote on GitHub"
                @click.stop="openDiscussion(node)"
              >
                <i class="fa-solid fa-arrow-up" /> {{ node.popularity.upvotes }}
              </button>
              <span v-else class="node-card__upvotes" title="upvotes"
                ><i class="fa-solid fa-arrow-up" /> {{ node.popularity.upvotes }}</span
              >
            </template>
          </div>
        </div>
        <div class="node-card__actions" @click.stop>
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
      </article>
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

    <NodeKernelSetupDialog
      v-model="setupOpen"
      :node-name="setupNode?.node_name ?? ''"
      :dependencies="setupNode?.dependencies ?? []"
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
import {
  mediaFileName,
  type CommunityNodeSummary,
  type InstalledAlert,
} from "../../api/communityNodes";
import { describeError, useCommunityNodesStore } from "../../stores/community-nodes-store";
import { fetchMediaObjectUrl } from "../../composables/useCommunityMedia";
import { getDefaultIconUrl } from "../../features/designer/utils";
import { KernelApi } from "../../api/kernel.api";
import NodeKernelSetupDialog from "../../components/kernel/NodeKernelSetupDialog.vue";
import CommunityConsentDialog from "./CommunityConsentDialog.vue";
import CommunityNodeDetailModal from "./CommunityNodeDetailModal.vue";
import { desktop } from "../../../lib/desktop";

const store = useCommunityNodesStore();
const defaultIcon = getDefaultIconUrl();

const iconUrls = reactive<Record<string, string>>({});
const shotUrls = reactive<Record<string, string>>({});

const consentOpen = ref(false);
const consentNode = ref<CommunityNodeSummary | null>(null);
const detailOpen = ref(false);
const detailNodeId = ref<string | null>(null);
const setupOpen = ref(false);
const setupNode = ref<CommunityNodeSummary | null>(null);

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

function openDiscussion(node: CommunityNodeSummary) {
  const url = node.popularity?.discussion_url;
  if (url) void desktop.openExternal(url);
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
    } else if (node.environment === "kernel" && node.dependencies?.length) {
      await offerKernelSetup(node);
    } else {
      ElMessage.success(`Installed ${node.node_name}. It's now available in the flow editor.`);
    }
  } catch (e) {
    ElMessage.error(describeError(e));
  }
}

// Best-effort: kernel matching being unavailable must never make the install look failed.
async function offerKernelSetup(node: CommunityNodeSummary) {
  let fullKernelName: string | null = null;
  try {
    const match = await KernelApi.matchKernels(node.dependencies, node.node_name);
    fullKernelName = match.matches.find((m) => m.level === "full")?.kernel_name ?? null;
  } catch {
    // fall through to the setup dialog, which handles matching being down itself
  }
  if (fullKernelName) {
    ElMessage.success(
      `Installed ${node.node_name}. Kernel "${fullKernelName}" already has its packages.`,
    );
    return;
  }
  ElMessage.success(`Installed ${node.node_name}. It's now available in the flow editor.`);
  setupNode.value = node;
  setupOpen.value = true;
}

function onDetailInstall(node: CommunityNodeSummary) {
  openConsent(node);
}

async function onDetailUninstall(nodeId: string) {
  const node = store.nodes.find((n) => n.id === nodeId);
  if (node) await confirmUninstall(node);
}

async function uninstallAlerted(alert: InstalledAlert) {
  try {
    await ElMessageBox.confirm(
      `Remove "${alert.node_id}"? Its file is deleted from disk; flows that use it will show a missing-node error.`,
      alert.kind === "blocked" ? "Uninstall blocked node" : "Uninstall withdrawn node",
      { confirmButtonText: "Uninstall", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  try {
    await store.uninstall(alert.node_id);
    ElMessage.success(`Uninstalled ${alert.node_id}`);
  } catch (e) {
    ElMessage.error(describeError(e));
  }
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
.header-actions .el-button i {
  margin-right: 6px;
}
.stale-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
}
.registry-alert {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 12px;
  margin-bottom: 10px;
  border: 1px solid var(--el-color-danger-light-5);
  background: var(--el-color-danger-light-9);
  border-radius: 6px;
  font-size: 12.5px;
}
.registry-alert i {
  color: var(--el-color-danger);
}
.registry-alert__text {
  flex: 1;
  color: var(--color-text-secondary);
}
.panel-hint {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 4px 0 12px;
}
.filter-bar {
  display: flex;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-4);
  flex-wrap: wrap;
  align-items: center;
}
.search-input {
  flex: 1 1 240px;
  max-width: 320px;
}
.category-select,
.sort-select {
  flex: 0 0 auto;
  width: 170px;
}

/* ── Card grid ─────────────────────────────────────────────────────────── */
.nodes-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 320px));
  justify-content: start;
  gap: var(--spacing-4);
}
.node-card {
  display: flex;
  flex-direction: column;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  overflow: hidden;
  cursor: pointer;
  transition:
    border-color var(--transition-fast),
    box-shadow var(--transition-fast),
    transform var(--transition-fast);
}
.node-card:hover {
  border-color: var(--color-accent);
  box-shadow: var(--shadow-sm);
  transform: translateY(-1px);
}

/* Cover: screenshot when present, else a soft icon poster so every card keeps
   the same silhouette and the grid lines up. */
.node-card__cover {
  position: relative;
  height: 116px;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-light);
}
.node-card__shot {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}
.node-card__cover-empty {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(
    135deg,
    var(--color-accent-subtle) 0%,
    var(--color-background-secondary) 100%
  );
}
.node-card__cover-glyph {
  width: 40px;
  height: 40px;
  object-fit: contain;
  opacity: 0.4;
}
.node-card__kernel {
  position: absolute;
  top: var(--spacing-2);
  right: var(--spacing-2);
  background: var(--color-warning-light);
  color: var(--color-warning);
}

/* Body fills the remaining height so the meta line pins to the bottom and
   aligns across cards regardless of description length. */
.node-card__body {
  display: flex;
  flex-direction: column;
  flex: 1;
  padding: var(--spacing-3) var(--spacing-4);
}
.node-card__title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-width: 0;
}
.node-card__icon {
  width: 20px;
  height: 20px;
  object-fit: contain;
  flex-shrink: 0;
}
.node-card__name {
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-base);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.node-card__category {
  align-self: flex-start;
  margin-top: var(--spacing-2);
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 2px var(--spacing-2);
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-full);
}
.node-card__desc {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.45;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.node-card__meta {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-1);
  margin-top: auto;
  padding-top: var(--spacing-3);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}
.dot {
  opacity: 0.5;
}
.node-card__upvotes i {
  color: var(--color-accent);
}
.node-card__upvotes--link {
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  font: inherit;
  color: inherit;
}
.node-card__upvotes--link:hover {
  text-decoration: underline;
}
.chip {
  display: inline-flex;
  align-items: center;
  font-size: var(--font-size-2xs);
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
}
.node-card__actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-4);
  border-top: 1px solid var(--color-border-light);
}
.btn-disabled {
  opacity: 0.55;
  cursor: not-allowed;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  color: var(--color-text-muted);
}
</style>
