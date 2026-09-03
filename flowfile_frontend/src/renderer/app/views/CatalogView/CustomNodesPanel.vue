<template>
  <div class="custom-nodes-panel">
    <div class="panel-top">
      <div class="panel-header">
        <h2>
          Custom Nodes
          <span v-if="countLabel" class="panel-count">{{ countLabel }}</span>
        </h2>
        <div class="header-actions">
          <el-button size="small" type="primary" @click="openDesigner()">
            <i class="fa-solid fa-plus" /> New node
          </el-button>
          <el-button size="small" @click="openManageFolders">
            <i class="fa-solid fa-folder-tree" /> Manage folders
          </el-button>
          <el-button size="small" :loading="rescanning" @click="rescan">
            <i class="fa-solid fa-arrows-rotate" /> Rescan
          </el-button>
        </div>
      </div>
      <p class="panel-hint">
        Custom nodes discovered in the default directory and any mounted folders. Broken files stay
        listed with their error so nothing silently disappears.
      </p>

      <el-input
        v-if="nodes.length"
        v-model="search"
        placeholder="Search nodes"
        clearable
        size="small"
        class="node-search"
        data-testid="catalog-node-search"
      >
        <template #prefix><i class="fa-solid fa-magnifying-glass" /></template>
      </el-input>
    </div>

    <EmptyState
      v-if="!loading && nodes.length === 0"
      icon="fa-solid fa-cube"
      title="No custom nodes yet"
      description="Build one in the Node Designer, or mount a folder that already contains custom-node .py files."
    >
      <template #actions>
        <el-button size="small" type="primary" @click="openDesigner()">
          <i class="fa-solid fa-plus" /> Open Node Designer
        </el-button>
        <el-button size="small" @click="openManageFolders">
          <i class="fa-solid fa-folder-plus" /> Mount a folder
        </el-button>
      </template>
    </EmptyState>

    <EmptyState
      v-else-if="!loading && visibleNodes.length === 0"
      icon="fa-solid fa-magnifying-glass"
      title="No nodes match your search"
      :description="`Nothing found for “${search.trim()}”.`"
    >
      <template #actions>
        <el-button size="small" @click="search = ''">Clear search</el-button>
      </template>
    </EmptyState>

    <div v-else v-loading="loading" class="nodes-list">
      <section v-for="group in groups" :key="group.key">
        <div class="node-group__header">
          <span>{{ group.label }}</span>
          <span>{{ group.nodes.length }}</span>
        </div>
        <CustomNodeRow
          v-for="node in group.nodes"
          :key="node.file_name"
          :node="node"
          :community="communityByFile[node.file_name] ?? null"
          :readiness="readinessFor(node)"
          @open="openDesigner(node.file_name)"
        />
      </section>
    </div>

    <!-- Manage folders dialog -->
    <el-dialog v-model="foldersOpen" title="Custom-node folders" width="620px" align-center>
      <p class="dialog-hint">
        Mounted folders are scanned alongside the default directory. Unmounting never deletes files.
      </p>

      <el-alert
        v-if="mountError"
        :title="mountError"
        type="error"
        show-icon
        :closable="true"
        class="mount-error"
        @close="mountError = null"
      />

      <el-table :data="mounts" size="small" empty-text="No mounted folders" class="mounts-table">
        <el-table-column label="Path" min-width="260">
          <template #default="{ row }">
            <code class="mount-path">{{ row.path }}</code>
            <span v-if="!row.exists" class="chip chip-missing">missing</span>
          </template>
        </el-table-column>
        <el-table-column label="Nodes" width="80" align="center">
          <template #default="{ row }">{{ row.node_count }}</template>
        </el-table-column>
        <el-table-column label="Errors" width="80" align="center">
          <template #default="{ row }">
            <span :class="{ 'error-count': row.error_count > 0 }">{{ row.error_count }}</span>
          </template>
        </el-table-column>
        <el-table-column width="90" align="right">
          <template #default="{ row }">
            <el-button
              size="small"
              text
              type="danger"
              :loading="removingPath === row.path"
              @click="removeMount(row.path)"
            >
              Unmount
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="add-mount-row">
        <el-input
          v-model="newMountPath"
          placeholder="Absolute path to a folder of custom-node .py files"
          size="small"
          clearable
          @keyup.enter="addMount"
        />
        <el-button size="small" @click="browseOpen = true">
          <i class="fa-solid fa-folder-open" /> Browse…
        </el-button>
        <el-button size="small" type="primary" :loading="adding" @click="addMount">
          <i class="fa-solid fa-plus" /> Mount
        </el-button>
      </div>
      <p class="dialog-note">
        Pick a folder with <strong>Browse…</strong>, or paste an absolute path to a folder of
        custom-node <code>.py</code> files.
      </p>

      <template #footer>
        <el-button @click="foldersOpen = false">Close</el-button>
      </template>
    </el-dialog>

    <!-- Directory picker (browses the machine running core; works in web and desktop) -->
    <el-dialog
      v-model="browseOpen"
      title="Select a custom-node folder"
      width="70%"
      :close-on-click-modal="false"
      append-to-body
    >
      <p class="dialog-hint">
        Open the folder you want scanned, then choose <strong>Select This Directory</strong>.
      </p>
      <FileBrowser
        v-if="browseOpen"
        mode="open"
        context="flows"
        :is-visible="browseOpen"
        allow-directory-selection
        @directory-selected="onPickDirectory"
      />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import {
  addCustomNodeMount,
  listCatalogCustomNodes,
  listCustomNodeMounts,
  listCustomNodes,
  removeCustomNodeMount,
  rescanCustomNodes,
  type CatalogCustomNode,
  type CustomNodeInfoResponse,
  type MountInfo,
} from "../../api/nodeDesigner";
import {
  fetchInstalled,
  type InstalledCommunityNode,
  type InstallReceipt,
} from "../../api/communityNodes";
import { filterNodes, groupByCategory } from "../../pages/nodeDesigner/customNodeFilter";
import { readinessKey, useKernelReadiness } from "../../composables/useKernelReadiness";
import type { KernelMatchBatchSummary } from "../../types";
import { EmptyState } from "../../components/common";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";
import CustomNodeRow from "./CustomNodeRow.vue";

const router = useRouter();

const nodes = ref<CatalogCustomNode[]>([]);
const loading = ref(false);
const rescanning = ref(false);
const search = ref("");
// Keyed by receipt.file_name: the receipt is the only marker that a local node
// came from the registry, and file_name is what both surfaces carry on disk.
const communityByFile = ref<Record<string, InstallReceipt>>({});
// /custom-node-mounts/nodes doesn't carry dependencies; join them in from
// /list-custom-nodes by node_key (both routes enumerate the same registry).
const depsByNodeKey = ref<Record<string, string[]>>({});
const { readiness, unavailable, ensureReadiness } = useKernelReadiness();

const visibleNodes = computed(() => filterNodes(nodes.value, search.value));
const groups = computed(() => groupByCategory(visibleNodes.value));

const countLabel = computed(() => {
  const total = nodes.value.length;
  if (!total) return "";
  const shown = visibleNodes.value.length;
  return shown === total ? `${total}` : `${shown} of ${total}`;
});

const foldersOpen = ref(false);
const browseOpen = ref(false);
const mounts = ref<MountInfo[]>([]);
const newMountPath = ref("");
const adding = ref(false);
const removingPath = ref<string | null>(null);
const mountError = ref<string | null>(null);

async function loadNodes() {
  loading.value = true;
  try {
    // /community_nodes/installed is JWT-only and reads a local sidecar, so it
    // works offline; a failure just means no Community chips.
    const [rows, installed, infos] = await Promise.all([
      listCatalogCustomNodes(),
      fetchInstalled().catch(() => [] as InstalledCommunityNode[]),
      listCustomNodes().catch(() => [] as CustomNodeInfoResponse[]),
    ]);
    nodes.value = rows;
    communityByFile.value = Object.fromEntries(
      installed.map((entry) => [entry.receipt.file_name, entry.receipt]),
    );
    depsByNodeKey.value = Object.fromEntries(
      infos
        .filter(
          (info) => info.node_key && info.environment === "kernel" && info.dependencies?.length,
        )
        .map((info) => [info.node_key as string, info.dependencies as string[]]),
    );
    void fetchReadiness();
  } catch {
    ElMessage.error("Failed to load custom nodes");
  } finally {
    loading.value = false;
  }
}

// One batch call covering every visible kernel node's dependency set.
async function fetchReadiness() {
  const items: Record<string, string[]> = {};
  for (const deps of Object.values(depsByNodeKey.value)) {
    items[readinessKey(deps)] = deps;
  }
  if (Object.keys(items).length) await ensureReadiness(items);
}

function readinessFor(node: CatalogCustomNode): KernelMatchBatchSummary | null {
  if (unavailable.value || node.environment !== "kernel") return null;
  const deps = depsByNodeKey.value[node.node_key];
  if (!deps?.length) return null;
  return readiness.value[readinessKey(deps)] ?? null;
}

async function rescan() {
  rescanning.value = true;
  try {
    const result = await rescanCustomNodes();
    await loadNodes();
    if (result.broken.length > 0) {
      ElMessage.warning(`Rescanned: ${result.loaded} loaded, ${result.broken.length} with errors`);
    } else {
      ElMessage.success(`Rescanned: ${result.loaded} custom node(s)`);
    }
  } catch {
    ElMessage.error("Rescan failed");
  } finally {
    rescanning.value = false;
  }
}

function openDesigner(fileName?: string) {
  // NodeDesigner.vue reads `openFile` and loads that node for editing.
  router.push({
    name: "nodeDesigner",
    query: fileName ? { openFile: fileName } : {},
  });
}

async function openManageFolders() {
  foldersOpen.value = true;
  mountError.value = null;
  await loadMounts();
}

async function loadMounts() {
  try {
    mounts.value = await listCustomNodeMounts();
  } catch {
    mountError.value = "Failed to load mounted folders";
  }
}

async function addMount() {
  const path = newMountPath.value.trim();
  if (!path) return;
  adding.value = true;
  mountError.value = null;
  try {
    const result = await addCustomNodeMount(path);
    mounts.value = result.mounts;
    newMountPath.value = "";
    await loadNodes();
    ElMessage.success("Folder mounted");
  } catch (e: unknown) {
    mountError.value = extractDetail(e) ?? "Failed to mount folder";
  } finally {
    adding.value = false;
  }
}

// Picked from the directory browser → mount it straight away; the path stays in
// the field so a rejected folder can be retried or edited.
async function onPickDirectory(dirPath: string) {
  browseOpen.value = false;
  newMountPath.value = dirPath;
  await addMount();
}

async function removeMount(path: string) {
  removingPath.value = path;
  mountError.value = null;
  try {
    const result = await removeCustomNodeMount(path);
    mounts.value = result.mounts;
    await loadNodes();
    ElMessage.success("Folder unmounted");
  } catch (e: unknown) {
    mountError.value = extractDetail(e) ?? "Failed to unmount folder";
  } finally {
    removingPath.value = null;
  }
}

function extractDetail(e: unknown): string | null {
  const detail = (e as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  return typeof detail === "string" ? detail : null;
}

onMounted(loadNodes);
</script>

<style scoped>
/* Fills .catalog-detail so the list is the only scroll surface and the header
   + search stay put; sticky group headers then pin at the list's own top. */
.custom-nodes-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
}
.panel-top {
  flex-shrink: 0;
  padding: 16px 16px 12px;
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
.panel-count {
  margin-left: var(--spacing-2);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-normal);
  color: var(--color-text-tertiary);
}
.header-actions {
  display: flex;
  gap: 8px;
}
.header-actions .el-button i {
  margin-right: 6px;
}
.panel-hint {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 4px 0 12px;
}
/* Not `.search-input` — _forms.css defines a global one whose own border and
   background would double up on the el-input root. */
.node-search {
  max-width: 320px;
}
.nodes-list {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  border-top: 1px solid var(--color-border-light);
}
.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  padding: 1px 8px;
  border-radius: 10px;
  background-color: var(--color-background-secondary);
  color: var(--color-text-secondary);
}
.chip-missing {
  margin-left: 8px;
  background-color: var(--color-danger-light);
  color: var(--color-danger, #e53e3e);
}
.dialog-hint,
.dialog-note {
  font-size: 12px;
  color: var(--color-text-secondary);
  margin: 0 0 12px;
}
.dialog-note {
  margin: 8px 0 0;
}
.mount-error {
  margin-bottom: 12px;
}
.mounts-table {
  margin-bottom: 12px;
}
.mount-path {
  font-family: var(--font-mono, monospace);
  font-size: 12px;
  word-break: break-all;
}
.error-count {
  color: var(--color-danger, #e53e3e);
  font-weight: 600;
}
.add-mount-row {
  display: flex;
  gap: 8px;
  align-items: center;
}
.add-mount-row .el-input {
  flex: 1;
}
</style>
