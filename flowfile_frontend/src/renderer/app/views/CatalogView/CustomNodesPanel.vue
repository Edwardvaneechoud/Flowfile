<template>
  <div class="custom-nodes-panel">
    <div class="panel-header">
      <h2>Custom Nodes</h2>
      <div class="header-actions">
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

    <el-table v-else v-loading="loading" :data="nodes" size="small">
      <el-table-column label="Name" min-width="180">
        <template #default="{ row }">
          <div class="node-name-cell">
            <span class="node-name">{{ row.node_name || row.file_name }}</span>
            <el-tooltip v-if="row.error" :content="row.error" placement="top">
              <i class="fa-solid fa-triangle-exclamation node-error-icon" />
            </el-tooltip>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="Category" width="150">
        <template #default="{ row }">
          <span v-if="row.node_category" class="chip chip-category">{{ row.node_category }}</span>
          <span v-else class="chip chip-muted">Custom</span>
        </template>
      </el-table-column>
      <el-table-column label="Environment" width="130">
        <template #default="{ row }">
          <span :class="['chip', row.environment === 'kernel' ? 'chip-kernel' : 'chip-local']">
            {{ row.environment === "kernel" ? "Kernel" : "Local" }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="Source" min-width="140">
        <template #default="{ row }">
          <el-tooltip
            v-if="row.source !== 'default'"
            :content="row.source"
            placement="top"
            :show-after="300"
          >
            <span class="chip chip-source"
              ><i class="fa-solid fa-folder" /> {{ row.source_label }}</span
            >
          </el-tooltip>
          <span v-else class="chip chip-muted">Default</span>
        </template>
      </el-table-column>
      <el-table-column width="120" align="right">
        <template #default="{ row }">
          <el-button size="small" text type="primary" @click="openDesigner(row.file_name)">
            Open
          </el-button>
        </template>
      </el-table-column>
    </el-table>

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
import { onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import {
  addCustomNodeMount,
  listCatalogCustomNodes,
  listCustomNodeMounts,
  removeCustomNodeMount,
  rescanCustomNodes,
  type CatalogCustomNode,
  type MountInfo,
} from "../../api/nodeDesigner";
import { EmptyState } from "../../components/common";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";

const router = useRouter();

const nodes = ref<CatalogCustomNode[]>([]);
const loading = ref(false);
const rescanning = ref(false);

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
    nodes.value = await listCatalogCustomNodes();
  } catch {
    ElMessage.error("Failed to load custom nodes");
  } finally {
    loading.value = false;
  }
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
.custom-nodes-panel {
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
  gap: 8px;
}
.panel-hint {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 4px 0 16px;
}
.node-name-cell {
  display: flex;
  align-items: center;
  gap: 6px;
}
.node-name {
  font-weight: 500;
}
.node-error-icon {
  color: var(--color-danger, #e53e3e);
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
.chip-category {
  background-color: var(--color-primary-bg, #ebf3fe);
  color: var(--color-primary, #3b6fd4);
}
.chip-kernel {
  background-color: var(--color-warning-bg, #fdf0e6);
  color: var(--color-warning, #b7791f);
}
.chip-local {
  background-color: var(--color-success-bg, #e6f4ea);
  color: var(--color-success, #1e7e34);
}
.chip-source {
  cursor: default;
}
.chip-muted {
  color: var(--color-text-secondary);
}
.chip-missing {
  margin-left: 8px;
  background-color: var(--color-danger-bg, #fde8e8);
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
