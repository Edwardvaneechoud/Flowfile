<template>
  <div class="notebook-panel">
    <!-- Top bar: open/new/save/delete + kernel + run controls -->
    <div class="nb-toolbar">
      <el-select
        :model-value="null"
        placeholder="Open notebook…"
        size="small"
        style="width: 190px"
        filterable
        @change="onOpenSaved"
      >
        <el-option v-for="nb in store.notebooks" :key="nb.id" :label="nb.name" :value="nb.id" />
      </el-select>

      <el-input
        v-if="store.active"
        :model-value="store.active.name"
        size="small"
        class="nb-name-input"
        placeholder="Notebook name"
        @update:model-value="(v: string) => store.setName(v)"
      />

      <el-button size="small" @click="store.newTab()">
        <i class="fa-solid fa-file-circle-plus" style="margin-right: 4px"></i> New
      </el-button>
      <el-button size="small" type="primary" :loading="store.active?.saving" @click="onSave">
        <i class="fa-solid fa-floppy-disk" style="margin-right: 4px"></i> Save
      </el-button>
      <el-button size="small" @click="onSaveAs">Save As</el-button>
      <el-button size="small" :disabled="!isPersisted" @click="onDelete">
        <i class="fa-solid fa-trash"></i>
      </el-button>

      <div class="nb-toolbar-spacer"></div>

      <!-- Kernel selector (Python cells); state is polled live -->
      <el-select
        :model-value="store.active?.kernelId ?? null"
        placeholder="Select kernel"
        size="small"
        style="width: 200px"
        clearable
        @change="(v: string | null) => store.setKernel(v)"
      >
        <el-option
          v-for="k in kernels"
          :key="k.id"
          :label="`${k.name} (${k.state})`"
          :value="k.id"
        />
      </el-select>

      <el-button size="small" type="success" :disabled="running" @click="runAll">
        <i class="fa-solid fa-forward" style="margin-right: 4px"></i> Run All
      </el-button>
      <el-button size="small" @click="store.clearOutputs()">Clear</el-button>
      <el-button size="small" :disabled="!store.active?.kernelId" @click="store.restartKernel()">
        Restart
      </el-button>
    </div>

    <!-- Open-notebook tab strip -->
    <el-tabs
      v-if="store.openNotebooks.length"
      :model-value="store.activeTabId ?? undefined"
      type="card"
      closable
      class="nb-tabs"
      @tab-change="onTabChange"
      @tab-remove="onTabRemove"
    >
      <el-tab-pane v-for="nb in store.openNotebooks" :key="nb.tabId" :name="nb.tabId">
        <template #label>
          {{ nb.name || "Untitled" }}<span v-if="nb.dirty" class="nb-dirty">*</span>
        </template>
      </el-tab-pane>
    </el-tabs>

    <!-- No-kernel / Docker banner -->
    <div v-if="store.hasPythonCells && !store.active?.kernelId" class="nb-banner">
      <i class="fa-solid fa-circle-info"></i>
      <span v-if="dockerAvailable">
        This notebook has Python cells — select or
        <router-link :to="{ name: 'kernelManager' }">start a kernel</router-link> to run them.
        Markdown cells run without a kernel.
      </span>
      <span v-else>
        Docker is not available — Python cells are disabled. Markdown cells still work.
      </span>
    </div>

    <!-- Cells of the active notebook -->
    <div v-if="store.active" class="nb-cells">
      <template v-for="(cell, idx) in store.active.cells" :key="cell.id">
        <CatalogNotebookCell
          :cell="cell"
          :index="idx"
          :cell-count="store.active.cells.length"
          :prior-cell-codes="priorCodes(idx)"
          @run="store.runCell(cell.id)"
          @update:code="(code: string) => store.setCellCode(cell.id, code)"
          @update:type="(t: CellType) => store.setCellType(cell.id, t)"
          @update:editing="(e: boolean) => store.setCellEditing(cell.id, e)"
          @move="(dir: -1 | 1) => store.moveCell(cell.id, dir)"
          @remove="store.removeCell(cell.id)"
        />

        <!-- Hover-to-insert: a faint "+" appears between cells; click to add a
             Python cell at this position. -->
        <div
          v-if="idx < store.active.cells.length - 1"
          class="nb-insert-zone"
          title="Add cell here"
          @click="onAddCell('python', idx)"
        >
          <span class="nb-insert-plus"><i class="fa-solid fa-plus"></i></span>
        </div>
      </template>

      <!-- Add cell (centered). Adds a Python cell by default; switch to
           Markdown via the per-cell type selector. -->
      <div class="nb-add-row">
        <el-button size="small" class="nb-add-btn" @click="onAddCell('python')">
          <i class="fa-solid fa-plus" style="margin-right: 4px"></i> Add cell
        </el-button>
      </div>
    </div>

    <!-- Save As: name + catalog namespace (so the notebook lands in the tree) -->
    <el-dialog v-model="saveAsVisible" title="Save notebook" width="420px" append-to-body>
      <div class="nb-saveas">
        <label class="nb-saveas-label">Name</label>
        <el-input v-model="saveAsName" placeholder="Notebook name" @keyup.enter="confirmSaveAs" />
        <label class="nb-saveas-label">Namespace</label>
        <el-select
          v-model="saveAsNamespaceId"
          placeholder="Select namespace"
          clearable
          style="width: 100%"
        >
          <el-option v-for="ns in schemaNamespaces" :key="ns.id" :label="ns.label" :value="ns.id" />
        </el-select>
        <p v-if="!schemaNamespaces.length" class="nb-saveas-hint">
          No namespaces available — the notebook will be saved without one and won't appear in the
          catalog tree.
        </p>
      </div>
      <template #footer>
        <el-button @click="saveAsVisible = false">Cancel</el-button>
        <el-button type="primary" :loading="saveAsSaving" @click="confirmSaveAs">Save</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from "vue";
import { ElMessage, ElMessageBox, type TabPaneName } from "element-plus";
import { useNotebookStore } from "../../stores/notebook-store";
import { useCatalogStore } from "../../stores/catalog-store";
import { KernelApi } from "../../api/kernel.api";
import CatalogNotebookCell from "../../components/notebook/CatalogNotebookCell.vue";
import type { CellType } from "../../components/notebook/types";
import type { KernelInfo } from "../../types/kernel.types";

const KERNEL_POLL_MS = 5000;

const store = useNotebookStore();
const catalogStore = useCatalogStore();

const saveAsVisible = ref(false);
const saveAsName = ref("");
const saveAsNamespaceId = ref<number | null>(null);
const saveAsSaving = ref(false);

// Selectable namespaces (level-1 schemas), mirroring RegisterFlowModal.
const schemaNamespaces = computed(() => {
  const result: { id: number; label: string }[] = [];
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      result.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
    }
  }
  return result;
});

const defaultNamespaceId = computed<number | null>(() => {
  const general = catalogStore.tree.find((c) => c.name === "General");
  return general?.children.find((s) => s.name === "default")?.id ?? null;
});

const kernels = ref<KernelInfo[]>([]);
const dockerAvailable = ref(true);
const running = ref(false);
let pollTimer: ReturnType<typeof setInterval> | null = null;

const isPersisted = computed(() => store.active?.persistedId != null);

function priorCodes(idx: number): string[] {
  return store.active ? store.active.cells.slice(0, idx).map((c) => c.code) : [];
}

async function loadKernels() {
  try {
    kernels.value = await KernelApi.getAll();
  } catch {
    kernels.value = [];
  }
}

onMounted(async () => {
  store.ensureHydrated();
  await store.loadList();
  try {
    dockerAvailable.value = (await KernelApi.getDockerStatus()).available;
  } catch {
    dockerAvailable.value = false;
  }
  if (dockerAvailable.value) {
    await loadKernels();
    pollTimer = setInterval(loadKernels, KERNEL_POLL_MS);
  }
});

onBeforeUnmount(() => {
  if (pollTimer) clearInterval(pollTimer);
  store.closeAllSessions();
});

async function runAll() {
  running.value = true;
  try {
    await store.runAll();
  } finally {
    running.value = false;
  }
}

function onOpenSaved(id: number | null) {
  if (id != null) store.openNotebook(id);
}

function onTabChange(name: TabPaneName) {
  store.setActiveTab(String(name));
}

function onTabRemove(name: TabPaneName) {
  store.closeTab(String(name));
}

function onAddCell(command: string, afterIndex?: number) {
  store.addCell(command as CellType, afterIndex);
}

async function onSave() {
  if (isPersisted.value) {
    try {
      await store.save();
      void catalogStore.loadTree();
      ElMessage.success("Notebook saved");
    } catch (e: any) {
      ElMessage.error(e?.message ?? "Failed to save notebook");
    }
    return;
  }
  await onSaveAs();
}

async function onSaveAs() {
  if (!catalogStore.tree.length) {
    await catalogStore.loadTree().catch(() => undefined);
  }
  saveAsName.value = isPersisted.value ? `${store.active?.name} copy` : "";
  saveAsNamespaceId.value = store.active?.namespaceId ?? defaultNamespaceId.value;
  saveAsVisible.value = true;
}

async function confirmSaveAs() {
  const name = saveAsName.value.trim();
  if (!name) {
    ElMessage.warning("A name is required");
    return;
  }
  saveAsSaving.value = true;
  try {
    await store.saveAs(name, saveAsNamespaceId.value);
    void catalogStore.loadTree();
    ElMessage.success("Notebook saved");
    saveAsVisible.value = false;
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? e?.message ?? "Failed to save notebook");
  } finally {
    saveAsSaving.value = false;
  }
}

async function onDelete() {
  const id = store.active?.persistedId;
  if (id == null) return;
  try {
    await ElMessageBox.confirm(`Delete notebook "${store.active?.name}"?`, "Delete notebook", {
      confirmButtonText: "Delete",
      cancelButtonText: "Cancel",
      type: "warning",
    });
  } catch {
    return;
  }
  try {
    await store.deleteNotebook(id);
    void catalogStore.loadTree();
    ElMessage.success("Notebook deleted");
  } catch (e: any) {
    ElMessage.error(e?.message ?? "Failed to delete notebook");
  }
}
</script>

<style scoped>
.notebook-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}
.nb-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--el-border-color-lighter, #e4e7ed);
  flex-wrap: wrap;
}
.nb-toolbar-spacer {
  flex: 1;
}
.nb-name-input {
  width: 200px;
}
.nb-saveas {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.nb-saveas-label {
  font-size: 12px;
  color: var(--el-text-color-regular, #606266);
  margin-top: 4px;
}
.nb-saveas-hint {
  font-size: 12px;
  color: var(--el-color-warning, #e6a23c);
  margin: 4px 0 0;
}
.nb-dirty {
  color: var(--el-color-warning, #e6a23c);
  margin-left: 2px;
}
.nb-tabs {
  /* 12px left gutter so the tab strip's card aligns with the toolbar above and
     the cells (canvas) below, which all inset content by 12px. */
  padding: 0 12px;
}
.nb-tabs :deep(.el-tabs__header) {
  margin: 0;
}
.nb-banner {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--el-color-info-light-9, #f4f4f5);
  color: var(--el-text-color-regular, #606266);
  font-size: 13px;
}
.nb-cells {
  flex: 1;
  overflow-y: auto;
  padding: 12px;
}
.nb-add-row {
  display: flex;
  justify-content: center;
  margin-top: 8px;
}

/* Hover-to-insert zone between cells: a thin gap that reveals a centered "+"
   (with a faint connecting line) only on hover. */
.nb-insert-zone {
  position: relative;
  height: 14px;
  margin: 2px 0;
  cursor: pointer;
}
.nb-insert-zone::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 0;
  right: 0;
  height: 1px;
  background: var(--el-color-primary, #409eff);
  opacity: 0;
  transition: opacity 0.15s ease;
}
.nb-insert-plus {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border-radius: 50%;
  background: var(--el-color-primary, #409eff);
  color: #fff;
  font-size: 10px;
  opacity: 0;
  transition: opacity 0.15s ease;
}
.nb-insert-zone:hover::before {
  opacity: 0.35;
}
.nb-insert-zone:hover .nb-insert-plus {
  opacity: 0.85;
}
</style>
