<template>
  <div class="notebook-panel">
    <!-- Command band: notebook selector + save menu + kernel + run controls,
         plus the open-notebook tabs, grouped as one pinned header. -->
    <div class="nb-header">
      <div class="nb-toolbar">
        <!-- Merged notebook name + open list: rename inline, caret opens saved notebooks -->
        <el-input
          :model-value="store.active?.name ?? ''"
          size="small"
          class="nb-name-input"
          placeholder="Untitled notebook"
          @update:model-value="(v: string) => store.setName(v)"
        >
          <template #suffix>
            <el-dropdown trigger="click" placement="bottom-start" :hide-on-click="true">
              <span class="nb-open-caret" title="Open notebook">
                <i class="fa-solid fa-angle-down"></i>
              </span>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item
                    v-for="nb in store.notebooks"
                    :key="nb.id"
                    @click="store.openNotebook(nb.id)"
                  >
                    {{ nb.name }}
                  </el-dropdown-item>
                  <el-dropdown-item v-if="!store.notebooks.length" disabled>
                    No saved notebooks
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </template>
        </el-input>

        <!-- Save split-button (canvas-style): Save + File menu (New / Save As / Delete) -->
        <div class="nb-split" data-tutorial="save-btn">
          <button
            class="nb-split-btn nb-split-btn--main"
            :disabled="store.active?.saving"
            @click="onSave"
          >
            <i
              class="nb-split-icon"
              :class="
                store.active?.saving ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-floppy-disk'
              "
            ></i>
            <span>Save</span>
          </button>
          <el-dropdown trigger="click" placement="bottom-end" :hide-on-click="true">
            <button class="nb-split-btn nb-split-btn--caret" aria-label="More save options">
              <i class="fa-solid fa-angle-down nb-split-icon"></i>
            </button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item @click="store.newTab()">
                  <i class="fa-solid fa-file-circle-plus nb-menu-icon"></i> New
                </el-dropdown-item>
                <el-dropdown-item @click="onSaveAs">
                  <i class="fa-solid fa-copy nb-menu-icon"></i> Save As…
                </el-dropdown-item>
                <el-dropdown-item divided :disabled="!isPersisted" @click="onDelete">
                  <i class="fa-solid fa-trash nb-menu-icon"></i> Delete
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>

        <div class="nb-toolbar-spacer"></div>

        <!-- Kernel selector (Python cells); state is polled live. The selected label
             carries a state dot, and a stale/stopped selection turns the field amber. -->
        <el-select
          :model-value="store.active?.kernelId ?? null"
          placeholder="Select kernel"
          size="small"
          class="nb-kernel-select"
          :class="{ 'nb-kernel-select--attention': needsAttention }"
          clearable
          @change="(v: string | null) => store.setKernel(v)"
        >
          <template #label>
            <span class="nb-kernel-label">
              <i
                v-if="kernelStatus.kind === 'missing'"
                class="fa-solid fa-triangle-exclamation nb-kernel-label__warn"
              ></i>
              <span
                v-else-if="'kernel' in kernelStatus"
                class="nb-kernel-dot"
                :class="`nb-kernel-dot--${kernelStatus.kernel.state}`"
              ></span>
              <span class="nb-kernel-label__name">{{ selectedKernelLabel }}</span>
            </span>
          </template>
          <el-option
            v-for="k in kernels"
            :key="k.id"
            :label="`${k.name} (${k.state})`"
            :value="k.id"
          >
            <span class="nb-kernel-option">
              <span class="nb-kernel-dot" :class="`nb-kernel-dot--${k.state}`"></span>
              <span>{{ k.name }}</span>
              <span class="nb-kernel-option__state">{{ k.state }}</span>
            </span>
          </el-option>
          <template #footer>
            <router-link :to="kernelsRoute" class="nb-kernel-footer-link">
              <i class="fa-solid fa-microchip"></i> Manage kernels
            </router-link>
          </template>
        </el-select>

        <el-button
          size="small"
          class="nb-overflow-btn"
          title="Notebook help"
          @click="showHelp = true"
        >
          <i class="fa-solid fa-circle-question"></i>
        </el-button>

        <!-- Overflow: kernel / output maintenance -->
        <el-dropdown trigger="click" placement="bottom-end" :hide-on-click="true">
          <el-button size="small" class="nb-overflow-btn" title="More actions">
            <i class="fa-solid fa-ellipsis"></i>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="store.clearOutputs()">
                <i class="fa-solid fa-eraser nb-menu-icon"></i> Clear outputs
              </el-dropdown-item>
              <el-dropdown-item :disabled="!store.active?.kernelId" @click="store.restartKernel()">
                <i class="fa-solid fa-rotate-right nb-menu-icon"></i> Restart kernel
              </el-dropdown-item>
              <el-dropdown-item
                v-if="kernelStatus.kind === 'stopped'"
                :disabled="startingKernel"
                @click="startKernel"
              >
                <i class="fa-solid fa-play nb-menu-icon"></i> Start kernel
              </el-dropdown-item>
              <el-dropdown-item divided @click="router.push(kernelsRoute)">
                <i class="fa-solid fa-microchip nb-menu-icon"></i> Manage kernels…
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>

        <el-button
          size="small"
          class="nb-run-all"
          :loading="running"
          :disabled="running"
          @click="runAll"
        >
          <i v-if="!running" class="fa-solid fa-forward" style="margin-right: 4px"></i> Run All
        </el-button>
      </div>

      <!-- Open-notebook tab strip -->
      <el-tabs
        v-if="store.openNotebooks.length"
        :model-value="store.activeTabId ?? undefined"
        type="card"
        closable
        addable
        class="nb-tabs"
        @tab-change="onTabChange"
        @tab-remove="onTabRemove"
        @tab-add="store.newTab()"
      >
        <el-tab-pane v-for="nb in store.openNotebooks" :key="nb.tabId" :name="nb.tabId">
          <template #label>
            <span class="nb-tab-label">
              <i class="fa-solid fa-book nb-tab-icon"></i>
              <span class="nb-tab-name">{{ nb.name || "Untitled" }}</span>
              <span v-if="nb.dirty" class="nb-dirty">*</span>
            </span>
          </template>
        </el-tab-pane>
      </el-tabs>
    </div>

    <!-- Kernel status banner: the one place that says why Python cells won't run
         (no kernel, Docker off, kernel gone, stopped, starting, errored) and what to do. -->
    <div v-if="banner" class="nb-banner" :class="`nb-banner--${banner.tone}`">
      <i :class="banner.icon"></i>
      <span class="nb-banner__text">{{ banner.text }}</span>
      <span class="nb-banner__actions">
        <el-button
          v-if="kernelStatus.kind === 'stopped' && !startingKernel"
          size="small"
          class="nb-banner__btn"
          @click="startKernel"
        >
          <i class="fa-solid fa-play" style="margin-right: 4px"></i> Start kernel
        </el-button>
        <el-button
          v-if="kernelStatus.kind === 'missing'"
          size="small"
          class="nb-banner__btn"
          @click="store.setKernel(null)"
        >
          Clear selection
        </el-button>
        <router-link :to="kernelsRoute" class="nb-banner__link">Manage kernels</router-link>
      </span>
    </div>

    <!-- Cells of the active notebook -->
    <div v-if="store.active" class="nb-cells">
      <!-- Quick-start primer, shown only while the notebook is empty. -->
      <div v-if="showPrimer" class="nb-primer">
        <div class="nb-primer-title"><i class="fa-solid fa-book"></i> New notebook</div>
        <p class="nb-primer-text">
          Pick a kernel above, write Python, and press <kbd>Shift</kbd>+<kbd>Enter</kbd> to run.
          Read from the catalog and show results:
        </p>
        <pre
          class="nb-primer-code"
        ><code>df = flowfile_ctx.read_catalog_table("fx_rates", schema="market")
flowfile_ctx.display(df)      # interactive table
flowfile_ctx.explore(df)      # full explorer</code></pre>
        <a class="nb-primer-link" @click="showHelp = true">Open full reference →</a>
      </div>

      <template v-for="(cell, idx) in store.active.cells" :key="cell.id">
        <CatalogNotebookCell
          :cell="cell"
          :index="idx"
          :cell-count="store.active.cells.length"
          :prior-cell-codes="priorCodes(idx)"
          :kernel-id="store.active.kernelId"
          :flow-id="store.active.sessionFlowId"
          :node-id="cellNodeId(cell.id)"
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

    <NotebookHelp v-if="showHelp" @close="showHelp = false" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox, type TabPaneName } from "element-plus";
import { useNotebookStore, cellNodeId } from "../../stores/notebook-store";
import { useCatalogStore } from "../../stores/catalog-store";
import { useWritableNamespaces } from "../../composables/useWritableNamespaces";
import { catalogSaveErrorMessage } from "../../composables/saveError";
import { KernelApi } from "../../api/kernel.api";
import CatalogNotebookCell from "../../components/notebook/CatalogNotebookCell.vue";
import NotebookHelp from "../../components/notebook/NotebookHelp.vue";
import {
  kernelStatusNeedsAttention,
  resolveNotebookKernelStatus,
} from "../../components/notebook/notebookKernelStatus";
import type { CellType } from "../../components/notebook/types";
import type { KernelInfo } from "../../types/kernel.types";

const KERNEL_POLL_MS = 5000;

const store = useNotebookStore();
const catalogStore = useCatalogStore();
const router = useRouter();
const kernelsRoute = { name: "compute", query: { tab: "kernels" } } as const;

const saveAsVisible = ref(false);
const saveAsName = ref("");
const saveAsNamespaceId = ref<number | null>(null);
const saveAsSaving = ref(false);

const { writableSchemaNamespaces: schemaNamespaces } = useWritableNamespaces();

// Prefer General/default when writable; otherwise the first writable schema.
const defaultNamespaceId = computed<number | null>(() => {
  const general = catalogStore.tree.find((c) => c.name === "General");
  const preferred = general?.children.find((s) => s.name === "default");
  if (preferred && schemaNamespaces.value.some((o) => o.id === preferred.id)) return preferred.id;
  return schemaNamespaces.value[0]?.id ?? null;
});

const kernels = ref<KernelInfo[]>([]);
const kernelsLoaded = ref(false);
const dockerAvailable = ref(true);
const running = ref(false);
const startingKernel = ref(false);
let pollTimer: ReturnType<typeof setInterval> | null = null;

const kernelStatus = computed(() =>
  resolveNotebookKernelStatus({
    kernelId: store.active?.kernelId ?? null,
    kernels: kernels.value,
    kernelsLoaded: kernelsLoaded.value,
    dockerAvailable: dockerAvailable.value,
  }),
);
const needsAttention = computed(() => kernelStatusNeedsAttention(kernelStatus.value));
const selectedKernelLabel = computed(() => {
  const s = kernelStatus.value;
  if (s.kind === "missing") return "Kernel not found";
  if ("kernel" in s) return s.kernel.name;
  return store.active?.kernelId ?? "";
});

interface KernelBanner {
  tone: "info" | "warning" | "danger";
  icon: string;
  text: string;
}

// Only the "no kernel" nudge waits for Python cells; a bad selection is always worth saying.
const banner = computed<KernelBanner | null>(() => {
  const s = kernelStatus.value;
  const name = "kernel" in s ? `"${s.kernel.name}"` : "";
  switch (s.kind) {
    case "docker-off":
      return store.hasPythonCells
        ? {
            tone: "info",
            icon: "fa-solid fa-circle-info",
            text: "Docker is not available — Python cells are disabled. Markdown cells still work.",
          }
        : null;
    case "none":
      return store.hasPythonCells
        ? {
            tone: "info",
            icon: "fa-solid fa-circle-info",
            text: "This notebook has Python cells — select a kernel to run them. Markdown cells run without one.",
          }
        : null;
    case "missing":
      return {
        tone: "warning",
        icon: "fa-solid fa-triangle-exclamation",
        text: "The kernel this notebook was using no longer exists. Pick another kernel or create one.",
      };
    case "stopped":
      if (!startingKernel.value) {
        return {
          tone: "warning",
          icon: "fa-solid fa-circle-pause",
          text: `Kernel ${name} is stopped — Python cells and autocomplete won't work until it starts.`,
        };
      }
      return {
        tone: "info",
        icon: "fa-solid fa-spinner fa-spin",
        text: `Starting kernel ${name}…`,
      };
    case "starting":
      return {
        tone: "info",
        icon: "fa-solid fa-spinner fa-spin",
        text: `Kernel ${name} is starting — you can run cells in a moment.`,
      };
    case "error":
      return {
        tone: "danger",
        icon: "fa-solid fa-circle-exclamation",
        text: `Kernel ${name} is in an error state. Check it on the Python Kernels page.`,
      };
    default:
      return null;
  }
});

async function startKernel() {
  const s = kernelStatus.value;
  if (s.kind !== "stopped") return;
  startingKernel.value = true;
  try {
    await KernelApi.start(s.kernel.id);
    await loadKernels();
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? e?.message ?? "Failed to start kernel");
  } finally {
    startingKernel.value = false;
  }
}

const isPersisted = computed(() => store.active?.persistedId != null);

const showHelp = ref(false);
// Primer shows only while the notebook has no code yet; it hides as soon as you type.
const showPrimer = computed(
  () => !!store.active && store.active.cells.every((c) => !c.code.trim()),
);

function priorCodes(idx: number): string[] {
  return store.active ? store.active.cells.slice(0, idx).map((c) => c.code) : [];
}

async function loadKernels() {
  try {
    kernels.value = await KernelApi.getAll();
    kernelsLoaded.value = true;
  } catch {
    // Keep the last known list: a transient fetch failure must not flag every kernel as gone.
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
      ElMessage.error(catalogSaveErrorMessage(e, "Failed to save notebook"));
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
  const current = store.active?.namespaceId ?? null;
  saveAsNamespaceId.value =
    current != null && schemaNamespaces.value.some((o) => o.id === current)
      ? current
      : defaultNamespaceId.value;
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
    ElMessage.error(catalogSaveErrorMessage(e, "Failed to save notebook"));
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
.nb-header {
  position: relative;
  z-index: 1;
  flex: 0 0 auto;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  box-shadow: var(--shadow-xs);
}
.nb-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  flex-wrap: wrap;
}
.nb-toolbar-spacer {
  flex: 1;
}
.nb-name-input {
  width: 220px;
}
.nb-open-caret {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  color: var(--color-text-muted, #909399);
  transition: color var(--transition-fast, 0.15s ease);
}
.nb-open-caret:hover {
  color: var(--color-primary, #409eff);
}
.nb-menu-icon {
  width: 16px;
  margin-right: 6px;
  text-align: center;
}
.nb-overflow-btn {
  padding-left: 9px;
  padding-right: 9px;
}
/* Run All mirrors the designer header's Run button (accent purple). */
.nb-run-all:not(:disabled) {
  background-color: var(--color-accent-purple);
  border-color: var(--color-accent-purple);
  color: #fff;
}
.nb-run-all:not(:disabled):hover {
  background-color: var(--color-accent-purple-hover);
  border-color: var(--color-accent-purple-hover);
  color: #fff;
}

/* Kernel picker: state dot in the field and in each option; amber when the
   selection is stale, stopped or errored so the header itself flags it. */
.nb-kernel-select {
  width: 220px;
}
.nb-kernel-select--attention :deep(.el-select__wrapper) {
  box-shadow: 0 0 0 1px var(--color-warning) inset;
}
.nb-kernel-label,
.nb-kernel-option {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1-5, 6px);
  min-width: 0;
}
.nb-kernel-label__name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.nb-kernel-label__warn {
  color: var(--color-warning);
  font-size: 11px;
}
.nb-kernel-option__state {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted, #909399);
}
.nb-kernel-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  background-color: var(--color-gray-400, #909399);
}
.nb-kernel-dot--idle {
  background-color: var(--color-success);
}
.nb-kernel-dot--executing {
  background-color: var(--color-warning);
}
.nb-kernel-dot--starting,
.nb-kernel-dot--creating {
  background-color: var(--color-info);
}
.nb-kernel-dot--error {
  background-color: var(--color-danger);
}
.nb-kernel-footer-link {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1-5, 6px);
  font-size: var(--font-size-xs);
  color: var(--color-primary);
  text-decoration: none;
}
.nb-kernel-footer-link:hover {
  text-decoration: underline;
}

/* Canvas-style Save split-button (mirrors HeaderButtons .action-btn-split):
   neutral, unified, with a divider between the Save half and the caret. Sized
   to the small toolbar controls so it lines up with the inputs/buttons. */
.nb-split {
  display: inline-flex;
  align-items: stretch;
  height: var(--el-component-size-small, 24px);
  box-shadow: var(--shadow-xs);
  border-radius: var(--border-radius-md, 6px);
}
.nb-split :deep(.el-dropdown) {
  display: inline-flex;
}
.nb-split-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1-5, 6px);
  height: var(--el-component-size-small, 24px);
  padding: 0 var(--spacing-3, 12px);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.nb-split-btn:hover:not(:disabled) {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}
.nb-split-btn:disabled {
  opacity: 0.6;
  cursor: default;
}
.nb-split-icon {
  font-size: 13px;
  color: var(--color-text-secondary);
}
.nb-split-btn:hover:not(:disabled) .nb-split-icon {
  color: var(--color-text-primary);
}
.nb-split-btn--main {
  border-top-left-radius: var(--border-radius-md, 6px);
  border-bottom-left-radius: var(--border-radius-md, 6px);
  border-right: none;
}
.nb-split-btn--caret {
  justify-content: center;
  min-width: 22px;
  padding: 0 var(--spacing-2, 8px);
  border-top-right-radius: var(--border-radius-md, 6px);
  border-bottom-right-radius: var(--border-radius-md, 6px);
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
/* Notebook tabs match the designer's flow-tabs (FlowSelectorView): top-rounded,
   transparent inactive with a right divider, active tab raised onto the canvas
   surface with an accent top border — a little rounder and shorter. */
.nb-tabs {
  padding: 0 var(--spacing-1);
}
.nb-tabs :deep(.el-tabs__header) {
  margin: 0;
  border-bottom: none;
  background-color: transparent;
}
.nb-tabs :deep(.el-tabs__nav-wrap)::after {
  display: none;
}
.nb-tabs :deep(.el-tabs__nav.is-top) {
  border: none;
}
.nb-tabs :deep(.el-tabs__content),
.nb-tabs :deep(.el-tab-pane) {
  background-color: transparent;
}
.nb-tabs :deep(.el-tabs__item.is-top) {
  height: 30px;
  line-height: 30px;
  padding: 0 var(--spacing-4);
  border: none;
  border-right: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg) var(--border-radius-lg) 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  transition: all var(--transition-fast);
}
.nb-tabs :deep(.el-tabs__item.is-top:not(.is-active):hover) {
  background-color: var(--color-background-hover);
  color: var(--color-text-primary);
}
.nb-tabs :deep(.el-tabs__item.is-active) {
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
  border-top: 2px solid var(--color-primary);
  box-shadow: var(--shadow-xs);
}
.nb-tab-label {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
}
.nb-tab-icon {
  font-size: var(--font-size-xs);
  color: var(--color-primary);
}
/* "+" new-notebook button: adapt to the band and theme. */
.nb-tabs :deep(.el-tabs__new-tab) {
  height: 26px;
  margin-left: var(--spacing-2);
  border-radius: var(--border-radius-md);
  color: var(--color-text-secondary);
  border-color: var(--color-border-light);
  transition: all var(--transition-fast);
}
.nb-tabs :deep(.el-tabs__new-tab):hover {
  color: var(--color-primary);
  border-color: var(--color-primary);
}
.nb-banner {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  background: var(--color-info-light);
  color: var(--color-text-secondary);
  border-bottom: 1px solid var(--color-border-light);
  font-size: 13px;
}
.nb-banner > i {
  color: var(--color-info);
}
.nb-banner--warning {
  background: var(--color-warning-light);
}
.nb-banner--warning > i {
  color: var(--color-warning);
}
.nb-banner--danger {
  background: var(--color-danger-light, rgba(239, 68, 68, 0.12));
}
.nb-banner--danger > i {
  color: var(--color-danger);
}
.nb-banner__text {
  flex: 1;
  min-width: 0;
}
.nb-banner__actions {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  flex-shrink: 0;
}
.nb-banner__btn {
  height: 24px;
}
.nb-banner__link {
  font-size: 12px;
  color: var(--color-primary);
  text-decoration: none;
  white-space: nowrap;
}
.nb-banner__link:hover {
  text-decoration: underline;
}
.nb-cells {
  flex: 1;
  overflow-y: auto;
  padding: 12px;
}
.nb-primer {
  margin-bottom: 12px;
  padding: 12px 14px;
  border: 1px solid var(--el-border-color-lighter, #ebeef5);
  border-radius: 6px;
  background: var(--el-fill-color-lighter, #fafafa);
}
.nb-primer-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--el-text-color-primary, #303133);
  margin-bottom: 4px;
}
.nb-primer-text {
  font-size: 12px;
  color: var(--el-text-color-regular, #606266);
  margin: 0 0 8px;
  line-height: 1.5;
}
.nb-primer-text kbd {
  font-family: inherit;
  font-size: 11px;
  padding: 0 4px;
  border: 1px solid var(--el-border-color, #dcdfe6);
  border-bottom-width: 2px;
  border-radius: 3px;
  background: var(--el-bg-color, #fff);
}
.nb-primer-code {
  margin: 0 0 8px;
  padding: 8px 10px;
  border-radius: 4px;
  background: #282c34;
  overflow-x: auto;
}
.nb-primer-code code {
  font-family: "Fira Code", monospace;
  font-size: 12px;
  color: #abb2bf;
  white-space: pre;
}
.nb-primer-link {
  font-size: 12px;
  color: var(--el-color-primary, #409eff);
  cursor: pointer;
}
.nb-primer-link:hover {
  text-decoration: underline;
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
