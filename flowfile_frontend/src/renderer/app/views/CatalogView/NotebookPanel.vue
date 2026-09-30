<template>
  <div class="notebook-panel">
    <!-- One header row: catalog tabs and "+" in catalog mode, then the tools. -->
    <div class="nb-header">
      <div class="nb-toolbar">
        <el-tabs
          v-if="!flowId && catalogTabs.length"
          :model-value="store.activeTabId ?? undefined"
          type="card"
          closable
          class="nb-tabs"
          @tab-change="onTabChange"
          @tab-remove="onTabRemove"
        >
          <el-tab-pane v-for="nb in catalogTabs" :key="nb.tabId" :name="nb.tabId">
            <template #label>
              <span class="nb-tab-label" @dblclick.stop="startRename(nb.tabId)">
                <i class="fa-solid fa-book nb-tab-icon"></i>
                <input
                  v-if="renamingTabId === nb.tabId"
                  ref="renameInputRef"
                  v-model="renameDraft"
                  class="nb-tab-rename"
                  aria-label="Notebook name"
                  @keydown.enter.prevent="commitRename"
                  @keydown.esc.prevent="cancelRename"
                  @keydown.stop
                  @blur="commitRename"
                  @click.stop
                  @mousedown.stop
                />
                <span v-else class="nb-tab-name">{{ nb.name || "Untitled" }}</span>
                <span v-if="nb.dirty && renamingTabId !== nb.tabId" class="nb-dirty">*</span>
              </span>
            </template>
          </el-tab-pane>
        </el-tabs>

        <!-- "+" — New, or open a saved notebook -->
        <el-dropdown v-if="!flowId" trigger="click" placement="bottom-start" :hide-on-click="true">
          <button class="nb-tab-add" title="New or open notebook" aria-label="New or open notebook">
            <i class="fa-solid fa-plus"></i>
          </button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="store.newTab()">
                <i class="fa-solid fa-file-circle-plus nb-menu-icon"></i> New notebook
              </el-dropdown-item>
              <el-dropdown-item
                v-for="(nb, i) in store.notebooks"
                :key="nb.id"
                :divided="i === 0"
                @click="store.openNotebook(nb.id)"
              >
                <i class="fa-solid fa-book nb-menu-icon"></i> {{ nb.name }}
              </el-dropdown-item>
              <el-dropdown-item v-if="!store.notebooks.length" divided disabled>
                No saved notebooks
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>

        <div class="nb-toolbar-spacer"></div>

        <!-- Kernel selector (Python cells); state is polled live. The selected label
             carries a state dot, and a stale/stopped selection turns the field amber.
             A flow tab lists only kernels that can run the notebook, plus "No kernel". -->
        <el-select
          v-if="!flowId || store.kernelSessions"
          ref="kernelSelectRef"
          :model-value="store.active?.kernelId ?? (flowId ? NO_KERNEL : null)"
          placeholder="Select kernel"
          size="small"
          class="nb-kernel-select"
          data-testid="nb-kernel-select"
          :class="{ 'nb-kernel-select--attention': needsAttention }"
          :clearable="!flowId"
          @change="onKernelChange"
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
          <el-option v-if="flowId" :value="NO_KERNEL" label="No kernel">
            <span class="nb-kernel-option">
              <span>No kernel</span>
              <span class="nb-kernel-option__state">runs on the canvas</span>
            </span>
          </el-option>
          <el-option
            v-for="k in pickerKernels"
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
            <button
              v-if="flowId && !pickerKernels.length"
              type="button"
              class="nb-kernel-footer-link nb-kernel-footer-create"
              @click="openCreateKernel"
            >
              <i class="fa-solid fa-plus"></i> Create notebook kernel…
            </button>
            <router-link :to="kernelsRoute" class="nb-kernel-footer-link">
              <i class="fa-solid fa-microchip"></i> Manage kernels
            </router-link>
          </template>
        </el-select>

        <div class="nb-tool-group">
          <button
            type="button"
            class="nb-tool-btn"
            title="Undo cell action (insert, delete, move, duplicate)"
            aria-label="Undo cell action"
            :disabled="structuralDisabled || !canUndo"
            @click="onUndoCellAction"
          >
            <i class="fa-solid fa-arrow-rotate-left"></i>
          </button>
          <button
            type="button"
            class="nb-tool-btn"
            title="Redo cell action"
            aria-label="Redo cell action"
            :disabled="structuralDisabled || !canRedo"
            @click="onRedoCellAction"
          >
            <i class="fa-solid fa-arrow-rotate-right"></i>
          </button>
          <button
            type="button"
            class="nb-tool-btn"
            title="Notebook help"
            aria-label="Notebook help"
            @click="showHelp = true"
          >
            <i class="fa-regular fa-circle-question"></i>
          </button>

          <!-- Overflow: kernel / output maintenance -->
          <el-dropdown trigger="click" placement="bottom-end" :hide-on-click="true">
            <button
              type="button"
              class="nb-tool-btn"
              title="More actions"
              aria-label="More actions"
            >
              <i class="fa-solid fa-ellipsis"></i>
            </button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item :disabled="batchBusy" @click="store.clearOutputs()">
                  <i class="fa-solid fa-eraser nb-menu-icon"></i> Clear outputs
                </el-dropdown-item>
                <el-dropdown-item
                  v-if="!flowId || store.active?.kernelId"
                  :disabled="batchBusy || resetPending"
                  :title="
                    flowId
                      ? 'Re-seed the session from the canvas as it is now'
                      : 'Clear this notebook\'s kernel variables; the kernel keeps running'
                  "
                  @click="onResetSession"
                >
                  <i class="fa-solid fa-rotate-right nb-menu-icon"></i> Reset session
                </el-dropdown-item>
                <el-dropdown-item
                  v-if="kernelStatus.kind === 'stopped'"
                  :disabled="startingKernel"
                  @click="startKernel"
                >
                  <i class="fa-solid fa-play nb-menu-icon"></i> Start kernel
                </el-dropdown-item>
                <el-dropdown-item v-if="!flowId" divided @click="router.push(kernelsRoute)">
                  <i class="fa-solid fa-microchip nb-menu-icon"></i> Manage kernels…
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>

        <span class="nb-toolbar-divider" aria-hidden="true"></span>

        <button
          v-if="flowId"
          type="button"
          class="nb-btn"
          data-testid="nb-push"
          :title="pushTitle"
          :disabled="batchBusy || editorStore.isRunning"
          @click="onPush"
        >
          <i :class="pushing ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-upload'"></i>
          <span>Push</span>
        </button>
        <!-- Save split-button (canvas-style): Save + File menu (Save As / Rename / Delete) -->
        <div v-else class="nb-split" data-tutorial="save-btn">
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
                <el-dropdown-item @click="onSaveAs">
                  <i class="fa-solid fa-copy nb-menu-icon"></i> Save As…
                </el-dropdown-item>
                <el-dropdown-item :disabled="!store.activeTabId" @click="startRename()">
                  <i class="fa-solid fa-pen nb-menu-icon"></i> Rename
                </el-dropdown-item>
                <el-dropdown-item divided :disabled="!isPersisted" @click="onDelete">
                  <i class="fa-solid fa-trash nb-menu-icon"></i> Delete
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>

        <span v-if="batchLabel" class="nb-batch-progress">{{ batchLabel }}</span>

        <button
          type="button"
          class="nb-btn nb-btn--run nb-run-all"
          data-testid="nb-run-all"
          :title="runAllTitle"
          :disabled="batchBusy"
          @click="store.runAll()"
        >
          <i :class="batchBusy ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-forward'"></i>
          <span>Run all</span>
        </button>
      </div>
    </div>

    <!-- Why Python cells won't run and what to do; in flow mode, why syncing is refused. -->
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
        <router-link v-if="!flowId" :to="kernelsRoute" class="nb-banner__link">
          Manage kernels
        </router-link>
      </span>
    </div>

    <!-- Cells of the active notebook -->
    <div v-if="store.active" ref="hostRef" class="nb-cells">
      <div
        v-if="drag.indicatorTop.value !== null"
        class="nb-drop-line"
        :style="{ top: `${drag.indicatorTop.value}px` }"
        aria-hidden="true"
      ></div>

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
          :data-cell-id="cell.id"
          :owner-id="store.active.tabId"
          :index="idx"
          :cell-count="store.active.cells.length"
          :prior-cell-codes="priorCodes(idx)"
          :prior-cells="priorCells(idx)"
          :kernel-id="store.active.kernelId"
          :flow-id="store.active.sessionFlowId"
          :node-id="cellNodeId(cell.id)"
          :structural-disabled="structuralDisabled"
          :runtime="runtimeFor(cell.id)"
          :busy="batchBusy"
          :type-in-menu="!!flowId"
          :sync-state="flowId ? cellSyncState(cell) : null"
          :sync-error="flowId ? cellErrorMark(store.active, cell) : null"
          :active="cell.id === store.active.focusedCellId"
          :dragging="drag.draggingId.value === cell.id"
          @run="store.runCell(cell.id)"
          @run-advance="onRunAdvance(cell.id)"
          @update:code="(code: string) => store.setCellCode(cell.id, code)"
          @update:type="(t: CellType) => store.setCellType(cell.id, t)"
          @update:editing="(e: boolean) => onEditingChange(cell.id, e)"
          @move="(dir: -1 | 1) => onMoveKey(cell.id, dir)"
          @move-key="(dir: -1 | 1) => onMoveKey(cell.id, dir)"
          @drag-start="(ev: PointerEvent) => drag.onHandlePointerDown(cell.id, ev)"
          @remove="onRemove(cell.id)"
          @duplicate="onDuplicate(cell.id)"
          @insert-above="onInsertAt(idx)"
          @insert-below="onInsertAt(idx + 1)"
          @activate="store.setFocusedCell(cell.id)"
          @cursor="(pos: number) => store.setCellCursor(cell.id, pos)"
        >
          <template v-if="flowId && store.active.nodeIds?.[cell.id]?.length" #menu-extra>
            <el-dropdown-item
              data-action="run-on-canvas"
              :disabled="batchBusy || editorStore.isRunning"
              @click="previewOnCanvas(cell.id)"
            >
              <i class="fa-solid fa-diagram-project nb-menu-icon"></i> Run and preview on canvas
            </el-dropdown-item>
          </template>
        </CatalogNotebookCell>

        <!-- Hover-to-insert: a faint "+" appears between cells; click to add a
             Python cell at this position. -->
        <div
          v-if="idx < store.active.cells.length - 1"
          class="nb-insert-zone"
          :class="{ 'nb-insert-zone--disabled': structuralDisabled }"
          title="Add cell here"
          @click="onAddCell('python', idx)"
        >
          <span class="nb-insert-plus"><i class="fa-solid fa-plus"></i></span>
        </div>
      </template>

      <!-- Adds Python; switch type from the cell's selector or menu. -->
      <div class="nb-add-row">
        <button
          type="button"
          class="nb-add-btn"
          :disabled="structuralDisabled"
          @click="onAddCell('python')"
        >
          <i class="fa-solid fa-plus"></i>
          <span>Add cell</span>
        </button>
      </div>
    </div>

    <div class="nb-sr-only" role="status" aria-live="polite">{{ announcement }}</div>

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

    <NotebookHelp
      v-if="showHelp"
      :flow-mode="!!flowId"
      :kernel-mode="!!flowId && !!store.active?.kernelId"
      @close="showHelp = false"
    />
    <CreateKernelDialog
      v-if="flowId"
      v-model="createKernelVisible"
      :suggestion="notebookKernelSuggestion"
      @created="onKernelCreated"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, h, nextTick, onMounted, onBeforeUnmount, watch } from "vue";
import debounce from "lodash/debounce";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox, type TabPaneName } from "element-plus";
import {
  useNotebookStore,
  cellNodeId,
  flowCellSyncState,
  planReview,
  registerFlowNotebookHooks,
  cellErrorMark,
  SYNC_NEEDS_ADMIN,
} from "../../stores/notebook-store";
import { useCatalogStore } from "../../stores/catalog-store";
import { useWritableNamespaces } from "../../composables/useWritableNamespaces";
import { catalogSaveErrorMessage } from "../../composables/saveError";
import { KernelApi } from "../../api/kernel.api";
import { NotebookApi } from "../../api/notebook.api";
import { useEditorStore } from "../../stores/editor-store";
import { useNodeStore } from "../../stores/column-store";
import { useDrawerStore } from "../../stores/drawer-store";
import { useFlowStore } from "../../stores/flow-store";
import { useResultsStore } from "../../stores/results-store";
import { whenMutationsIdle } from "../../services/axios.config";
import { flushPendingEdits } from "../../services/mutationChannel";
import { currentNodeId, seedNodeId } from "../../composables/useDragAndDrop";
import CatalogNotebookCell from "../../components/notebook/CatalogNotebookCell.vue";
import CreateKernelDialog from "../../components/kernel/CreateKernelDialog.vue";
import NotebookHelp from "../../components/notebook/NotebookHelp.vue";
import { cellMoveAnnouncement } from "../../components/notebook/cellOperations";
import { cellPresentation } from "../../components/notebook/cellPresentation";
import { cellSelector, focusCell, ownerIdForNotebook } from "../../components/notebook/editorViews";
import { attachDataframeSchemas } from "../../components/notebook/useDataframeSchemas";
import { scanCatalogRefs } from "../../components/nodes/node-types/elements/pythonScript/dataframeSchemaInference";
import { useCellDrag } from "../../components/notebook/useCellDrag";
import { getCellHistory } from "../../components/notebook/useCellHistory";
import {
  batchProgress,
  cellRuntime,
  isBatchActive,
} from "../../components/notebook/notebookRuntimeState";
import {
  kernelStatusNeedsAttention,
  resolveNotebookKernelStatus,
} from "../../components/notebook/notebookKernelStatus";
import type { CellOperation } from "../../components/notebook/cellOperations";
import type { CellType, NotebookCellModel } from "../../components/notebook/types";
import type { KernelInfo, KernelSuggestion } from "../../types/kernel.types";

const KERNEL_POLL_MS = 5000;
const NO_KERNEL = "__no_kernel__";

/** A kernel can run the canvas notebook when it has flowfile installed or is a notebook image. */
const runsNotebook = (k: KernelInfo): boolean =>
  k.packages.some((p) => /^flowfile\s*($|[=<>!~[;@ ])/i.test(p.trim())) ||
  (k.custom_image ?? "").includes("notebook");

/** With `flowId` the panel is that flow's canvas notebook: one ephemeral tab rendered from the canvas. */
const props = defineProps<{ flowId?: number }>();

const store = useNotebookStore();
const editorStore = useEditorStore();
const catalogTabs = computed(() => store.openNotebooks.filter((n) => n.flowId == null));
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
const startingKernel = ref(false);
const resetPending = ref(false);
let pollTimer: ReturnType<typeof setInterval> | null = null;
let unregisterFlowHooks: (() => void) | null = null;

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
  return store.active?.kernelId ?? (props.flowId ? "No kernel" : "");
});

const pickerKernels = computed(() =>
  props.flowId ? kernels.value.filter(runsNotebook) : kernels.value,
);

function onKernelChange(value: string | null | undefined) {
  store.setKernel(!value || value === NO_KERNEL ? null : value);
}

const createKernelVisible = ref(false);
const kernelSelectRef = ref<{ blur: () => void } | null>(null);
function openCreateKernel() {
  // The footer lives inside the dropdown, so close it or it floats above the dialog.
  kernelSelectRef.value?.blur();
  createKernelVisible.value = true;
}
const notebookKernelSuggestion = computed<KernelSuggestion>(() => ({
  config: {
    id: "notebook",
    name: "Notebook",
    packages: [__APP_VERSION__ ? `flowfile==${__APP_VERSION__}` : "flowfile"],
    cpu_cores: 2,
    memory_gb: 4,
    gpu: false,
    image_flavour: "lite",
    custom_image: null,
    mounted_folders: [],
  },
  covered_by_flavour: [],
  flavour_image_available: null,
}));

async function onKernelCreated(kernel: KernelInfo) {
  store.setKernel(kernel.id);
  await loadKernels();
}

/** A kernel session keeps the stale badges; the canvas sync state shows only when it failed. */
function cellSyncState(cell: NotebookCellModel) {
  const nb = store.active!;
  const state = flowCellSyncState(nb, cell);
  return nb.kernelId && state !== "error" ? null : state;
}

interface KernelBanner {
  tone: "info" | "warning" | "danger";
  icon: string;
  text: string;
}

// Only the "no kernel" nudge waits for Python cells; a bad selection is always worth saying.
const banner = computed<KernelBanner | null>(() => {
  const s = kernelStatus.value;
  if (props.flowId && store.active?.syncForbidden) {
    return { tone: "warning", icon: "fa-solid fa-lock", text: SYNC_NEEDS_ADMIN };
  }
  if (props.flowId && !store.active?.kernelId) return null;
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

const runAllTitle = computed(() => {
  if (!props.flowId) return undefined;
  if (store.active?.kernelId)
    return "Run every cell in the kernel session; the canvas is unchanged";
  return store.active?.syncForbidden
    ? SYNC_NEEDS_ADMIN
    : "Sync the cells, run the flow on the canvas and refresh every cell";
});

const pushTitle = computed(() => {
  if (store.active?.syncForbidden) return SYNC_NEEDS_ADMIN;
  return store.active?.kernelId
    ? "Run the cells on the kernel and apply what they build to the canvas"
    : "Sync the cells to the canvas without running them";
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

// Inline rename on the active tab (double-click, or Save menu → Rename).
const renamingTabId = ref<string | null>(null);
const renameDraft = ref("");
const renameInputRef = ref<HTMLInputElement[] | HTMLInputElement | null>(null);
function startRename(tabId: string | null = store.activeTabId) {
  if (!tabId) return;
  if (tabId !== store.activeTabId) store.setActiveTab(tabId);
  renamingTabId.value = tabId;
  renameDraft.value = store.active?.name ?? "";
  void nextTick(() => {
    const el = Array.isArray(renameInputRef.value) ? renameInputRef.value[0] : renameInputRef.value;
    el?.focus();
    el?.select();
  });
}
function commitRename() {
  if (renamingTabId.value == null) return;
  const name = renameDraft.value.trim();
  if (name && name !== store.active?.name) store.setName(name);
  renamingTabId.value = null;
}
function cancelRename() {
  renamingTabId.value = null;
}
// Primer shows only while the notebook has no code yet; it hides as soon as you type.
const showPrimer = computed(
  () => !props.flowId && !!store.active && store.active.cells.every((c) => !c.code.trim()),
);

function priorCodes(idx: number): string[] {
  return store.active ? store.active.cells.slice(0, idx).map((c) => c.code) : [];
}

// Column inference only reads Python source, and it needs the cell id to date each assignment.
function priorCells(idx: number): { id: string; code: string }[] {
  if (!store.active) return [];
  return store.active.cells
    .slice(0, idx)
    .filter((c) => c.cellType === "python")
    .map((c) => ({ id: c.id, code: c.code }));
}

function pythonCellsOf(tabId: string): NotebookCellModel[] {
  const nb = store.openNotebooks.find((n) => n.tabId === tabId);
  return (nb?.cells ?? []).filter((c) => c.cellType === "python");
}

const schemaDetachers = new Map<string, () => void>();

// One attachment per open tab: the schema cache is owner-keyed and the kernel is read live.
watch(
  () =>
    (props.flowId
      ? store.openNotebooks.filter((n) => n.flowId === props.flowId)
      : catalogTabs.value
    ).map((n) => n.tabId),
  (tabIds) => {
    const live = new Set(tabIds);
    for (const [tabId, detach] of Array.from(schemaDetachers)) {
      if (live.has(tabId)) continue;
      detach();
      schemaDetachers.delete(tabId);
    }
    for (const tabId of tabIds) {
      if (schemaDetachers.has(tabId)) continue;
      schemaDetachers.set(
        tabId,
        attachDataframeSchemas(ownerIdForNotebook(tabId), () => {
          const nb = store.openNotebooks.find((n) => n.tabId === tabId);
          const flowId = nb?.flowId;
          const kernelId = nb?.kernelId ?? null;
          return {
            kernelId,
            flowId: nb?.sessionFlowId ?? 0,
            nodeId: 0,
            catalogRefs: () => pythonCellsOf(tabId).flatMap((c) => scanCatalogRefs(c.code)),
            fetchSchemas:
              flowId != null && kernelId
                ? () => NotebookApi.sessionSchemas({ flow_id: flowId, kernel_id: kernelId })
                : null,
          };
        }),
      );
    }
  },
  { immediate: true },
);

const hostRef = ref<HTMLElement | null>(null);
const announcement = ref("");

const activeOwnerId = computed(() =>
  store.activeTabId ? ownerIdForNotebook(store.activeTabId) : null,
);

const batchBusy = computed(() => !!activeOwnerId.value && isBatchActive(activeOwnerId.value));

const batchLabel = computed(() => {
  const progress = activeOwnerId.value ? batchProgress(activeOwnerId.value) : null;
  if (!progress?.total) return "";
  return `Running cell ${Math.min(progress.done + 1, progress.total)} of ${progress.total}`;
});

const structuralDisabled = computed(() => batchBusy.value);

function runtimeFor(cellId: string) {
  return activeOwnerId.value ? (cellRuntime(activeOwnerId.value, cellId) ?? null) : null;
}

const canUndo = computed(() =>
  store.activeTabId ? getCellHistory(ownerIdForNotebook(store.activeTabId)).canUndo.value : false,
);
const canRedo = computed(() =>
  store.activeTabId ? getCellHistory(ownerIdForNotebook(store.activeTabId)).canRedo.value : false,
);

function announce(move: { from: number; to: number; total: number }) {
  announcement.value = cellMoveAnnouncement(move.from, move.to, move.total);
}

function announceIfMove(op: CellOperation<NotebookCellModel> | null) {
  if (op?.kind === "move") {
    announce({ from: op.from, to: op.to, total: store.active?.cells.length ?? 0 });
  }
}

function onDropCell(cellId: string, targetIndex: number) {
  const r = store.moveCellToIndex(cellId, targetIndex);
  if (r) announce(r);
}

const drag = useCellDrag({
  getHost: () => hostRef.value,
  onCommit: onDropCell,
  isDisabled: () => structuralDisabled.value,
});

function onMoveKey(cellId: string, dir: -1 | 1) {
  const r = store.moveCell(cellId, dir);
  if (!r) return;
  announce(r);
  // The row re-renders, so re-focus the handle to keep repeated Alt+arrows working.
  void nextTick(() => {
    hostRef.value?.querySelector<HTMLElement>(`${cellSelector(cellId)} .nb-drag-handle`)?.focus();
  });
}

/** Focus after the DOM settles, but never steal it from a notebook tab the user switched to. */
function focusAfterTick(ownerTab: string, cellId: string | null) {
  if (!cellId) return;
  // A collapsed editor is display:none and cannot take the caret, so reveal it first.
  cellPresentation(ownerIdForNotebook(ownerTab), cellId).codeCollapsed = false;
  void nextTick(() => {
    if (store.activeTabId !== ownerTab) return;
    focusCell(ownerIdForNotebook(ownerTab), cellId, hostRef.value);
  });
}

/** Opening a markdown editor has to take the caret with it — Enter on the cell root is a
 * keyboard-only path with no textarea to click. */
function onEditingChange(cellId: string, editing: boolean) {
  store.setCellEditing(cellId, editing);
  const tab = store.activeTabId;
  if (editing && tab) focusAfterTick(tab, cellId);
}

function onRunAdvance(cellId: string) {
  const tab = store.activeTabId;
  const nb = store.active;
  if (!tab || !nb || structuralDisabled.value) return;
  const idx = nb.cells.findIndex((c) => c.id === cellId);
  if (idx < 0) return;
  // Advance on submit, Jupyter-style: the result is never waited for.
  if (idx < nb.cells.length - 1) focusAfterTick(tab, nb.cells[idx + 1].id);
  else focusAfterTick(tab, store.addCell("python")?.id ?? null);
  void store.runCell(cellId);
}

function onDuplicate(cellId: string) {
  const tab = store.activeTabId;
  if (!tab) return;
  focusAfterTick(tab, store.duplicateCell(cellId)?.id ?? null);
}

/** `index` is the new cell's final position, so "above" is `idx` and "below" is `idx + 1`. */
function onInsertAt(index: number) {
  const tab = store.activeTabId;
  if (!tab || structuralDisabled.value) return;
  focusAfterTick(tab, store.insertCellAt("python", index)?.id ?? null);
}

function onRemove(cellId: string) {
  const tab = store.activeTabId;
  if (!tab) return;
  focusAfterTick(tab, store.removeCell(cellId));
}

function onUndoCellAction() {
  announceIfMove(store.undoCellAction());
}

function onRedoCellAction() {
  announceIfMove(store.redoCellAction());
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
  if (props.flowId) {
    await store.loadFlowStatus();
    await openFlow();
    if (!store.kernelSessions) return;
  } else {
    store.ensureHydrated();
    await store.loadList();
  }
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
  unregisterFlowHooks?.();
  for (const detach of schemaDetachers.values()) detach();
  schemaDetachers.clear();
  if (!props.flowId) store.closeAllSessions();
  refreshSoon.cancel();
});

// A catalog panel never shows a flow's tab: fall back to a catalog tab when one was active.
watch(
  () => [store.hydrated, store.active?.flowId] as const,
  ([hydrated, activeFlowId]) => {
    if (props.flowId) {
      const own = store.openNotebooks.find((n) => n.flowId === props.flowId);
      if (own && activeFlowId !== props.flowId) store.setActiveTab(own.tabId);
    } else if (hydrated && activeFlowId != null) {
      if (catalogTabs.value.length) store.setActiveTab(catalogTabs.value[0].tabId);
      else store.newTab();
    }
  },
  { immediate: true },
);

const errorText = (e: any, fallback: string): string => {
  const detail = e?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) return detail.map((d) => d?.msg ?? String(d)).join("\n");
  return detail?.message ?? e?.message ?? fallback;
};

async function openFlow() {
  const flowId = props.flowId!;
  unregisterFlowHooks = registerFlowNotebookHooks(flowId, {
    prepare: prepareFlowAction,
    clientMaxNodeId: currentNodeId,
    confirm: (plan, trigger) =>
      ElMessageBox.confirm(
        h(
          "div",
          planReview(plan).map((line) => h("div", line)),
        ),
        trigger === "push" ? "Push to the canvas?" : "Run deletes canvas nodes",
        {
          confirmButtonText: trigger === "push" ? "Push" : "Sync and run",
          cancelButtonText: "Cancel",
          type: "warning",
        },
      ).then(
        () => true,
        () => false,
      ),
    pushed: (result) => {
      seedNodeId(result.max_node_id);
      useFlowStore().requestReload();
    },
    runStarted: () => {
      editorStore.isRunning = true;
    },
    runEnded: (info) => {
      editorStore.isRunning = false;
      if (info) useResultsStore().insertRunResult(info);
    },
  });
  try {
    await store.openFlowNotebook(flowId, `Flow ${flowId}`);
  } catch (e) {
    ElMessage.error(errorText(e, "Could not render the notebook"));
  }
}

const refreshSoon = debounce(async () => {
  await whenMutationsIdle();
  await store.refreshFlowNotebook(props.flowId!).catch(() => undefined);
}, 400);

// Canvas edits re-render the cells once the edit queue settles; layout moves keep the fingerprint.
watch(
  () => editorStore.graphVersion,
  () => {
    if (!props.flowId) return;
    refreshSoon();
  },
);

const pushing = ref(false);

/** Save and close the node settings drawer; false when its save was refused. */
async function closeSettingsDrawer(): Promise<boolean> {
  if (!(await editorStore.saveDrawerBeforeLeave())) return false;
  useNodeStore().nodeId = -1;
  editorStore.activeDrawerComponent = null;
  return true;
}

/** Before a sync or canvas run: flush canvas edits and save the drawer; never during a run. */
async function prepareFlowAction(): Promise<boolean> {
  if (!editorStore.isRunning) {
    await flushPendingEdits();
    await whenMutationsIdle();
    if (!(await closeSettingsDrawer())) return false;
    await whenMutationsIdle();
  }
  if (!editorStore.isRunning) return true;
  ElMessage.warning("The flow is running; try again when it finishes.");
  return false;
}

// Flow actions report through the tab's notice; a refused sync also keeps the admin banner up.
watch(
  () => store.active?.notice,
  (notice) => {
    if (!props.flowId || !notice) return;
    ElMessage({ type: notice.tone, message: notice.message });
  },
);

// A refused sync can stop at a cell that is scrolled out of view.
watch(
  () => store.active?.syncError?.cell_id,
  (cellId) => {
    if (!props.flowId || !cellId) return;
    hostRef.value?.querySelector(cellSelector(cellId))?.scrollIntoView({ block: "nearest" });
  },
);

async function onPush() {
  pushing.value = true;
  try {
    await store.syncFlowNotebook();
  } finally {
    pushing.value = false;
  }
}

/** Run a node cell (syncing first when needed), then show its node in the canvas preview. */
async function previewOnCanvas(cellId: string) {
  if (!(await store.runFlowCell(cellId))) return;
  const nodeId = store.active?.nodeIds?.[cellId]?.at(-1);
  // The run can report the node gone, and the preview's data route 500s on a missing node.
  if (nodeId == null || !useFlowStore().vueFlowInstance?.findNode?.(String(nodeId))) return;
  useDrawerStore().selectNodeForPreview(nodeId);
}

async function onResetSession() {
  if (resetPending.value) return;
  resetPending.value = true;
  try {
    await store.resetSession();
    ElMessage.success("Session reset");
  } catch (e: any) {
    ElMessage.error(e?.message ?? "Failed to reset the session");
  } finally {
    resetPending.value = false;
  }
}

function onTabChange(name: TabPaneName) {
  store.setActiveTab(String(name));
}

function onTabRemove(name: TabPaneName) {
  store.closeTab(String(name));
}

function onAddCell(command: string, afterIndex?: number) {
  const tab = store.activeTabId;
  if (!tab || structuralDisabled.value) return;
  focusAfterTick(tab, store.addCell(command as CellType, afterIndex)?.id ?? null);
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
}
.nb-toolbar {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-height: 40px;
  padding: 0 var(--spacing-3);
}
.nb-toolbar-spacer {
  flex: 1;
}
.nb-toolbar-divider {
  flex: none;
  width: 1px;
  height: 18px;
  background: var(--color-border-primary);
}
/* "+" beside the tabs: New / Open saved notebook. */
.nb-tab-add {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  padding: 0;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.nb-tab-add:hover {
  color: var(--color-primary);
  border-color: var(--color-primary);
}
.nb-tab-rename {
  width: 140px;
  height: 20px;
  padding: 0 4px;
  border: 1px solid var(--color-primary);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font: inherit;
  outline: none;
}
.nb-menu-icon {
  width: 16px;
  margin-right: 6px;
  text-align: center;
}

/* Ghost icon buttons for panel-local tools. */
.nb-tool-group {
  display: inline-flex;
  align-items: center;
  gap: 2px;
}
.nb-tool-group :deep(.el-dropdown) {
  display: inline-flex;
}
.nb-tool-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  padding: 0;
  border: none;
  border-radius: var(--border-radius-md);
  background: transparent;
  color: var(--color-text-tertiary);
  font-size: 13px;
  cursor: pointer;
  transition:
    background-color var(--transition-fast),
    color var(--transition-fast);
}
.nb-tool-btn:hover:not(:disabled),
.nb-tool-btn[aria-expanded="true"] {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}
.nb-tool-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

/* Chrome buttons: the designer header's 28px recipe. */
.nb-btn {
  display: inline-flex;
  flex: none;
  align-items: center;
  gap: 6px;
  height: 28px;
  padding: 0 10px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  box-shadow: var(--shadow-xs);
  color: var(--color-text-primary);
  font-family: inherit;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  white-space: nowrap;
  cursor: pointer;
  transition: all var(--transition-fast);
}
.nb-btn > i {
  font-size: 11px;
  color: var(--color-text-secondary);
}
.nb-btn:hover:not(:disabled) {
  background: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}
.nb-btn:active:not(:disabled) {
  transform: translateY(1px);
  box-shadow: none;
}
.nb-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
/* Run all mirrors the designer header's Run button (accent purple). */
.nb-btn--run {
  border-color: var(--color-accent-purple);
  background: var(--color-accent-purple);
  color: #fff;
}
.nb-btn--run > i {
  color: inherit;
}
.nb-btn--run:hover:not(:disabled) {
  border-color: var(--color-accent-purple-hover);
  background: var(--color-accent-purple-hover);
}
.nb-batch-progress {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  white-space: nowrap;
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
  gap: var(--spacing-1-5);
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
  color: var(--color-text-muted);
}
.nb-kernel-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  background-color: var(--color-gray-400);
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
  gap: var(--spacing-1-5);
  font-size: var(--font-size-xs);
  color: var(--color-primary);
  text-decoration: none;
}
.nb-kernel-footer-link:hover {
  text-decoration: underline;
}
.nb-kernel-footer-create {
  display: flex;
  margin-bottom: var(--spacing-1-5);
  padding: 0;
  border: none;
  background: none;
  cursor: pointer;
}

/* Canvas-style Save split-button (mirrors HeaderButtons .action-btn-split):
   neutral, unified, with a divider between the Save half and the caret. Sized
   to the small toolbar controls so it lines up with the inputs/buttons. */
.nb-split {
  display: inline-flex;
  align-items: stretch;
  height: 28px;
  box-shadow: var(--shadow-xs);
  border-radius: var(--border-radius-md);
}
.nb-split :deep(.el-dropdown) {
  display: inline-flex;
}
.nb-split-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1-5);
  height: 28px;
  padding: 0 var(--spacing-3);
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
  border-top-left-radius: var(--border-radius-md);
  border-bottom-left-radius: var(--border-radius-md);
  border-right: none;
}
.nb-split-btn--caret {
  justify-content: center;
  min-width: 22px;
  padding: 0 var(--spacing-2);
  border-top-right-radius: var(--border-radius-md);
  border-bottom-right-radius: var(--border-radius-md);
}
.nb-saveas {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.nb-saveas-label {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  margin-top: 4px;
}
.nb-saveas-hint {
  font-size: 12px;
  color: var(--color-warning-dark);
  margin: 4px 0 0;
}
.nb-dirty {
  color: var(--color-warning-dark);
  margin-left: 2px;
}
/* Notebook tabs match the designer's flow-tabs (FlowSelectorView): top-rounded,
   transparent inactive with a right divider, active tab raised onto the canvas
   surface with an accent top border — a little rounder and shorter. */
.nb-tabs {
  flex: 0 1 auto;
  min-width: 0;
  max-width: 60%;
  align-self: flex-end;
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
/* Soft tinted strip, inset from the panel edges. */
.nb-banner {
  display: flex;
  flex: 0 0 auto;
  align-items: center;
  gap: var(--spacing-2);
  margin: var(--spacing-3) var(--spacing-3) 0;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid color-mix(in srgb, var(--color-info) 25%, transparent);
  border-radius: var(--border-radius-lg);
  background: var(--color-info-light);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
}
.nb-banner > i {
  flex: none;
  color: var(--color-info);
}
.nb-banner--warning {
  border-color: color-mix(in srgb, var(--color-warning) 30%, transparent);
  background: var(--color-warning-light);
}
.nb-banner--warning > i {
  color: var(--color-warning);
}
.nb-banner--danger {
  border-color: color-mix(in srgb, var(--color-danger) 25%, transparent);
  background: var(--color-danger-light);
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
  font-size: var(--font-size-sm);
  color: var(--color-primary);
  text-decoration: none;
  white-space: nowrap;
}
.nb-banner__link:hover {
  text-decoration: underline;
}
.nb-cells {
  position: relative;
  flex: 1;
  overflow-y: auto;
  padding: var(--spacing-3) var(--spacing-3) var(--spacing-6);
  background: var(--color-background-primary);
}
/* Drop indicator for a cell drag; the list itself never reorders mid-gesture. */
.nb-drop-line {
  position: absolute;
  left: 0;
  right: 0;
  z-index: 2;
  height: 2px;
  border-radius: 1px;
  background: var(--color-accent);
  pointer-events: none;
}
.nb-sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
}
.nb-primer {
  margin-bottom: var(--spacing-3);
  padding: var(--spacing-3) 14px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  background: var(--color-background-secondary);
}
.nb-primer-title {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  margin-bottom: 4px;
}
.nb-primer-text {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  margin: 0 0 8px;
  line-height: 1.5;
}
.nb-primer-text kbd {
  font-family: var(--font-family-mono);
  font-size: 10.5px;
  padding: 1px 5px;
  border: 1px solid var(--color-border-secondary);
  border-bottom-width: 2px;
  border-radius: var(--border-radius-sm);
  background: var(--color-background-tertiary);
}
.nb-primer-code {
  margin: 0 0 8px;
  padding: 8px 10px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  overflow-x: auto;
}
.nb-primer-code code {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  white-space: pre;
}
.nb-primer-link {
  font-size: var(--font-size-sm);
  color: var(--color-primary);
  cursor: pointer;
}
.nb-primer-link:hover {
  text-decoration: underline;
}
.nb-add-row {
  display: flex;
  justify-content: center;
  margin-top: var(--spacing-3);
}
.nb-add-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 28px;
  padding: 0 12px;
  border: 1px dashed var(--color-border-secondary);
  border-radius: var(--border-radius-md);
  background: transparent;
  color: var(--color-text-secondary);
  font-family: inherit;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast);
}
.nb-add-btn > i {
  font-size: 10px;
}
.nb-add-btn:hover:not(:disabled) {
  border-style: solid;
  border-color: var(--color-accent);
  background: var(--color-accent-subtle);
  color: var(--color-accent-dark);
}
.nb-add-btn:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

/* Hover-to-insert zone between cells: a hairline with a small ghost "+" chip. */
.nb-insert-zone {
  position: relative;
  height: 16px;
  cursor: pointer;
}
.nb-insert-zone::before {
  content: "";
  position: absolute;
  top: 50%;
  left: var(--spacing-3);
  right: var(--spacing-3);
  height: 1px;
  background: var(--color-accent);
  opacity: 0;
  transition: opacity var(--transition-base) ease;
}
.nb-insert-plus {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 18px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-full);
  background: var(--color-background-primary);
  box-shadow: var(--shadow-xs);
  color: var(--color-accent-dark);
  font-size: 9px;
  opacity: 0;
  transition: opacity var(--transition-base) ease;
}
.nb-insert-zone:hover::before {
  opacity: 0.4;
}
.nb-insert-zone:hover .nb-insert-plus {
  opacity: 1;
}
.nb-insert-zone--disabled {
  pointer-events: none;
  opacity: 0;
}
</style>
