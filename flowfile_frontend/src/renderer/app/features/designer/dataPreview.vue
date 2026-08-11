<template>
  <!-- Loading Spinner -->
  <div v-if="isLoading" class="spinner-overlay">
    <div class="spinner"></div>
  </div>

  <!-- Table Container -->
  <div v-show="!isLoading" class="table-container">
    <!-- Placeholder when no step is selected -->
    <div v-if="currentNodeId == null" class="dp-empty">
      <span class="material-icons dp-empty-icon">ads_click</span>
      <p>Select a step to observe the data</p>
    </div>
    <template v-else>
      <!-- Tab Bar (only when artifacts exist for the node) -->
      <div v-if="nodeArtifacts" class="preview-tabs">
        <button
          class="preview-tab"
          :class="{ active: activeTab === 'data' }"
          @click="activeTab = 'data'"
        >
          Data
        </button>
        <button
          class="preview-tab"
          :class="{ active: activeTab === 'artifacts' }"
          @click="activeTab = 'artifacts'"
        >
          Artifacts
          <span class="dp-tab-badge">{{
            nodeArtifacts.published_count +
            nodeArtifacts.consumed_count +
            nodeArtifacts.deleted_count
          }}</span>
        </button>
      </div>

      <!-- Data Tab Content -->
      <div v-show="activeTab === 'data'" class="dp-tab-content">
        <!-- Output selector for multi-output nodes -->
        <div v-if="hasMultipleOutputs" class="output-selector">
          <span class="output-selector__label">Output:</span>
          <button
            v-for="output in nodeOutputs"
            :key="output.id"
            class="output-selector__button"
            :class="{ active: output.id === selectedOutputHandle }"
            :title="output.title"
            @click="selectOutput(output.id)"
          >
            <span
              v-if="output.title && output.label && output.title !== output.label"
              class="output-selector__letter"
              >{{ output.label }}</span
            >
            <span class="output-selector__name">{{
              output.title || output.label || output.id
            }}</span>
          </button>
        </div>

        <!-- AG Grid -->
        <ag-grid-vue
          ref="gridComponentRef"
          :default-col-def="defaultColDef"
          :column-defs="columnDefs"
          :suppress-field-dot-notation="true"
          class="ag-theme-balham dp-grid"
          :class="{ 'dp-grid--collapsed': showFetchButton }"
          :row-data="rowData"
          :overlay-no-rows-template="overlayNoRowsTemplate"
          row-selection="multiple"
          :rows-multi-select-with-click="true"
          :context="gridContext"
          @grid-ready="onGridReady"
          @body-scroll="onBodyScroll"
          @column-resized="closeStatsPanel"
          @column-moved="closeStatsPanel"
        />

        <div v-if="dataAvailable" class="dp-status-bar">
          showing {{ formatCount(sampleRowCount) }} of {{ formatCount(dataLength) }} rows ·
          {{ formatCount(columnLength) }} columns
        </div>

        <div v-if="showFetchButton" class="fetch-data-section">
          <p>Step has not stored any data yet. Click here to trigger a run for this node</p>
          <button
            class="fetch-data-button"
            :disabled="nodeStore.isRunning || isFetching"
            @click="handleFetchData"
          >
            <span v-if="!nodeStore.isRunning && !isFetching">Fetch Data</span>
            <span v-else>Fetching...</span>
          </button>
        </div>
      </div>

      <!-- Artifacts Tab Content -->
      <ArtifactsPanel
        v-if="activeTab === 'artifacts' && nodeArtifacts"
        :summary="nodeArtifacts"
        :flow-id="props.flowId || nodeStore.flow_id"
      />
    </template>
  </div>

  <!-- Column stats popover: teleported so AG Grid header virtualization can't
       tear it down; anchored to the rect captured at ⓘ-click time. Mounting
       waits out a grace window so a fast reply opens it in its final state. -->
  <Teleport to="body">
    <ColumnStatsPanel
      v-if="statsPanelOpen && activeStatsColumn && statsAnchorRect"
      :column-name="activeStatsColumn"
      :data-type="activeStatsDataType"
      :stats="statsData"
      :loading="statsLoading"
      :error-kind="statsErrorKind"
      :error-detail="statsErrorDetail"
      :anchor-rect="statsAnchorRect"
      @close="closeStatsPanel"
      @retry="onStatsRetry"
    />
  </Teleport>
</template>

<script setup lang="ts">
// TODO(refactor): large component. Plan to extract:
//   - DataTabs.vue, OutputSelector.vue
//   - useTableData composable: AG Grid setup + refresh
import { ref, onMounted, onUnmounted, computed, watch } from "vue";
import ArtifactsPanel from "./ArtifactsPanel.vue";
import debounce from "lodash/debounce";
import { TableExample, FileColumn } from "../../components/nodes/baseNode/nodeInterfaces";
import { NodeApi } from "../../api/node.api";
import { useNodeStore } from "../../stores/column-store";
import { useFlowStore } from "../../stores/flow-store";
import { useFlowExecution } from "./composables/useFlowExecution";
import ColumnStatsHeader from "./dataPreview/ColumnStatsHeader.vue";
import ColumnStatsPanel from "./dataPreview/ColumnStatsPanel.vue";
import { formatCount, totalRowCount } from "./dataPreview/columnQuality";
import { classifyStatsError, statsCacheKey, type StatsVerdict } from "./dataPreview/statsRequest";
import { AgGridVue } from "@ag-grid-community/vue3";
import { GridApi, BodyScrollEvent } from "@ag-grid-community/core";
import { ModuleRegistry } from "@ag-grid-community/core";
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";
import { DEFAULT_OUTPUT_HANDLE } from "../../utils/outputHandle";
import "@ag-grid-community/styles/ag-grid.css";
import "@ag-grid-community/styles/ag-theme-balham.css";

ModuleRegistry.registerModules([ClientSideRowModelModule]);

const isLoading = ref(false);
const activeTab = ref<"data" | "artifacts">("data");
const flowStore = useFlowStore();
const rowData = ref<Record<string, any>[] | Record<string, never>>([]);
const showTable = ref(false);
const nodeStore = useNodeStore();
const dataPreview = ref<TableExample>();
const dataAvailable = ref(false);
// null ⇒ the backend doesn't know the total row count (shown as "? rows").
const dataLength = ref<number | null>(null);
const columnLength = ref(0);
const gridApi = ref<GridApi | null>(null);
// Component ref on <ag-grid-vue> — `.value.$el` gives us this grid's root DOM
// node so the window-level Cmd+C/A handler can scope itself to *this* grid
// instead of any element matching `.ag-theme-balham`.
const gridComponentRef = ref<{ $el?: HTMLElement } | null>(null);
const columnDefs = ref([{}]);
const showFetchButton = ref(false);
// True from the moment Fetch Data is clicked until the grid is reloaded, so the
// button can't be clicked again in the gap between run-complete and data landing.
const isFetching = ref(false);
const currentNodeId = ref<number | null>(null);
const selectedOutputHandle = ref<string>(DEFAULT_OUTPUT_HANDLE);

// Available output handles for the currently previewed node, read from the
// VueFlow node's data.outputs (populated by useDragAndDrop's buildOutputHandles).
const nodeOutputs = computed(() => {
  if (currentNodeId.value == null) return [];
  const vfInstance = flowStore.vueFlowInstance;
  if (!vfInstance) return [];
  const vfNode = vfInstance.findNode(String(currentNodeId.value));
  return (vfNode?.data?.outputs as Array<{ id: string; label?: string; title?: string }>) ?? [];
});

const hasMultipleOutputs = computed(() => nodeOutputs.value.length > 1);

async function selectOutput(handle: string) {
  if (handle === selectedOutputHandle.value) return;
  selectedOutputHandle.value = handle;
  if (currentNodeId.value != null) {
    await downloadData(currentNodeId.value);
  }
}

interface Props {
  hideTitle?: boolean;
  flowId?: number;
  // The node to preview (null ⇒ "select a step" placeholder). Reactive: the
  // dock just binds this; no imperative downloadData() call from the host.
  nodeId?: number | null;
  // Bump to force a re-fetch of the same node (e.g. re-clicking an empty node).
  refreshToken?: number;
  // Only fetch while the Data tab is actually shown (the host gates this).
  active?: boolean;
}

const props = withDefaults(defineProps<Props>(), {
  hideTitle: true,
  flowId: undefined,
  nodeId: null,
  refreshToken: 0,
  active: true,
});

// Use the flow execution composable with persistent polling for node fetches.
// Getter form so Save As re-keying nodeStore.flow_id doesn't leave us polling
// the old (template) id.
const { triggerNodeFetch, isPollingActive } = useFlowExecution(
  () => props.flowId || nodeStore.flow_id,
  { interval: 2000, enabled: true },
  {
    persistPolling: true, // Keep polling even when component unmounts
    pollingKey: `table_flow_${props.flowId || nodeStore.flow_id}`,
  },
);

const overlayNoRowsTemplate = computed(() => {
  if (showFetchButton.value) {
    return "<span></span>";
  }
  // Return undefined to use AG-Grid's default "No Rows To Show" message
  return undefined;
});

const nodeArtifacts = computed(() => {
  if (currentNodeId.value == null) return null;
  return flowStore.getNodeArtifactSummary(currentNodeId.value);
});

watch(
  () => currentNodeId.value,
  () => {
    activeTab.value = "data";
    selectedOutputHandle.value = DEFAULT_OUTPUT_HANDLE;
  },
);

// If the current node's output set shrinks (e.g. user removed a split) and the
// previously selected handle no longer exists, fall back to the default so the
// next preview fetch isn't sent with a stale handle.
watch(
  () => nodeOutputs.value.map((o) => o.id).join(","),
  () => {
    if (
      selectedOutputHandle.value !== DEFAULT_OUTPUT_HANDLE &&
      !nodeOutputs.value.some((o) => o.id === selectedOutputHandle.value)
    ) {
      selectedOutputHandle.value = DEFAULT_OUTPUT_HANDLE;
    }
  },
);

// No `filter`: the header menu (and its column filter) was deliberately dropped.
const defaultColDef = {
  editable: true,
  sortable: true,
  resizable: true,
  headerComponent: ColumnStatsHeader,
};

// Cells with tab/newline/carriage-return/quote chars need Excel-style quoting,
// otherwise they corrupt the TSV (a tab inside a value becomes a column break,
// a newline becomes a row break). Wrap in double-quotes and double any existing
// quotes — matches what Excel and Google Sheets emit when copying.
const serializeCell = (v: unknown): string => {
  if (v === null || v === undefined) return "";
  const raw = typeof v === "object" ? JSON.stringify(v) : String(v);
  if (/[\t\n\r"]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
};

const buildTsvFromRows = (rows: Record<string, any>[]): string => {
  const cols = (columnDefs.value as Array<{ field?: string; headerName?: string }>).filter(
    (c) => c && c.field,
  );
  if (!cols.length || !rows.length) return "";
  const headerLine = cols.map((c) => serializeCell(c.headerName ?? c.field ?? "")).join("\t");
  const dataLines = rows.map((row) =>
    cols.map((c) => serializeCell(row[c.field as string])).join("\t"),
  );
  return [headerLine, ...dataLines].join("\n");
};

const onGridReady = (params: { api: GridApi }) => {
  gridApi.value = params.api;

  if (showFetchButton.value) {
    gridApi.value.hideOverlay();
  }
};

// --- Column statistics popover state.
// The panel is owned here (not by the header cells — AG Grid virtualizes and
// destroys those); the anchor rect is captured at ⓘ-click time. statsCache is
// cleared whenever the preview reloads, since that's exactly when stats go stale.
const statsCache = new Map<string, StatsVerdict>();
const activeStatsColumn = ref<string | null>(null);
const statsAnchorRect = ref<DOMRect | null>(null);
const statsData = ref<FileColumn | null>(null);
const statsLoading = ref(false);
const statsErrorKind = ref<"not-run" | "error" | null>(null);
const statsErrorDetail = ref<string | null>(null);
let statsRequestSeq = 0;

// Grace window before the popover appears: a reply that beats it opens the panel
// straight into its final body, so a fast 409 never flashes a skeleton.
const STATS_OPEN_DELAY_MS = 120;
// activeStatsColumn means "the user asked for this column" (set synchronously,
// so toggle-off and outside-click still work); this means "the panel is mounted".
const statsPanelOpen = ref(false);
let statsOpenTimer: ReturnType<typeof setTimeout> | null = null;

function clearStatsOpenTimer() {
  if (statsOpenTimer !== null) {
    clearTimeout(statsOpenTimer);
    statsOpenTimer = null;
  }
}

function scheduleStatsPanelOpen() {
  clearStatsOpenTimer();
  statsOpenTimer = setTimeout(() => {
    statsOpenTimer = null;
    statsPanelOpen.value = true;
  }, STATS_OPEN_DELAY_MS);
}

function openStatsPanelNow() {
  clearStatsOpenTimer();
  statsPanelOpen.value = true;
}

const activeStatsDataType = computed(
  () => dataPreview.value?.table_schema?.find((c) => c.name === activeStatsColumn.value)?.data_type,
);

const sampleRowCount = computed(() => (Array.isArray(rowData.value) ? rowData.value.length : 0));

function onRequestColumnStats(columnName: string, anchorRect: DOMRect) {
  const wasActive = activeStatsColumn.value === columnName;
  // Switching columns re-runs the open sequence, so the panel never shows one
  // column's body under another's title.
  closeStatsPanel();
  if (wasActive) return;
  activeStatsColumn.value = columnName;
  statsAnchorRect.value = anchorRect;
  void fetchColumnStats(columnName);
}

function applyStatsVerdict(verdict: StatsVerdict) {
  if (verdict.kind === "ok") {
    statsData.value = verdict.stats;
    statsErrorKind.value = null;
    statsErrorDetail.value = null;
    // The stats pass counted the whole table — fill in an unknown footer count.
    if (dataLength.value == null) dataLength.value = totalRowCount(verdict.stats);
  } else {
    statsData.value = null;
    statsErrorKind.value = "not-run";
    statsErrorDetail.value = verdict.detail;
  }
  statsLoading.value = false;
}

async function fetchColumnStats(columnName: string) {
  const nodeId = currentNodeId.value;
  if (nodeId == null) return;
  const seq = ++statsRequestSeq;
  const cacheKey = statsCacheKey(nodeId, selectedOutputHandle.value, columnName);
  const cached = statsCache.get(cacheKey);
  if (cached) {
    applyStatsVerdict(cached);
    openStatsPanelNow();
    return;
  }
  statsLoading.value = true;
  statsErrorKind.value = null;
  statsErrorDetail.value = null;
  statsData.value = null;
  scheduleStatsPanelOpen();
  try {
    const stats = await NodeApi.getColumnStats(
      props.flowId || nodeStore.flow_id,
      nodeId,
      columnName,
      selectedOutputHandle.value,
    );
    if (seq !== statsRequestSeq) return;
    const verdict: StatsVerdict = { kind: "ok", stats };
    statsCache.set(cacheKey, verdict);
    applyStatsVerdict(verdict);
  } catch (error) {
    if (seq !== statsRequestSeq) return;
    const failure = classifyStatsError(error);
    if (failure.kind === "not-run") {
      const verdict: StatsVerdict = { kind: "not-run", detail: failure.detail };
      statsCache.set(cacheKey, verdict);
      applyStatsVerdict(verdict);
    } else {
      // Transient: not cached, so Retry stays meaningful.
      statsErrorKind.value = "error";
      statsErrorDetail.value = null;
    }
  } finally {
    if (seq === statsRequestSeq) {
      statsLoading.value = false;
      openStatsPanelNow();
    }
  }
}

function onStatsRetry() {
  if (activeStatsColumn.value) void fetchColumnStats(activeStatsColumn.value);
}

function closeStatsPanel() {
  statsRequestSeq += 1;
  clearStatsOpenTimer();
  statsPanelOpen.value = false;
  activeStatsColumn.value = null;
  statsAnchorRect.value = null;
  statsData.value = null;
  statsLoading.value = false;
  statsErrorKind.value = null;
  statsErrorDetail.value = null;
}

function clearColumnStats() {
  statsCache.clear();
  closeStatsPanel();
}

// Header cells reach the panel through grid context (see ColumnStatsHeader.vue).
const gridContext = { onRequestColumnStats };

function onBodyScroll(event: BodyScrollEvent) {
  // Horizontal scroll moves the header cells away from the captured anchor.
  if (event.direction === "horizontal") closeStatsPanel();
}

async function downloadData(nodeId: number) {
  clearColumnStats();
  // Spinner only when the node actually changes; a same-node refresh updates in
  // place so it doesn't blank-and-reappear (flicker).
  const isNodeSwitch = nodeId !== currentNodeId.value;
  try {
    if (isNodeSwitch) isLoading.value = true;
    showFetchButton.value = false;
    currentNodeId.value = nodeId;

    let resp = await nodeStore.getTableExample(
      nodeStore.flow_id,
      nodeId,
      selectedOutputHandle.value,
    );

    if (resp) {
      dataPreview.value = resp;
      columnDefs.value = (dataPreview.value.table_schema ?? []).map((item) => ({
        field: item.name,
        headerName: item.name,
        resizable: true,
        headerComponentParams: { dataType: item.data_type },
      }));

      if (resp.has_example_data === false) {
        showFetchButton.value = true;
        rowData.value = [];
        showTable.value = true;
        dataAvailable.value = false;
      } else {
        if (dataPreview.value) {
          rowData.value = dataPreview.value.data;
          dataLength.value = dataPreview.value.number_of_records;
          columnLength.value = dataPreview.value.number_of_columns;
        }
        showTable.value = true;
        dataAvailable.value = true;
        showFetchButton.value = false;
      }
    }
  } finally {
    isLoading.value = false;
  }
}

async function handleFetchData() {
  if (currentNodeId.value !== null) {
    try {
      if (isFetching.value || isPollingActive(`node_${currentNodeId.value}`)) {
        console.log("Fetch already in progress for this node");
        return;
      }

      isFetching.value = true;
      await triggerNodeFetch(currentNodeId.value, { focusResultPanels: false });

      // Polling is persistent, so poll for completion. Keep isFetching held
      // until the grid is actually reloaded — the run flips isRunning off ~1s
      // before the data lands, and without this the button would be clickable
      // again in that gap even though the data is already fetched.
      const checkInterval = setInterval(async () => {
        if (!isPollingActive(`node_${currentNodeId.value}`)) {
          clearInterval(checkInterval);
          try {
            await downloadData(currentNodeId.value!);
          } finally {
            isFetching.value = false;
          }
        }
      }, 250);

      // Safety timeout to prevent infinite checking
      setTimeout(() => {
        clearInterval(checkInterval);
        isFetching.value = false;
      }, 60000); // 1 minute max
    } catch (error) {
      console.error("Error fetching data:", error);
      isFetching.value = false;
    }
  }
}

function removeData() {
  clearColumnStats();
  isLoading.value = false;
  rowData.value = [];
  showTable.value = false;
  dataAvailable.value = false;
  dataLength.value = null;
  columnDefs.value = [{}];
  showFetchButton.value = false;
  currentNodeId.value = null;
}

// Reactive entry point: the host binds :node-id / :refresh-token / :active and we
// load (or clear) accordingly. Debounced so rapid node clicks coalesce into one
// fetch; gated on `active` so we don't fetch while another tab (e.g. Logs) is shown.
const debouncedDownload = debounce((id: number) => downloadData(id), 150);
watch(
  () => [props.nodeId, props.refreshToken, props.active] as const,
  () => {
    if (props.nodeId == null) {
      debouncedDownload.cancel();
      removeData();
      return;
    }
    if (props.active) {
      // Raise the spinner up-front on a node switch so the debounce window never
      // flashes the previous node's data (or the placeholder) before loading.
      if (props.nodeId !== currentNodeId.value) isLoading.value = true;
      debouncedDownload(props.nodeId);
    } else {
      debouncedDownload.cancel();
    }
  },
  { immediate: true },
);

const windowKeyHandler = async (e: KeyboardEvent) => {
  const mod = e.ctrlKey || e.metaKey;
  if (!mod) return;

  const key = e.key.toLowerCase();
  if (key !== "c" && key !== "a") return;

  const target = e.target as HTMLElement | null;
  // Don't fight the browser when the user is in a text input or editor.
  if (
    target &&
    (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable)
  ) {
    return;
  }
  // Only act when focus is inside *this* grid — scope by component-rooted DOM
  // node, not by theme class, so a second AG Grid mounted elsewhere doesn't
  // co-fire. If the API isn't ready yet, the grid isn't usable; bail rather
  // than swallow the user's keystroke.
  if (!gridApi.value) return;
  const gridRoot = gridComponentRef.value?.$el;
  if (!gridRoot || !target || !gridRoot.contains(target)) return;

  if (key === "a") {
    gridApi.value.selectAll();
    e.preventDefault();
    return;
  }

  // Cmd/Ctrl+C → copy selected rows as TSV.
  const selected = gridApi.value.getSelectedRows();
  if (!selected.length) return;

  const tsv = buildTsvFromRows(selected);
  if (!tsv) return;

  // preventDefault must run before the await (post-await it's a no-op and the
  // copy event would fall through to the canvas's document-level handler).
  e.preventDefault();
  try {
    await navigator.clipboard.writeText(tsv);
  } catch {
    // Clipboard write rejected (permissions, insecure context).
  }
};

// Dismiss the stats panel on Escape or any press outside it. Capture-phase
// mousedown so a click that AG Grid swallows still closes the panel; clicks on
// an ⓘ button are left to onRequestColumnStats (which toggles).
const statsOutsideHandler = (e: MouseEvent) => {
  if (!activeStatsColumn.value) return;
  const target = e.target as HTMLElement | null;
  if (target?.closest(".column-stats-panel") || target?.closest(".dp-col-header__info")) return;
  closeStatsPanel();
};

const statsKeyHandler = (e: KeyboardEvent) => {
  if (e.key === "Escape" && activeStatsColumn.value) closeStatsPanel();
};

// A window resize reflows the grid, so the click-captured anchor rect is stale.
const statsResizeHandler = () => {
  if (activeStatsColumn.value) closeStatsPanel();
};

// A finished run replaces node results without reloading this preview.
watch(
  () => nodeStore.isRunning,
  (running, wasRunning) => {
    if (wasRunning && !running) clearColumnStats();
  },
);

onMounted(() => {
  window.addEventListener("keydown", windowKeyHandler);
  window.addEventListener("keydown", statsKeyHandler);
  window.addEventListener("mousedown", statsOutsideHandler, true);
  window.addEventListener("resize", statsResizeHandler);
});

onUnmounted(() => {
  window.removeEventListener("keydown", windowKeyHandler);
  window.removeEventListener("keydown", statsKeyHandler);
  window.removeEventListener("mousedown", statsOutsideHandler, true);
  window.removeEventListener("resize", statsResizeHandler);
  clearStatsOpenTimer();
  debouncedDownload.cancel();
});
</script>

<style>
.spinner-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: var(--color-background-primary);
  opacity: 0.9;
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

.spinner {
  width: 50px;
  height: 50px;
  border: 5px solid var(--color-border-primary);
  border-top: 5px solid var(--color-accent);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}

.table-container {
  display: flex;
  flex-direction: column;
  height: 100%;
  width: 100%;
  position: relative;
}

/* Placeholder shown when no step is selected. */
.dp-empty {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  color: var(--color-text-tertiary, var(--color-text-secondary));
}

.dp-empty-icon {
  font-size: 30px;
  opacity: 0.55;
}

.dp-empty p {
  margin: 0;
  font-size: 13px;
}

/* Fetch Data Section Styles */
.fetch-data-section {
  padding: 20px;
  text-align: center;
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-top: none;
  border-radius: 0 0 8px 8px;
}

.fetch-data-section p {
  color: var(--color-text-secondary);
  font-size: 14px;
  margin-bottom: 12px;
}

.fetch-data-button {
  background-color: var(--color-button-secondary);
  color: var(--color-text-inverse);
  border: none;
  padding: 8px 20px;
  font-size: 14px;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s ease;
  position: relative;
}

.fetch-data-button:hover:not(:disabled) {
  background-color: var(--color-button-secondary-hover);
  transform: translateY(-1px);
  box-shadow: var(--shadow-md);
}

.fetch-data-button:active:not(:disabled) {
  transform: translateY(0);
}

.fetch-data-button:disabled {
  background-color: var(--color-button-secondary-light);
  cursor: not-allowed;
  opacity: 0.7;
}

/* The grid fills the dock via flex (not an absolute px height), so it tracks
   dock resizes and leaves room for the status bar / fetch section below. */
.dp-grid {
  width: 100%;
  flex: 1 1 0;
  min-height: 0;
}

.dp-grid--collapsed {
  flex: 0 0 80px;
}

.dp-status-bar {
  flex-shrink: 0;
  padding: 3px 10px;
  border-top: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  font-size: 11px;
}

/* AG Grid Theme Customization */
.ag-theme-balham {
  max-width: 100%;
  position: relative;
  --ag-background-color: var(--color-background-primary);
  --ag-odd-row-background-color: var(--color-background-primary);
  --ag-row-background-color: var(--color-background-primary);
  --ag-header-background-color: var(--color-background-secondary);
  --ag-header-foreground-color: var(--color-text-primary);
  --ag-foreground-color: var(--color-text-primary);
  --ag-border-color: var(--color-border-primary);
  --ag-secondary-foreground-color: var(--color-text-secondary);
  --ag-row-hover-color: var(--color-background-hover);
  --ag-selected-row-background-color: var(--color-background-selected);
}

/* ============================================================
   Tab Bar
   ============================================================ */

.preview-tabs {
  display: flex;
  gap: 0;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
  flex-shrink: 0;
}

.preview-tab {
  padding: 6px 14px;
  border: none;
  background: transparent;
  cursor: pointer;
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
  border-bottom: 2px solid transparent;
  transition: all 0.15s ease;
  display: flex;
  align-items: center;
  gap: 5px;
}

.preview-tab:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}

.preview-tab.active {
  color: var(--color-text-primary);
  border-bottom-color: var(--color-accent, #6366f1);
}

.dp-tab-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 16px;
  height: 16px;
  padding: 0 4px;
  border-radius: 8px;
  font-size: 10px;
  font-weight: 600;
  background: rgba(99, 102, 241, 0.15);
  color: #6366f1;
}

.dp-tab-content {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  position: relative;
}

/* ============================================================
   Output Selector (multi-output nodes)
   ============================================================ */

.output-selector {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 10px;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
  flex-shrink: 0;
  flex-wrap: wrap;
}

.output-selector__label {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-right: 2px;
}

.output-selector__button {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 8px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: 12px;
  cursor: pointer;
  transition: all 0.12s ease;
}

.output-selector__button:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.output-selector__button.active {
  background: var(--color-accent, #6366f1);
  border-color: var(--color-accent, #6366f1);
  color: var(--color-text-inverse, #fff);
}

.output-selector__letter {
  display: inline-flex;
  justify-content: center;
  align-items: center;
  width: 18px;
  height: 18px;
  background: rgba(0, 0, 0, 0.08);
  border-radius: 3px;
  font-weight: 600;
  font-size: 11px;
}

.output-selector__button.active .output-selector__letter {
  background: rgba(255, 255, 255, 0.2);
}

.output-selector__name {
  font-weight: 500;
}
</style>
