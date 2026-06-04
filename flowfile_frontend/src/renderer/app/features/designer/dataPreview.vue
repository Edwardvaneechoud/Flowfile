<template>
  <!-- Loading Spinner -->
  <div v-if="isLoading" class="spinner-overlay">
    <div class="spinner"></div>
  </div>

  <!-- Table Container -->
  <div v-show="!isLoading" class="table-container">
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
          nodeArtifacts.published_count + nodeArtifacts.consumed_count + nodeArtifacts.deleted_count
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
          <span class="output-selector__letter">{{ output.label || output.id }}</span>
          <span v-if="output.title" class="output-selector__name">{{ output.title }}</span>
        </button>
      </div>

      <!-- AG Grid -->
      <ag-grid-vue
        ref="gridComponentRef"
        :default-col-def="defaultColDef"
        :column-defs="columnDefs"
        class="ag-theme-balham"
        :row-data="rowData"
        :style="{ width: '100%', height: gridHeightComputed }"
        :overlay-no-rows-template="overlayNoRowsTemplate"
        row-selection="multiple"
        :rows-multi-select-with-click="true"
        @grid-ready="onGridReady"
      />

      <div v-if="showFetchButton" class="fetch-data-section">
        <p>Step has not stored any data yet. Click here to trigger a run for this node</p>
        <button class="fetch-data-button" :disabled="nodeStore.isRunning" @click="handleFetchData">
          <span v-if="!nodeStore.isRunning">Fetch Data</span>
          <span v-else>Fetching...</span>
        </button>
      </div>
    </div>

    <!-- Artifacts Tab Content -->
    <div v-if="activeTab === 'artifacts' && nodeArtifacts" class="dp-tab-content artifacts-panel">
      <div v-if="nodeArtifacts.kernel_id" class="artifact-section-meta">
        Kernel: <code>{{ nodeArtifacts.kernel_id }}</code>
      </div>

      <div v-if="nodeArtifacts.published.length > 0" class="artifact-section">
        <div class="artifact-section-header">Published</div>
        <table class="artifact-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Module</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="art in nodeArtifacts.published" :key="art.name">
              <td class="artifact-name">{{ art.name }}</td>
              <td>{{ art.type_name || "-" }}</td>
              <td class="artifact-module">{{ art.module || "-" }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="nodeArtifacts.consumed.length > 0" class="artifact-section">
        <div class="artifact-section-header">Consumed</div>
        <table class="artifact-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Source Node</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="art in nodeArtifacts.consumed" :key="art.name">
              <td class="artifact-name">{{ art.name }}</td>
              <td>{{ art.type_name || "-" }}</td>
              <td>{{ art.source_node_id != null ? `Node ${art.source_node_id}` : "-" }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="nodeArtifacts.deleted.length > 0" class="artifact-section">
        <div class="artifact-section-header">Deleted</div>
        <table class="artifact-table">
          <thead>
            <tr>
              <th>Name</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="name in nodeArtifacts.deleted" :key="name">
              <td class="artifact-name artifact-deleted">{{ name }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div
        v-if="
          nodeArtifacts.published.length === 0 &&
          nodeArtifacts.consumed.length === 0 &&
          nodeArtifacts.deleted.length === 0
        "
        class="artifact-empty"
      >
        No artifacts recorded for this node.
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// TODO(refactor): large component. Plan to extract:
//   - DataTabs.vue, OutputSelector.vue, ArtifactsPanel.vue
//   - useTableData composable: AG Grid setup + refresh
import { ref, onMounted, onUnmounted, computed, watch } from "vue";
import { TableExample } from "../../components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "../../stores/column-store";
import { useFlowStore } from "../../stores/flow-store";
import { useFlowExecution } from "./composables/useFlowExecution";
import { AgGridVue } from "@ag-grid-community/vue3";
import { GridApi } from "@ag-grid-community/core";
import { ModuleRegistry } from "@ag-grid-community/core";
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";
import { DEFAULT_OUTPUT_HANDLE } from "../../utils/outputHandle";
import "@ag-grid-community/styles/ag-grid.css";
import "@ag-grid-community/styles/ag-theme-balham.css";

ModuleRegistry.registerModules([ClientSideRowModelModule]);

const isLoading = ref(false);
const activeTab = ref<"data" | "artifacts">("data");
const flowStore = useFlowStore();
const gridHeight = ref("");
const rowData = ref<Record<string, any>[] | Record<string, never>>([]);
const showTable = ref(false);
const nodeStore = useNodeStore();
const dataPreview = ref<TableExample>();
const dataAvailable = ref(false);
const dataLength = ref(0);
const columnLength = ref(0);
const gridApi = ref<GridApi | null>(null);
// Component ref on <ag-grid-vue> — `.value.$el` gives us this grid's root DOM
// node so the window-level Cmd+C/A handler can scope itself to *this* grid
// instead of any element matching `.ag-theme-balham`.
const gridComponentRef = ref<{ $el?: HTMLElement } | null>(null);
const columnDefs = ref([{}]);
const showFetchButton = ref(false);
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
  showFileStats?: boolean;
  hideTitle?: boolean;
  flowId?: number;
}

const props = withDefaults(defineProps<Props>(), {
  showFileStats: false,
  hideTitle: true,
  flowId: undefined,
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

const gridHeightComputed = computed(() => {
  if (showFetchButton.value) {
    return "80px";
  }
  return gridHeight.value || "100%";
});

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

const defaultColDef = {
  editable: true,
  filter: true,
  sortable: true,
  resizable: true,
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

const calculateGridHeight = () => {
  const otherElementsHeight = 300;
  const availableHeight = window.innerHeight - otherElementsHeight;
  gridHeight.value = `${availableHeight}px`;
};

let schema_dict: any = {};

async function downloadData(nodeId: number) {
  try {
    isLoading.value = true;
    showFetchButton.value = false;
    currentNodeId.value = nodeId;

    let resp = await nodeStore.getTableExample(
      nodeStore.flow_id,
      nodeId,
      selectedOutputHandle.value,
    );

    if (resp) {
      dataPreview.value = resp;
      const _cd: Array<{ field: string; headerName: string; resizable: boolean }> = [];
      const _columns = dataPreview.value.table_schema;

      if (props.showFileStats) {
        _columns?.forEach((item) => {
          _cd.push({
            field: item.name,
            headerName: item.name,
            resizable: true,
          });
          schema_dict[item.name] = item;
        });
      } else {
        _columns?.forEach((item) => {
          _cd.push({
            field: item.name,
            headerName: item.name,
            resizable: true,
          });
        });
      }

      columnDefs.value = _cd;

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
      if (isPollingActive(`node_${currentNodeId.value}`)) {
        console.log("Fetch already in progress for this node");
        return;
      }

      await triggerNodeFetch(currentNodeId.value);

      // Since polling is persistent, we need to check periodically
      const checkInterval = setInterval(async () => {
        if (!isPollingActive(`node_${currentNodeId.value}`)) {
          clearInterval(checkInterval);
          await downloadData(currentNodeId.value!);
        }
      }, 1000);

      // Safety timeout to prevent infinite checking
      setTimeout(() => clearInterval(checkInterval), 60000); // 1 minute max
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  }
}

function removeData() {
  rowData.value = [];
  showTable.value = false;
  dataAvailable.value = false;
  columnDefs.value = [{}];
  showFetchButton.value = false;
  currentNodeId.value = null;
}

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

  try {
    await navigator.clipboard.writeText(tsv);
    e.preventDefault();
  } catch {
    // Clipboard write rejected (permissions, insecure context); leave default behavior.
  }
};

onMounted(() => {
  calculateGridHeight();
  window.addEventListener("resize", calculateGridHeight);
  window.addEventListener("keydown", windowKeyHandler);
});

onUnmounted(() => {
  window.removeEventListener("resize", calculateGridHeight);
  window.removeEventListener("keydown", windowKeyHandler);
});

defineExpose({ downloadData, removeData, rowData, dataLength, columnLength });
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
   Tab Bar & Artifacts Panel
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

.artifacts-panel {
  padding: 12px 16px;
  overflow-y: auto;
}

.artifact-section-meta {
  font-size: 11px;
  color: var(--color-text-secondary);
  margin-bottom: 12px;
}

.artifact-section-meta code {
  background: var(--color-background-tertiary, var(--color-background-secondary));
  padding: 1px 5px;
  border-radius: 3px;
  font-size: 11px;
}

.artifact-section {
  margin-bottom: 16px;
}

.artifact-section-header {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary);
  margin-bottom: 6px;
}

.artifact-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.artifact-table th {
  text-align: left;
  padding: 4px 8px;
  font-weight: 500;
  color: var(--color-text-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  font-size: 11px;
}

.artifact-table td {
  padding: 5px 8px;
  border-bottom: 1px solid var(--color-border-light, var(--color-border-primary));
  color: var(--color-text-primary);
}

.artifact-table tbody tr:hover {
  background: var(--color-background-hover);
}

.artifact-name {
  font-weight: 500;
}

.artifact-module {
  font-size: 11px;
  color: var(--color-text-secondary);
}

.artifact-deleted {
  text-decoration: line-through;
  color: var(--color-text-secondary);
  opacity: 0.7;
}

.artifact-empty {
  color: var(--color-text-secondary);
  font-size: 13px;
  text-align: center;
  padding: 24px;
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
