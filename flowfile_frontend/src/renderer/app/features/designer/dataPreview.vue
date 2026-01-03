<template>
  <!-- Loading Spinner -->
  <div v-if="isLoading" class="spinner-overlay">
    <div class="spinner"></div>
  </div>

  <!-- Table Container -->
  <div v-show="!isLoading" class="table-container">
    <!-- Button for when there is sample data, but the sample data is outdated -->
    <div v-if="showOutdatedDataBanner" class="outdated-data-banner">
      <p>
        Displayed data might be outdated.
        <button class="refresh-link-button" @click="handleRefresh">Refresh now</button>
      </p>
      <button
        class="dismiss-button"
        aria-label="Dismiss notification"
        @click="dismissOutdatedBanner"
      >
        ×
      </button>
    </div>

    <!-- AG Grid -->
    <ag-grid-vue
      :default-col-def="defaultColDef"
      :column-defs="columnDefs"
      class="ag-theme-balham"
      :row-data="rowData"
      :style="{ width: '100%', height: gridHeightComputed }"
      :overlay-no-rows-template="overlayNoRowsTemplate"
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
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed } from "vue";
import { TableExample } from "../../components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "../../stores/column-store";
import { useFlowExecution } from "./composables/useFlowExecution";
import { AgGridVue } from "@ag-grid-community/vue3";
import { GridApi } from "@ag-grid-community/core";
import { ModuleRegistry } from "@ag-grid-community/core";
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";
import "@ag-grid-community/styles/ag-grid.css";
import "@ag-grid-community/styles/ag-theme-balham.css";

ModuleRegistry.registerModules([ClientSideRowModelModule]);

const isLoading = ref(false);
const gridHeight = ref("");
const rowData = ref<Record<string, any>[] | Record<string, never>>([]);
const showTable = ref(false);
const nodeStore = useNodeStore();
const dataPreview = ref<TableExample>();
const dataAvailable = ref(false);
const dataLength = ref(0);
const columnLength = ref(0);
const gridApi = ref<GridApi | null>(null);
const columnDefs = ref([{}]);
const showFetchButton = ref(false);
const currentNodeId = ref<number | null>(null);
const showOutdatedDataBanner = ref(false);

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

// Use the flow execution composable with persistent polling for node fetches
const { triggerNodeFetch, isPollingActive } = useFlowExecution(
  props.flowId || nodeStore.flow_id,
  { interval: 2000, enabled: true },
  {
    persistPolling: true, // Keep polling even when component unmounts
    pollingKey: `table_flow_${props.flowId || nodeStore.flow_id}`,
  },
);

// Computed property for dynamic grid height
const gridHeightComputed = computed(() => {
  if (showFetchButton.value) {
    return "80px";
  }
  return gridHeight.value || "100%";
});

// Custom overlay template to hide "no rows" message when fetch button is available
const overlayNoRowsTemplate = computed(() => {
  if (showFetchButton.value) {
    return "<span></span>";
  }
  // Return undefined to use AG-Grid's default "No Rows To Show" message
  return undefined;
});

const defaultColDef = {
  editable: true,
  filter: true,
  sortable: true,
  resizable: true,
};

const onGridReady = (params: { api: GridApi }) => {
  gridApi.value = params.api;

  // Optionally, you can also programmatically control the overlay
  if (showFetchButton.value) {
    gridApi.value.hideOverlay();
  }
};

function dismissOutdatedBanner() {
  showOutdatedDataBanner.value = false;
}

async function handleRefresh() {
  // Hide banner and trigger the existing fetch logic
  showOutdatedDataBanner.value = false;
  await handleFetchData();
}

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
    showOutdatedDataBanner.value = false;
    currentNodeId.value = nodeId;

    let resp = await nodeStore.getTableExample(nodeStore.flow_id, nodeId);

    if (resp) {
      dataPreview.value = resp;
      showOutdatedDataBanner.value = !resp.has_run_with_current_setup && resp.has_example_data;
      // Always set up columns
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

      // Check if data has been run
      if (resp.has_example_data === false) {
        showFetchButton.value = true;
        // Show empty grid with just headers
        rowData.value = [];
        showTable.value = true;
        dataAvailable.value = false;
      } else {
        // Load the actual data
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
      // Check if already fetching this node
      if (isPollingActive(`node_${currentNodeId.value}`)) {
        console.log("Fetch already in progress for this node");
        return;
      }

      // Use the composable to trigger node fetch with proper state management
      await triggerNodeFetch(currentNodeId.value);

      // Set up a watcher for when the fetch completes
      // Since polling is persistent, we need to check periodically
      const checkInterval = setInterval(async () => {
        if (!isPollingActive(`node_${currentNodeId.value}`)) {
          clearInterval(checkInterval);
          // Reload the data once fetch is complete
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

onMounted(() => {
  calculateGridHeight();
  window.addEventListener("resize", calculateGridHeight);
});

onUnmounted(() => {
  window.removeEventListener("resize", calculateGridHeight);
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

/* Modern Outdated Data Banner Styles */
.outdated-data-banner {
  position: absolute;
  top: 10px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-width: 380px;
  max-width: 90%;
  padding: 12px 16px;
  background: linear-gradient(135deg, rgba(254, 243, 199, 0.98) 0%, rgba(253, 230, 138, 0.98) 100%);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(251, 191, 36, 0.3);
  border-radius: 12px;
  box-shadow:
    0 4px 20px rgba(251, 191, 36, 0.15),
    0 2px 8px rgba(0, 0, 0, 0.05);
  font-size: 14px;
  animation: slideDown 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  overflow: hidden;
}

/* Shimmer effect */
.outdated-data-banner::before {
  content: "";
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
  background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.4), transparent);
  animation: shimmer 3s infinite;
}

/* Warning icon animation */
@keyframes pulse {
  0%,
  100% {
    transform: scale(1);
  }
  50% {
    transform: scale(1.1);
  }
}

@keyframes shimmer {
  0% {
    left: -100%;
  }
  100% {
    left: 100%;
  }
}

@keyframes slideDown {
  from {
    opacity: 0;
    transform: translate(-50%, -20px);
  }
  to {
    opacity: 1;
    transform: translate(-50%, 0);
  }
}

.outdated-data-banner p {
  margin: 0;
  color: #78350f;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 8px;
  position: relative;
  z-index: 1;
}

/* Add warning icon */
.outdated-data-banner p::before {
  content: "⚠️";
  font-size: 16px;
  animation: pulse 2s ease-in-out infinite;
}

.refresh-link-button {
  background: linear-gradient(135deg, #ffffff 0%, #fef3c7 100%);
  border: 1px solid #f59e0b;
  color: #92400e;
  border-radius: 6px;
  padding: 5px 14px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  text-decoration: none;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  position: relative;
  overflow: hidden;
  white-space: nowrap;
  margin-left: 4px;
}

/* Refresh icon */
.refresh-link-button::before {
  content: "↻";
  font-size: 14px;
  transition: transform 0.3s ease;
}

.refresh-link-button:hover {
  background: linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%);
  color: white;
  border-color: #d97706;
  transform: translateY(-1px);
  box-shadow:
    0 4px 12px rgba(245, 158, 11, 0.3),
    inset 0 1px 0 rgba(255, 255, 255, 0.2);
}

.refresh-link-button:hover::before {
  transform: rotate(180deg);
}

.refresh-link-button:active {
  transform: translateY(0);
  box-shadow:
    0 2px 6px rgba(245, 158, 11, 0.2),
    inset 0 1px 2px rgba(0, 0, 0, 0.1);
}

.dismiss-button {
  background: rgba(255, 255, 255, 0.9);
  border: 1px solid rgba(251, 191, 36, 0.3);
  width: 24px;
  height: 24px;
  border-radius: 50%;
  font-size: 16px;
  line-height: 1;
  cursor: pointer;
  color: #92400e;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  flex-shrink: 0;
  margin-left: 12px;
  position: relative;
  z-index: 1;
}

.dismiss-button:hover {
  background: white;
  border-color: #f59e0b;
  color: #78350f;
  transform: rotate(90deg) scale(1.1);
  box-shadow: 0 3px 10px rgba(0, 0, 0, 0.1);
}

.dismiss-button:active {
  transform: rotate(90deg) scale(0.95);
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
  background-color: #3498db;
  color: white;
  border: none;
  padding: 8px 20px;
  font-size: 14px;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s ease;
  position: relative;
}

.fetch-data-button:hover:not(:disabled) {
  background-color: #2980b9;
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(52, 152, 219, 0.3);
}

.fetch-data-button:active:not(:disabled) {
  transform: translateY(0);
}

.fetch-data-button:disabled {
  background-color: #93c5fd;
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
</style>
