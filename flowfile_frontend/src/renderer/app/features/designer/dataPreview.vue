<template>
  <!-- Loading Spinner -->
  <div v-if="isLoading" class="spinner-overlay">
    <div class="spinner"></div>
  </div>

  <!-- Table Container -->
  <div v-show="!isLoading" class="table-container">
    
    <!-- Button for when there is sample data, but the sample dat is outdated -->
    <div v-if="showOutdatedDataBanner" class="outdated-data-banner">
      <p>
        Displayed data might be outdated.
        <button @click="handleRefresh" class="refresh-link-button">
          Click here to refresh.
        </button>
      </p>
      <button @click="dismissOutdatedBanner" class="dismiss-button">&times;</button>
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
      <button 
        @click="handleFetchData" 
        class="fetch-data-button"
        :disabled="nodeStore.isRunning"
      >
        <span v-if="!nodeStore.isRunning">Fetch Data</span>
        <span v-else>Fetching...</span>
      </button>
    </div>
  </div>

</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed } from "vue";
import { TableExample } from "./baseNode/nodeInterfaces";
import { useNodeStore } from "../../stores/column-store";
import { useFlowExecution } from "./composables/useFlowExecution";
import { AgGridVue } from "@ag-grid-community/vue3";
import { GridApi, GridReadyEvent } from "@ag-grid-community/core";
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
const showOutdatedDataBanner = ref(false); // <-- ADD THIS NEW STATE VARIABLE



interface Props {
  showFileStats?: boolean;
  hideTitle?: boolean;
  flowId?: number;
}

const props = withDefaults(defineProps<Props>(), {
  showFileStats: false,
  hideTitle: true,
});

// Use the flow execution composable with persistent polling for node fetches
const { triggerNodeFetch, isPollingActive } = useFlowExecution(
  props.flowId || nodeStore.flow_id,
  { interval: 2000, enabled: true },
  { 
    persistPolling: true,  // Keep polling even when component unmounts
    pollingKey: `table_flow_${props.flowId || nodeStore.flow_id}`
  }
);

// Computed property for dynamic grid height
const gridHeightComputed = computed(() => {
  if (showFetchButton.value) {
    return '80px';
  }
  return gridHeight.value || '100%';
});

// Custom overlay template to hide "no rows" message when fetch button is available
const overlayNoRowsTemplate = computed(() => {
  if (showFetchButton.value) {
    return '<span></span>';
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
            resizable: true 
          });
          schema_dict[item.name] = item;
        });
      } else {
        _columns?.forEach((item) => {
          _cd.push({ 
            field: item.name, 
            headerName: item.name,
            resizable: true 
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
      console.error("Failed to fetch node data:", error);
      // Error notification is already handled by the composable
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

const parentElement = ref(null);

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
  background-color: rgba(255, 255, 255, 0.8);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

.spinner {
  width: 50px;
  height: 50px;
  border: 5px solid #f3f3f3;
  border-top: 5px solid #3498db;
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


.fetch-data-section {
  padding: 20px;
  text-align: center;
  background-color: #f9fafb;
  border: 1px solid #e5e7eb;
  border-top: none;
  border-radius: 0 0 8px 8px;
}

.fetch-data-section p {
  color: #6b7280;
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
}

.fetch-data-button:active:not(:disabled) {
  transform: translateY(1px);
}

.fetch-data-button:disabled {
  background-color: #93c5fd;
  cursor: not-allowed;
  opacity: 0.7;
}

.ag-theme-balham {
  max-width: 100%;
  --ag-odd-row-background-color: rgb(255, 255, 255);
  --ag-row-background-color: rgb(255, 255, 255);
  --ag-header-background-color: rgb(246, 247, 251);
}

.outdated-data-banner {
  position: absolute;
  top: 10px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 10;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  background-color: #fffbe6; /* Light yellow */
  border: 1px solid #fde68a;
  border-radius: 8px;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1);
  font-size: 14px;
  color: #92400e;
}

.outdated-data-banner p {
  margin: 0;
  margin-right: 16px;
}

.refresh-link-button {
  background: none;
  border: none;
  color: #065fd4;
  text-decoration: underline;
  cursor: pointer;
  padding: 0;
  font-size: inherit;
}

.refresh-link-button:hover {
  color: #04499b;
}

.dismiss-button {
  background: none;
  border: none;
  font-size: 20px;
  line-height: 1;
  cursor: pointer;
  color: #9ca3af;
  padding: 0 4px;
}

.dismiss-button:hover {
  color: #4b5563;
}

</style>