<template>
  <!-- Loading Spinner -->
  <div v-if="isLoading" class="spinner-overlay">
    <div class="spinner"></div>
  </div>

  <!-- Table Container -->
  <div v-show="!isLoading" class="table-container">
    <!-- AG Grid -->
    <ag-grid-vue
      :default-col-def="defaultColDef"
      :column-defs="columnDefs"
      class="ag-theme-balham"
      :row-data="rowData"
      :style="{ width: '100%', height: gridHeightComputed }"
      @grid-ready="onGridReady"
    />
    
    <!-- Fetch Data Button (shown when has_run is false) -->
    <div v-if="showFetchButton" class="fetch-data-section">
      <p>{{ fetchStatusMessage }}</p>
      <button 
        @click="handleFetchData" 
        class="fetch-data-button"
        :disabled="isFetching"
      >
        <span v-if="!isFetching">Fetch Data</span>
        <span v-else>Fetching...</span>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed, watch } from "vue";
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
const isFetching = ref(false);
const pendingFetch = ref(false);

interface Props {
  showFileStats?: boolean;
  hideTitle?: boolean;
  flowId?: number;
}

const props = withDefaults(defineProps<Props>(), {
  showFileStats: false,
  hideTitle: true,
});

// Use the flow execution composable
const { triggerNodeFetch, isPolling } = useFlowExecution(
  props.flowId || nodeStore.flow_id,
  { interval: 2000, enabled: true }
);

// Computed property for fetch status message
const fetchStatusMessage = computed(() => {
  if (pendingFetch.value) {
    return "Fetch completed! Refreshing data...";
  }
  if (isFetching.value) {
    return "Fetching data, please wait...";
  }
  return "Data has not been generated yet";
});

// Computed property for dynamic grid height
const gridHeightComputed = computed(() => {
  if (showFetchButton.value) {
    return '80px'; // Just show headers when has_run is false
  }
  return gridHeight.value || '100%';
});

// Watch for component becoming visible again to check if fetch completed
watch(() => showTable.value, (newVal) => {
  if (newVal && pendingFetch.value && currentNodeId.value) {
    // Component is visible again and there was a pending fetch
    downloadData(currentNodeId.value);
    pendingFetch.value = false;
  }
});

const defaultColDef = {
  editable: true,
  filter: true,
  sortable: true,
  resizable: true,
};

const onGridReady = (params: { api: GridApi }) => {
  gridApi.value = params.api;
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
    
    // Check if there's an active fetch for this node
    if (isPolling(nodeId)) {
      isFetching.value = true;
      // Don't proceed with loading data yet, just show the fetching state
      return;
    }
    
    isFetching.value = false;
    
    let resp = await nodeStore.getTableExample(nodeStore.flow_id, nodeId);

    if (resp) {
      dataPreview.value = resp;
      
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
      if (resp.has_run === false) {
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
  if (currentNodeId.value !== null && !isFetching.value) {
    try {
      isFetching.value = true;
      
      // Trigger node fetch with completion callback
      await triggerNodeFetch(currentNodeId.value, async () => {
        // This callback runs when the fetch completes
        pendingFetch.value = true;
        isFetching.value = false;
        
        // If the component is still mounted, reload the data
        if (showTable.value && currentNodeId.value) {
          await downloadData(currentNodeId.value);
          pendingFetch.value = false;
        }
        // Otherwise, the watch will handle it when component becomes visible
      });
    } catch (error) {
      console.error("Failed to fetch node data:", error);
      isFetching.value = false;
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
  isFetching.value = false;
  pendingFetch.value = false;
}

const parentElement = ref(null);

onMounted(() => {
  calculateGridHeight();
  window.addEventListener("resize", calculateGridHeight);
  
  // Check if there's an ongoing fetch for this node when component mounts
  if (currentNodeId.value && isPolling(currentNodeId.value)) {
    isFetching.value = true;
  }
});

onUnmounted(() => {
  window.removeEventListener("resize", calculateGridHeight);
  // Note: We do NOT stop polling here - it continues in the service
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
  font-weight: 500;
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
</style>