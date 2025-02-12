<template>
  <!-- Loading Spinner -->
  <div v-if="isLoading" class="spinner-overlay">
    <div class="spinner"></div>
  </div>

  <!-- AG Grid -->
  <ag-grid-vue
    v-show="!isLoading"
    :default-col-def="defaultColDef"
    :column-defs="columnDefs"
    class="ag-theme-balham"
    :row-data="rowData"
    @grid-ready="onGridReady"
  />
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";
import { TableExample } from "./baseNode/nodeInterfaces";
import { useNodeStore } from "../../stores/column-store";
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

const defaultColDef = {
  editable: true,
  filter: true,
  sortable: true,
  resizable: true,
};

const onGridReady = (params: { api: GridApi }) => {
  gridApi.value = params.api;
  if (gridApi.value) {
    gridApi.value.sizeColumnsToFit();
  }
};

interface Props {
  showFileStats?: boolean;
  hideTitle?: boolean;
  flowId?: number;
}

const props = withDefaults(defineProps<Props>(), {
  showFileStats: false,
  hideTitle: true,
  flowId: -1,
});

const calculateGridHeight = () => {
  const otherElementsHeight = 300;
  const availableHeight = window.innerHeight - otherElementsHeight;
  gridHeight.value = `${availableHeight}px`;
};

let schema_dict: any = {};

async function downloadData(nodeId: number) {
  try {
    isLoading.value = true;
    let resp = await nodeStore.getTableExample(props.flowId, nodeId);

    if (resp) {
      dataPreview.value = resp;
      const _cd: Array<{ field: string; resizable: boolean }> = [];
      const _columns = dataPreview.value.table_schema;

      if (props.showFileStats) {
        _columns?.forEach((item) => {
          _cd.push({ field: item.name, resizable: true });
          schema_dict[item.name] = item;
        });
      } else {
        _columns?.forEach((item) => {
          _cd.push({ field: item.name, resizable: true });
        });
      }

      columnDefs.value = _cd;

      if (dataPreview.value) {
        rowData.value = dataPreview.value.data;
        dataLength.value = dataPreview.value.number_of_records;
        columnLength.value = dataPreview.value.number_of_columns;
      }

      showTable.value = true;
      dataAvailable.value = true;
    }
  } finally {
    isLoading.value = false;
  }
}

function removeData() {
  rowData.value = [];
  showTable.value = false;
  dataAvailable.value = false;
  columnDefs.value = [{}];
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

.ag-theme-balham {
  max-width: 100%;
  height: 100%;
  --ag-odd-row-background-color: rgb(255, 255, 255);
  --ag-row-background-color: rgb(255, 255, 255);
  --ag-header-background-color: rgb(246, 247, 251);
}
</style>
