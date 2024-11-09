<template>
  <ag-grid-vue
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
import "@ag-grid-community/styles/ag-grid.css";
import "@ag-grid-community/styles/ag-theme-alpine.css";
import "@ag-grid-community/styles/ag-theme-balham.css";
import { GridApi } from "@ag-grid-community/core";
import { ModuleRegistry } from "@ag-grid-community/core";
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";

ModuleRegistry.registerModules([ClientSideRowModelModule]);

const gridHeight = ref("");
const rowData = ref<Record<string, any>[] | Record<string, never>>([]);
const showTable = ref(false);
const nodeStore = useNodeStore();
const dataPreview = ref<TableExample>();
const dataAvailable = ref(false);
const dataLength = ref(0);
const columnLength = ref(0);
const gridApi = ref<GridApi | null>(null);
const onGridReady = (params: { api: GridApi }) => {
  gridApi.value = params.api;
  console.log(gridApi.value);
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
  const otherElementsHeight = 300; // Height of other elements in pixels
  console.log(window.innerHeight);
  const availableHeight = window.innerHeight - otherElementsHeight;
  gridHeight.value = `${availableHeight}px`;
  console.log(gridHeight.value);
};

let schema_dict: any = {};

const defaultColDef = ref({
  editable: true,
  filter: true,
  sortable: true,
});

const columnDefs = ref([{}]);

async function downloadData(nodeId: number) {
  console.log("Downloading data...");
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
      console.log("length of data", dataLength.value);
    }
    showTable.value = true;
    dataAvailable.value = true;
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
  console.log(parentElement.value);
  console.log("Logging the props");
});

onUnmounted(() => {
  window.removeEventListener("resize", calculateGridHeight);
});
defineExpose({ downloadData, removeData, rowData, dataLength, columnLength });
</script>

<style>
.ag-theme-balham {
  max-width: 100%;
  height: 100%;
  --ag-odd-row-background-color: rgb(255, 255, 255);
  --ag-row-background-color: rgb(255, 255, 255);
  --ag-header-background-color: rgb(246, 247, 251);
}
</style>
