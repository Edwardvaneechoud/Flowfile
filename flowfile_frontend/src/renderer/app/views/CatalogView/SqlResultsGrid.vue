<template>
  <div class="sql-results-grid">
    <ag-grid-vue
      :default-col-def="defaultColDef"
      :column-defs="columnDefs"
      class="ag-theme-balham"
      :row-data="rowData"
      style="width: 100%; height: 100%"
      :overlay-no-rows-template="'No results'"
      @grid-ready="onGridReady"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { AgGridVue } from "@ag-grid-community/vue3";
import { type GridApi, ModuleRegistry } from "@ag-grid-community/core";
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";
import "@ag-grid-community/styles/ag-grid.css";
import "@ag-grid-community/styles/ag-theme-balham.css";
import type { SqlQueryResult } from "../../types";

ModuleRegistry.registerModules([ClientSideRowModelModule]);

const props = defineProps<{
  result: SqlQueryResult;
}>();

const gridApi = ref<GridApi | null>(null);

const defaultColDef = {
  editable: false,
  filter: true,
  sortable: true,
  resizable: true,
};

const columnDefs = computed(() =>
  props.result.columns.map((col, idx) => ({
    headerName: col,
    field: col,
    headerTooltip: `${col} (${props.result.dtypes[idx] ?? ""})`,
  })),
);

const rowData = computed(() =>
  props.result.rows.map((row) => {
    const obj: Record<string, any> = {};
    props.result.columns.forEach((col, idx) => {
      obj[col] = row[idx];
    });
    return obj;
  }),
);

const onGridReady = (params: { api: GridApi }) => {
  gridApi.value = params.api;
  params.api.autoSizeAllColumns();
};
</script>

<style scoped>
.sql-results-grid {
  width: 100%;
  height: 100%;
}
</style>
