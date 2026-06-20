<script setup lang="ts">
// Read-only AG Grid table for flowfile_ctx.display(df), mirroring dataPreview.vue.
import { computed } from "vue";
import { AgGridVue } from "@ag-grid-community/vue3";
import { ModuleRegistry } from "@ag-grid-community/core";
import { ClientSideRowModelModule } from "@ag-grid-community/client-side-row-model";
import "@ag-grid-community/styles/ag-grid.css";
import "@ag-grid-community/styles/ag-theme-balham.css";

ModuleRegistry.registerModules([ClientSideRowModelModule]);

interface Props {
  columns: string[];
  rows: Record<string, unknown>[];
}

const props = defineProps<Props>();

const defaultColDef = {
  editable: false,
  filter: true,
  sortable: true,
  resizable: true,
};

const columnDefs = computed(() =>
  (props.columns ?? []).map((name) => ({
    field: name,
    headerName: name,
    // Render List/Struct cells as JSON, not "[object Object]".
    valueFormatter: (p: { value: unknown }) =>
      p.value !== null && typeof p.value === "object"
        ? JSON.stringify(p.value)
        : (p.value as string),
  })),
);
</script>

<template>
  <ag-grid-vue
    :default-col-def="defaultColDef"
    :column-defs="columnDefs"
    :row-data="rows"
    :suppress-field-dot-notation="true"
    class="ag-theme-balham notebook-data-table"
    :style="{ width: '100%', height: '100%' }"
  />
</template>

<style scoped>
.notebook-data-table {
  /* AG Grid balham defaults to a light surface; keep it readable in both
     themes by letting the host container set the background. */
  font-size: 0.8rem;
}
</style>
