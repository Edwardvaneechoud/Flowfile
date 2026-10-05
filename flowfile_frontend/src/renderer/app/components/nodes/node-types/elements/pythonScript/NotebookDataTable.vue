<script setup lang="ts">
// Read-only AG Grid table for flowfile_ctx.display(df), mirroring dataPreview.vue.
import { computed } from "vue";
import { cellValueFormatter } from "../../../../../utils/cellFormat";
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
const emit = defineEmits<{ "selection-change": [rows: Record<string, unknown>[]] }>();

const defaultColDef = {
  editable: false,
  filter: true,
  sortable: true,
  resizable: true,
  flex: 1,
  minWidth: 110,
};

const ROW_HEIGHT = 28;
const HEADER_HEIGHT = 32;
const MAX_VISIBLE_ROWS = 10;

// Up to ten rows the grid sizes to its content; beyond that it scrolls inside a fixed height.
const autoHeight = computed(() => (props.rows?.length ?? 0) <= MAX_VISIBLE_ROWS);
const gridStyle = computed(() => ({
  width: "100%",
  height: autoHeight.value ? undefined : `${HEADER_HEIGHT + MAX_VISIBLE_ROWS * ROW_HEIGHT + 2}px`,
}));

const columnDefs = computed(() =>
  (props.columns ?? []).map((name) => ({
    field: name,
    headerName: name,
    valueFormatter: cellValueFormatter,
  })),
);
</script>

<template>
  <ag-grid-vue
    :default-col-def="defaultColDef"
    :column-defs="columnDefs"
    :row-data="rows"
    :suppress-field-dot-notation="true"
    :row-height="ROW_HEIGHT"
    :header-height="HEADER_HEIGHT"
    :dom-layout="autoHeight ? 'autoHeight' : 'normal'"
    row-selection="multiple"
    :rows-multi-select-with-click="true"
    class="ag-theme-balham notebook-data-table"
    :style="gridStyle"
    @grid-ready="emit('selection-change', [])"
    @selection-changed="emit('selection-change', $event.api.getSelectedRows())"
  />
</template>

<style scoped>
/* Token mapping lives here too: dataPreview.vue's global copy isn't loaded on every surface. */
.notebook-data-table {
  --ag-background-color: var(--color-background-primary);
  --ag-odd-row-background-color: var(--color-background-primary);
  --ag-row-background-color: var(--color-background-primary);
  --ag-header-background-color: var(--color-background-secondary);
  --ag-header-foreground-color: var(--color-text-secondary);
  --ag-foreground-color: var(--color-text-primary);
  --ag-border-color: var(--color-border-primary);
  --ag-row-border-color: var(--color-border-light);
  --ag-secondary-foreground-color: var(--color-text-secondary);
  --ag-row-hover-color: var(--color-background-hover);
  --ag-selected-row-background-color: var(--color-background-selected);
  --ag-font-family: var(--font-family-base);
  --ag-font-size: 12px;
  font-size: 12px;
}
/* The host card already draws the frame. */
.notebook-data-table :deep(.ag-root-wrapper) {
  border: none;
}
/* AG Grid's 50px auto-height floor would pad a one-row result. */
.notebook-data-table :deep(.ag-layout-auto-height .ag-center-cols-viewport),
.notebook-data-table :deep(.ag-layout-auto-height .ag-center-cols-container) {
  min-height: 28px;
}
</style>
