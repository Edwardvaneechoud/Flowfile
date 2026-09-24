<template>
  <div class="table-export-menu">
    <div class="export-split" :title="copyDisabled ? (tableDisabled ?? undefined) : undefined">
      <button
        class="export-btn export-btn--main"
        :disabled="busy || copyDisabled"
        @click="emit('copy', selectedCount > 0 ? 'selection' : 'table')"
      >
        <span class="material-icons export-icon">content_copy</span>
        Copy
      </button>
      <el-dropdown trigger="click" placement="top-end" :disabled="busy">
        <button class="export-btn export-btn--caret" aria-label="Copy options" :disabled="busy">
          <span class="material-icons export-icon">arrow_drop_down</span>
        </button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item :disabled="selectedCount === 0" @click="emit('copy', 'selection')">
              Selected rows ({{ selectedCount.toLocaleString("en-US") }})
            </el-dropdown-item>
            <el-dropdown-item
              :disabled="!!tableDisabled"
              :title="tableDisabled ?? undefined"
              @click="emit('copy', 'table')"
            >
              {{ copyTableLabel }}
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
    <button
      class="export-btn export-btn--solo"
      :disabled="downloadDisabled"
      :title="tableDisabled ?? downloadTitle"
      @click="emit('download')"
    >
      <span class="material-icons export-icon">download</span>
      Download CSV
    </button>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { DOWNLOAD_DEFAULT_ROWS, rowsForCellCap } from "../../../utils/tableExport";

const props = withDefaults(
  defineProps<{
    selectedCount: number;
    /** Rows the whole-table copy and the download draw from; null when unknown. */
    rowCount: number | null;
    columnCount: number;
    tableDisabled?: string | null;
    busy?: boolean;
  }>(),
  { tableDisabled: null, busy: false },
);

const emit = defineEmits<{
  copy: ["selection" | "table"];
  download: [];
}>();

const copyDisabled = computed(() => props.selectedCount === 0 && !!props.tableDisabled);
const downloadDisabled = computed(() => props.busy || !!props.tableDisabled);

const fmt = (n: number) => n.toLocaleString("en-US");
const rowsLabel = (limit: number) =>
  props.rowCount == null
    ? `Up to ${fmt(limit)} rows`
    : `${fmt(Math.min(props.rowCount, limit))} rows`;
const copyTableLabel = computed(() => rowsLabel(rowsForCellCap(props.columnCount)));
const downloadTitle = computed(
  () =>
    `Download ${rowsLabel(DOWNLOAD_DEFAULT_ROWS).toLowerCase()}. Use a Write data node for more.`,
);
</script>

<style scoped>
.table-export-menu {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.export-split {
  display: inline-flex;
  align-items: stretch;
}

.export-btn {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  height: 20px;
  padding: 0 6px;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  color: var(--color-text-primary);
  font-size: 11px;
  cursor: pointer;
}

.export-btn:hover:not(:disabled) {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.export-btn:disabled {
  cursor: not-allowed;
  opacity: 0.5;
}

.export-btn--main {
  border-radius: 4px 0 0 4px;
  border-right: none;
}

.export-btn--caret {
  padding: 0;
  border-radius: 0 4px 4px 0;
}

.export-btn--solo {
  border-radius: 4px;
}

.export-icon {
  font-size: 14px;
  color: var(--color-text-secondary);
}
</style>
