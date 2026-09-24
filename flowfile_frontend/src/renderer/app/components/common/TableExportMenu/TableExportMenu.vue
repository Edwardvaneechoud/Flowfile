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
    <div class="export-split" :title="tableDisabled ?? undefined">
      <button
        class="export-btn export-btn--main"
        :disabled="downloadDisabled"
        @click="emit('download', 'first')"
      >
        <span class="material-icons export-icon">download</span>
        Download CSV
      </button>
      <el-dropdown trigger="click" placement="top-end" :disabled="downloadDisabled">
        <button
          class="export-btn export-btn--caret"
          aria-label="Download options"
          :disabled="downloadDisabled"
        >
          <span class="material-icons export-icon">arrow_drop_down</span>
        </button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item @click="emit('download', 'first')">
              First {{ DOWNLOAD_DEFAULT_ROWS.toLocaleString("en-US") }} rows
            </el-dropdown-item>
            <el-dropdown-item
              :disabled="!!allRowsDisabled"
              :title="allRowsDisabled ?? undefined"
              @click="emit('download', 'all')"
            >
              {{ allRowsLabel }}
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { DOWNLOAD_DEFAULT_ROWS } from "../../../utils/tableExport";

const props = withDefaults(
  defineProps<{
    selectedCount: number;
    copyTableLabel: string;
    allRowsLabel: string;
    tableDisabled?: string | null;
    allRowsDisabled?: string | null;
    busy?: boolean;
  }>(),
  { tableDisabled: null, allRowsDisabled: null, busy: false },
);

const emit = defineEmits<{
  copy: ["selection" | "table"];
  download: ["first" | "all"];
}>();

const copyDisabled = computed(() => props.selectedCount === 0 && !!props.tableDisabled);
const downloadDisabled = computed(() => props.busy || !!props.tableDisabled);
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

.export-icon {
  font-size: 14px;
  color: var(--color-text-secondary);
}
</style>
