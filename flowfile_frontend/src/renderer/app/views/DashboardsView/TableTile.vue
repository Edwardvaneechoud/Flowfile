<template>
  <div class="table-tile" @dblclick="onDblclick">
    <div v-if="source == null" class="table-state">
      <el-button v-if="mode === 'edit'" size="small" @mousedown.stop @click="openSettings">
        <el-icon><Grid /></el-icon>
        <span>Configure table</span>
      </el-button>
      <span v-else class="table-muted">Table not configured</span>
    </div>
    <div v-else-if="missing" class="table-state tile-missing">
      <el-icon><WarningFilled /></el-icon>
      <span>{{ missingSourceText(source) }}</span>
    </div>
    <div v-else-if="fieldsError" class="table-state tile-error" :title="fieldsError">
      <el-icon><Warning /></el-icon>
      <span>{{ fieldsError }}</span>
    </div>
    <DashboardRowsGrid
      v-else
      ref="gridRef"
      :source="source"
      :config="tile.table"
      :fields="fields"
      :dtypes="dtypes"
      :filters="targetedFilters"
      :refresh-key="refreshNonce"
      @missing="markMissing"
    />

    <TableSettingsDialog
      v-if="showSettings"
      v-model="showSettings"
      :tile="tile"
      @save="(patch) => emit('update:tile', { ...tile, ...patch })"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { Grid, Warning, WarningFilled } from "@element-plus/icons-vue";
import { filtersTargetingTile, missingSourceText } from "../../composables/useDashboardComputation";
import { useDashboardTileSource } from "../../composables/useDashboardTileSource";
import type { DashboardFilter, DashboardTile } from "../../types";
import DashboardRowsGrid from "./DashboardRowsGrid.vue";
import TableSettingsDialog from "./TableSettingsDialog.vue";

const props = defineProps<{
  tile: DashboardTile;
  mode: "edit" | "view";
  filters: DashboardFilter[];
  tileDatasource?: (tileId: string) => string | null;
  refreshNonce?: number;
}>();

const emit = defineEmits<{
  (e: "update:tile", value: DashboardTile): void;
}>();

const { source, fields, sourceName, dtypes, missing, fieldsError, markMissing } =
  useDashboardTileSource(
    () => props.tile,
    () => props.refreshNonce ?? 0,
  );

// A datasource filter reaches this table exactly as it reaches a chart on the same table.
const targetedFilters = computed(() =>
  filtersTargetingTile(
    props.filters,
    props.tile.id,
    (id) => props.tileDatasource?.(id) ?? null,
    fields.value,
  ),
);

const showSettings = ref(false);
const gridRef = ref<InstanceType<typeof DashboardRowsGrid> | null>(null);

const openSettings = () => {
  if (props.mode === "edit") showSettings.value = true;
};

const onDblclick = (e: MouseEvent) => {
  if (props.mode !== "edit" || source.value != null) return;
  e.stopPropagation();
  openSettings();
};

const downloadCsv = (title?: string | null) => gridRef.value?.downloadCsv(title);

defineExpose({ openSettings, downloadCsv, sourceName });
</script>

<style scoped>
.table-tile {
  height: 100%;
  padding: 2px 8px 6px;
  box-sizing: border-box;
}
.table-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  height: 100%;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.table-state .el-button {
  cursor: pointer;
}
.table-muted {
  color: var(--el-text-color-secondary);
}
.tile-missing,
.tile-error {
  color: var(--el-color-warning);
}
</style>
