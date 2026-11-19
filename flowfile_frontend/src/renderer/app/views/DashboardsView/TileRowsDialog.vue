<template>
  <el-dialog
    :model-value="modelValue"
    :title="`Rows behind ${title}`"
    width="80vw"
    top="6vh"
    append-to-body
    destroy-on-close
    data-testid="tile-rows-dialog"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div class="tile-rows-body">
      <div v-if="missing && source" class="tile-rows-state">
        {{ missingSourceText(source) }}
      </div>
      <div v-else-if="fieldsError" class="tile-rows-state">{{ fieldsError }}</div>
      <DashboardRowsGrid
        v-else
        ref="gridRef"
        :source="source"
        :fields="fields"
        :dtypes="dtypes"
        :filters="targetedFilters"
        :chart-filters="chartFilters"
        :refresh-key="refreshNonce"
        @missing="markMissing"
      />
    </div>
    <template #footer>
      <el-button :disabled="source == null || fields == null" @click="downloadCsv">
        <el-icon><Download /></el-icon>
        <span>Download CSV</span>
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { Download } from "@element-plus/icons-vue";
import {
  filtersTargetingTile,
  missingSourceText,
  type VisFilter,
} from "../../composables/useDashboardComputation";
import { useDashboardTileSource } from "../../composables/useDashboardTileSource";
import type { DashboardFilter, DashboardTile } from "../../types";
import DashboardRowsGrid from "./DashboardRowsGrid.vue";

/**
 * The rows a chart or KPI tile is computed from: its own source, read through the same route and
 * access check as the tile, with the dashboard filters that reach it and a chart's own row filters.
 */
const props = defineProps<{
  modelValue: boolean;
  tile: DashboardTile;
  title: string;
  filters: DashboardFilter[];
  tileDatasource?: (tileId: string) => string | null;
  chartFilters?: VisFilter[];
  refreshNonce?: number;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
}>();

const { source, fields, dtypes, missing, fieldsError, markMissing } = useDashboardTileSource(
  () => props.tile,
  () => props.refreshNonce ?? 0,
);

const targetedFilters = computed(() =>
  filtersTargetingTile(
    props.filters,
    props.tile.id,
    (id) => props.tileDatasource?.(id) ?? null,
    fields.value,
  ),
);

const gridRef = ref<InstanceType<typeof DashboardRowsGrid> | null>(null);

const downloadCsv = () => gridRef.value?.downloadCsv(`${props.title} rows`);
</script>

<style scoped>
.tile-rows-body {
  height: calc(88vh - 170px);
  min-height: 240px;
}
.tile-rows-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  font-size: 12px;
  color: var(--el-color-warning);
}
</style>
