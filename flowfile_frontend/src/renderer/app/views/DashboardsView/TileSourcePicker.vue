<template>
  <div class="source-picker">
    <el-radio-group v-model="kind" size="small" @change="onKindChange">
      <el-radio-button value="table">Table</el-radio-button>
      <el-radio-button value="viz">Chart</el-radio-button>
    </el-radio-group>
    <el-select
      v-if="kind === 'table'"
      :model-value="tableId ?? undefined"
      placeholder="Select a catalog table"
      filterable
      :loading="tablesLoading"
      data-testid="tile-source-table"
      @update:model-value="pickTable"
    >
      <el-option v-for="t in tableOptions" :key="t.id" :value="t.id" :label="t.label" />
    </el-select>
    <el-select
      v-else
      :model-value="vizId ?? undefined"
      placeholder="Select a visualization"
      filterable
      :loading="catalogStore.loadingVisualizationLibrary"
      @update:model-value="pickViz"
    >
      <el-option-group v-for="group in vizGroups" :key="group.label" :label="group.label">
        <el-option v-for="v in group.items" :key="v.id" :value="v.id" :label="v.name" />
      </el-option-group>
    </el-select>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import { useDashboardsStore } from "../../stores/dashboards-store";
import { chartsForDashboard } from "../CatalogView/chartLibraryFilter";

/** Picks what a KPI or table tile reads: a catalog table directly, or a saved chart's source
 * (how a SQL source is reached). Exactly one of the two is set. */
const props = defineProps<{ vizId: number | null; tableId: number | null }>();

const emit = defineEmits<{
  (e: "change", value: { viz_id: number | null; table_id: number | null }): void;
}>();

const catalogStore = useCatalogStore();
const dashboardsStore = useDashboardsStore();
const kind = ref<"table" | "viz">(props.vizId != null && props.tableId == null ? "viz" : "table");
const tablesLoading = ref(false);

watch(
  () => [props.vizId, props.tableId],
  ([vizId, tableId]) => {
    if (tableId != null) kind.value = "table";
    else if (vizId != null) kind.value = "viz";
  },
);

const onKindChange = () => {
  if (props.vizId != null || props.tableId != null)
    emit("change", { viz_id: null, table_id: null });
};

const pickTable = (id: number | undefined) =>
  emit("change", { viz_id: null, table_id: id ?? null });
const pickViz = (id: number | undefined) => emit("change", { viz_id: id ?? null, table_id: null });

const tableOptions = computed(() =>
  catalogStore.allTables
    .map((t) => ({ id: t.id, label: t.full_table_name ?? t.name }))
    .sort((a, b) => a.label.localeCompare(b.label)),
);

const vizGroups = computed(() => {
  const groups = new Map<string, { label: string; items: { id: number; name: string }[] }>();
  for (const v of chartsForDashboard(
    catalogStore.visualizationLibrary,
    dashboardsStore.current?.id,
  )) {
    const label =
      v.source_type === "sql"
        ? "SQL"
        : (v.table_full_name ?? v.table_name ?? `Table #${v.catalog_table_id}`);
    if (!groups.has(label)) groups.set(label, { label, items: [] });
    groups.get(label)!.items.push({ id: v.id, name: v.name });
  }
  return [...groups.values()].sort((a, b) =>
    a.label === "SQL" ? 1 : b.label === "SQL" ? -1 : a.label.localeCompare(b.label),
  );
});

onMounted(async () => {
  if (!catalogStore.visualizationLibrary.length) {
    catalogStore.loadVisualizationLibrary().catch(() => undefined);
  }
  if (!catalogStore.allTables.length) {
    tablesLoading.value = true;
    try {
      await catalogStore.loadAllTables();
    } finally {
      tablesLoading.value = false;
    }
  }
});
</script>

<style scoped>
.source-picker {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
</style>
