<template>
  <div class="visuals-panel">
    <div class="visuals-subtabs">
      <el-radio-group v-model="kind" size="default" @change="onKindChange">
        <el-radio-button value="charts">
          <i class="fa-solid fa-chart-column subtab-icon"></i>
          Charts
        </el-radio-button>
        <el-radio-button value="dashboards">
          <i class="fa-solid fa-table-cells-large subtab-icon"></i>
          Dashboards
        </el-radio-button>
      </el-radio-group>
    </div>
    <div class="visuals-body">
      <VisualizationsLibraryPanel
        v-if="kind === 'charts'"
        @view-table="(id) => emit('viewTable', id)"
      />
      <DashboardLibraryPanel v-else />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import VisualizationsLibraryPanel from "./VisualizationsLibraryPanel.vue";
import DashboardLibraryPanel from "../DashboardsView/DashboardLibraryPanel.vue";

type Kind = "charts" | "dashboards";

const emit = defineEmits<{
  (e: "viewTable", tableId: number): void;
}>();

const route = useRoute();
const router = useRouter();

function readKind(): Kind {
  return route.query.kind === "dashboards" ? "dashboards" : "charts";
}

const kind = ref<Kind>(readKind());

watch(
  () => route.query.kind,
  () => {
    const next = readKind();
    if (next !== kind.value) kind.value = next;
  },
);

function onKindChange(value: string | number | boolean | undefined) {
  const next = value === "dashboards" ? "dashboards" : "charts";
  if (route.query.kind === next) return;
  router.replace({
    name: "catalog",
    query: { ...route.query, tab: "visuals", kind: next },
  });
}
</script>

<style scoped>
.visuals-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.visuals-subtabs {
  flex-shrink: 0;
  padding: 12px 24px 0;
}

.visuals-body {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.visuals-body > * {
  flex: 1;
  min-height: 0;
}

.subtab-icon {
  margin-right: 6px;
}
</style>
