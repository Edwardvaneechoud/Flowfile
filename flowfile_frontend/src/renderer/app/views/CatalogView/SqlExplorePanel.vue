<template>
  <div :class="['sql-explore-panel', { 'is-fullscreen': isFullscreen }]">
    <div class="sql-explore-toolbar">
      <button
        v-if="sourceQuery"
        class="action-btn"
        :disabled="saving"
        title="Save current chart to the catalog"
        @click="onSaveChart"
      >
        <i class="fa-regular fa-bookmark"></i>
        Save chart
      </button>
      <button
        class="fullscreen-toggle"
        :title="isFullscreen ? 'Exit fullscreen (Esc)' : 'Fullscreen'"
        @click="toggleFullscreen"
      >
        <i :class="isFullscreen ? 'fa-solid fa-compress' : 'fa-solid fa-expand'"></i>
      </button>
    </div>
    <VueGraphicWalker
      ref="vueGraphicWalkerRef"
      :data="gwData"
      :fields="gwFields"
      default-tab="data"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onBeforeUnmount, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import type {
  IMutField,
  IRow,
  ISemanticType,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/interfaces";
import type { SqlQueryResult } from "../../types";
import { CatalogApi } from "../../api/catalog.api";

const props = defineProps<{
  result: SqlQueryResult;
  /**
   * The SQL query that produced ``result``. When supplied, a "Save chart"
   * button appears: saving promotes the query to a query-virtual-table and
   * attaches the chart to it so the visualization can be reopened later.
   */
  sourceQuery?: string;
  /** Namespace under which the auto-promoted query-virtual-table is created. */
  saveNamespaceId?: number | null;
}>();

const vueGraphicWalkerRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);
const saving = ref(false);

const isFullscreen = ref(false);

function toggleFullscreen() {
  isFullscreen.value = !isFullscreen.value;
}

function handleEsc(e: KeyboardEvent) {
  if (e.key === "Escape" && isFullscreen.value) {
    isFullscreen.value = false;
  }
}

watch(isFullscreen, (val) => {
  if (val) {
    window.addEventListener("keydown", handleEsc);
    document.body.style.overflow = "hidden";
  } else {
    window.removeEventListener("keydown", handleEsc);
    document.body.style.overflow = "";
  }
});

onBeforeUnmount(() => {
  window.removeEventListener("keydown", handleEsc);
  document.body.style.overflow = "";
});

function getSemanticType(dtype: string): ISemanticType {
  const d = dtype.toLowerCase();
  if (
    d.includes("utf8") ||
    d.includes("string") ||
    d.includes("categorical") ||
    d.includes("bool")
  ) {
    return "nominal";
  }
  if (
    d.includes("int") ||
    d.includes("float") ||
    d.includes("decimal") ||
    d.includes("uint") ||
    d.includes("duration")
  ) {
    return "quantitative";
  }
  if (d.includes("date") || d.includes("time")) {
    return "temporal";
  }
  return "nominal";
}

const gwFields = computed<IMutField[]>(() =>
  props.result.columns.map((col, idx) => {
    const semanticType = getSemanticType(props.result.dtypes[idx] ?? "");
    return {
      fid: col,
      name: col,
      basename: col,
      key: col,
      semanticType,
      analyticType: semanticType === "quantitative" ? "measure" : "dimension",
    };
  }),
);

const gwData = computed<IRow[]>(() =>
  props.result.rows.map((row) => {
    const obj: Record<string, any> = {};
    props.result.columns.forEach((col, idx) => {
      obj[col] = row[idx];
    });
    return obj;
  }),
);

function defaultSavedChartName(): string {
  const now = new Date();
  const ts = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(
    now.getDate(),
  ).padStart(2, "0")} ${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;
  return `Saved chart ${ts}`;
}

async function onSaveChart() {
  if (!props.sourceQuery || !vueGraphicWalkerRef.value) return;
  const charts = await vueGraphicWalkerRef.value.exportCode();
  if (!charts || !charts.length) {
    ElMessage.warning("Build a chart in the editor before saving.");
    return;
  }

  let promptResult: { value: string } | null = null;
  try {
    promptResult = (await ElMessageBox.prompt(
      "Save this chart to the catalog. The current SQL query is promoted to a query-virtual-table so the chart can be reopened later.",
      "Save chart",
      {
        confirmButtonText: "Save",
        cancelButtonText: "Cancel",
        inputPlaceholder: "Name for the saved chart and table",
        inputValue: defaultSavedChartName(),
        inputValidator: (val) => (val && val.trim().length > 0 ? true : "Name is required"),
      },
    )) as { value: string };
  } catch {
    return;
  }
  const name = promptResult.value.trim();

  saving.value = true;
  try {
    const table = await CatalogApi.createQueryVirtualTable({
      name,
      sql_query: props.sourceQuery,
      namespace_id: props.saveNamespaceId ?? undefined,
    });
    await CatalogApi.createVisualization(table.id, {
      name,
      spec: charts[0] as Record<string, any>,
    });
    ElMessage.success(`Saved chart "${name}" under catalog table.`);
  } catch (err: any) {
    ElMessage.error(err?.response?.data?.detail ?? err?.message ?? String(err));
  } finally {
    saving.value = false;
  }
}
</script>

<style scoped>
.sql-explore-panel {
  width: 100%;
  height: 100%;
  position: relative;
}

.sql-explore-panel.is-fullscreen {
  position: fixed;
  inset: 0;
  z-index: 2000;
  background: var(--el-bg-color, #ffffff);
  padding: 8px;
}

.sql-explore-toolbar {
  position: absolute;
  top: 8px;
  right: 12px;
  z-index: 10;
  display: flex;
  align-items: center;
  gap: 8px;
}

.fullscreen-toggle {
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--color-border-light, #e4e7ed);
  border-radius: 4px;
  background: var(--el-bg-color, #ffffff);
  color: var(--color-text-secondary, #606266);
  cursor: pointer;
  font-size: 13px;
  transition:
    background 0.15s,
    color 0.15s;
}

.action-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 28px;
  padding: 0 10px;
  border: 1px solid var(--color-border-light, #e4e7ed);
  border-radius: 4px;
  background: var(--el-bg-color, #ffffff);
  color: var(--color-text-secondary, #606266);
  cursor: pointer;
  font-size: 13px;
}
.action-btn:hover:not(:disabled) {
  background: var(--color-background-hover, #f0f0f0);
  color: var(--el-color-primary, #409eff);
}
.action-btn:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}

.fullscreen-toggle:hover {
  background: var(--color-background-hover, #f0f0f0);
  color: var(--el-color-primary, #409eff);
}

.is-fullscreen .sql-explore-toolbar {
  top: 12px;
  right: 16px;
}
</style>
