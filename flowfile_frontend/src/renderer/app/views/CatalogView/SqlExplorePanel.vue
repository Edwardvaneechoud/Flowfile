<template>
  <div :class="['sql-explore-panel', { 'is-fullscreen': isFullscreen }]">
    <div class="sql-explore-toolbar">
      <button
        v-if="sourceQuery"
        class="action-btn"
        :disabled="saving"
        title="Save current chart to the catalog"
        @click="openSaveDialog"
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
      :computation="useWorkerCompute ? computeOnWorker : undefined"
      :data="useWorkerCompute ? undefined : gwData"
      :fields="gwFields"
      default-tab="data"
    />

    <el-dialog
      v-model="saveDialogOpen"
      title="Save chart"
      width="480px"
      append-to-body
      destroy-on-close
    >
      <el-form label-position="top" @submit.prevent="onConfirmSave">
        <el-form-item label="Name" required>
          <el-input v-model="saveForm.name" placeholder="e.g. Revenue by industry" />
        </el-form-item>
        <el-form-item label="Catalog / Schema">
          <el-select
            v-model="saveForm.namespaceId"
            placeholder="Pick a namespace so the chart shows up in the catalog tree"
            clearable
            style="width: 100%"
          >
            <el-option
              v-for="ns in schemaNamespaces"
              :key="ns.id"
              :label="ns.label"
              :value="ns.id"
            />
          </el-select>
          <div class="save-form-hint">
            Charts without a namespace are accessible from the Visualizations tab but won't appear
            in the namespace tree.
          </div>
        </el-form-item>
        <el-form-item label="Description">
          <el-input
            v-model="saveForm.description"
            type="textarea"
            :rows="2"
            placeholder="Optional"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button :disabled="saving" @click="saveDialogOpen = false">Cancel</el-button>
        <el-button
          type="primary"
          :loading="saving"
          :disabled="!saveForm.name.trim()"
          @click="onConfirmSave"
        >
          Save
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref, onBeforeUnmount, watch } from "vue";
import { ElMessage } from "element-plus";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import type {
  IMutField,
  IRow,
  ISemanticType,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/interfaces";
import type { SqlQueryResult } from "../../types";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import { useGraphicWalkerCompute } from "../../composables/useGraphicWalkerCompute";

const catalogStore = useCatalogStore();

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

/**
 * Use the worker compute path when we have the SQL query in hand. This makes
 * every chart aggregation in GraphicWalker round-trip through the worker via
 * polars-gw, so the chart aggregates over the full table behind the SQL —
 * not just the (typically 10k-row) preview the SQL editor cached.
 */
const useWorkerCompute = computed(() => !!props.sourceQuery);

const { computation: computeOnWorker } = useGraphicWalkerCompute(async (payload) => {
  if (!props.sourceQuery) return { rows: [], error: null };
  return CatalogApi.computeAdHocVisualization(
    { source_type: "sql", sql_query: props.sourceQuery },
    payload,
    100_000,
  );
}, "sql-explore");

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
  ).padStart(
    2,
    "0",
  )} ${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;
  return `Saved chart ${ts}`;
}

const saveDialogOpen = ref(false);
const saveForm = reactive({
  name: "",
  description: "",
  namespaceId: null as number | null,
});
const exportedCharts = ref<Record<string, any>[]>([]);

// Flatten the namespace tree to a list of schema-level entries the user
// can attach a chart to. Mirrors the existing virtual-table save form.
const schemaNamespaces = computed(() => {
  const items: { id: number; label: string }[] = [];
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      items.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
    }
  }
  return items;
});

async function openSaveDialog() {
  if (!props.sourceQuery || !vueGraphicWalkerRef.value) return;
  const charts = await vueGraphicWalkerRef.value.exportCode();
  if (!charts || !charts.length) {
    ElMessage.warning("Build a chart in the editor before saving.");
    return;
  }
  exportedCharts.value = charts as Record<string, any>[];
  saveForm.name = defaultSavedChartName();
  saveForm.description = "";
  // Default to the namespace the SQL editor is configured against, if any.
  saveForm.namespaceId = props.saveNamespaceId ?? null;
  // Make sure the picker has data to render.
  if (catalogStore.tree.length === 0) {
    catalogStore.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
  }
  saveDialogOpen.value = true;
}

async function onConfirmSave() {
  if (!props.sourceQuery) return;
  const name = saveForm.name.trim();
  if (!name) {
    ElMessage.warning("Enter a name to save the visualization.");
    return;
  }
  saving.value = true;
  try {
    await CatalogApi.createVisualization({
      name,
      description: saveForm.description.trim() || null,
      // Save the full IChart[] so multi-tab specs round-trip.
      spec: exportedCharts.value,
      source_type: "sql",
      sql_query: props.sourceQuery,
      namespace_id: saveForm.namespaceId ?? null,
    });
    const where = saveForm.namespaceId
      ? (schemaNamespaces.value.find((n) => n.id === saveForm.namespaceId)?.label ?? "the catalog")
      : "the Visualizations tab";
    ElMessage.success(`Saved chart "${name}" to ${where}.`);
    saveDialogOpen.value = false;
    // Refresh the namespace tree so the new chart shows up under its
    // catalog/schema right away.
    catalogStore.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
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

.save-form-hint {
  margin-top: 4px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
  line-height: 1.4;
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
