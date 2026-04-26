<template>
  <div class="viz-viewer">
    <div class="viz-viewer-toolbar">
      <div class="viz-viewer-meta">
        <div class="viz-viewer-source">
          <i
            :class="
              viz?.source_type === 'sql'
                ? 'fa-solid fa-code source-icon sql'
                : 'fa-solid fa-table source-icon'
            "
          ></i>
          <span class="viz-source-label">{{ sourceLabel }}</span>
          <button
            v-if="viz?.source_type === 'sql' && viz.sql_query"
            class="viz-toggle-sql"
            type="button"
            @click="sqlExpanded = !sqlExpanded"
          >
            {{ sqlExpanded ? "Hide query" : "Show query" }}
          </button>
        </div>
        <div v-if="viz?.description" class="viz-viewer-desc">{{ viz.description }}</div>
      </div>
      <div class="viz-viewer-actions">
        <div class="viz-namespace-picker">
          <span class="viz-namespace-label">Catalog</span>
          <el-select
            v-model="namespaceDraft"
            size="small"
            placeholder="(none)"
            clearable
            :disabled="loadingMeta || loadingData || saving"
            @change="onNamespaceChange"
          >
            <el-option
              v-for="ns in schemaNamespaces"
              :key="ns.id"
              :label="ns.label"
              :value="ns.id"
            />
          </el-select>
        </div>
        <el-button size="small" :disabled="saving || loadingData" @click="reload">Reset</el-button>
        <el-button
          type="primary"
          size="small"
          :disabled="loadingData"
          :loading="saving"
          @click="onSave"
        >
          Save changes
        </el-button>
      </div>
    </div>

    <pre v-if="sqlExpanded && viz?.source_type === 'sql' && viz.sql_query" class="viz-sql-block">{{
      viz.sql_query
    }}</pre>

    <div v-if="loadingMeta || loadingData" class="viz-viewer-state">
      <el-skeleton :rows="6" animated />
    </div>

    <div v-else-if="errorMessage" class="viz-viewer-state">
      <el-alert :title="errorMessage" type="error" :closable="false" show-icon />
    </div>

    <VueGraphicWalker
      v-else
      ref="gwRef"
      :computation="computeOnWorker"
      :fields="plainFields"
      :spec-list="plainSpecList"
      :appearance="appearance"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ElMessage } from "element-plus";
import type { IChart, IDarkMode, IMutField } from "@kanaries/graphic-walker/dist/interfaces";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import type { CatalogVisualization } from "../../types";

const props = defineProps<{
  vizId: number;
  appearance?: IDarkMode;
}>();

defineEmits<{
  (e: "close"): void;
  (e: "deleted", vizId: number): void;
}>();

const store = useCatalogStore();
const gwRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const viz = ref<CatalogVisualization | null>(null);
const fields = ref<IMutField[]>([]);
const loadingMeta = ref(true);
const loadingData = ref(true);
const errorMessage = ref<string | null>(null);
const saving = ref(false);

// User-edited namespace; persisted to the viz on save (or eagerly on change).
const namespaceDraft = ref<number | null>(null);

const SAMPLE_ROWS = 100_000;

/**
 * GraphicWalker computation callback — every aggregation the user builds in
 * the chart hits this function with an IDataQueryPayload, and we POST it
 * straight to the worker via /catalog/visualizations/{id}/compute. The
 * worker session cache keeps the source LazyFrame warm so successive
 * aggregations skip the load step.
 */
async function computeOnWorker(payload: any): Promise<any[]> {
  try {
    const resp = await CatalogApi.computeSavedVisualization(props.vizId, {
      payload,
      maxRows: SAMPLE_ROWS,
    });
    if (resp.error) {
      console.error("[viz] saved compute failed:", resp.error);
      return [];
    }
    return resp.rows;
  } catch (err: any) {
    console.error("[viz] saved compute threw:", err);
    return [];
  }
}

// Flat schema-level list for the namespace picker. Mirrors the SQL save dialog.
const schemaNamespaces = computed(() => {
  const items: { id: number; label: string }[] = [];
  for (const cat of store.tree) {
    for (const schema of cat.children) {
      items.push({ id: schema.id, label: `${cat.name} / ${schema.name}` });
    }
  }
  return items;
});

// Deep-clone JSON so GraphicWalker's web worker can structuredClone the
// payload without tripping on Vue reactive proxies / getters.
const toPlainJson = <T,>(value: T): T => JSON.parse(JSON.stringify(value));

const plainFields = computed(() => toPlainJson(fields.value));
const plainSpecList = computed<IChart[] | undefined>(() =>
  viz.value?.spec && viz.value.spec.length
    ? (toPlainJson(viz.value.spec) as unknown as IChart[])
    : undefined,
);

const sqlExpanded = ref(false);

const sourceLabel = computed(() => {
  if (!viz.value) return "";
  if (viz.value.source_type === "sql") {
    return "SQL query";
  }
  // Prefer the qualified ``namespace.tablename`` form; fall back to the bare
  // table name when no namespace is set, then to a placeholder for orphans.
  return viz.value.table_full_name ?? viz.value.table_name ?? "(deleted table)";
});

async function load() {
  errorMessage.value = null;
  loadingMeta.value = true;
  try {
    viz.value = await CatalogApi.getVisualization(props.vizId);
    namespaceDraft.value = viz.value?.namespace_id ?? null;
  } catch (err: any) {
    errorMessage.value = err?.response?.data?.detail ?? err?.message ?? String(err);
    loadingMeta.value = false;
    loadingData.value = false;
    return;
  }
  loadingMeta.value = false;
  // Make sure the namespace tree is available so the picker is populated.
  if (store.tree.length === 0) {
    store.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
  }

  loadingData.value = true;
  try {
    // We only fetch the field schema up front — GraphicWalker pulls rows
    // on demand through computeOnWorker so every aggregation pushes down
    // to polars-gw.
    const fieldsResp = await CatalogApi.getSavedVisualizationFields(props.vizId);
    if (fieldsResp.error) {
      errorMessage.value = fieldsResp.error;
    } else {
      fields.value = fieldsResp.fields as IMutField[];
    }
  } catch (err: any) {
    errorMessage.value = err?.response?.data?.detail ?? err?.message ?? String(err);
  } finally {
    loadingData.value = false;
  }
}

async function reload() {
  await load();
}

// Match the server-side cap; drop the thumbnail rather than fail the save
// when GW returns a larger PNG.
const THUMBNAIL_MAX_BYTES = 200_000;

async function captureThumbnail(): Promise<string | null> {
  if (!gwRef.value || typeof gwRef.value.exportImage !== "function") return null;
  try {
    const dataUrl = await gwRef.value.exportImage();
    if (!dataUrl) return null;
    if (dataUrl.length > THUMBNAIL_MAX_BYTES) {
      console.warn(`[viz] thumbnail too large (${dataUrl.length} bytes), skipping`);
      return null;
    }
    return dataUrl;
  } catch (err) {
    console.warn("[viz] thumbnail capture failed:", err);
    return null;
  }
}

async function onSave() {
  if (!gwRef.value || !viz.value) return;
  const charts = await gwRef.value.exportCode();
  if (!charts || !charts.length) {
    ElMessage.error("No chart to save — build one in the editor first.");
    return;
  }
  const thumbnail_data_url = await captureThumbnail();
  saving.value = true;
  try {
    const updated = await store.updateVisualization(props.vizId, {
      spec: charts as Record<string, any>[],
      namespace_id: namespaceDraft.value,
      thumbnail_data_url,
    });
    viz.value = updated;
    namespaceDraft.value = updated.namespace_id ?? null;
    ElMessage.success("Saved chart updates");
    store.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
  } catch (err: any) {
    ElMessage.error(err?.response?.data?.detail ?? err?.message ?? String(err));
  } finally {
    saving.value = false;
  }
}

/** Persist a namespace move immediately so users don't have to click Save
 * just to relocate a chart. The chart spec is left untouched. */
async function onNamespaceChange(value: number | null | undefined) {
  if (!viz.value) return;
  const next = value ?? null;
  if (next === viz.value.namespace_id) return;
  saving.value = true;
  try {
    const updated = await store.updateVisualization(props.vizId, {
      namespace_id: next,
    });
    viz.value = updated;
    namespaceDraft.value = updated.namespace_id ?? null;
    store.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
  } catch (err: any) {
    // Roll the picker back if the update failed.
    namespaceDraft.value = viz.value.namespace_id ?? null;
    ElMessage.error(err?.response?.data?.detail ?? err?.message ?? String(err));
  } finally {
    saving.value = false;
  }
}

onMounted(load);
</script>

<style scoped>
.viz-viewer {
  display: flex;
  flex-direction: column;
  gap: 12px;
  height: 75vh;
}
.viz-viewer-toolbar {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
}
.viz-viewer-meta {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
  flex: 1;
}
.viz-viewer-source {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--el-text-color-secondary);
  font-size: 13px;
}
.viz-source-label {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viz-toggle-sql {
  border: none;
  background: transparent;
  color: var(--el-color-primary);
  cursor: pointer;
  padding: 0;
  font-size: 12px;
  text-decoration: underline;
}
.viz-toggle-sql:hover {
  color: var(--el-color-primary-light-3);
}
.viz-sql-block {
  margin: 0;
  padding: 8px 10px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
  background: var(--el-fill-color-lighter);
  color: var(--el-text-color-regular);
  font-family: var(--el-font-family-monospace, monospace);
  font-size: 12px;
  white-space: pre-wrap;
  max-height: 30vh;
  overflow: auto;
}
.source-icon {
  color: var(--el-text-color-regular);
}
.source-icon.sql {
  color: var(--el-color-primary);
}
.viz-viewer-desc {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}
.viz-viewer-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.viz-namespace-picker {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-right: 4px;
}
.viz-namespace-label {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.viz-namespace-picker .el-select {
  width: 220px;
}
.viz-viewer-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
}
</style>
