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
          <span>{{ sourceLabel }}</span>
        </div>
        <div v-if="viz?.description" class="viz-viewer-desc">{{ viz.description }}</div>
      </div>
      <div class="viz-viewer-actions">
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

    <div v-if="loadingMeta || loadingData" class="viz-viewer-state">
      <el-skeleton :rows="6" animated />
    </div>

    <div v-else-if="errorMessage" class="viz-viewer-state">
      <el-alert :title="errorMessage" type="error" :closable="false" show-icon />
    </div>

    <VueGraphicWalker
      v-else
      ref="gwRef"
      :data="plainRows"
      :fields="plainFields"
      :spec-list="plainSpecList"
      :appearance="appearance"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ElMessage } from "element-plus";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import type { CatalogVisualization } from "../../types";

const props = defineProps<{
  vizId: number;
  appearance?: string;
}>();

const emit = defineEmits<{
  (e: "close"): void;
  (e: "deleted", vizId: number): void;
}>();

const store = useCatalogStore();
const gwRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const viz = ref<CatalogVisualization | null>(null);
const rows = ref<Record<string, any>[]>([]);
const fields = ref<Record<string, any>[]>([]);
const loadingMeta = ref(true);
const loadingData = ref(true);
const errorMessage = ref<string | null>(null);
const saving = ref(false);

const SAMPLE_ROWS = 100_000;

// Deep-clone JSON so GraphicWalker's web worker can structuredClone the
// payload without tripping on Vue reactive proxies / getters.
const toPlainJson = <T,>(value: T): T => JSON.parse(JSON.stringify(value));

const plainRows = computed(() => toPlainJson(rows.value));
const plainFields = computed(() => toPlainJson(fields.value));
const plainSpecList = computed(() =>
  viz.value?.spec ? [toPlainJson(viz.value.spec)] : undefined,
);

const sourceLabel = computed(() => {
  if (!viz.value) return "";
  if (viz.value.source_type === "sql") {
    return viz.value.sql_query
      ? `SQL · ${viz.value.sql_query.slice(0, 60)}${viz.value.sql_query.length > 60 ? "…" : ""}`
      : "SQL query";
  }
  return viz.value.catalog_table_id !== null ? `Table id ${viz.value.catalog_table_id}` : "Table";
});

async function load() {
  errorMessage.value = null;
  loadingMeta.value = true;
  try {
    viz.value = await CatalogApi.getVisualization(props.vizId);
  } catch (err: any) {
    errorMessage.value = err?.response?.data?.detail ?? err?.message ?? String(err);
    loadingMeta.value = false;
    loadingData.value = false;
    return;
  }
  loadingMeta.value = false;

  loadingData.value = true;
  try {
    const [data, fieldsResp] = await Promise.all([
      CatalogApi.computeSavedVisualization(props.vizId, SAMPLE_ROWS),
      CatalogApi.getSavedVisualizationFields(props.vizId),
    ]);
    if (data.error) {
      errorMessage.value = data.error;
    } else {
      rows.value = data.rows;
    }
    if (fieldsResp.error) {
      errorMessage.value = errorMessage.value ?? fieldsResp.error;
    } else {
      fields.value = fieldsResp.fields;
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

async function onSave() {
  if (!gwRef.value || !viz.value) return;
  const charts = await gwRef.value.exportCode();
  if (!charts || !charts.length) {
    ElMessage.error("No chart to save — build one in the editor first.");
    return;
  }
  saving.value = true;
  try {
    const updated = await store.updateVisualization(props.vizId, {
      spec: charts[0] as Record<string, any>,
    });
    viz.value = updated;
    ElMessage.success("Saved chart updates");
  } catch (err: any) {
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
  gap: 6px;
  color: var(--el-text-color-secondary);
  font-size: 13px;
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
  gap: 8px;
}
.viz-viewer-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
}
</style>
