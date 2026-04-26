<template>
  <div class="viz-card">
    <div class="viz-card-header">
      <div class="viz-card-title">
        <span class="viz-name">{{ viz.name }}</span>
        <span v-if="viz.chart_type" class="viz-chart-type">{{ viz.chart_type }}</span>
      </div>
      <el-dropdown trigger="click">
        <el-icon class="viz-card-menu"><MoreFilled /></el-icon>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item @click="emit('edit')">
              <el-icon><Edit /></el-icon>
              Edit
            </el-dropdown-item>
            <el-dropdown-item @click="emit('delete')" divided>
              <el-icon><Delete /></el-icon>
              Delete
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>

    <div class="viz-card-body">
      <div v-if="loading" class="viz-card-state">
        <el-skeleton :rows="4" animated />
      </div>
      <div v-else-if="errorMessage" class="viz-card-state viz-card-error">
        <el-alert :title="errorMessage" type="error" :closable="false" show-icon />
      </div>
      <VueGraphicWalker
        v-else
        :computation="computeOnWorker"
        :fields="plainFields"
        :spec-list="plainSpecList"
        :appearance="appearance"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { Delete, Edit, MoreFilled } from "@element-plus/icons-vue";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import { CatalogApi } from "../../api/catalog.api";
import type { CatalogVisualization, VizSourceDescriptor } from "../../types";

/**
 * Deep-clone via JSON round-trip so GraphicWalker's web worker can
 * structuredClone the payload. Vue refs/proxies and any getters can
 * otherwise trigger ``Failed to execute 'postMessage' on 'Worker'``.
 */
function toPlainJson<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

const props = defineProps<{
  viz: CatalogVisualization;
  source: VizSourceDescriptor;
  appearance?: string;
}>();

const emit = defineEmits<{ (e: "edit"): void; (e: "delete"): void }>();

const fields = ref<Record<string, any>[]>([]);
const loading = ref(false);
const errorMessage = ref<string | null>(null);

const plainFields = computed(() => toPlainJson(fields.value));
const plainSpecList = computed(() =>
  props.viz.spec && props.viz.spec.length ? toPlainJson(props.viz.spec) : undefined,
);

const SAMPLE_ROWS = 100_000;

/** Hand every GW aggregation to the worker via polars-gw. */
async function computeOnWorker(payload: any): Promise<any[]> {
  try {
    const resp = await CatalogApi.computeSavedVisualization(props.viz.id, {
      payload,
      maxRows: SAMPLE_ROWS,
    });
    if (resp.error) {
      console.error("[viz] card compute failed:", resp.error);
      return [];
    }
    return resp.rows;
  } catch (err: any) {
    console.error("[viz] card compute threw:", err);
    return [];
  }
}

const load = async () => {
  loading.value = true;
  errorMessage.value = null;
  try {
    // Only the field schema is fetched up front; rows are pulled on demand
    // by GraphicWalker through the computeOnWorker callback.
    const fieldsResp = await CatalogApi.getSavedVisualizationFields(props.viz.id);
    if (fieldsResp.error) {
      errorMessage.value = fieldsResp.error;
    } else {
      fields.value = fieldsResp.fields;
    }
  } catch (err: any) {
    errorMessage.value = err?.response?.data?.detail ?? err?.message ?? String(err);
  } finally {
    loading.value = false;
  }
};

onMounted(load);

watch(() => props.viz.id, load);
</script>

<style scoped>
.viz-card {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--el-border-color-light);
  border-radius: 8px;
  background: var(--el-bg-color);
  overflow: hidden;
}
.viz-card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 12px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}
.viz-card-title {
  display: flex;
  align-items: baseline;
  gap: 8px;
  min-width: 0;
}
.viz-name {
  font-weight: 600;
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viz-chart-type {
  font-size: 11px;
  color: var(--el-text-color-secondary);
  text-transform: uppercase;
}
.viz-card-menu {
  cursor: pointer;
}
.viz-card-body {
  position: relative;
  height: 320px;
}
.viz-card-state {
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
}
.viz-card-error {
  align-items: stretch;
}
</style>
