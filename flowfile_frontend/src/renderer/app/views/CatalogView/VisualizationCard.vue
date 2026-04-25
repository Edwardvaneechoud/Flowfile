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
        v-else-if="rows.length"
        :data="rows"
        :fields="fields"
        :spec-list="[viz.spec]"
        :appearance="appearance"
      />
      <div v-else class="viz-card-state">
        <el-empty :image-size="60" description="No data" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from "vue";
import { Delete, Edit, MoreFilled } from "@element-plus/icons-vue";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import type { CatalogVisualization, VizSourceDescriptor } from "../../types";

const props = defineProps<{
  viz: CatalogVisualization;
  source: VizSourceDescriptor;
  appearance?: string;
}>();

const emit = defineEmits<{ (e: "edit"): void; (e: "delete"): void }>();

const store = useCatalogStore();

const rows = ref<Record<string, any>[]>([]);
const fields = ref<Record<string, any>[]>([]);
const loading = ref(false);
const errorMessage = ref<string | null>(null);

const load = async () => {
  loading.value = true;
  errorMessage.value = null;
  try {
    const [data, schemaFields] = await Promise.all([
      CatalogApi.computeSavedVisualization(props.viz.catalog_table_id, props.viz.id),
      store.loadVisualizationFields(props.source),
    ]);
    if (data.error) {
      errorMessage.value = data.error;
      rows.value = [];
    } else {
      rows.value = data.rows;
    }
    fields.value = schemaFields;
  } catch (err: any) {
    errorMessage.value = err?.response?.data?.detail ?? err?.message ?? String(err);
  } finally {
    loading.value = false;
  }
};

onMounted(load);

watch(() => props.viz.id, load);
watch(
  () => props.viz.spec,
  load,
  { deep: true },
);
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
