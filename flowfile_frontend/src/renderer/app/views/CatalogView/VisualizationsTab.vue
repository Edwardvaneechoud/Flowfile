<template>
  <div class="visualizations-tab">
    <div class="viz-header">
      <h3>Saved visualizations</h3>
      <el-button type="primary" size="small" @click="openEditor">
        <el-icon><Plus /></el-icon>
        New visualization
      </el-button>
    </div>

    <div v-if="loading" class="viz-loading">
      <el-skeleton :rows="3" animated />
    </div>

    <div v-else-if="!visualizations.length" class="viz-empty">
      <el-empty description="No visualizations yet. Create one to get started." />
    </div>

    <div v-else class="viz-grid">
      <VisualizationCard
        v-for="viz in visualizations"
        :key="viz.id"
        :viz="viz"
        :source="tableSource"
        :appearance="appearance"
        @edit="openEditor(viz)"
        @delete="onDelete(viz)"
      />
    </div>

    <el-dialog
      v-model="editorOpen"
      :title="editingViz ? `Edit: ${editingViz.name}` : 'New visualization'"
      width="92vw"
      destroy-on-close
      append-to-body
    >
      <VisualizationEditor
        v-if="editorOpen"
        :source="tableSource"
        :viz="editingViz"
        :appearance="appearance"
        @saved="onSaved"
        @cancel="editorOpen = false"
      />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { Plus } from "@element-plus/icons-vue";
import { useCatalogStore } from "../../stores/catalog-store";
import { useGraphicWalkerAppearance } from "../../composables/useGraphicWalkerAppearance";
import type { CatalogVisualization, VizSourceDescriptor } from "../../types";
import VisualizationCard from "./VisualizationCard.vue";
import VisualizationEditor from "./VisualizationEditor.vue";

const props = defineProps<{ tableId: number }>();

const store = useCatalogStore();
const appearance = useGraphicWalkerAppearance();

const editorOpen = ref(false);
const editingViz = ref<CatalogVisualization | null>(null);

const visualizations = computed(
  () => store.visualizationsByTable[props.tableId] ?? [],
);

const loading = computed(
  () => store.loadingVisualizations && !visualizations.value.length,
);

const tableSource = computed<VizSourceDescriptor>(() => ({
  source_type: "table",
  table_id: props.tableId,
}));

const reload = async () => {
  try {
    await store.loadVisualizations(props.tableId);
  } catch (err: any) {
    ElMessage.error(`Failed to load visualizations: ${err?.message ?? err}`);
  }
};

onMounted(reload);

watch(
  () => props.tableId,
  (id, prev) => {
    if (id !== prev) reload();
  },
);

const openEditor = (viz?: CatalogVisualization) => {
  editingViz.value = viz ?? null;
  editorOpen.value = true;
};

const onSaved = () => {
  editorOpen.value = false;
  editingViz.value = null;
  reload();
};

const onDelete = async (viz: CatalogVisualization) => {
  try {
    await ElMessageBox.confirm(
      `Delete visualization "${viz.name}"?`,
      "Confirm delete",
      { type: "warning" },
    );
  } catch {
    return;
  }
  try {
    await store.deleteVisualization(props.tableId, viz.id);
    ElMessage.success(`Deleted "${viz.name}"`);
  } catch (err: any) {
    ElMessage.error(`Failed to delete: ${err?.message ?? err}`);
  }
};
</script>

<style scoped>
.visualizations-tab {
  display: flex;
  flex-direction: column;
  height: 100%;
  gap: 16px;
  padding: 16px;
}
.viz-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.viz-header h3 {
  margin: 0;
  font-size: 16px;
}
.viz-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 16px;
}
.viz-loading,
.viz-empty {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
}
</style>
