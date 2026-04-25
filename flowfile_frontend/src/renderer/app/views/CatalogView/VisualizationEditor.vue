<template>
  <div class="viz-editor">
    <div class="viz-editor-toolbar">
      <el-input
        v-model="name"
        placeholder="Visualization name"
        size="small"
        class="viz-name-input"
        :disabled="saving"
      />
      <div class="viz-editor-actions">
        <el-button size="small" :disabled="saving" @click="emit('cancel')">Cancel</el-button>
        <el-button
          type="primary"
          size="small"
          :loading="saving"
          :disabled="!canSave"
          @click="save"
        >
          {{ viz ? "Save changes" : "Save visualization" }}
        </el-button>
      </div>
    </div>

    <div v-if="loadingSample" class="viz-editor-state">
      <el-skeleton :rows="6" animated />
    </div>

    <div v-else-if="loadError" class="viz-editor-state">
      <el-alert :title="loadError" type="error" :closable="false" show-icon />
    </div>

    <VueGraphicWalker
      v-else
      ref="gwRef"
      :data="sampleRows"
      :fields="fields"
      :spec-list="initialSpec ? [initialSpec] : undefined"
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
import type {
  CatalogVisualization,
  VizSourceDescriptor,
} from "../../types";

const props = defineProps<{
  source: VizSourceDescriptor;
  viz?: CatalogVisualization | null;
  appearance?: string;
  /** Optional override for the table id when saving a viz from an ad-hoc SQL source. */
  saveTargetTableId?: number | null;
}>();

const emit = defineEmits<{
  (e: "saved", viz: CatalogVisualization): void;
  (e: "cancel"): void;
}>();

const store = useCatalogStore();
const gwRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const name = ref(props.viz?.name ?? "");
const saving = ref(false);

const loadingSample = ref(true);
const loadError = ref<string | null>(null);
const sampleRows = ref<Record<string, any>[]>([]);
const fields = ref<Record<string, any>[]>([]);

const initialSpec = computed(() => props.viz?.spec ?? null);

const canSave = computed(() => name.value.trim().length > 0 && !loadingSample.value);

const SAMPLE_ROWS = 5_000;

onMounted(async () => {
  loadingSample.value = true;
  loadError.value = null;
  try {
    fields.value = await store.loadVisualizationFields(props.source);
    // Pull a small representative sample so the GraphicWalker editor has data
    // to draw against while the user iterates. Heavy aggregations the user
    // builds on top route through the sample client-side; saving and the
    // saved-viz playback always re-run server-side via the worker cache.
    const sample = await CatalogApi.computeAdHocVisualization(
      props.source,
      { workflow: [{ type: "view", query: [{ op: "raw", fields: ["*"] }] }] },
      SAMPLE_ROWS,
    );
    if (sample.error) {
      loadError.value = sample.error;
    } else {
      sampleRows.value = sample.rows;
    }
  } catch (err: any) {
    loadError.value = err?.response?.data?.detail ?? err?.message ?? String(err);
  } finally {
    loadingSample.value = false;
  }
});

const resolveTargetTableId = (): number | null => {
  if (props.saveTargetTableId !== undefined && props.saveTargetTableId !== null) {
    return props.saveTargetTableId;
  }
  if (props.viz) return props.viz.catalog_table_id;
  if (props.source.source_type === "table" && props.source.table_id) {
    return props.source.table_id;
  }
  return null;
};

const save = async () => {
  if (!gwRef.value) return;
  const charts = await gwRef.value.exportCode();
  if (!charts || !charts.length) {
    ElMessage.error("No chart to save — build one in the editor first.");
    return;
  }
  const tableId = resolveTargetTableId();
  if (tableId === null) {
    ElMessage.error(
      "This chart is built from a SQL query that hasn't been promoted to a catalog table yet.",
    );
    return;
  }
  const spec = charts[0] as Record<string, any>;
  saving.value = true;
  try {
    let saved: CatalogVisualization;
    if (props.viz) {
      saved = await store.updateVisualization(tableId, props.viz.id, {
        name: name.value.trim(),
        spec,
      });
    } else {
      saved = await store.createVisualization(tableId, {
        name: name.value.trim(),
        spec,
      });
    }
    ElMessage.success(`Saved "${saved.name}"`);
    emit("saved", saved);
  } catch (err: any) {
    ElMessage.error(err?.response?.data?.detail ?? err?.message ?? String(err));
  } finally {
    saving.value = false;
  }
};
</script>

<style scoped>
.viz-editor {
  display: flex;
  flex-direction: column;
  gap: 12px;
  height: 75vh;
}
.viz-editor-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  justify-content: space-between;
}
.viz-name-input {
  max-width: 360px;
}
.viz-editor-actions {
  display: flex;
  gap: 8px;
}
.viz-editor-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
}
</style>
