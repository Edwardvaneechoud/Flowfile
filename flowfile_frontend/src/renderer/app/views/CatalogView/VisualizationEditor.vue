<template>
  <div class="viz-editor">
    <el-alert
      v-if="restoreAvailable"
      type="info"
      :closable="false"
      show-icon
      class="viz-draft-banner"
    >
      <template #title>
        Pick up where you left off? Your unsaved chart from this source is still here.
      </template>
      <div class="viz-draft-actions">
        <el-button size="small" type="primary" @click="onRestoreDraft">Restore</el-button>
        <el-button size="small" @click="onDiscardDraft">Discard</el-button>
      </div>
    </el-alert>

    <div class="viz-editor-toolbar">
      <div class="viz-name-field">
        <label class="viz-name-label">
          Name
          <span class="viz-name-required">*</span>
        </label>
        <el-input
          v-model="name"
          placeholder="e.g. Revenue by region"
          size="small"
          class="viz-name-input"
          :disabled="saving"
        />
      </div>
      <div class="viz-editor-actions">
        <el-tooltip v-if="disabledReason" :content="disabledReason" placement="top">
          <span class="viz-disabled-hint">{{ disabledReason }}</span>
        </el-tooltip>
        <el-button size="small" :disabled="saving" @click="onCancel">Cancel</el-button>
        <el-button type="primary" size="small" :loading="saving" :disabled="!canSave" @click="save">
          {{ viz ? "Save changes" : "Save visualization" }}
        </el-button>
      </div>
    </div>

    <div class="viz-scroll-area">
      <div v-if="loadingSample" class="viz-editor-state">
        <el-skeleton :rows="6" animated />
      </div>

      <div v-else-if="loadError" class="viz-editor-state">
        <el-alert :title="loadError" type="error" :closable="false" show-icon />
      </div>

      <template v-else>
        <el-alert
          v-if="computeError"
          :title="computeError"
          type="error"
          :closable="false"
          show-icon
        />
        <VueGraphicWalker
          ref="gwRef"
          :key="gwMountKey"
          :computation="computeOnWorker"
          :fields="plainFields"
          :spec-list="plainInitialSpecList"
          :appearance="appearance"
          :hide-chart-nav="true"
          :watch-spec="isCreate"
          @spec-changed="onSpecChanged"
        />
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import type { IChart, IDarkMode, IMutField } from "@kanaries/graphic-walker/interfaces";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import { useAuthStore } from "../../stores/auth-store";
import { createLocalDraft, type LocalDraft } from "../../composables/localDraft";
import { vizDraftKey } from "./vizAutosave";
import { captureThumbnail } from "../../composables/useChartThumbnail";
import { useGraphicWalkerCompute } from "../../composables/useGraphicWalkerCompute";
import { toPlainJson } from "../../utils/structuredClone";
import { catalogSaveErrorMessage } from "../../composables/saveError";
import type {
  CatalogVisualization,
  VisualizationCreatePayload,
  VisualizationUpdatePayload,
  VizSourceDescriptor,
} from "../../types";

const props = defineProps<{
  source: VizSourceDescriptor;
  viz?: CatalogVisualization | null;
  appearance?: IDarkMode;
  /** Optional override for the table id when saving a viz from an ad-hoc SQL source. */
  saveTargetTableId?: number | null;
}>();

const emit = defineEmits<{
  (e: "saved", viz: CatalogVisualization): void;
  (e: "cancel"): void;
}>();

const store = useCatalogStore();
const gwRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const name = ref(props.viz?.name ?? "Untitled chart");
const saving = ref(false);

const isCreate = computed(() => !isExistingViz(props.viz));
const initialName = name.value;

// Create-mode crash/close protection via a localStorage draft + restore banner.
const specDirty = ref(false);
const restoreAvailable = ref(false);
const specOverride = ref<Record<string, any>[] | null>(null);
const gwMountKey = ref(0);
let lastSpec: Record<string, any>[] | null = null;
let draft: LocalDraft<{ name: string; spec: Record<string, any>[] }> | null = null;
let pendingDraft: { name: string; spec: Record<string, any>[] } | null = null;

const createCompleted = ref(false);
const isDirty = computed(
  () =>
    isCreate.value &&
    !createCompleted.value &&
    (specDirty.value || name.value.trim() !== initialName.trim()),
);

// First edit claims the single draft slot; the restore offer is now stale.
const dismissRestoreOnEdit = () => {
  if (restoreAvailable.value) {
    restoreAvailable.value = false;
    pendingDraft = null;
  }
};

const onSpecChanged = async () => {
  const charts = await gwRef.value?.exportCode();
  if (charts && charts.length) {
    dismissRestoreOnEdit();
    lastSpec = charts as unknown as Record<string, any>[];
    specDirty.value = true;
    draft?.scheduleWrite();
  }
};

watch(name, () => {
  if (isCreate.value && isDirty.value) {
    dismissRestoreOnEdit();
    draft?.scheduleWrite();
  }
});

const onRestoreDraft = () => {
  if (!pendingDraft) return;
  name.value = pendingDraft.name;
  specOverride.value = pendingDraft.spec;
  lastSpec = pendingDraft.spec;
  specDirty.value = true;
  gwMountKey.value += 1;
  restoreAvailable.value = false;
};

const onDiscardDraft = () => {
  draft?.discard();
  pendingDraft = null;
  restoreAvailable.value = false;
};

/** Pull the latest spec out of GW (while it is still mounted) and persist the draft. */
const flushDraft = async () => {
  // A clean-session write would delete a previous session's still-offered draft.
  if (!draft || !isCreate.value || !isDirty.value) return;
  const charts = await gwRef.value?.exportCode();
  if (charts && charts.length) lastSpec = charts as unknown as Record<string, any>[];
  draft.writeNow();
};

const onCancel = async () => {
  await flushDraft();
  emit("cancel");
};

defineExpose({ isDirty, flushDraft });

const loadingSample = ref(true);
const loadError = ref<string | null>(null);
const fields = ref<IMutField[]>([]);

const initialSpec = computed(() => props.viz?.spec ?? null);

const plainFields = computed(() => toPlainJson(fields.value));
const plainInitialSpecList = computed<IChart[] | undefined>(() => {
  if (specOverride.value && specOverride.value.length > 0) {
    return toPlainJson(specOverride.value) as unknown as IChart[];
  }
  return initialSpec.value && initialSpec.value.length > 0
    ? (toPlainJson(initialSpec.value) as unknown as IChart[])
    : undefined;
});

const SAMPLE_ROWS = 5_000;

// Every chart aggregation in GraphicWalker is forwarded as an
// IDataQueryPayload to the ad-hoc compute endpoint; the worker resolves the
// source once into its session cache and runs polars-gw on top.
const { computation: computeOnWorker, lastError: computeError } = useGraphicWalkerCompute(
  (payload) => CatalogApi.computeAdHocVisualization(props.source, payload, SAMPLE_ROWS),
  "ad-hoc",
);

const canSave = computed(() => name.value.trim().length > 0 && !loadingSample.value);

const disabledReason = computed(() => {
  if (loadingSample.value) return "Loading data sample...";
  if (!name.value.trim()) return "Enter a name to save";
  return null;
});

onMounted(async () => {
  if (isCreate.value) {
    const auth = useAuthStore();
    const userKey = auth.currentUser?.id ?? auth.currentUser?.username ?? "anon";
    draft = createLocalDraft({
      key: vizDraftKey(props.source, userKey),
      version: 1,
      serialize: () => (isDirty.value ? { name: name.value, spec: lastSpec ?? [] } : null),
    });
    const existing = draft.peek();
    if (existing) {
      pendingDraft = existing.data;
      restoreAvailable.value = true;
    }
  }
  loadingSample.value = true;
  loadError.value = null;
  try {
    // Just fetch the field schema. GraphicWalker pulls rows on demand via
    // computeOnWorker so every aggregation pushes down to polars-gw on the
    // worker (matching the polars-gw walk() reference pattern).
    fields.value = (await store.loadVisualizationFields(props.source)) as IMutField[];
  } catch (err: any) {
    loadError.value = err?.response?.data?.detail ?? err?.message ?? String(err);
  } finally {
    loadingSample.value = false;
  }
});

onBeforeUnmount(() => {
  // Dirty sessions only: a clean write would delete a still-offered prior draft.
  if (isDirty.value) draft?.writeNow();
  draft?.dispose();
});

const isExistingViz = (v: any): v is CatalogVisualization =>
  v && typeof v === "object" && typeof v.id === "number";

const save = async () => {
  if (!gwRef.value) return;
  if (!name.value.trim()) {
    ElMessage.warning("Enter a name to save the visualization.");
    return;
  }
  const charts = await gwRef.value.exportCode();
  if (!charts || !charts.length) {
    ElMessage.error("No chart to save — build one in the editor first.");
    return;
  }
  // Save the full IChart[] so multi-tab specs round-trip.
  const spec = charts as Record<string, any>[];
  const thumbnail_data_url = await captureThumbnail(gwRef);
  saving.value = true;
  try {
    let saved: CatalogVisualization;
    if (isExistingViz(props.viz)) {
      const updatePayload: VisualizationUpdatePayload = {
        name: name.value.trim(),
        spec,
      };
      if (thumbnail_data_url) updatePayload.thumbnail_data_url = thumbnail_data_url;
      saved = await store.updateVisualization(props.viz.id, updatePayload);
    } else {
      const createPayload: VisualizationCreatePayload = {
        name: name.value.trim(),
        spec,
        source_type: props.source.source_type,
        catalog_table_id:
          props.source.source_type === "table" ? (props.source.table_id ?? null) : null,
        sql_query: props.source.source_type === "sql" ? (props.source.sql_query ?? null) : null,
      };
      if (thumbnail_data_url) createPayload.thumbnail_data_url = thumbnail_data_url;
      saved = await store.createVisualization(createPayload);
      // Created for real now; createCompleted keeps the unmount write from reviving the draft.
      createCompleted.value = true;
      specDirty.value = false;
      draft?.discard();
    }
    ElMessage.success(`Saved "${saved.name}"`);
    emit("saved", saved);
  } catch (err: any) {
    ElMessage.error(catalogSaveErrorMessage(err, "Failed to save visualization"));
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
  /* Fill the dialog body and prevent any growth past it — only the inner
     .viz-scroll-area should scroll. */
  height: 100%;
  min-height: 0;
  overflow: hidden;
}
/* Constraint layer for graphic-walker. We *clip* (not scroll) here so that
   graphic-walker's own internal `.overflow-auto` chart pane handles the
   chart's vertical scroll natively — that keeps the encoding controls
   (X-Axis / Y-Axis bars) sticky above a scrolling chart instead of pushing
   them off-screen. Takes whatever space is left after the toolbar. */
.viz-scroll-area {
  flex: 1 1 auto;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.viz-editor-toolbar {
  display: flex;
  align-items: flex-end;
  gap: 12px;
  justify-content: space-between;
}
.viz-name-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1;
  max-width: 360px;
}
.viz-name-label {
  font-size: 12px;
  color: var(--el-text-color-regular);
}
.viz-name-required {
  color: var(--el-color-danger);
  margin-left: 2px;
}
.viz-name-input {
  width: 100%;
}
.viz-editor-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.viz-disabled-hint {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  font-style: italic;
}
.viz-editor-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
}
.viz-draft-banner {
  flex-shrink: 0;
}
.viz-draft-actions {
  display: flex;
  gap: 8px;
  margin-top: 4px;
}
</style>
