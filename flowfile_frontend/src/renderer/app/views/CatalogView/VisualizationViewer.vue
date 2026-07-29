<template>
  <div class="viz-viewer">
    <div class="viz-viewer-toolbar">
      <div class="viz-viewer-meta">
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
            :disabled="loadingMeta || loadingData || saving"
          />
        </div>
        <div class="viz-viewer-source">
          <i
            :class="
              viz?.source_type === 'sql'
                ? 'fa-solid fa-code source-icon sql'
                : 'fa-solid fa-table source-icon'
            "
          ></i>
          <span class="viz-source-label">{{ sourceLabel }}</span>
          <SharedBadge :access="viz?.access" />
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
              v-for="ns in namespaceOptions"
              :key="ns.id"
              :label="ns.label"
              :value="ns.id"
              :disabled="ns.disabled"
            />
          </el-select>
        </div>
        <el-button v-if="viz && canShare(viz)" size="small" @click="showShareDialog = true">
          <i class="fa-solid fa-share-nodes" />&nbsp;Share
        </el-button>
        <SaveStatusIndicator :state="autosaveState" />
        <el-button
          size="small"
          :disabled="saving || loadingData || !canRevert"
          :loading="reverting"
          @click="onRevert"
        >
          Revert
        </el-button>
      </div>
    </div>

    <AutosaveConflictBanner
      :state="autosaveState"
      resource-label="chart"
      :busy="conflictBusy"
      @reload="onConflictReload"
      @overwrite="onConflictOverwrite"
    />

    <ShareDialog
      v-if="viz"
      v-model="showShareDialog"
      resource-type="visualization"
      :resource-id="viz.id"
      :resource-name="viz.name"
      :can-manage-grants="canManageGrants(viz)"
    />

    <div class="viz-scroll-area">
      <pre
        v-if="sqlExpanded && viz?.source_type === 'sql' && viz.sql_query"
        class="viz-sql-block"
        >{{ viz.sql_query }}</pre
      >

      <div v-if="loadingMeta || loadingData" class="viz-viewer-state">
        <el-skeleton :rows="6" animated />
      </div>

      <div v-else-if="errorMessage" class="viz-viewer-state">
        <el-alert :title="errorMessage" type="error" :closable="false" show-icon />
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
          :computation="computeOnWorker"
          :fields="plainFields"
          :spec-list="plainSpecList"
          :appearance="appearance"
          :hide-chart-nav="true"
          :watch-spec="true"
          @spec-changed="onSpecChanged"
        />
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import type { IChart, IDarkMode, IMutField } from "@kanaries/graphic-walker/interfaces";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import SaveStatusIndicator from "../../components/common/SaveStatusIndicator.vue";
import AutosaveConflictBanner from "../../components/common/AutosaveConflictBanner.vue";
import { useAutosave } from "../../composables/useAutosave";
import {
  AUTOSAVE_REQUEST_TIMEOUT_MS,
  classifyAutosaveError,
} from "../../composables/autosaveEngine";
import { buildVizSaveBody, shouldCaptureThumbnail } from "./vizAutosave";
import { CatalogApi } from "../../api/catalog.api";
import { useCatalogStore } from "../../stores/catalog-store";
import { captureThumbnail } from "../../composables/useChartThumbnail";
import { useGraphicWalkerCompute } from "../../composables/useGraphicWalkerCompute";
import { toPlainJson } from "../../utils/structuredClone";
import type { CatalogVisualization, VisualizationUpdatePayload } from "../../types";
import { findNamespacePath } from "../../types/catalog.types";
import ShareDialog from "../../components/sharing/ShareDialog.vue";
import SharedBadge from "../../components/sharing/SharedBadge.vue";
import { useResourceSharing } from "../../composables/useResourceSharing";
import { useWritableNamespaces } from "../../composables/useWritableNamespaces";
import { catalogSaveErrorMessage } from "../../composables/saveError";

const showShareDialog = ref(false);
const { canShare, canManageGrants } = useResourceSharing();

const props = defineProps<{
  vizId: number;
  appearance?: IDarkMode;
}>();

const emit = defineEmits<{
  (e: "updated", viz: CatalogVisualization): void;
}>();

const store = useCatalogStore();
const gwRef = ref<InstanceType<typeof VueGraphicWalker> | null>(null);

const viz = ref<CatalogVisualization | null>(null);
const name = ref("");
const fields = ref<IMutField[]>([]);
const loadingMeta = ref(true);
const loadingData = ref(true);
const errorMessage = ref<string | null>(null);
const saving = ref(false);

// User-edited namespace; persisted to the viz eagerly on change.
const namespaceDraft = ref<number | null>(null);

// Revert anchor: the state captured when the viewer opened this viz.
const openSnapshot = ref<{ name: string; spec: Record<string, any>[] } | null>(null);
const openSerialized = ref("");
// CAS token echoed into every PUT; refreshed from each response.
const expectedUpdatedAt = ref<string | null>(null);
const sessionChanged = ref(false);
// A save landed without a thumbnail; refresh the catalog card at flush time.
const thumbnailStale = ref(false);
const reverting = ref(false);
const conflictBusy = ref(false);

const serializeViz = (vizName: string, spec: unknown) => JSON.stringify({ name: vizName, spec });

// Metadata-only merge: a slow response must never roll back newer local edits.
function adoptSaved(updated: CatalogVisualization) {
  expectedUpdatedAt.value = updated.updated_at;
  if (viz.value) {
    viz.value = {
      ...viz.value,
      name: updated.name,
      updated_at: updated.updated_at,
      namespace_id: updated.namespace_id,
      namespace_name: updated.namespace_name,
      access: updated.access ?? viz.value.access,
    };
  }
  emit("updated", updated);
}

const { engine, state: autosaveState } = useAutosave<VisualizationUpdatePayload>({
  getSnapshot: async () => {
    if (!viz.value || !gwRef.value) return null;
    const trimmed = name.value.trim();
    if (!trimmed) return null;
    const charts = await gwRef.value.exportCode();
    if (!charts || !charts.length) return null;
    const spec = charts as unknown as Record<string, any>[];
    return { payload: { name: trimmed, spec }, serialized: serializeViz(trimmed, spec) };
  },
  save: async (payload, { reason }) => {
    const thumb = shouldCaptureThumbnail(reason) ? await captureThumbnail(gwRef) : null;
    const body = buildVizSaveBody(
      { name: payload.name ?? "", spec: payload.spec ?? [] },
      { reason, thumbnail: thumb, expectedUpdatedAt: expectedUpdatedAt.value },
    );
    const updated = await store.updateVisualization(props.vizId, body, {
      refreshLibrary: false,
      config: { timeout: AUTOSAVE_REQUEST_TIMEOUT_MS },
    });
    sessionChanged.value = serializeViz(payload.name ?? "", payload.spec) !== openSerialized.value;
    thumbnailStale.value = !body.thumbnail_data_url;
    adoptSaved(updated);
  },
});

const onSpecChanged = () => engine.notifyChange();
watch(name, (v) => {
  if (viz.value && v.trim() !== viz.value.name) engine.notifyChange();
});

const canRevert = computed(() => {
  const st = autosaveState.value.status;
  if (st === "conflict" || st === "blocked") return false;
  return !!openSnapshot.value && (sessionChanged.value || st !== "idle");
});

const SAMPLE_ROWS = 100_000;

// Every aggregation the user builds in the chart is POSTed straight to the
// worker via /catalog/visualizations/{id}/compute. The worker session cache
// keeps the source LazyFrame warm so successive aggregations skip the load.
const { computation: computeOnWorker, lastError: computeError } = useGraphicWalkerCompute(
  (payload) =>
    CatalogApi.computeSavedVisualization(props.vizId, {
      payload,
      maxRows: SAMPLE_ROWS,
    }),
  "saved",
);

// Flat schema-level list for the namespace picker; read-only grants excluded.
const { writableSchemaNamespaces } = useWritableNamespaces();

// The viz's current namespace may be read-only or system-managed and thus absent
// from the writable list — keep it rendered with its label (disabled) instead of
// showing a raw numeric id.
const namespaceOptions = computed(() => {
  const options = writableSchemaNamespaces.value.map((o) => ({ ...o, disabled: false }));
  const current = namespaceDraft.value;
  if (current != null && !options.some((o) => o.id === current)) {
    const path = findNamespacePath(store.tree, current);
    options.push({
      id: current,
      label: path.length ? path.join(" / ") : `Namespace ${current}`,
      disabled: true,
    });
  }
  return options;
});

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

async function load(opts: { keepAnchor?: boolean } = {}) {
  errorMessage.value = null;
  loadingMeta.value = true;
  try {
    viz.value = await CatalogApi.getVisualization(props.vizId);
    name.value = viz.value?.name ?? "";
    namespaceDraft.value = viz.value?.namespace_id ?? null;
    expectedUpdatedAt.value = viz.value?.updated_at ?? null;
    const serialized = serializeViz(name.value, viz.value?.spec ?? []);
    if (opts.keepAnchor) {
      // Adopt server state without moving the revert anchor.
      engine.resolveConflictReloaded(serialized);
    } else {
      openSnapshot.value = {
        name: name.value,
        spec: (viz.value?.spec ?? []) as Record<string, any>[],
      };
      openSerialized.value = serialized;
      sessionChanged.value = false;
      engine.reset(serialized);
    }
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

async function onRevert() {
  if (!openSnapshot.value || !viz.value) return;
  try {
    await ElMessageBox.confirm(
      "Take this chart back to how it was when you opened it? Changes from this session will be undone.",
      "Revert chart",
      { confirmButtonText: "Revert", cancelButtonText: "Keep editing", type: "warning" },
    );
  } catch {
    return;
  }
  reverting.value = true;
  saving.value = true;
  try {
    // Drain any in-flight save (no new write) so the CAS token is current.
    engine.suspend();
    await engine.flush();
    const body: VisualizationUpdatePayload = {
      name: openSnapshot.value.name,
      spec: openSnapshot.value.spec,
    };
    if (expectedUpdatedAt.value) body.expected_updated_at = expectedUpdatedAt.value;
    const updated = await store.updateVisualization(props.vizId, body, {
      refreshLibrary: false,
      config: { timeout: AUTOSAVE_REQUEST_TIMEOUT_MS },
    });
    adoptSaved(updated);
    engine.markSaved(serializeViz(openSnapshot.value.name, openSnapshot.value.spec));
    sessionChanged.value = false;
    // Remount GW with the reverted spec (load's v-if swap recreates it).
    await load({ keepAnchor: true });
  } catch (err: any) {
    if (classifyAutosaveError(err) === "conflict") {
      // The revert itself hit a stale write: enter the normal conflict flow.
      engine.enterConflict(catalogSaveErrorMessage(err, "Changed somewhere else"));
    } else {
      ElMessage.error(catalogSaveErrorMessage(err, "Failed to revert chart"));
    }
  } finally {
    engine.resume();
    reverting.value = false;
    saving.value = false;
  }
}

async function onConflictReload() {
  conflictBusy.value = true;
  try {
    await load({ keepAnchor: true });
  } finally {
    conflictBusy.value = false;
  }
}

async function onConflictOverwrite() {
  if (!gwRef.value) return;
  conflictBusy.value = true;
  try {
    // Token-only re-GET keeps a third concurrent writer detectable.
    const fresh = await CatalogApi.getVisualization(props.vizId);
    const trimmed = name.value.trim() || fresh.name;
    const charts = await gwRef.value.exportCode();
    if (!charts || !charts.length) return;
    const spec = charts as unknown as Record<string, any>[];
    const body: VisualizationUpdatePayload = {
      name: trimmed,
      spec,
      expected_updated_at: fresh.updated_at,
    };
    const thumb = await captureThumbnail(gwRef);
    if (thumb) body.thumbnail_data_url = thumb;
    const updated = await store.updateVisualization(props.vizId, body, {
      refreshLibrary: false,
      config: { timeout: AUTOSAVE_REQUEST_TIMEOUT_MS },
    });
    const adopted = serializeViz(trimmed, spec);
    sessionChanged.value = adopted !== openSerialized.value;
    thumbnailStale.value = !body.thumbnail_data_url;
    adoptSaved(updated);
    engine.markSaved(adopted);
    // Edits made while the overwrite PUT was in flight must not vanish silently.
    const chartsNow = await gwRef.value?.exportCode();
    if (chartsNow?.length && serializeViz(name.value.trim(), chartsNow) !== adopted) {
      engine.notifyChange();
    }
  } catch (err: any) {
    ElMessage.error(catalogSaveErrorMessage(err, "Failed to save your version"));
  } finally {
    conflictBusy.value = false;
  }
}

/** Persist a namespace move immediately so users don't have to wait for the
 * autosave tick just to relocate a chart. The chart spec is left untouched. */
async function onNamespaceChange(value: number | null | undefined) {
  if (!viz.value) return;
  const next = value ?? null;
  if (next === viz.value.namespace_id) return;
  saving.value = true;
  try {
    // Flush first so tokens can't race, then hold the engine for the out-of-band PUT.
    await engine.flush();
    engine.suspend();
    const body: VisualizationUpdatePayload = { namespace_id: next };
    if (expectedUpdatedAt.value) body.expected_updated_at = expectedUpdatedAt.value;
    const updated = await store.updateVisualization(props.vizId, body);
    namespaceDraft.value = updated.namespace_id ?? null;
    adoptSaved(updated);
    store.loadTree().catch((err) => console.warn("[catalog] tree refresh failed", err));
  } catch (err: any) {
    // Roll the picker back if the update failed.
    namespaceDraft.value = viz.value.namespace_id ?? null;
    ElMessage.error(catalogSaveErrorMessage(err, "Failed to move visualization"));
  } finally {
    engine.resume();
    saving.value = false;
  }
}

const handleBeforeUnload = (event: BeforeUnloadEvent) => {
  if (engine.isDirty()) {
    event.preventDefault();
    event.returnValue = "";
  }
};

onMounted(() => {
  window.addEventListener("beforeunload", handleBeforeUnload);
  load();
});
onBeforeUnmount(() => window.removeEventListener("beforeunload", handleBeforeUnload));

// Drain pending edits, then refresh the catalog-card thumbnail if it went stale.
async function flushAutosave(): Promise<boolean> {
  const clean = await engine.flush();
  if (clean && thumbnailStale.value && gwRef.value && viz.value) {
    engine.suspend();
    try {
      const thumb = await captureThumbnail(gwRef);
      if (thumb) {
        const body: VisualizationUpdatePayload = { thumbnail_data_url: thumb };
        if (expectedUpdatedAt.value) body.expected_updated_at = expectedUpdatedAt.value;
        const updated = await store.updateVisualization(props.vizId, body, {
          refreshLibrary: false,
          config: { timeout: AUTOSAVE_REQUEST_TIMEOUT_MS },
        });
        adoptSaved(updated);
        thumbnailStale.value = false;
      }
    } catch {
      // Cosmetic only — the spec itself is already saved.
    } finally {
      engine.resume();
    }
  }
  return clean;
}

defineExpose({ flushAutosave });
</script>

<style scoped>
.viz-viewer {
  display: flex;
  flex-direction: column;
  gap: 12px;
  /* Fill the dialog body and clip — only .viz-scroll-area scrolls. */
  height: 100%;
  min-height: 0;
  overflow: hidden;
}
/* Constraint layer — see VisualizationEditor.vue for rationale. We clip
   instead of scrolling so graphic-walker's own internal scroll handles the
   chart's overflow. */
.viz-scroll-area {
  flex: 1 1 auto;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  gap: 12px;
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
.viz-name-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
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
