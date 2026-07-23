<template>
  <div v-if="loading" class="p-4 text-center text-gray-500">Loading Node UI...</div>

  <!-- Node type not installed on this machine (404 with node_item). -->
  <div v-else-if="schemaError?.kind === 'missing'" class="udn-state-card udn-state-card--missing">
    <div class="udn-state-title">Custom node not installed</div>
    <p class="udn-state-body">
      Custom node "<code>{{ schemaError.nodeItem }}</code
      >" is not installed on this machine. Its saved settings are kept and will re-load once the
      node is added.
    </p>
  </div>

  <!-- Registered but broken node (409 with the registry error). -->
  <div v-else-if="schemaError?.kind === 'broken'" class="udn-state-card udn-state-card--broken">
    <div class="udn-state-title">Custom node failed to load</div>
    <p class="udn-state-body">
      "<code>{{ schemaError.nodeItem }}</code
      >" is installed but could not be loaded:
    </p>
    <pre class="udn-state-detail">{{ schemaError.detail || schemaError.message }}</pre>
  </div>

  <div v-else-if="error" class="p-4 text-red-600 bg-red-100 rounded-md">
    <strong>Error:</strong> {{ error }}
  </div>

  <!-- This wrapper prevents rendering until the schema and formData are ready -->
  <div v-else-if="schema && formData && nodeUserDefined" class="custom-node-wrapper">
    <!-- eslint-disable-next-line vue/no-v-html -- sanitised by renderSafeMarkdown -->
    <div v-if="introHtml" class="listbox-subtitle udn-intro" v-html="introHtml"></div>

    <!-- Drift warning: saved settings hold keys the current schema dropped. -->
    <div v-if="driftFields.length" class="udn-drift-banner">
      <strong>Settings changed since this node was last configured.</strong>
      <div class="udn-drift-detail">
        No longer in the node: {{ driftFields.join(", ") }}. Matching values are kept; others are
        ignored.
      </div>
    </div>

    <generic-node-settings v-model="nodeUserDefined">
      <!-- Execution environment row: kernel nodes only — local nodes need no kernel UI. -->
      <div v-if="isKernelEnv" class="listbox-wrapper env-section">
        <div class="section-title">Execution</div>
        <div class="env-row">
          <span class="env-badge" :class="isKernelEnv ? 'env-badge--kernel' : 'env-badge--local'">
            {{ isKernelEnv ? "Isolated kernel" : "Local" }}
          </span>
          <span
            v-if="isKernelEnv && (schema.dependencies?.length ?? 0) > 0"
            class="env-deps"
            :title="schema.dependencies?.join(', ')"
          >
            deps: {{ schema.dependencies?.join(", ") }}
          </span>
        </div>

        <!-- Kernel-instance picker (only for isolated-kernel nodes). -->
        <template v-if="isKernelEnv">
          <!-- Kernels hydrate in the background; never flash the empty state while loading. -->
          <div v-if="kernelsLoading" class="kernel-loading-state">Loading kernels…</div>
          <div v-else-if="availableKernels.length" class="kernel-select-row">
            <label class="kernel-label" for="kernel-select">Kernel instance</label>
            <select id="kernel-select" v-model="selectedKernelId" class="kernel-select">
              <option :value="null">Select a kernel…</option>
              <option v-for="k in availableKernels" :key="k.id" :value="k.id">
                {{ k.name }}
                <template v-if="k.packages.length">
                  ({{ k.packages.slice(0, 3).join(", ")
                  }}<template v-if="k.packages.length > 3">...</template>)
                </template>
              </option>
            </select>
          </div>
          <!-- No kernels available: actionable state instead of an empty dropdown. -->
          <div v-else class="kernel-empty-state">
            <p class="kernel-empty-message">
              This node runs in an isolated kernel, but no kernels are available. Start one (Docker
              required) to configure and run it.
            </p>
            <router-link :to="{ name: 'kernelManager' }" class="kernel-manager-link">
              Open Kernel Manager
            </router-link>
          </div>
          <div v-if="kernelRequiredError" class="kernel-error">
            Kernel execution is required for this node. Select a kernel to enable it.
          </div>
        </template>
      </div>

      <CustomNodeForm
        v-model:form-data="formData"
        :schema="schema.settings_schema"
        :incoming-columns="availableColumns"
        :column-types="columnTypes"
        :artifact-options="artifactOptions"
        :global-artifacts="globalArtifacts"
        :columns-loading="columnsLoading"
        :artifacts-loading="artifactsLoading"
      />
    </generic-node-settings>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import axios from "axios";
import { CustomNodeSchema } from "./interface";
import type { ArtifactOption, GlobalArtifactOption } from "./interface";
import { getCustomNodeSchema, CustomNodeSchemaError } from "./interface";
import { initializeFormData, schemaWantsGlobalArtifacts } from "./formInit";
import type { CustomNodeFormData } from "./formInit";
import { useNodeStore } from "../../../../../stores/column-store";
import { NodeUserDefined } from "../../../baseNode/nodeInput";
import { NodeData, FileColumn } from "../../../baseNode/nodeInterfaces";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import CustomNodeForm from "./CustomNodeForm.vue";
import { renderSafeMarkdown } from "../../../../../lib/markdown";

// Kernel info type (matches backend KernelInfo)
interface KernelInfo {
  id: string;
  name: string;
  state: string;
  packages: string[];
}

// Component State
const schema = ref<CustomNodeSchema | null>(null);
const formData = ref<CustomNodeFormData | null>(null);
const loading = ref(true);
const error = ref<string>("");
const schemaError = ref<CustomNodeSchemaError | null>(null);
const nodeStore = useNodeStore();
const nodeData = ref<NodeData | null>(null);
const availableColumns = ref<string[]>([]);
const currentNodeId = ref<number | null>(null);
const nodeUserDefined = ref<NodeUserDefined | null>(null);
const columnTypes = ref<FileColumn[]>([]);
const artifactOptions = ref<ArtifactOption[]>([]);
const globalArtifacts = ref<GlobalArtifactOption[]>([]);

// Background-hydration state: the form renders after stage 1 (settings + schema);
// columns/kernels/artifacts fill in afterwards behind these flags.
const columnsLoading = ref(false);
const kernelsLoading = ref(false);
const artifactsLoading = ref(false);
// The prediction warning renders in the shared GenericNodeSettings banner,
// sourced from the store slot the stage-2 full fetch below populates.
// Kernel id the artifacts were last fetched for — idempotence guard for the
// selectedKernelId watcher (a timing-based guard would double-fetch).
const lastArtifactsKernelId = ref<string | null | undefined>(undefined);
// Monotonic load token: the drawer reuses this component across node switches
// and never awaits loadNodeData, so every post-await write must check it.
let loadSeq = 0;

// Kernel state
const availableKernels = ref<KernelInfo[]>([]);
const selectedKernelId = ref<string | null>(null);

// Isolated-kernel node when environment=="kernel" (or a legacy requires_kernel flag).
const isKernelEnv = computed(
  () => schema.value?.environment === "kernel" || !!schema.value?.requires_kernel,
);

// Reactive so the "select a kernel" warning clears the moment a kernel is chosen;
// suppressed while the kernel list is still hydrating.
const kernelRequiredError = computed(
  () => isKernelEnv.value && !selectedKernelId.value && !kernelsLoading.value,
);

const introHtml = computed(() =>
  schema.value?.intro ? renderSafeMarkdown(schema.value.intro) : "",
);

// Dropped sections + components the saved settings still reference.
const driftFields = computed(() => {
  const drift = schema.value?.drift;
  if (!drift) return [];
  return [...(drift.unknown_sections ?? []), ...(drift.unknown_components ?? [])];
});

async function fetchKernels() {
  try {
    const response = await axios.get("/kernels/");
    availableKernels.value = response.data || [];
  } catch {
    // Kernels endpoint may not be available (no Docker), silently ignore
    availableKernels.value = [];
  }
}

async function fetchAvailableArtifacts(nodeId: number, kernelId: string | null) {
  // Both callers record lastArtifactsKernelId before firing, so it doubles as the
  // ordering token: a stale in-flight response must never clobber a newer kernel's list.
  const isCurrent = () =>
    currentNodeId.value === nodeId && lastArtifactsKernelId.value === kernelId;
  try {
    const response = await axios.get("/flow/node_available_artifacts", {
      params: { flow_id: nodeStore.flow_id, node_id: nodeId, kernel_id: kernelId },
    });
    if (!isCurrent()) return;
    const artifacts = response.data?.artifacts ?? [];
    artifactOptions.value = artifacts.map((a: any) => ({
      name: a.name,
      type_name: a.type_name,
      module: a.module,
      status: a.status,
    }));
  } catch {
    if (isCurrent()) artifactOptions.value = [];
  }
}

// Trailing slash: /artifacts/ is the router root; a missing slash 307-drops the body in Docker.
async function fetchGlobalArtifacts() {
  try {
    const response = await axios.get("/artifacts/", { params: { limit: 500 } });
    const raw = response.data;
    const items: any[] = Array.isArray(raw) ? raw : (raw?.artifacts ?? raw?.items ?? []);
    globalArtifacts.value = items.map((a) => ({
      name: a.name,
      python_type: a.python_type ?? null,
      namespace_id: a.namespace_id ?? null,
      version: a.version,
    }));
  } catch {
    globalArtifacts.value = [];
  }
}

// Re-fetch upstream artifacts when the picked kernel changes. Idempotence guard:
// the hydration task records the kernel id it fetched for before this watcher
// flushes, so the initial load's own assignment never double-fetches.
watch(selectedKernelId, (kernelId) => {
  if (loading.value || currentNodeId.value === null) return;
  if (kernelId === lastArtifactsKernelId.value) return;
  lastArtifactsKernelId.value = kernelId;
  void refetchArtifactsForKernel(currentNodeId.value, kernelId);
});

// Global (catalog) artifacts are kernel-independent — only the upstream list refetches.
async function refetchArtifactsForKernel(nodeId: number, kernelId: string | null) {
  artifactsLoading.value = true;
  try {
    await fetchAvailableArtifacts(nodeId, kernelId);
  } finally {
    // The flag is owned by the latest artifact request (kernel token + node).
    if (currentNodeId.value === nodeId && lastArtifactsKernelId.value === kernelId) {
      artifactsLoading.value = false;
    }
  }
}

// --- Lifecycle Methods (exposed to parent) ---

const loadNodeData = async (nodeId: number) => {
  const seq = ++loadSeq;
  loading.value = true;
  error.value = "";
  schemaError.value = null;
  currentNodeId.value = nodeId;
  availableColumns.value = [];
  columnTypes.value = [];
  artifactOptions.value = [];
  globalArtifacts.value = [];
  columnsLoading.value = false;
  kernelsLoading.value = false;
  artifactsLoading.value = false;
  lastArtifactsKernelId.value = undefined;

  // Stage 1 (blocks the spinner): saved settings + UI schema — everything the
  // form structure needs. Columns/kernels/artifacts hydrate in stage 2.
  try {
    const [nodeRes, schemaRes] = await Promise.allSettled([
      nodeStore.getNodeDataLight(nodeId),
      getCustomNodeSchema(nodeStore.flow_id, nodeId),
    ]);
    if (seq !== loadSeq) return;
    // The schema error decides the drawer surface (404 missing / 409 broken cards).
    if (schemaRes.status === "rejected") throw schemaRes.reason;
    if (nodeRes.status === "rejected") throw nodeRes.reason;
    const inputNodeData = nodeRes.value;
    if (!inputNodeData) {
      error.value = "Failed to load node data.";
      return;
    }
    const schemaData = schemaRes.value;

    schema.value = schemaData;
    nodeData.value = inputNodeData;
    nodeUserDefined.value = inputNodeData.setting_input;

    if (!inputNodeData.setting_input.is_setup && nodeUserDefined.value) {
      nodeUserDefined.value.settings = {};
    }

    selectedKernelId.value = nodeUserDefined.value?.kernel_id ?? schemaData.kernel_id ?? null;

    formData.value = initializeFormData(schemaData, inputNodeData.setting_input);
  } catch (err: any) {
    if (seq !== loadSeq) return;
    if (err instanceof CustomNodeSchemaError) {
      schemaError.value = err;
    } else {
      error.value = err.message || "An unknown error occurred while loading node data.";
    }
  } finally {
    if (seq === loadSeq) loading.value = false;
  }

  // Stage 2 (fire-and-forget): option data behind per-section loading flags.
  if (seq !== loadSeq || !schema.value || !formData.value) return;
  void hydrateColumns(nodeId, seq);
  if (isKernelEnv.value) void hydrateKernels(seq);
  void hydrateArtifacts(nodeId, selectedKernelId.value ?? schema.value.kernel_id ?? null, seq);
};

async function hydrateColumns(nodeId: number, seq: number) {
  // Source-style nodes have no inputs — nothing to hydrate, no prediction to warm.
  if ((schema.value?.number_of_inputs ?? 0) === 0) return;
  columnsLoading.value = true;
  try {
    const full = await nodeStore.getNodeData(nodeId, false);
    if (seq !== loadSeq || !full) return;
    nodeData.value = full;
    const mainColumns = full.main_input?.columns ?? [];
    if (mainColumns.length) {
      availableColumns.value = mainColumns;
      columnTypes.value = full.main_input?.table_schema ?? [];
    } else if (!full.prediction_warning) {
      console.warn(
        `No main_input or columns found for node ${nodeId}. Select components may be empty.`,
      );
    }
  } finally {
    if (seq === loadSeq) columnsLoading.value = false;
  }
}

async function hydrateKernels(seq: number) {
  kernelsLoading.value = true;
  try {
    await fetchKernels();
  } finally {
    if (seq === loadSeq) kernelsLoading.value = false;
  }
}

async function hydrateArtifacts(nodeId: number, kernelId: string | null, seq: number) {
  // Recorded synchronously, before Vue flushes the selectedKernelId watcher,
  // so the initial assignment never triggers a duplicate fetch.
  lastArtifactsKernelId.value = kernelId;
  artifactsLoading.value = true;
  try {
    await Promise.all([
      fetchAvailableArtifacts(nodeId, kernelId),
      schema.value && schemaWantsGlobalArtifacts(schema.value)
        ? fetchGlobalArtifacts()
        : Promise.resolve(),
    ]);
  } finally {
    // The flag is owned by the latest artifact request (kernel token + load).
    if (seq === loadSeq && lastArtifactsKernelId.value === kernelId) {
      artifactsLoading.value = false;
    }
  }
}

const pushNodeData = async () => {
  if (!nodeData.value || currentNodeId.value === null) {
    console.warn("Cannot push data: node data or ID is not available.");
    return;
  }
  if (nodeUserDefined.value) {
    nodeUserDefined.value.settings = formData.value;
    nodeUserDefined.value.is_user_defined = true;
    nodeUserDefined.value.is_setup = true;
    nodeUserDefined.value.kernel_id = selectedKernelId.value;
    if (schema.value?.output_names) {
      nodeUserDefined.value.output_names = schema.value.output_names;
    }
  }
  nodeStore.updateUserDefinedSettings(nodeUserDefined);
};

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
.custom-node-wrapper {
  padding: 1.5rem;
  background-color: var(--color-background-primary);
}

.node-header {
  padding-bottom: 1rem;
  border-bottom: 1px solid var(--color-border-primary);
  margin-bottom: 1.5rem;
}

.node-title {
  font-size: 1.25rem;
  font-weight: 700;
  color: var(--color-text-primary);
}

.node-category {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  margin-top: 0.25rem;
}

/* Kept for the kernel section header; the sections form itself renders via CustomNodeForm. */
.section-title {
  font-size: var(--font-size-md, 13px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
  padding: var(--spacing-1, 4px) 0 var(--spacing-1, 4px) var(--spacing-2, 8px);
  margin-bottom: var(--spacing-3, 12px);
  border-left: 3px solid var(--color-accent, #0891b2);
}

.env-section {
  margin-bottom: var(--spacing-4, 16px);
}

.env-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3, 12px);
  padding: 0 var(--spacing-4, 16px) var(--spacing-2, 8px);
}

.env-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 10px;
  font-size: var(--font-size-xs, 12px);
  font-weight: var(--font-weight-medium, 500);
  border-radius: var(--border-radius-full, 999px);
}

.env-badge--local {
  color: var(--color-text-secondary);
  background-color: var(--color-background-tertiary, #f1f3f5);
}

.env-badge--kernel {
  color: var(--color-accent, #0891b2);
  background-color: var(--color-accent-soft, rgba(8, 145, 178, 0.12));
}

.env-deps {
  font-size: var(--font-size-xs, 12px);
  color: var(--color-text-secondary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.kernel-empty-state {
  margin: 0 var(--spacing-4, 16px) var(--spacing-2, 8px);
  padding: var(--spacing-3, 12px);
  background-color: var(--color-background-tertiary, #f1f3f5);
  border-radius: var(--border-radius-md, 6px);
}

.kernel-loading-state {
  margin: 0 var(--spacing-4, 16px) var(--spacing-2, 8px);
  font-size: var(--font-size-xs, 12px);
  color: var(--color-text-secondary);
}

.kernel-empty-message {
  margin: 0 0 var(--spacing-2, 8px);
  font-size: var(--font-size-xs, 12px);
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.kernel-manager-link {
  font-size: var(--font-size-xs, 12px);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-accent, #0891b2);
  text-decoration: none;
}

.kernel-manager-link:hover {
  text-decoration: underline;
}

.udn-intro :deep(p) {
  margin: 0 0 var(--spacing-2, 8px);
}

.udn-intro :deep(p:last-child) {
  margin-bottom: 0;
}

.udn-intro :deep(code) {
  font-family: var(--font-family-mono, monospace);
  font-size: 0.9em;
  background: var(--color-background-muted);
  padding: 0 3px;
  border-radius: 3px;
}

.udn-drift-banner {
  margin: 0 0 var(--spacing-4, 16px);
  padding: var(--spacing-3, 12px) var(--spacing-4, 16px);
  background-color: var(--color-warning-soft, #fff8e1);
  border-left: 3px solid var(--color-warning, #f59e0b);
  border-radius: var(--border-radius-md, 6px);
  font-size: var(--font-size-sm, 13px);
  color: var(--color-text-primary);
}

.udn-drift-detail {
  margin-top: var(--spacing-1, 4px);
  font-size: var(--font-size-xs, 12px);
  color: var(--color-text-secondary);
}

.udn-state-card {
  margin: var(--spacing-4, 16px);
  padding: var(--spacing-4, 16px);
  border-radius: var(--border-radius-md, 6px);
  border: 1px solid var(--color-border-primary, #d1d5db);
}

.udn-state-card--missing {
  background-color: var(--color-background-tertiary, #f1f3f5);
}

.udn-state-card--broken {
  background-color: var(--color-danger-soft, #fef2f2);
  border-color: var(--color-danger, #dc2626);
}

.udn-state-title {
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-2, 8px);
}

.udn-state-body {
  margin: 0;
  font-size: var(--font-size-sm, 13px);
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.udn-state-body code,
.udn-state-detail {
  font-family: var(--font-family-mono, monospace);
}

.udn-state-detail {
  margin-top: var(--spacing-2, 8px);
  padding: var(--spacing-2, 8px);
  font-size: var(--font-size-xs, 12px);
  background-color: var(--color-background-primary, #fff);
  border-radius: var(--border-radius-sm, 4px);
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 160px;
  overflow: auto;
}

.kernel-select-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3, 12px);
  padding: 0 var(--spacing-4, 16px);
}

.kernel-label {
  font-size: var(--font-size-sm, 13px);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-secondary);
  white-space: nowrap;
}

.kernel-select {
  flex: 1;
  padding: var(--spacing-2, 8px) var(--spacing-3, 12px);
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-md, 6px);
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm, 13px);
  cursor: pointer;
}

.kernel-select:focus {
  outline: none;
  border-color: var(--color-accent, #0891b2);
  box-shadow: 0 0 0 2px rgba(8, 145, 178, 0.15);
}

.kernel-error {
  padding: var(--spacing-2, 8px) var(--spacing-4, 16px) 0;
  font-size: var(--font-size-xs, 12px);
  color: var(--color-text-danger, #dc2626);
}
</style>
