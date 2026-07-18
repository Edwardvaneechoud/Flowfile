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
          <div v-if="availableKernels.length" class="kernel-select-row">
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
      />
    </generic-node-settings>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from "vue";
import axios from "axios";
import { CustomNodeSchema, SectionComponent } from "./interface";
import type { ArtifactOption, GlobalArtifactOption } from "./interface";
import { getCustomNodeSchema, CustomNodeSchemaError } from "./interface";
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
const formData = ref<any>(null);
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

// Kernel state
const availableKernels = ref<KernelInfo[]>([]);
const selectedKernelId = ref<string | null>(null);

// Isolated-kernel node when environment=="kernel" (or a legacy requires_kernel flag).
const isKernelEnv = computed(
  () => schema.value?.environment === "kernel" || !!schema.value?.requires_kernel,
);

// Reactive so the "select a kernel" warning clears the moment a kernel is chosen.
const kernelRequiredError = computed(() => isKernelEnv.value && !selectedKernelId.value);

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
  try {
    const response = await axios.get("/flow/node_available_artifacts", {
      params: { flow_id: nodeStore.flow_id, node_id: nodeId, kernel_id: kernelId },
    });
    const artifacts = response.data?.artifacts ?? [];
    artifactOptions.value = artifacts.map((a: any) => ({
      name: a.name,
      type_name: a.type_name,
      module: a.module,
      status: a.status,
    }));
  } catch {
    artifactOptions.value = [];
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

// True when any select in the schema opts into the global (catalog) artifact source.
function schemaWantsGlobalArtifacts(schemaData: CustomNodeSchema): boolean {
  for (const sectionKey in schemaData.settings_schema) {
    const section = schemaData.settings_schema[sectionKey];
    for (const componentKey in section.components) {
      const options = (section.components[componentKey] as any).options;
      if (
        options?.__type__ === "AvailableArtifacts" &&
        (options.scope === "global" || options.scope === "all")
      )
        return true;
    }
  }
  return false;
}

// Re-fetch upstream artifacts when the picked kernel changes; skip the initial
// load's own assignment (fetched explicitly there).
watch(selectedKernelId, (kernelId) => {
  if (loading.value || currentNodeId.value === null) return;
  void fetchAvailableArtifacts(currentNodeId.value, kernelId);
});

// --- Lifecycle Methods (exposed to parent) ---

const loadNodeData = async (nodeId: number) => {
  loading.value = true;
  error.value = "";
  schemaError.value = null;
  currentNodeId.value = nodeId;

  try {
    const inputNodeData = await nodeStore.getNodeData(nodeId, false);
    if (!inputNodeData) {
      return;
    }
    const [schemaData] = await Promise.all([
      getCustomNodeSchema(nodeStore.flow_id, nodeId),
      fetchKernels(),
    ]);

    schema.value = schemaData;
    nodeData.value = inputNodeData;
    nodeUserDefined.value = nodeData.value?.setting_input;

    if (!nodeData.value?.setting_input.is_setup && nodeUserDefined.value) {
      nodeUserDefined.value.settings = {};
    }

    selectedKernelId.value = nodeUserDefined.value?.kernel_id ?? schemaData.kernel_id ?? null;

    const mainColumns = inputNodeData?.main_input?.columns ?? [];
    if (mainColumns.length) {
      availableColumns.value = mainColumns;
      columnTypes.value = inputNodeData.main_input?.table_schema ?? [];
    } else {
      console.warn(
        `No main_input or columns found for node ${nodeId}. Select components may be empty.`,
      );
    }

    await fetchAvailableArtifacts(nodeId, selectedKernelId.value ?? schemaData.kernel_id ?? null);
    if (schemaWantsGlobalArtifacts(schemaData)) {
      await fetchGlobalArtifacts();
    }

    initializeFormData(schemaData, inputNodeData?.setting_input);
  } catch (err: any) {
    if (err instanceof CustomNodeSchemaError) {
      schemaError.value = err;
    } else {
      error.value = err.message || "An unknown error occurred while loading node data.";
    }
  } finally {
    loading.value = false;
  }
};

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

// --- Helper Functions ---

function initializeFormData(schemaData: CustomNodeSchema, savedSettings: any) {
  const data: any = {};

  for (const sectionKey in schemaData.settings_schema) {
    data[sectionKey] = {};
    const section: SectionComponent = schemaData.settings_schema[sectionKey];
    for (const componentKey in section.components) {
      const component = section.components[componentKey];

      const savedValue = savedSettings?.[sectionKey]?.[componentKey];

      if (savedValue !== undefined) {
        data[sectionKey][componentKey] = savedValue;
      } else if (component.value !== undefined) {
        data[sectionKey][componentKey] = component.value;
      } else {
        let defaultValue = component.default ?? null;
        if (component.input_type === "array" && defaultValue === null) {
          defaultValue = [];
        }
        data[sectionKey][componentKey] = defaultValue;
      }
    }
  }
  formData.value = data;
}

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
