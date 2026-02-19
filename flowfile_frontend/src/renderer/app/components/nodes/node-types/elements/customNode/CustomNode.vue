<template>
  <div v-if="loading" class="p-4 text-center text-gray-500">Loading Node UI...</div>
  <div v-else-if="error" class="p-4 text-red-600 bg-red-100 rounded-md">
    <strong>Error:</strong> {{ error }}
  </div>
  <!-- This wrapper prevents rendering until the schema and formData are ready -->
  <div v-else-if="schema && formData && nodeUserDefined" class="custom-node-wrapper">
    <div v-if="schema.intro" class="listbox-subtitle">
      {{ schema.intro }}
    </div>
    <generic-node-settings v-model="nodeUserDefined">
      <!-- Kernel selector -->
      <div v-if="schema.requires_kernel" class="listbox-wrapper kernel-selector-section">
        <div class="section-title">Execution</div>
        <div class="kernel-select-row">
          <label class="kernel-label" for="kernel-select">Kernel</label>
          <select id="kernel-select" v-model="selectedKernelId" class="kernel-select">
            <option :value="null">Local (default)</option>
            <option v-for="k in availableKernels" :key="k.id" :value="k.id">
              {{ k.name }}
              <template v-if="k.packages.length">
                ({{ k.packages.slice(0, 3).join(", ")
                }}<template v-if="k.packages.length > 3">...</template>)
              </template>
            </option>
          </select>
        </div>
        <div v-if="kernelRequiredError" class="kernel-error">
          Kernel execution is required for this node. Select a kernel to enable it.
        </div>
      </div>

      <!-- Loop through each section in the settings_schema -->
      <div
        v-for="(section, sectionKey) in schema.settings_schema"
        v-show="!section.hidden"
        :key="sectionKey"
        class="listbox-wrapper"
      >
        <div class="section-title">
          {{ section.title || sectionKey.toString().replace(/_/g, " ") }}
        </div>
        <p v-if="section.description" class="section-description">{{ section.description }}</p>

        <div class="components-container">
          <!-- Loop through each component within the section's 'components' object -->
          <div
            v-for="(component, componentKey) in section.components"
            :key="componentKey"
            class="component-item"
          >
            <TextInput
              v-if="component.component_type === 'TextInput'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
            />

            <NumericInput
              v-else-if="component.component_type === 'NumericInput'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
            />

            <SliderInput
              v-else-if="component.component_type === 'SliderInput'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
            />

            <MultiSelect
              v-else-if="component.component_type === 'MultiSelect'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
              :incoming-columns="availableColumns"
              :available-artifacts="artifactOptions"
            />

            <SingleSelect
              v-else-if="component.component_type === 'SingleSelect'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
              :incoming-columns="availableColumns"
              :available-artifacts="artifactOptions"
            />

            <ToggleSwitch
              v-else-if="component.component_type === 'ToggleSwitch'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
            />

            <ColumnSelector
              v-else-if="component.component_type === 'ColumnSelector'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
              :incoming-columns="columnTypes"
            />

            <SecretSelector
              v-else-if="component.component_type === 'SecretSelector'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
            />

            <ColumnActionInput
              v-else-if="component.component_type === 'ColumnActionInput'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
              :incoming-columns="columnTypes"
            />

            <div v-else class="text-red-500 text-xs">
              Unknown component type: {{ (component as any).component_type }}
            </div>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import axios from "axios";
import { CustomNodeSchema, SectionComponent } from "./interface";
import { getCustomNodeSchema } from "./interface";
import { useNodeStore } from "../../../../../stores/column-store";
import { NodeUserDefined } from "../../../baseNode/nodeInput";
import { NodeData, FileColumn } from "../../../baseNode/nodeInterfaces";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
// Import individual UI components
import MultiSelect from "./components/MultiSelect.vue";
import ToggleSwitch from "./components/ToggleSwitch.vue";
import TextInput from "./components/TextInput.vue";
import NumericInput from "./components/NumericInput.vue";
import SliderInput from "./components/SliderInput.vue";
import SingleSelect from "./components/SingleSelect.vue";
import ColumnSelector from "./components/ColumnSelector.vue";
import SecretSelector from "./components/SecretSelector.vue";
import ColumnActionInput from "./components/ColumnActionInput.vue";

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
const nodeStore = useNodeStore();
const nodeData = ref<NodeData | null>(null);
const availableColumns = ref<string[]>([]);
const currentNodeId = ref<number | null>(null);
const nodeUserDefined = ref<NodeUserDefined | null>(null);
const columnTypes = ref<FileColumn[]>([]);
const artifactOptions = ref<string[]>([]);

// Kernel state
const availableKernels = ref<KernelInfo[]>([]);
const selectedKernelId = ref<string | null>(null);
const kernelRequiredError = ref(false);

async function fetchKernels() {
  try {
    const response = await axios.get("/kernels/");
    availableKernels.value = response.data || [];
  } catch {
    // Kernels endpoint may not be available (no Docker), silently ignore
    availableKernels.value = [];
  }
}

async function fetchAvailableArtifacts(nodeId: number) {
  try {
    const response = await axios.get("/flow/node_available_artifacts", {
      params: { flow_id: nodeStore.flow_id, node_id: nodeId },
    });
    const artifacts = response.data?.artifacts ?? [];
    artifactOptions.value = artifacts.map((artifact: any) => artifact.name);
  } catch {
    artifactOptions.value = [];
  }
}

// --- Lifecycle Methods (exposed to parent) ---

const loadNodeData = async (nodeId: number) => {
  loading.value = true;
  error.value = "";
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

    // Initialize kernel selection: saved setting > schema default > null
    selectedKernelId.value =
      nodeUserDefined.value?.kernel_id ?? schemaData.kernel_id ?? null;
    kernelRequiredError.value = !!schemaData.requires_kernel && !selectedKernelId.value;

    const mainColumns = inputNodeData?.main_input?.columns ?? [];
    if (mainColumns.length) {
      availableColumns.value = mainColumns;
      columnTypes.value = inputNodeData.main_input.table_schema;
    } else {
      console.warn(
        `No main_input or columns found for node ${nodeId}. Select components may be empty.`,
      );
    }

    await fetchAvailableArtifacts(nodeId);

    initializeFormData(schemaData, inputNodeData?.setting_input);
  } catch (err: any) {
    error.value = err.message || "An unknown error occurred while loading node data.";
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
    kernelRequiredError.value = !!schema.value?.requires_kernel && !selectedKernelId.value;
    nodeUserDefined.value.settings = formData.value;
    nodeUserDefined.value.is_user_defined = true;
    nodeUserDefined.value.is_setup = true;
    // Pass the selected kernel to the backend
    nodeUserDefined.value.kernel_id = selectedKernelId.value;
    // Preserve output_names from schema if available
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
        // Use saved settings first (highest priority)
        data[sectionKey][componentKey] = savedValue;
      } else if (component.value !== undefined) {
        // Use component.value from schema (second priority)
        data[sectionKey][componentKey] = component.value;
      } else {
        // Use default value (lowest priority)
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

// Expose the methods to the parent component
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

.section-description {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  margin-top: 0.25rem;
  margin-bottom: 1.25rem;
  padding-left: 0.5rem;
}

.components-container {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.section-title {
  font-size: var(--font-size-lg, 15px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
  padding: var(--spacing-3, 12px) var(--spacing-4, 16px);
  margin-bottom: var(--spacing-3, 12px);
  background-color: var(--color-background-tertiary, #f1f3f5);
  border-radius: var(--border-radius-md, 6px);
  border-left: 3px solid var(--color-accent, #0891b2);
}

.kernel-selector-section {
  margin-bottom: var(--spacing-4, 16px);
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
