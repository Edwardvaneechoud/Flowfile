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
      <!-- Kernel Selection (shown when node requires kernel) -->
      <div v-if="schema.use_kernel" class="kernel-selection-block">
        <div class="section-title">Kernel</div>
        <div class="kernel-row">
          <el-select
            v-model="selectedKernelId"
            placeholder="Select a kernel..."
            class="kernel-select"
            size="small"
            :loading="kernelsLoading"
            @change="handleKernelChange"
          >
            <el-option
              v-for="kernel in kernels"
              :key="kernel.id"
              :value="kernel.id"
              :label="`${kernel.name} (${kernel.state})`"
              :disabled="!kernelMatchesRequirements(kernel)"
            >
              <span class="kernel-option">
                <span
                  class="kernel-state-dot"
                  :class="`kernel-state-dot--${kernel.state}`"
                ></span>
                <span>{{ kernel.name }}</span>
                <span class="kernel-state-label">({{ kernel.state }})</span>
                <span
                  v-if="!kernelMatchesRequirements(kernel)"
                  class="kernel-missing"
                  :title="`Missing: ${getMissingPackages(kernel).join(', ')}`"
                >
                  missing packages
                </span>
              </span>
            </el-option>
          </el-select>
          <router-link :to="{ name: 'kernelManager' }" class="manage-kernels-link">
            Manage Kernels
          </router-link>
        </div>
        <div v-if="schema.required_packages.length > 0" class="required-packages">
          <span class="packages-label">Required:</span>
          <span
            v-for="pkg in schema.required_packages"
            :key="pkg"
            class="package-tag"
          >{{ pkg }}</span>
        </div>
        <div v-if="!selectedKernelId" class="kernel-warning">
          <i class="fa-solid fa-triangle-exclamation"></i>
          No kernel selected. A kernel is required to run this node.
        </div>
        <div v-else-if="selectedKernelState && selectedKernelState !== 'idle'" class="kernel-warning">
          <i class="fa-solid fa-triangle-exclamation"></i>
          Kernel is {{ selectedKernelState }}.
          <template v-if="selectedKernelState === 'stopped'">Start it from the Kernel Manager.</template>
          <template v-else-if="selectedKernelState === 'error'">Check the Kernel Manager for details.</template>
          <template v-else-if="selectedKernelState === 'starting'">Please wait for it to become idle.</template>
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
            />

            <SingleSelect
              v-else-if="component.component_type === 'SingleSelect'"
              v-model="formData[sectionKey][componentKey]"
              :schema="component"
              :incoming-columns="availableColumns"
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
import { ref, computed, onUnmounted } from "vue";
import { CustomNodeSchema, SectionComponent } from "./interface";
import { getCustomNodeSchema } from "./interface";
import { useNodeStore } from "../../../../../stores/column-store";
import { NodeUserDefined } from "../../../baseNode/nodeInput";
import { NodeData, FileColumn } from "../../../baseNode/nodeInterfaces";
import type { KernelInfo } from "../../../../../types/kernel.types";
import { KernelApi } from "../../../../../api/kernel.api";
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

// Kernel state
const kernels = ref<KernelInfo[]>([]);
const kernelsLoading = ref(false);
const selectedKernelId = ref<string | null>(null);
let kernelPollTimer: ReturnType<typeof setInterval> | null = null;

const selectedKernelState = computed(() => {
  if (!selectedKernelId.value) return null;
  const kernel = kernels.value.find((k) => k.id === selectedKernelId.value);
  return kernel?.state ?? null;
});

function kernelMatchesRequirements(kernel: KernelInfo): boolean {
  const required = schema.value?.required_packages ?? [];
  if (required.length === 0) return true;
  const installed = new Set(kernel.packages.map((p) => p.toLowerCase()));
  return required.every((pkg) => installed.has(pkg.toLowerCase()));
}

function getMissingPackages(kernel: KernelInfo): string[] {
  const required = schema.value?.required_packages ?? [];
  const installed = new Set(kernel.packages.map((p) => p.toLowerCase()));
  return required.filter((pkg) => !installed.has(pkg.toLowerCase()));
}

const loadKernels = async () => {
  kernelsLoading.value = true;
  try {
    kernels.value = await KernelApi.getAll();
  } catch (err) {
    console.error("Failed to load kernels:", err);
  } finally {
    kernelsLoading.value = false;
  }
};

const startKernelPolling = () => {
  stopKernelPolling();
  kernelPollTimer = setInterval(async () => {
    try {
      kernels.value = await KernelApi.getAll();
    } catch {
      // Silently ignore poll errors
    }
  }, 5000);
};

const stopKernelPolling = () => {
  if (kernelPollTimer !== null) {
    clearInterval(kernelPollTimer);
    kernelPollTimer = null;
  }
};

const handleKernelChange = (kernelId: string | null) => {
  if (nodeUserDefined.value) {
    nodeUserDefined.value.kernel_id = kernelId ?? null;
  }
};

onUnmounted(() => {
  stopKernelPolling();
});

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
    const [schemaData] = await Promise.all([getCustomNodeSchema(nodeStore.flow_id, nodeId)]);

    schema.value = schemaData;
    nodeData.value = inputNodeData;
    nodeUserDefined.value = nodeData.value?.setting_input;

    if (!nodeData.value?.setting_input.is_setup && nodeUserDefined.value) {
      nodeUserDefined.value.settings = {};
    }

    if (inputNodeData?.main_input?.columns) {
      availableColumns.value = inputNodeData.main_input.columns;
      columnTypes.value = inputNodeData.main_input.table_schema;
    } else {
      console.warn(
        `No main_input or columns found for node ${nodeId}. Select components may be empty.`,
      );
    }

    initializeFormData(schemaData, inputNodeData?.setting_input);

    // Load kernels if this is a kernel-backed node
    if (schemaData.use_kernel) {
      selectedKernelId.value = nodeUserDefined.value?.kernel_id ?? null;
      await loadKernels();
      startKernelPolling();
    }
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
    nodeUserDefined.value.settings = formData.value;
    nodeUserDefined.value.is_user_defined = true;
    nodeUserDefined.value.is_setup = true;
    if (schema.value?.use_kernel) {
      nodeUserDefined.value.kernel_id = selectedKernelId.value;
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

/* Kernel selection */
.kernel-selection-block {
  margin-bottom: 1rem;
}

.kernel-row {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.kernel-select {
  flex: 1;
}

.manage-kernels-link {
  font-size: 0.8rem;
  color: var(--el-color-primary);
  text-decoration: none;
  white-space: nowrap;
}

.manage-kernels-link:hover {
  text-decoration: underline;
}

.kernel-option {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.kernel-state-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.kernel-state-dot--idle {
  background-color: #67c23a;
}

.kernel-state-dot--executing {
  background-color: #e6a23c;
}

.kernel-state-dot--starting {
  background-color: #409eff;
}

.kernel-state-dot--stopped {
  background-color: #909399;
}

.kernel-state-dot--error {
  background-color: #f56c6c;
}

.kernel-state-label {
  font-size: 0.8rem;
  color: var(--el-text-color-secondary);
}

.kernel-missing {
  font-size: 0.7rem;
  color: var(--el-color-danger);
  font-style: italic;
}

.required-packages {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.3rem;
  margin-top: 0.5rem;
  font-size: 0.8rem;
}

.packages-label {
  font-weight: 500;
  color: var(--color-text-secondary);
  margin-right: 0.25rem;
}

.package-tag {
  display: inline-flex;
  padding: 0.1rem 0.4rem;
  background-color: var(--el-color-primary-light-9, #ecf5ff);
  color: var(--el-color-primary, #409eff);
  border-radius: 3px;
  font-size: 0.75rem;
}

.kernel-warning {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.35rem 0.6rem;
  margin-top: 0.5rem;
  font-size: 0.75rem;
  color: var(--el-color-warning-dark-2, #b88230);
  background-color: var(--el-color-warning-light-9, #fdf6ec);
  border: 1px solid var(--el-color-warning-light-5, #f3d19e);
  border-radius: 4px;
}

.kernel-warning i {
  flex-shrink: 0;
}
</style>
