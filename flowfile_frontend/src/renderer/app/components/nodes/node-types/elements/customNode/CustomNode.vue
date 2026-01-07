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
      <!-- Loop through each section in the settings_schema -->
      <div
        v-for="(section, sectionKey) in schema.settings_schema"
        v-show="!section.hidden"
        :key="sectionKey"
        class="listbox-wrapper"
      >
        <div class="listbox-subtitle">
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

            <RollingWindowInput
              v-else-if="component.component_type === 'RollingWindowInput'"
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
import RollingWindowInput from "./components/RollingWindowInput.vue";

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
</style>
