<template>
  <div v-if="loading" class="p-4 text-center text-gray-500">Loading Node UI...</div>
  <div v-else-if="error" class="p-4 text-red-600 bg-red-100 rounded-md">
    <strong>Error:</strong> {{ error }}
  </div>

  <!-- This wrapper prevents rendering until the schema and formData are ready -->
  <div v-else-if="schema && formData && nodeUserDefined" class="custom-node-wrapper">
    <generic-node-settings v-model="nodeUserDefined">

    <!-- Loop through each section in the settings_schema -->
    <div
      v-for="(section, sectionKey) in schema.settings_schema"
      :key="sectionKey"
      v-show="!section.hidden"
      class="listbox-wrapper"
    >
      <div class="listbox-subtitle">
        {{ section.title || sectionKey.toString().replace(/_/g, ' ') }}
      </div>
      <p v-if="section.description" class="section-description">{{ section.description }}</p>

      <div class="components-container">
        <!-- Loop through each component within the section's 'components' object -->
        <div v-for="(component, componentKey) in section.components" :key="componentKey" class="component-item">
          
          <!-- The label is now handled by the child components for better styling -->
          <TextInput
            v-if="component.component_type === 'TextInput'"
            :schema="component"
            v-model="formData[sectionKey][componentKey]"
          />
          <MultiSelect
            v-else-if="component.component_type === 'MultiSelect'"
            :schema="component"
            v-model="formData[sectionKey][componentKey]"
            :incoming-columns="availableColumns"
          />
          <ToggleSwitch
            v-else-if="component.component_type === 'ToggleSwitch'"
            :schema="component"
            v-model="formData[sectionKey][componentKey]"
          />
          <div v-else class="text-red-500 text-xs">Unknown component type: {{ (component as any).component_type }}</div>
        </div>
      </div>
    </div>
  </generic-node-settings>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { CustomNodeSchema, SectionComponent } from "./interface";
import { getCustomNodeSchema } from "./interface";
import { useNodeStore } from "../../../../../stores/column-store";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import {NodeUserDefined} from "../../../baseNode/nodeInput";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
// Import individual UI components
import MultiSelect from "./components/MultiSelect.vue";
import ToggleSwitch from "./components/ToggleSwitch.vue"
import TextInput from "./components/TextInput.vue";

// Component State
const schema = ref<CustomNodeSchema | null>(null);
const formData = ref<any>(null);
const loading = ref(true);
const error = ref<string>('');
const nodeStore = useNodeStore();
const nodeData = ref<NodeData | null>(null);
const availableColumns = ref<string[]>([]);
const currentNodeId = ref<number | null>(null);
const nodeUserDefined = ref<NodeUserDefined | null>(null)

// --- Lifecycle Methods (exposed to parent) ---

const loadNodeData = async (nodeId: number) => {
  loading.value = true;
  error.value = '';
  currentNodeId.value = nodeId;

  try {
    const [schemaData, inputNodeData] = await Promise.all([
      getCustomNodeSchema(),
      nodeStore.getNodeData(nodeId, false)
    ]);

    schema.value = schemaData;
    nodeData.value = inputNodeData;
    nodeUserDefined.value = nodeData.value?.setting_input
    if (inputNodeData?.main_input?.columns) {
      availableColumns.value = inputNodeData.main_input.columns;
    } else {
      console.warn(`No main_input or columns found for node ${nodeId}. Select components may be empty.`);
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
  
  // Construct the complete settings object that would be pushed to the store.
  // This merges the original settings with the user's current changes.
  const finalSettingsPayload = {
    ...nodeData.value.setting_input, // Start with the existing settings from the store
    ...formData.value               // Overwrite/add the current form values
  };

  console.log(`--- Would push the following settings for node ${currentNodeId.value} ---`);
  console.log(JSON.stringify(finalSettingsPayload, null, 2));

  // The actual store update would be called here:
  // await nodeStore.updateSettings(currentNodeId.value, finalSettingsPayload);
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
      } else {
        let defaultValue = component.default ?? null;
        if (component.input_type === 'array' && defaultValue === null) {
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
  background-color: #f9fafb; /* bg-gray-50 */
}

.node-header {
  padding-bottom: 1rem;
  border-bottom: 1px solid #e5e7eb; /* border-gray-200 */
  margin-bottom: 1.5rem;
}

.node-title {
  font-size: 1.25rem; /* text-xl */
  font-weight: 700; /* font-bold */
  color: #1f2937; /* text-gray-800 */
}

.node-category {
  font-size: 0.875rem; /* text-sm */
  color: #6b7280; /* text-gray-500 */
  margin-top: 0.25rem;
}

.section-description {
  font-size: 0.875rem;
  color: #6b7280; /* text-gray-500 */
  margin-top: 0.25rem;
  margin-bottom: 1.25rem;
  padding-left: 0.5rem;
}

.components-container {
  display: flex;
  flex-direction: column;
  gap: 1.25rem; /* space-y-5 */
}
</style>

