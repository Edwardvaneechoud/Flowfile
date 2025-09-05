<template>
  <div v-if="isLoaded && nodeFilter">
    <generic-node-settings v-model="nodeFilter">
      <div class="listbox-wrapper">
        <div style="border-radius: 20px">
          <el-switch
            v-model="isAdvancedFilter"
            class="mb-2"
            active-text="Advanced filter options"
            inactive-text="Basic filter"
          />
        </div>
        <div v-if="isAdvancedFilter">
          Advanced filter
          <mainEditorRef ref="editorChild" :editor-string="editorString" />
        </div>
        <div v-if="!isAdvancedFilter">
          Standard Filter

          <div class="selectors-row">
            <div v-if="nodeFilter?.filter_input.basic_filter">
              <column-selector
                v-model="nodeFilter.filter_input.basic_filter.field"
                :value="nodeFilter.filter_input.basic_filter.field"
                :column-options="nodeData?.main_input?.columns"
                @update:value="(value: string) => handleFieldChange(value)"
              />
            </div>
            <div v-if="nodeFilter?.filter_input.basic_filter">
              <column-selector
                :value="translateSymbolToDes(nodeFilter.filter_input.basic_filter.filter_type)"
                :column-options="comparisonOptions"
                @update:value="(value: string) => handleFilterTypeChange(value)"
              />
            </div>
            <div v-if="nodeFilter?.filter_input.basic_filter">
              <input
                v-model="nodeFilter.filter_input.basic_filter.filter_value"
                type="text"
                class="input-field"
                @focus="showOptions = true"
              />
            </div>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";

import ColumnSelector from "../../../baseNode/page_objects/dropDown.vue";
import { useNodeStore } from "../../../../../stores/column-store";
import mainEditorRef from "../../../editor/fullEditor.vue";
import { NodeFilter } from "../../../baseNode/nodeInput";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const editorString = ref<string>("");
const isLoaded = ref<boolean>(false);
const isAdvancedFilter = ref<boolean>(true);
const nodeStore = useNodeStore();
const nodeFilter = ref<NodeFilter | null>(null);
const nodeData = ref<NodeData | null>(null);
const showOptions = ref<boolean>(false);
interface EditorChildType {
  showHideOptions: () => void;
  showTools: boolean;
}
const editorChild = ref<EditorChildType | null>(null);

const comparisonMapping: Record<string, string> = {
  Equals: "=",
  "Smaller then": "<",
  "Greater then": ">",
  Contains: "contains", // or any other representation you prefer
  "Does not equal": "!=",
  "Smaller or equal": "<=",
  "Greater or equal": ">=",
};

const reversedMapping: Record<string, string> = {};

Object.entries(comparisonMapping).forEach(([key, value]) => {
  reversedMapping[value] = key;
});

const translateSymbolToDes = (symbol: string): string => {
  return reversedMapping[symbol] ?? symbol;
};

const comparisonOptions = Object.keys(comparisonMapping);

const handleFieldChange = (newValue: string) => {
  if (nodeFilter.value?.filter_input.basic_filter) {
    nodeFilter.value.filter_input.basic_filter.field = newValue;
  }
};

function translateComparison(input: string): string {
  return comparisonMapping[input] ?? input;
}

const handleFilterTypeChange = (newValue: string) => {
  if (nodeFilter.value?.filter_input.basic_filter) {
    const symbolicType = translateComparison(newValue);
    nodeFilter.value.filter_input.basic_filter.filter_type = symbolicType;
  }
};

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  if (nodeData.value) {
    nodeFilter.value = nodeData.value.setting_input;
    if (nodeFilter.value?.filter_input.advanced_filter) {
      editorString.value = nodeFilter.value?.filter_input.advanced_filter;
    }
    isAdvancedFilter.value = nodeFilter.value?.filter_input.filter_type === "advanced";
  }
  isLoaded.value = true;
};

const updateAdvancedFilter = () => {
  if (nodeFilter.value) {
    nodeFilter.value.filter_input.advanced_filter = nodeStore.inputCode;
    console.log(nodeFilter.value);
  }
};
const pushNodeData = async () => {
  if (nodeFilter.value) {
    if (isAdvancedFilter.value) {
      updateAdvancedFilter();
      nodeFilter.value.filter_input.filter_type = "advanced";
    } else {
      nodeFilter.value.filter_input.filter_type = "basic";
    }
    nodeStore.updateSettings(nodeFilter);
  }
};

defineExpose({ loadNodeData, pushNodeData });
</script>

<style lang="scss" scoped>
.x-flip {
  transform: scaleX(-100%);
}

.input-field {
  width: 100%; /* Full width to fit container */
  padding: 6px 10px; /* Reduced padding */
  font-size: 14px; /* Smaller font size */
  line-height: 1.4; /* Adjust line height for better text alignment */
  border: 1px solid #e0e0e0; /* Lighter border color */
  border-radius: 4px; /* Slightly rounded corners for a softer look */
  box-shadow: none; /* Remove shadow for a flatter style */
  outline: none; /* Remove the default focus outline */
  transition:
    border-color 0.2s,
    box-shadow 0.2s; /* Smooth transition for focus */
}

.selectors-row {
  display: flex;
  justify-content: space-between;
  margin-bottom: 20px; /* Spacing between rows */
  margin-left: 20px;
}
</style>
