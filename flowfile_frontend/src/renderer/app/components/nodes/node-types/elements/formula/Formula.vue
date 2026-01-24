<template>
  <div v-if="dataLoaded && nodeFormula" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeFormula"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div v-if="nodeStore.is_loaded">
        <div v-if="formulaInput && nodeFormula" class="selector-container">
          <DropDownGeneric
            v-model="formulaInput.field.name"
            title="Output field"
            :allow-other="true"
            :option-list="nodeData?.main_input?.columns ?? []"
            placeholder="Select or create field"
          />
          <DropDownGeneric
            v-model="formulaInput.field.data_type"
            :option-list="dataTypes"
            title="Data type"
            :allow-other="false"
          />
        </div>
        <mainEditorRef
          v-if="showEditor && formulaInput"
          ref="editorChild"
          :editor-string="formulaInput.function"
        />
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import mainEditorRef from "../../../../../features/designer/editor/fullEditor.vue";
import DropDownGeneric from "../../../baseNode/page_objects/dropDownGeneric.vue";
import { createFormulaNode } from "./formula";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

import { NodeFormula, FormulaInput } from "../../../baseNode/nodeInput";

const showEditor = ref<boolean>(false);
const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeFormula = ref<NodeFormula | null>(null);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFormula,
  onBeforeSave: () => {
    if (!nodeFormula.value || !formulaInput.value) {
      return false;
    }
    nodeFormula.value.function.function = nodeStore.inputCode;
    return true;
  },
});

interface OutputColumnSelectorType {
  selectedValue: string;
}

interface EditorChildType {
  showHideOptions: () => void;
  showTools: boolean;
}
const outputColumnSelector = ref<OutputColumnSelectorType>({
  selectedValue: "",
});
const editorChild = ref<EditorChildType | null>(null);
const formulaInput = ref<FormulaInput | null>(null);
const dataTypes = [...nodeStore.getDataTypes(), "Auto"];
const nodeData = ref<null | NodeData>(null);

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  if (nodeData.value && nodeData.value.setting_input && nodeData.value.setting_input.is_setup) {
    nodeFormula.value = nodeData.value.setting_input;
    if (nodeFormula.value && nodeFormula.value.function) {
      formulaInput.value = nodeFormula.value.function;
      outputColumnSelector.value.selectedValue = formulaInput.value.field.name;
    }
  } else {
    nodeFormula.value = createFormulaNode(nodeStore.flow_id, nodeStore.node_id);
    nodeFormula.value.depending_on_id = nodeData.value?.main_input?.node_id;
    formulaInput.value = nodeFormula.value.function;
  }
  showEditor.value = true;
  dataLoaded.value = true;
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style lang="scss" scoped>
.x-flip {
  transform: scaleX(-1);
}

.va-navbar__item {
  cursor: pointer;
  margin-bottom: 20px; /* Added margin for spacing */
}

.data-type-select {
  margin-top: 10px;
  margin-bottom: 20px;
}
.selector-container {
  display: flex; /* Make children align horizontally */
  align-items: center; /* Center items vertically */
  gap: 10px; /* Add space between items */
  margin-bottom: 12px; /* Added padding between selector and editor */
}
</style>
