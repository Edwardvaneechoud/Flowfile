<template>
  <div v-if="dataLoaded && NodeFormula" class="listbox-wrapper">
    <generic-node-settings v-model="NodeFormula">
      <div v-if="nodeStore.is_loaded">
        <div v-if="formulaInput && formulaNode" class="selector-container">
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
import { useNodeStore } from "../../../../../stores/column-store";
import mainEditorRef from "../../../editor/fullEditor.vue";
import DropDownGeneric from "../../../baseNode/page_objects/dropDownGeneric.vue";
import { createFormulaNode } from "./formula";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

import { NodeFormula, FormulaInput } from "../../../baseNode/nodeInput";

const showEditor = ref<boolean>(false);
const nodeStore = useNodeStore();
const dataLoaded = ref(false);

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
const formulaNode = ref<NodeFormula | null>(null);
const formulaInput = ref<FormulaInput | null>(null);
const dataTypes = nodeStore.getDataTypes();
const nodeData = ref<null | NodeData>(null);

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(1, nodeId, false);
  if (nodeData.value && nodeData.value.setting_input && nodeData.value.setting_input.is_setup) {
    formulaNode.value = nodeData.value.setting_input;
    if (formulaNode.value && formulaNode.value.function) {
      formulaInput.value = formulaNode.value.function;
      outputColumnSelector.value.selectedValue = formulaInput.value.field.name;
    }
  } else {
    formulaNode.value = createFormulaNode(nodeStore.flow_id, nodeStore.node_id);
    formulaNode.value.depending_on_id = nodeData.value?.main_input?.node_id;
    formulaInput.value = formulaNode.value.function;
  }
  showEditor.value = true;
  dataLoaded.value = true;
};

const pushNodeData = async () => {
  if (!formulaNode.value || !formulaInput.value) {
    return;
  }
  formulaNode.value.cache_results = false;
  formulaNode.value.is_setup = true;
  formulaNode.value.function.function = nodeStore.inputCode;
  nodeStore.updateSettings(formulaNode);
  showEditor.value = false;
  dataLoaded.value = false;
};

defineExpose({ loadNodeData, pushNodeData });
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
