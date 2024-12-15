<template>
  <div v-if="dataLoaded">
    <generic-node-settings v-model="nodeSelect">
    <select-dynamic
      :select-inputs="nodeSelect.select_input"
      :show-keep-option="true"
      :show-data-type="true"
      :show-new-columns="true"
      :show-old-columns="true"
      :show-headers="true"
      :show-title="false"
      :show-data="true"
      title="Select data"
      @update-select-inputs="updateSelectInputsHandler"
    />
  </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import {
  createNodeSelect,
  updateNodeSelect,
} from "../../../baseNode/selectComponents/nodeSelectLogic";
import { NodeSelect } from "../../../baseNode/nodeInput";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/column-store";
import { SelectInput } from "../../../baseNode/nodeInput";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import GenericNodeSettings from '../../../baseNode/genericNodeSettings.vue'

const keepMissing = ref(false);
const nodeStore = useNodeStore();
const nodeSelect = ref<NodeSelect>(createNodeSelect().value);
const dataLoaded = ref(false);

const loadNodeData = async (nodeId: number) => {
  dataLoaded.value = false;
  const result = await nodeStore.getNodeData(1, nodeId, false);
  if (result) {
    dataLoaded.value = true;
    nodeSelect.value = createNodeSelect(nodeStore.flow_id, nodeStore.node_id).value;
    const main_input = result.main_input;
    try {
      // Try to parse the result.value.setting_input
      if (result?.setting_input && main_input && result?.setting_input.is_setup) {
        nodeSelect.value = result.setting_input;
        keepMissing.value = nodeSelect.value.keep_missing;
        updateNodeSelect(main_input, nodeSelect);
      } else {
        throw new Error("Setting input not available");
      }
    } catch (error) {
      // If there's an error, fall back to this logic
      if (main_input && nodeSelect.value) {
        console.log("doing this");
        nodeSelect.value.depending_on_id = main_input.node_id;
        nodeSelect.value.flow_id = nodeStore.flow_id;
        nodeSelect.value.node_id = nodeStore.node_id;
        nodeSelect.value.keep_missing = keepMissing.value;
        updateNodeSelect(main_input, nodeSelect);
      }
    }
  }
  console.log(nodeSelect.value);
};

const pushNodeData = async () => {
  nodeSelect.value.select_input.sort((a, b) => a.position - b.position);
  const originalData = nodeStore.getCurrentNodeData();
  const newColumnSettings = nodeSelect.value.select_input;
  nodeSelect.value.keep_missing = keepMissing.value;
  if (originalData) {
    newColumnSettings.forEach((newColumnSetting, index) => {
      let original_object = originalData.main_input?.table_schema[index];
      newColumnSetting.is_altered = original_object?.data_type !== newColumnSetting.data_type;
      newColumnSetting.data_type_change = newColumnSetting.is_altered;
      newColumnSetting.position = index;
    });
  }
  await nodeStore.updateSettings(nodeSelect);
};
const updateSelectInputsHandler = (updatedInputs: SelectInput[]) => {
  nodeSelect.value.select_input = updatedInputs;
};

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>
