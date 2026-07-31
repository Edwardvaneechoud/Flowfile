<template>
  <div v-if="dataLoaded" class="select-node-root">
    <generic-node-settings
      v-model="nodeSelect"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <select-dynamic
        v-model:sortedBy="nodeSelect.sorted_by"
        :select-inputs="nodeSelect.select_input"
        :show-keep-option="true"
        :show-data-type="true"
        :show-new-columns="true"
        :show-old-columns="true"
        :show-headers="true"
        :show-title="false"
        title="Select data"
        @update-select-inputs="updateSelectInputsHandler"
      />
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import {
  applySelectPositions,
  createNodeSelect,
  updateNodeSelect,
} from "../../../baseNode/selectComponents/nodeSelectLogic";
import { NodeSelect } from "../../../baseNode/nodeInput";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { SelectInput } from "../../../baseNode/nodeInput";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const keepMissing = ref(false);
const nodeStore = useNodeStore();
const nodeSelect = ref<NodeSelect>(createNodeSelect().value);
const dataLoaded = ref(false);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeSelect,
  onBeforeSave: () => {
    nodeSelect.value.keep_missing = keepMissing.value;
    applySelectPositions(
      nodeSelect.value.select_input,
      nodeStore.getCurrentNodeData()?.main_input?.table_schema,
    );
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  const result = await nodeStore.getNodeData(nodeId, false);
  if (result) {
    const main_input = result.main_input;
    try {
      if (result.setting_input && main_input && result.setting_input.is_setup) {
        nodeSelect.value = result.setting_input;
        keepMissing.value = nodeSelect.value.keep_missing;
        updateNodeSelect(main_input, nodeSelect);
      } else {
        throw new Error("Setting input not available");
      }
    } catch (error) {
      if (main_input && nodeSelect.value) {
        nodeSelect.value = createNodeSelect(nodeStore.flow_id, nodeStore.node_id).value;
        nodeSelect.value.depending_on_id = main_input.node_id;
        nodeSelect.value.flow_id = nodeStore.flow_id;
        nodeSelect.value.node_id = nodeStore.node_id;
        nodeSelect.value.keep_missing = keepMissing.value;
        updateNodeSelect(main_input, nodeSelect);
      }
    }
  }
  dataLoaded.value = true;
};

const updateSelectInputsHandler = (updatedInputs: SelectInput[]) => {
  nodeSelect.value.select_input = updatedInputs;
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.select-node-root {
  height: 100%;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

/* Make the select-dynamic chain fill the tab pane so the table can flex
   to the available height instead of using its default 700px cap, which
   leaves an awkward dead zone at the bottom of the panel. We deliberately
   leave the listbox card's padding / box-shadow / border-radius alone so
   the inner table keeps its existing visual style. */
.select-node-root :deep(.select-dynamic-root),
.select-node-root :deep(.select-dynamic-content) {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.select-node-root :deep(.select-dynamic-listbox) {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.select-node-root :deep(.select-dynamic-listbox .table-wrapper) {
  flex: 1;
  min-height: 0;
  max-height: none;
}
</style>
