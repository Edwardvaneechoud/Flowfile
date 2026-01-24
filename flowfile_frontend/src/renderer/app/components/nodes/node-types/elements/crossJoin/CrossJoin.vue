<template>
  <div v-if="dataLoaded && nodeCrossJoin">
    <generic-node-settings
      v-model="nodeCrossJoin"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <select-dynamic
          :select-inputs="nodeCrossJoin?.cross_join_input.left_select.renames"
          :show-keep-option="true"
          :show-title="true"
          :show-headers="true"
          :show-data="true"
          title="Left data"
          @update-select-inputs="(updatedInputs) => updateSelectInputsHandler(updatedInputs, true)"
        />
        <select-dynamic
          :select-inputs="nodeCrossJoin?.cross_join_input.right_select.renames"
          :show-keep-option="true"
          :show-headers="true"
          :show-title="true"
          :show-data="true"
          title="Right data"
          @update-select-inputs="(updatedInputs) => updateSelectInputsHandler(updatedInputs, false)"
        />
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>
<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { SelectInput, NodeCrossJoin } from "../../../baseNode/nodeInput";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const result = ref<NodeData | null>(null);
const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeCrossJoin = ref<NodeCrossJoin | null>(null);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeCrossJoin,
});

const updateSelectInputsHandler = (updatedInputs: SelectInput[], isLeft: boolean) => {
  if (isLeft && nodeCrossJoin.value) {
    nodeCrossJoin.value.cross_join_input.left_select.renames = updatedInputs;
  } else if (nodeCrossJoin.value) {
    nodeCrossJoin.value.cross_join_input.right_select.renames = updatedInputs;
  }
};

const loadNodeData = async (nodeId: number) => {
  result.value = await nodeStore.getNodeData(nodeId, false);
  nodeCrossJoin.value = result.value?.setting_input;
  if (result.value) {
    dataLoaded.value = true;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.selectors-row {
  display: flex;
  justify-content: space-between;
  margin-bottom: 10px; /* Spacing between rows */
}

.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: var(--color-background-primary);
  padding: 8px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  border-radius: 4px;
  user-select: none;
}

.context-menu button {
  display: block;
  background: none;
  border: none;
  padding: 4px 8px;
  text-align: left;
  width: 100%;
  cursor: pointer;
  z-index: 100;
}

.context-menu button:hover {
  background-color: #f0f0f0;
}

.table-wrapper {
  max-height: 250px; /* Adjust this value as needed */
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1); /* subtle shadow for depth */
  border-radius: 8px; /* rounded corners */
  overflow: auto; /* ensures the rounded corners are applied to the child elements */
  margin: 5px; /* adds a small margin around the table */
}

.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: var(--color-background-primary);
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  border-radius: 4px;
}

.context-menu ul {
  list-style: none;
  padding: 0;
  margin: 0;
}

.context-menu li {
  padding: 8px 16px;
  cursor: pointer;
}

.context-menu li:hover {
  background-color: #f0f0f0;
}

.selectors-container {
  display: flex;
  flex-direction: column;
}

.selectors-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.add-button-container {
  padding-left: 20px; /* Adjust as needed */
  display: flex;
  align-items: center;
}

.action-buttons {
  display: flex;
  align-items: center;
  justify-content: flex-end; /* Adjust if needed */
}
.remove-join-button,
.add-join-button {
  cursor: pointer;
  margin-left: 10px; /* Space between buttons */
  padding: 0 6px; /* Adjust padding */
  height: 20px; /* Adjust height to fit */
  width: 30px; /* Ensure equal width for both buttons */
  min-width: 30px; /* Minimum width to ensure buttons don't shrink */
  text-align: center; /* Ensure the symbols are centered */
}

.selectors-header {
  display: flex;
  justify-content: space-between;
  padding: 0 15px; /* Adjust padding for finer control */
}

.selectors-title {
  flex: 1; /* Adjust this based on your needs */
  text-align: center; /* Center the titles */
  padding: 0 10px; /* Add padding to ensure alignment with the columns below */
}

.action-buttons {
  flex-basis: 60px; /* Adjust according to the width of your buttons */
  display: flex;
  justify-content: center; /* Center the buttons within the action column */
}
</style>
../../../../stores/column-store
