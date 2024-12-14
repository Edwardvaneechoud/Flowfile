<template>
  <div v-if="dataLoaded">
    <div class="listbox-wrapper">
      <div class="listbox-subtitle">Join columns</div>
      <div class="join-content">
        <div class="join-type-selector">
          <label class="join-type-label">Join Type:</label>
          <drop-down
            v-if="nodeJoin"
            v-model="nodeJoin.join_input.how"
            :column-options="joinTypes"
            placeholder="Select join type"
            :allow-other="false"
            @error="handleJoinTypeError"
          />
        </div>
        <div class="join-mapping-section">
          <div class="table-wrapper">
            <div class="selectors-header">
              <div class="selectors-title">L</div>
              <div class="selectors-title">R</div>
              <div class="selectors-title"></div>
            </div>
            <div class="selectors-container">
              <div
                v-for="(selector, index) in nodeJoin?.join_input.join_mapping"
                :key="index"
                class="selectors-row"
              >
                <drop-down
                  v-model="selector.left_col"
                  :value="selector.left_col"
                  :column-options="result?.main_input?.columns"
                  @update:value="(value: string) => handleChange(value, index, 'left')"
                />
                <drop-down
                  v-model="selector.right_col"
                  :value="selector.right_col"
                  :column-options="result?.right_input?.columns"
                  @update:value="(value: string) => handleChange(value, index, 'right')"
                />
                <div class="action-buttons">
                  <button
                    v-if="index !== (nodeJoin?.join_input.join_mapping.length ?? 0) - 1"
                    class="action-button remove-button"
                    @click="removeJoinCondition(index)"
                  >
                    -
                  </button>
                  <button
                    v-if="index === (nodeJoin?.join_input.join_mapping?.length ?? 0) - 1"
                    class="action-button add-button"
                    @click="addJoinCondition"
                  >
                    +
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <select-dynamic
        :select-inputs="nodeJoin?.join_input.left_select.renames"
        :show-keep-option="true"
        :show-title="true"
        :show-headers="true"
        :show-data="true"
        title="Left data"
        @update-select-inputs="
          (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, true)
        "
      />
      <select-dynamic
        :select-inputs="nodeJoin?.join_input.right_select.renames"
        :show-keep-option="true"
        :show-headers="true"
        :show-title="true"
        :show-data="true"
        title="Right data"
        @update-select-inputs="
          (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, true)
        "
      />
    </div>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/column-store";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { SelectInput } from "../../../baseNode/nodeInput";
import { NodeJoin } from "./joinInterfaces";
import DropDown from "../../../baseNode/page_objects/dropDown.vue";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";

const joinTypes = ["inner", "left", "right", "full", "semi", "anti", "cross"];

const handleJoinTypeError = (error: string) => {
  console.error("Join type error:", error);
  // Handle the error as needed
};

const result = ref<NodeData | null>(null);
const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeJoin = ref<NodeJoin | null>(null);

const updateSelectInputsHandler = (updatedInputs: SelectInput[], isLeft: boolean) => {
  if (isLeft && nodeJoin.value) {
    nodeJoin.value.join_input.left_select.renames = updatedInputs;
  } else if (nodeJoin.value) {
    nodeJoin.value.join_input.right_select.renames = updatedInputs;
  }
};

const loadNodeData = async (nodeId: number) => {
  result.value = await nodeStore.getNodeData(1, nodeId, false);
  nodeJoin.value = result.value?.setting_input;
  console.log(result.value);
  if (result.value) {
    console.log("Data loaded");
    dataLoaded.value = true;
  }
  nodeStore.isDrawerOpen = true;
};

const addJoinCondition = () => {
  if (nodeJoin.value) {
    nodeJoin.value.join_input.join_mapping.push({
      left_col: "",
      right_col: "",
    });
  }
};

const removeJoinCondition = (index: number) => {
  if (nodeJoin.value && index >= 0) {
    nodeJoin.value.join_input.join_mapping.splice(index, 1);
  }
};

const handleChange = (newValue: string, index: number, side: string) => {
  if (side === "left") {
    if (nodeJoin.value) {
      nodeJoin.value.join_input.join_mapping[index].left_col = newValue;
    }
  } else {
    if (nodeJoin.value) {
      nodeJoin.value.join_input.join_mapping[index].right_col = newValue;
    }
  }
};

const pushNodeData = async () => {
  console.log("Pushing node data");
  nodeStore.updateSettings(nodeJoin);
  //dataLoaded.value = false
  nodeStore.isDrawerOpen = false;
};

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
/* Join Type Selector */
.join-type-selector {
  display: flex;
  align-items: center;
  margin: 12px;
  gap: 10px;
}

.join-type-label {
  font-size: 12px;
  color: #333;
  font-weight: 500;
  min-width: 70px;
}

/* Join Mapping Section */
.table-wrapper {
  border: 1px solid #eee;
  border-radius: 6px;
  overflow: hidden;
  margin: 5px;
}

.selectors-header {
  display: flex;
  justify-content: space-between;
  padding: 8px 16px;
  background-color: #fafafa;
  border-bottom: 1px solid #eee;
}

.selectors-title {
  flex: 1;
  text-align: center;
  font-size: 12px;
  color: #666;
  font-weight: 500;
}

.selectors-container {
  padding: 12px;
  box-sizing: border-box;
  width: 100%;
  display: flex;
  justify-content: space-between; /* This puts maximum space between the children */
}

.selectors-row {
  display: flex;
  gap: 12px;
  margin-bottom: 8px;
  width: 100%;
  display: flex;
  justify-content: space-between;
}

.selectors-row:last-child {
  margin-bottom: 0;
}

/* Action Buttons */
.action-buttons {
  display: flex;
  gap: 4px;
  min-width: 60px;
  justify-content: center;
}

.action-button,
.add-join-button,
.remove-join-button {
  cursor: pointer;
  width: 24px;
  height: 24px;
  border-radius: 4px;
  border: 1px solid #ddd;
  background-color: #fff;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  transition: all 0.2s ease;
}

.add-join-button {
  color: #45a049;
  border-color: #45a049;
}

.add-join-button:hover {
  background-color: #45a049;
  color: #fff;
}

.remove-join-button {
  color: #d32f2f;
  border-color: #d32f2f;
}

.remove-join-button:hover {
  background-color: #d32f2f;
  color: #fff;
}

/* Custom scrollbar */
.selectors-container::-webkit-scrollbar {
  width: 8px;
}

.selectors-container::-webkit-scrollbar-track {
  background: transparent;
}

.selectors-container::-webkit-scrollbar-thumb {
  background-color: rgba(0, 0, 0, 0.1);
  border-radius: 4px;
}

.selectors-container::-webkit-scrollbar-thumb:hover {
  background-color: rgba(0, 0, 0, 0.2);
}
</style>
