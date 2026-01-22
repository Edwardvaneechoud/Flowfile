<template>
  <div v-if="dataLoaded && nodeJoin" class="listbox-wrapper">
    <generic-node-settings v-model="nodeJoin" @request-save="pushNodeData">
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
                <div class="selector-wrapper">
                  <drop-down
                    v-model="selector.left_col"
                    :value="selector.left_col"
                    :column-options="result?.main_input?.columns"
                    @update:value="(value: string) => handleChange(value, index, 'left')"
                  />
                </div>
                <div class="selector-wrapper">
                  <drop-down
                    v-model="selector.right_col"
                    :value="selector.right_col"
                    :column-options="result?.right_input?.columns"
                    @update:value="(value: string) => handleChange(value, index, 'right')"
                  />
                </div>
                <div class="action-buttons">
                  <button
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
        v-if="showColumnSelection"
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
        v-if="showColumnSelection"
        :select-inputs="nodeJoin?.join_input.right_select.renames"
        :show-keep-option="true"
        :show-headers="true"
        :show-title="true"
        :show-data="true"
        title="Right data"
        @update-select-inputs="
          (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, false)
        "
      />
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { SelectInput } from "../../../baseNode/nodeInput";
import { NodeJoin } from "./joinInterfaces";
import DropDown from "../../../baseNode/page_objects/dropDown.vue";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

type JoinType = "inner" | "left" | "right" | "full" | "semi" | "anti" | "cross";

const joinTypes: JoinType[] = ["inner", "left", "right", "full", "semi", "anti", "cross"];

const JOIN_TYPES_WITHOUT_COLUMN_SELECTION: JoinType[] = ["anti", "semi"];

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
  result.value = await nodeStore.getNodeData(nodeId, false);
  nodeJoin.value = result.value?.setting_input;
  if (result.value) {
    dataLoaded.value = true;
  }
};

const addJoinCondition = () => {
  if (nodeJoin.value) {
    nodeJoin.value.join_input.join_mapping.push({
      left_col: "",
      right_col: "",
    });
  }
};

const showColumnSelection = computed(() => {
  const joinType = nodeJoin.value?.join_input.how;
  return joinType && !JOIN_TYPES_WITHOUT_COLUMN_SELECTION.includes(joinType);
});

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
  color: var(--color-text-primary);
  font-weight: 500;
  min-width: 70px;
}

/* Join Mapping Section */
.table-wrapper {
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  overflow: hidden;
  margin: 5px;
}

.selectors-header {
  display: flex;
  justify-content: space-between;
  padding: 8px 16px;
  background-color: var(--color-background-muted);
  border-bottom: 1px solid var(--color-border-primary);
}

.selectors-title {
  flex: 1;
  text-align: center;
  font-size: 12px;
  color: var(--color-text-secondary);
  font-weight: 500;
}

.selectors-container {
  padding: 12px;
  box-sizing: border-box;
  width: 100%;
  display: flex;
  justify-content: space-between;
  flex-direction: column;
}

.selectors-row {
  display: flex;
  gap: 12px;
  margin-bottom: 8px;
  width: 100%;
  justify-content: space-between;
  align-items: center;
}

.selectors-row:last-child {
  margin-bottom: 0;
}

.selector-wrapper {
  flex: 1;
  min-width: 0;
}

/* Action Buttons */
.action-buttons {
  display: flex;
  gap: 4px;
  width: 56px;
  justify-content: flex-end;
  align-items: center;
}

.action-button,
.add-join-button,
.remove-join-button {
  cursor: pointer;
  width: 24px;
  height: 24px;
  border-radius: 4px;
  border: 1px solid var(--color-border-primary);
  background-color: var(--color-background-primary);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  transition: all 0.2s ease;
}

.add-join-button,
.add-button {
  color: var(--color-success);
  border-color: var(--color-success);
}

.add-join-button:hover,
.add-button:hover {
  background-color: var(--color-success);
  color: var(--color-text-inverse);
}

.remove-join-button,
.remove-button {
  color: var(--color-danger);
  border-color: var(--color-danger);
}

.remove-join-button:hover,
.remove-button:hover {
  background-color: var(--color-danger);
  color: var(--color-text-inverse);
}

/* Custom scrollbar */
.selectors-container::-webkit-scrollbar {
  width: 8px;
}

.selectors-container::-webkit-scrollbar-track {
  background: transparent;
}

.selectors-container::-webkit-scrollbar-thumb {
  background-color: var(--color-border-primary);
  border-radius: 4px;
}

.selectors-container::-webkit-scrollbar-thumb:hover {
  background-color: var(--color-border-secondary);
}
</style>
