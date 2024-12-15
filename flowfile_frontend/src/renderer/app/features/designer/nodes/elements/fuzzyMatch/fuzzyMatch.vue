<template>
  <div v-if="isLoaded && nodeFuzzyJoin" class="listbox-wrapper">
    <generic-node-settings v-model="nodeFuzzyJoin">
    <div class="listbox-wrapper">
      <div class="listbox-subtitle">Fuzzy match settings</div>
      <div class="table-wrapper">
        <div v-if="nodeFuzzyJoin?.join_input" class="selectors-container">
          <div
            v-for="(fuzzyMap, index) in nodeFuzzyJoin?.join_input.join_mapping"
            :key="index"
            class="selectors-row"
          >
            <div class="listbox-wrapper">
              <div class="listbox-subtitle">
                <unavailableField
                  v-if="
                    !(
                      containsVal(result?.main_input?.columns ?? [], fuzzyMap.left_col) &&
                      containsVal(result?.right_input?.columns ?? [], fuzzyMap.right_col)
                    )
                  "
                  tooltip-text="Join is not valid"
                  class="unavailable-field"
                />
                Setting {{ index + 1 }}
              </div>
              <div class="selectors-row">
                <el-row style="overflow: visible">
                  <div class="grid-content">
                    Left column
                    <column-selector
                      v-model="fuzzyMap.left_col"
                      :value="fuzzyMap.left_col"
                      :column-options="result?.main_input?.columns"
                      @update:value="(value: string) => handleChange(value, index, 'left')"
                    />
                  </div>
                  <div class="grid-content">
                    Right column
                    <column-selector
                      v-model="fuzzyMap.right_col"
                      :value="fuzzyMap.right_col"
                      :column-options="result?.right_input?.columns"
                      @update:value="(value: string) => handleChange(value, index, 'right')"
                    />
                  </div>
                </el-row>
              </div>

              <el-row>
                <el-col :span="10" class="grid-content">Threshold score</el-col>
                <el-col :span="8" class="grid-content"
                  ><input
                    v-model="fuzzyMap.threshold_score"
                    type="number"
                    min="0"
                    max="100"
                    step="1"
                /></el-col>
              </el-row>
              <el-row>
                <el-col :span="10" class="grid-content">Type of matching</el-col>
                <el-col :span="8" class="grid-content"
                  ><select v-model="fuzzyMap.fuzzy_type">
                    <option
                      v-for="option in fuzzyMatchOptions"
                      :key="option.value"
                      :value="option.value"
                    ></option>
                  </select>
                </el-col>
              </el-row>

              <div class="action-buttons">
                <button
                  v-if="nodeFuzzyJoin?.join_input.join_mapping.length > 1"
                  class="remove-setting"
                  @click="removeJoinCondition(index)"
                >
                  Remove setting
                </button>
              </div>
            </div>
          </div>
          <div class="action-buttons">
            <button class="add-setting" @click="addJoinCondition()">Add setting</button>
          </div>
        </div>
      </div>
    </div>
    <div class="listbox-subtitle">Select fields</div>

    <select-dynamic
      v-if="nodeFuzzyJoin?.join_input"
      :select-inputs="nodeFuzzyJoin?.join_input.right_select.renames"
      :show-keep-option="true"
      :show-headers="true"
      :show-new-columns="false"
      :show-title="true"
      :show-data="true"
      title="Right data"
      @update-select-inputs="(updatedInputs) => updateSelectInputsHandler(updatedInputs, false)"
    />

    <select-dynamic
      v-if="nodeFuzzyJoin?.join_input"
      :select-inputs="nodeFuzzyJoin?.join_input.left_select.renames"
      :show-keep-option="true"
      :show-title="true"
      :show-headers="true"
      :show-new-columns="false"
      :show-data="true"
      title="Left data"
      @update-select-inputs="(updatedInputs) => updateSelectInputsHandler(updatedInputs, true)"
    />
  </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>
<script lang="ts" setup>
import { ref, onMounted, nextTick, computed } from "vue";
import { useNodeStore } from "../../../../../stores/column-store";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { NodeJoin, FuzzyJoinSettings, SelectInput, FuzzyMap } from "../../../baseNode/nodeInput";
import ColumnSelector from "../../../baseNode/page_objects/dropDown.vue";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import unavailableField from "../../../baseNode/selectComponents/UnavailableFields.vue";
import GenericNodeSettings from '../../../baseNode/genericNodeSettings.vue'
import { CodeLoader } from "vue-content-loader";

const containsVal = (arr: string[], val: string) => {
  return arr.includes(val);
};

const result = ref<NodeData | null>(null);
const nodeStore = useNodeStore();
const isLoaded = ref(false);
const nodeFuzzyJoin = ref<NodeJoin | null>(null);

const createSelectInput = (field: string): SelectInput => {
  return {
    old_name: field,
    new_name: field,
    position: 0,
    keep: true,
    is_altered: false,
    data_type_change: false,
    is_available: true,
  };
};
const updateSelectInputsHandler = (updatedInputs: SelectInput[], isLeft: boolean) => {
  if (isLeft && nodeFuzzyJoin.value) {
    nodeFuzzyJoin.value.join_input.left_select.renames = updatedInputs;
  } else if (nodeFuzzyJoin.value) {
    nodeFuzzyJoin.value.join_input.right_select.renames = updatedInputs;
  }
};

const fuzzyMatchOptions = [
  { value: "levenshtein", label: "Levenshtein" },
  { value: "jaro_winkler", label: "Jaro Winkler" },
  { value: "damerau_levenshtein", label: "Damerau Levenshtein" },
];

const hasInvalidFields = computed(() => {
  if (!nodeFuzzyJoin.value?.join_input || !result.value) {
    return false;
  }
  return nodeFuzzyJoin.value.join_input.join_mapping.some((fuzzyMap) => {
    const leftValid = containsVal(result.value?.main_input?.columns ?? [], fuzzyMap.left_col);
    const rightValid = containsVal(result.value?.right_input?.columns ?? [], fuzzyMap.right_col);
    return !(leftValid && rightValid);
  });
});

const getEmptySetup = (left_fields: string[], right_fields: string[]): FuzzyJoinSettings => {
  return {
    join_mapping: [
      {
        left_col: "",
        right_col: "",
        threshold_score: 75,
        fuzzy_type: "levenshtein",
        valid: true,
      },
    ],
    left_select: {
      renames: left_fields.map(createSelectInput),
    },
    right_select: {
      renames: right_fields.map(createSelectInput),
    },
    aggregate_output: false,
  };
};

const loadNodeData = async (nodeId: number) => {
  console.log("doing this for fuzzy mathcing");
  result.value = await nodeStore.getNodeData(1, nodeId, true);
  nodeFuzzyJoin.value = result.value?.setting_input;
  if (!nodeFuzzyJoin.value?.is_setup && result.value?.main_input) {
    console.log("doing the initial set up again ");
    if (nodeFuzzyJoin.value) {
      if (result.value?.main_input.columns && result.value?.right_input?.columns) {
        nodeFuzzyJoin.value.join_input = getEmptySetup(
          result.value.main_input.columns,
          result.value.right_input.columns,
        );
        isLoaded.value = true;
      }
    }
  } else {
    isLoaded.value = true;
    console.log("Data reloaded");
  }
  isLoaded.value = true;
};

const addJoinCondition = () => {
  const newCondition: FuzzyMap = {
    left_col: "",
    right_col: "",
    threshold_score: 75,
    fuzzy_type: "levenshtein",
    valid: true,
  };
  nodeFuzzyJoin.value?.join_input?.join_mapping.push(newCondition);
};
const removeJoinCondition = (index: number) => {
  nodeFuzzyJoin.value?.join_input?.join_mapping.splice(index, 1);
};

const handleChange = (newValue: string, index: number, side: string) => {
  if (side === "left") {
    if (nodeFuzzyJoin.value?.join_input) {
      nodeFuzzyJoin.value.join_input.join_mapping[index].left_col = newValue;
    }
  } else {
    if (nodeFuzzyJoin.value?.join_input) {
      nodeFuzzyJoin.value.join_input.join_mapping[index].right_col = newValue;
    }
  }
};

const pushNodeData = async () => {
  //await insertSelect(nodeSelect.value)
  isLoaded.value = false;
  nodeStore.isDrawerOpen = false;
  if (nodeFuzzyJoin.value) {
    nodeFuzzyJoin.value.is_setup = true;
  }
  // console.log(nodeFuzzyJoin.value)
  // console.log('Normally this would push the data')
  nodeStore.updateSettings(nodeFuzzyJoin);
  if (hasInvalidFields.value && nodeFuzzyJoin.value) {
    nodeStore.setNodeValidation(nodeFuzzyJoin.value.node_id, {
      isValid: false,
      error: "Join fields are not valid",
    });
  } else if (nodeFuzzyJoin.value) {
    nodeStore.setNodeValidation(nodeFuzzyJoin.value.node_id, {
      isValid: true,
      error: "",
    });
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  hasInvalidFields,
});

onMounted(async () => {
  await nextTick();
});
</script>

<style scoped>
/* Flex container for rows of items */
.selectors-row,
.fuzzy-settings-row,
.action-buttons {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
}

/* Individual wrappers for aligning form elements with their labels */
.column-selector-wrapper,
.threshold-score,
.fuzzy-type {
  display: flex;
  align-items: center;
}

/* Input and select box styling */
input[type="number"],
select {
  padding: 2px;
  border: 1px solid #ccc;
  border-radius: 5px;
  margin-left: 10px; /* Space between label and input/select */
  margin-top: 5px;
  margin-bottom: 5px;
}

/* Button styling */
button {
  padding: 5px 10px;
  border: 1px solid #ccc;
  border-radius: 5px;
  cursor: pointer;
  background-color: #f5f5f5;
  text-align: center;
  white-space: nowrap; /* Prevents wrapping of text in buttons */
}

/* Hover effect for buttons */
button:hover {
  background-color: #e6e6e6;
}
.grid-content {
  border-radius: 2px;
  min-height: 2px;
}
.unavailable-field {
  margin-right: 8px;
}
</style>
