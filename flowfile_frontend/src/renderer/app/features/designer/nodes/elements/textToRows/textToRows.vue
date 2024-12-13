<template>
  <div v-if="isLoaded" class="listbox">
    <div class="table">
      <div v-if="nodeTextToRows?.text_to_rows_input" class="selectors">
        <div
          v-if="
            !containsVal(
              result?.main_input?.columns ?? [],
              nodeTextToRows.text_to_rows_input.column_to_split,
            )
          "
          class="error-msg"
        >
          <unavailableField tooltip-text="Setup is not valid" class="error-icon" />
          Check the column you want to split
        </div>

        <!-- Column to Split Input -->
        <div class="row">
          <el-row>
            <div class="input-wrapper">
              <label>Column to split</label>
              <column-selector
                v-model="nodeTextToRows.text_to_rows_input.column_to_split"
                :column-options="result?.main_input?.columns"
                @update:value="(value: string) => handleChange(value, 'columnToSplit')"
              />
            </div>
          </el-row>
        </div>

        <!-- Radio Buttons to Switch Between Split Options -->
        <div class="row">
          <el-radio-group
            v-model="nodeTextToRows.text_to_rows_input.split_by_fixed_value"
            size="large"
          >
            <el-radio :label="true">Split by a fixed value</el-radio>
            <el-radio :label="false">Split by a column</el-radio>
          </el-radio-group>
        </div>
        <!-- Split by Fixed Value -->
        <div v-if="nodeTextToRows.text_to_rows_input.split_by_fixed_value" class="row">
          <el-col :span="10">
            <label>Split by value</label>
          </el-col>
          <el-col :span="8">
            <input
              v-model="nodeTextToRows.text_to_rows_input.split_fixed_value"
              type="text"
              placeholder="Enter split value"
            />
          </el-col>
        </div>

        <!-- Split by Column -->
        <div v-if="!nodeTextToRows.text_to_rows_input.split_by_fixed_value" class="row">
          <el-col :span="10">
            <label>Column that contains the value to split</label>
          </el-col>
          <el-col :span="8">
            <column-selector
              v-model="nodeTextToRows.text_to_rows_input.split_by_column"
              :column-options="result?.main_input?.columns"
              :allow-other="false"
              @update:value="(value: string) => handleChange(value, 'splitValueColumn')"
            />
          </el-col>
        </div>
        <!-- Output Column Name Input -->
        <div class="row">
          <el-col :span="10"><label>Output column name</label></el-col>
          <el-col :span="8">
            <column-selector
              v-model="nodeTextToRows.text_to_rows_input.output_column_name"
              :column-options="result?.main_input?.columns"
              :allow-other="true"
              placeholder="Enter output column name"
            />
          </el-col>
        </div>
      </div>
    </div>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, onMounted, nextTick, computed } from "vue";
import { useNodeStore } from "../../../../../stores/column-store";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { NodeTextToRows, TextToRowsInput } from "../../../baseNode/nodeInput";
import ColumnSelector from "../../../baseNode/page_objects/dropDown.vue";
import unavailableField from "../../../baseNode/selectComponents/UnavailableFields.vue";

import { CodeLoader } from "vue-content-loader";

const containsVal = (arr: string[], val: string) => {
  return arr.includes(val);
};

const result = ref<NodeData | null>(null);
const nodeStore = useNodeStore();
const isLoaded = ref(false);
const nodeTextToRows = ref<NodeTextToRows | null>(null);

const hasInvalidFields = computed(() => {
  return false;
});

const getEmptySetup = (): TextToRowsInput => {
  return {
    column_to_split: "",
    output_column_name: "",
    split_by_fixed_value: true,
    split_fixed_value: ",",
    split_by_column: "",
  };
};

const loadNodeData = async (nodeId: number) => {
  console.log("doing this for fuzzy mathcing");
  result.value = await nodeStore.getNodeData(1, nodeId, true);
  nodeTextToRows.value = result.value?.setting_input as NodeTextToRows;
  if (!nodeTextToRows.value?.is_setup && result.value?.main_input) {
    nodeTextToRows.value.text_to_rows_input = getEmptySetup();
  } else {
    isLoaded.value = true;
  }
  isLoaded.value = true;
};

const handleChange = (newValue: string, type: "columnToSplit" | "splitValueColumn") => {
  if (nodeTextToRows.value?.text_to_rows_input)
    if (type === "columnToSplit") {
      nodeTextToRows.value.text_to_rows_input.column_to_split = newValue;
    } else {
      nodeTextToRows.value.text_to_rows_input.split_by_column = newValue;
    }
};

const pushNodeData = async () => {
  isLoaded.value = false;
  nodeStore.isDrawerOpen = false;
  if (nodeTextToRows.value) {
    nodeTextToRows.value.is_setup = true;
  }
  nodeStore.updateSettings(nodeTextToRows);
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
.selectors {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.input-wrapper {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

label {
  font-weight: 500;
  color: #333;
  font-size: 14px;
}

input {
  padding: 3px;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 14px;
  width: 100%;
}

.row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.error-msg {
  font-size: 13px;
  color: #ff4d4f;
  display: flex;
  align-items: center;
  gap: 6px;
}

.error-icon {
  margin-right: 6px;
}

.table {
  padding: 12px;
  border-radius: 6px;
  background: #f8f8f8;
  box-shadow: 0 1px 5px rgba(0, 0, 0, 0.05);
}

el-checkbox {
  margin-top: 10px;
}
</style>
