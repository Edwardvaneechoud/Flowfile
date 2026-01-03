<template>
  <div v-if="dataLoaded">
    <el-card class="p-3">
      <div class="clearfix">
        <span>Select columns</span>
      </div>
      <el-card>
        <el-checkbox v-model="keepMissing" label="Keep unseen fields" border />
        <div>
          <div class="row">
            <div class="flex flex-col md3">
              <th>Original column name</th>
            </div>
            <div class="flex flex-col md3">
              <th>New column name</th>
            </div>
            <div class="flex flex-col md3">
              <th>Data type</th>
            </div>
            <div class="flex flex-col md3">
              <th>Select</th>
            </div>
          </div>
          <el-divider />
          <div v-for="column in nodeSelect?.select_input" :key="column.old_name">
            <el-row :gutter="20">
              <el-col :span="6">
                <el-text class="mx-1" size="small">{{ column.old_name }}</el-text>
              </el-col>
              <el-col :span="6">
                <el-input v-model="column.new_name" size="small" />
              </el-col>
              <el-col :span="6">
                <el-select v-model="column.data_type" size="small">
                  <el-option
                    v-for="dataType in dataTypes"
                    :key="dataType"
                    :label="dataType"
                    :value="dataType"
                  />
                </el-select>
              </el-col>
              <el-col :span="6">
                <el-checkbox v-model="column.keep" />
              </el-col>
              <el-divider />
            </el-row>
          </div>
        </div>
      </el-card>
    </el-card>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted } from "vue";
import { createNodeSelect, updateNodeSelect } from "./nodeSelectLogic";
import { useNodeStore } from "../../../../stores/column-store";
const keepMissing = ref(false);
const nodeStore = useNodeStore();
const nodeSelect = createNodeSelect();
const dataLoaded = ref(false);
const dataTypes = nodeStore.getDataTypes();

const loadNodeData = async () => {
  const result = await nodeStore.getCurrentNodeData();

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
        nodeSelect.value.depending_on_id = main_input.node_id;
        nodeSelect.value.flow_id = nodeStore.flow_id;
        nodeSelect.value.node_id = nodeStore.node_id;
        nodeSelect.value.keep_missing = keepMissing.value;
        updateNodeSelect(main_input, nodeSelect);
      }
    }
  }
};

const pushNodeData = async () => {
  //await insertSelect(nodeSelect.value)
  const originalData = nodeStore.getCurrentNodeData();
  const newColumnSettings = nodeSelect.value.select_input;
  nodeSelect.value.keep_missing = keepMissing.value;
  if (originalData) {
    newColumnSettings.forEach((newColumnSetting, index) => {
      let original_object = originalData.main_input?.table_schema[index];
      newColumnSetting.is_altered = original_object?.data_type !== newColumnSetting.data_type;
      newColumnSetting.data_type_change = newColumnSetting.is_altered;
    });
  }
  //console.log(nodeSelect.value)
  await nodeStore.updateSettings(nodeSelect);
};

defineExpose({
  loadNodeData,
  pushNodeData,
});

onMounted(loadNodeData);
</script>
