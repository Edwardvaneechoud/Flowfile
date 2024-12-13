<template>
  <div v-if="dataLoaded" class="listbox-wrapper">
    <div class="listbox-subtitle">Select the type of external source</div>
    <el-select
      v-model="selectedExternalSource"
      class="m-2"
      placeholder="Select type of external source"
      size="small"
      @change="loadTemplateValue"
    >
      <el-option v-for="item in writingOptions" :key="item" :label="item" :value="item" />
    </el-select>
  </div>
  <div class="listbox-wrapper">
    <div v-if="typeSelected" class="file-upload-container">
      <div
        v-if="selectedExternalSource === 'sample_users' && sampleUsers"
        class="file-upload-wrapper"
      ></div>
    </div>
    <CodeLoader v-else />
  </div>
</template>

<script lang="ts" setup>
import { ref, watch, watchEffect } from "vue";
import { CodeLoader } from "vue-content-loader";
import { get_template_source_type } from "./createTemplateExternalSource";
import { SampleUsers, NodeExternalSource } from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/column-store";
import { WatchStopHandle } from "vue";
const nodeStore = useNodeStore();
const sampleUsers = ref<SampleUsers | null>(null);
const nodeExternalSource = ref<null | NodeExternalSource>(null);
const dataLoaded = ref(false);
const typeSelected = ref(false);
const writingOptions = ["sample_users"];
const selectedExternalSource = ref<string | null>(null);
const isDirty = ref(false);

let activeWatchStopHandle: WatchStopHandle | null = null;
watchEffect(() => {
  // Clean up the previous watch if the condition changes
  if (activeWatchStopHandle) {
    activeWatchStopHandle();
    activeWatchStopHandle = null;
  }
});

const loadNodeData = async (nodeId: number) => {
  const nodeResult = await nodeStore.getNodeData(1, nodeId, false);
  nodeExternalSource.value = nodeResult?.setting_input;
  if (nodeExternalSource.value?.is_setup)
    if (nodeExternalSource.value?.identifier == "sample_users") {
      sampleUsers.value = nodeExternalSource.value?.source_settings as SampleUsers;
      selectedExternalSource.value = "sample_users";
    }
  typeSelected.value = true;
  dataLoaded.value = true;
  isDirty.value = false;
};

const loadTemplateValue = () => {
  console.log(selectedExternalSource.value);
  if (selectedExternalSource.value === "sample_users") {
    sampleUsers.value = get_template_source_type("SAMPLE_USERS") as SampleUsers;
    if (nodeExternalSource.value) {
      nodeExternalSource.value.source_settings = sampleUsers.value;
    }
    isDirty.value = true;
  }
  typeSelected.value = true;
  if (nodeExternalSource.value && selectedExternalSource.value) {
    nodeExternalSource.value.identifier = selectedExternalSource.value;
  }
};

const pushNodeDataAction = async () => {
  console.log("pushing node data");
  if (nodeExternalSource.value && isDirty.value) {
    console.log(nodeExternalSource.value);
    nodeExternalSource.value.is_setup = true;
    nodeExternalSource.value.source_settings.fields = [];
    isDirty.value = false;
  }
  console.log(nodeExternalSource.value);
  await nodeStore.updateSettings(nodeExternalSource);
  console.log("pushed");
  if (nodeExternalSource.value) {
    console.log("pushing");
    await nodeStore.getNodeData(1, Number(nodeExternalSource.value.node_id), false);
  }
  console.log("pushed");
};

const pushNodeData = async () => {
  // Your existing code to handle the operation
  // await insertSelect(nodeSelect.value)
  dataLoaded.value = false;
  if (nodeExternalSource.value)
    if (isDirty.value || nodeExternalSource.value.identifier) {
      await pushNodeDataAction();
    }
};

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: white;
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
  max-height: 300px; /* Adjust this value as needed */
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1); /* subtle shadow for depth */
  border-radius: 8px; /* rounded corners */
  overflow: auto; /* ensures the rounded corners are applied to the child elements */
  margin: 5px; /* adds a small margin around the table */
}

.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: white;
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

.file-upload-wrapper {
  position: relative;
  width: 100%;
}

.file-upload-input {
  width: 100%;
  height: 40px;
  opacity: 0;
  position: absolute;
  top: 0;
  left: 0;
  z-index: 1;
  cursor: pointer;
}

.file-upload-label {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  background-color: #f5f5f5;
  border: 1px solid #ddd;
  border-radius: 4px;
  padding: 10px 15px;
  color: #333;
  font-size: 16px;
  font-weight: 500;
  text-align: left;
  user-select: none;
  cursor: pointer;
  transition: background-color 0.3s ease;
}

.file-upload-label:hover {
  background-color: #e4e4e4;
}

.file-icon {
  margin-right: 10px;
  font-size: 20px; /* Slightly larger icon for better visual balance */
}

.file-label-text {
  flex-grow: 1; /* Ensures text takes the available space */
  margin-left: 10px; /* Spacing between icon and text */
}

/* Additional styling to align with Vuestic's modern and minimalistic design */
</style>
