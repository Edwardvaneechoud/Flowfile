<template>
  <div class="action-buttons">
    <el-button
      size="small"
      style="background-color: rgb(92, 92, 92); color: white"
      round
      @click="openSaveModal"
      >Save</el-button
    >
    <el-button
      size="small"
      style="background-color: rgb(92, 92, 92); color: white"
      round
      @click="modalVisibleForOpen = true"
      >Open</el-button
    >
    <el-button
      size="small"
      style="background-color: rgb(92, 92, 92); color: white"
      round
      @click="modalVisibleForCreate = true"
      >Create</el-button
    >
    <RunButton ref="runButton" :flow-id="1"></RunButton>
    <div v-if="flowSettings" class="dropdown-container">
      <span>Execution Mode:</span>
      <el-select
        v-model="flowSettings.execution_mode"
        size="small"
        style="width: 200px"
        placeholder="Select run mode"
        @change="pushFlowSettings"
      >
        <el-option
          v-for="eM in executionModes"
          :key="eM"
          :label="eM"
          :value="eM"
        />
      </el-select>
      <pop-over
        title="Execution mode"
        content="<strong>Development Mode</strong>: Let's you view the data in every step of the process, at the cost of performance.
<strong>Performance Mode</strong>: Only executes steps that are needed for the output (e.g.) writing data. Allowing for query optimizations and better performance."
        placement="bottom"
      >
        <Question class="help-icon">?</Question>
      </pop-over>
    </div>
  </div>

  <el-dialog
    v-model="modalVisibleForOpen"
    title="Select or Enter a Flow File"
    width="70%"
  >
    <file-browser
      :allowed-file-types="['flowfile']"
      mode="open"
      @file-selected="openFlowAction"
    >
    </file-browser>
  </el-dialog>
  <el-dialog
    v-model="modalVisibleForSave"
    title="Select save location"
    width="50%"
  >
    <file-browser
      ref="fileBrowserRef"
      :allowed-file-types="['flowfile']"
      mode="create"
      :initial-file-path="savePath"
      @create-file="saveFlowAction"
      @overwrite-file="saveFlowAction"
    >
    </file-browser>
  </el-dialog>

  <el-dialog
    v-model="modalVisibleForCreate"
    title="Select save location"
    width="50%"
  >
    <file-browser
      :allowed-file-types="['flowfile']"
      mode="create"
      @create-file="handleCreateAction"
      @overwrite-file="handleCreateAction"
    >
    </file-browser>
  </el-dialog>
</template>

<script setup lang="ts">
import { defineProps, ref, onMounted, defineExpose } from "vue";
import { saveFlow } from "./utils";
import RunButton from "../../editor/run.vue";
import FileBrowser from "../fileBrowser/fileBrowser.vue";
import { FileInfo } from "../fileBrowser/types";
import PopOver from "../../editor/PopOver.vue";
import {
  createFlow,
  getFlowSettings,
  FlowSettings,
  updateFlowSettings,
  ExecutionMode,
} from "../../nodes/nodeLogic";

const modalVisibleForOpen = ref(false);
const modalVisibleForSave = ref(false);
const modalVisibleForCreate = ref(false);
const flowSettings = ref<FlowSettings | null>(null);
const savePath = ref<string | undefined>(undefined);

const executionModes = ref<ExecutionMode[]>(["Development", "Performance"]);

const props = withDefaults(
  defineProps<{
    flowId?: number;
  }>(),
  {
    flowId: 1,
  },
);
const emit = defineEmits(["openFlow", "refreshFlow"]);

const loadFlowSettings = async () => {
  flowSettings.value = await getFlowSettings(props.flowId);
};

const pushFlowSettings = async (execution_lcoation: ExecutionMode) => {
  console.log("updateFlowSettings", flowSettings);
  if (flowSettings.value) {
    flowSettings.value.execution_mode = execution_lcoation;
  }

  if (flowSettings.value) {
    await updateFlowSettings(flowSettings.value);
  }
};

const fileBrowserRef = ref<{
  refresh: () => Promise<void>;
  handleInitialFileSelection: (filePath?: string) => Promise<void>;
  loadCurrentDirectory: () => Promise<void>;
  navigateUpDirectory: () => Promise<void>;
  selectedFile: FileInfo | null;
} | null>(null);

const saveFlowAction = async (flowPath: string, _1: string, _2: string) => {
  await saveFlow(1, flowPath);
  modalVisibleForSave.value = false;
};

function openFlowAction(inputSelectedFile: FileInfo | null) {
  console.log("openFlowAction", inputSelectedFile);
  if (inputSelectedFile) {
    emit("openFlow", {
      message: "Flow opened",
      flowPath: inputSelectedFile.path,
    });
  }
  modalVisibleForOpen.value = false;
}

const openSaveModal = async () => {
  let flowSettings = await getFlowSettings(props.flowId);
  if (flowSettings && flowSettings.path) {
    savePath.value = flowSettings.path;
  }
  modalVisibleForSave.value = true;
  await fileBrowserRef.value?.handleInitialFileSelection();
};

const handleCreateAction = async (flowPath: string, _1: string, _2: string) => {
  console.log("handleCreateAction", flowPath);
  // Remove any existing extension and add .flowfile
  const pathWithoutExtension = flowPath.replace(/\.[^/.]+$/, "");
  const normalizedPath = `${pathWithoutExtension}.flowfile`;

  let createdFlowId = await createFlow(normalizedPath);
  await saveFlow(createdFlowId, normalizedPath);
  emit("refreshFlow");
  modalVisibleForCreate.value = false;
};

defineExpose({
  loadFlowSettings,
  openCreateDialog: () => (modalVisibleForCreate.value = true),
  openOpenDialog: () => (modalVisibleForOpen.value = true),
});

onMounted(async () => {
  await loadFlowSettings();
});
</script>

<style scoped>
.action-buttons {
  padding-left: 20px;
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  height: 50px;
}

.dropdown-container {
  display: flex;
  align-items: center;
  gap: 5px;
}
.help-icon {
  width: 16px;
  height: 16px;
  color: #909399;
  margin: 0 4px;
}

.help-icon:hover {
  color: #606266;
}
</style>
