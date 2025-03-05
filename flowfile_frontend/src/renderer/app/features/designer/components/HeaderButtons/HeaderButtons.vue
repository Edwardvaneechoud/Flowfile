<template>
  <div class="action-buttons">
    <el-button
      size="small"
      style="background-color: rgb(92, 92, 92); color: white"
      round
      @click="openSaveModal"
    >
      Save
    </el-button>
    <el-button
      size="small"
      style="background-color: rgb(92, 92, 92); color: white"
      round
      @click="modalVisibleForOpen = true"
    >
      Open
    </el-button>
    <el-button
      size="small"
      style="background-color: rgb(92, 92, 92); color: white"
      round
      @click="modalVisibleForCreate = true"
    >
      Create
    </el-button>
    <run-button ref="runButton" :flow-id="nodeStore.flow_id" />
    <el-button
      size="small"
      style="background-color: rgb(92, 92, 92); color: white"
      round
      @click="openSettingsModal"
    >
      Settings
    </el-button>
  </div>

  <el-dialog v-model="modalVisibleForOpen" title="Select or Enter a Flow File" width="70%">
    <file-browser :allowed-file-types="['flowfile']" mode="open" @file-selected="openFlowAction" />
  </el-dialog>

  <el-dialog v-model="modalVisibleForSave" title="Select save location" width="70%">
    <file-browser
      ref="fileBrowserRef"
      :allowed-file-types="['flowfile']"
      mode="create"
      :initial-file-path="savePath"
      @create-file="saveFlowAction"
      @overwrite-file="saveFlowAction"
    />
  </el-dialog>

  <el-dialog v-model="modalVisibleForCreate" title="Select save location" width="50%">
    <file-browser
      :allowed-file-types="['flowfile']"
      mode="create"
      @create-file="handleCreateAction"
      @overwrite-file="handleCreateAction"
    />
  </el-dialog>

  <el-dialog v-model="modalVisibleForSettings" title="Execution Settings" width="30%">
    <div v-if="flowSettings">
      <div class="settings-modal-content">
        <div class="form-group">
          <label>Execution Mode:</label>
          <el-select
            v-model="flowSettings.execution_mode"
            size="small"
            placeholder="Select run mode"
            style="width: 100%"
            @change="pushFlowSettings"
          >
            <el-option v-for="eM in executionModes" :key="eM" :label="eM" :value="eM" />
          </el-select>
        </div>
        <div class="form-group">
          <el-checkbox
            v-model="flowSettings.show_detailed_progress"
            label="Show details during execution"
            size="small"
            @change="pushFlowSettings"
          />
        </div>
      </div>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, onMounted, defineExpose, computed, watch } from "vue";
import { saveFlow } from "./utils";
import RunButton from "./run.vue";
import FileBrowser from "../fileBrowser/fileBrowser.vue";
import { FileInfo } from "../fileBrowser/types";
import { useNodeStore } from "../../../../stores/column-store";
import {
  createFlow,
  getFlowSettings,
  FlowSettings,
  updateFlowSettings,
  ExecutionMode,
  updateRunStatus,
} from "../../nodes/nodeLogic";

const nodeStore = useNodeStore();

const modalVisibleForOpen = ref(false);
const modalVisibleForSave = ref(false);
const modalVisibleForCreate = ref(false);
const modalVisibleForSettings = ref(false);

const flowSettings = ref<FlowSettings | null>(null);
const savePath = ref<string | undefined>(undefined);
const runButton = ref<InstanceType<typeof RunButton> | null>(null);

const executionModes = ref<ExecutionMode[]>(["Development", "Performance"]);

const emit = defineEmits(["openFlow", "refreshFlow", "logs-start", "logs-stop"]);

const loadFlowSettings = async () => {
  if (!(nodeStore.flow_id && nodeStore.flow_id > 0)) return;

  flowSettings.value = await getFlowSettings(nodeStore.flow_id);
  if (!flowSettings.value) return;

  flowSettings.value.execution_mode = flowSettings.value.execution_mode || "Development";
  nodeStore.displayLogViewer = flowSettings.value.show_detailed_progress;

  if (!runButton.value) return;

  if (flowSettings.value.is_running) {
    nodeStore.isRunning = true;
    runButton.value.startPolling(runButton.value.checkRunStatus);
  } else {
    nodeStore.isRunning = false;
    runButton.value.stopPolling();
    updateRunStatus(nodeStore.flow_id, nodeStore, false);
  }
};

const pushFlowSettings = async () => {
  if (flowSettings.value) {
    await updateFlowSettings(flowSettings.value);
    nodeStore.displayLogViewer = flowSettings.value.show_detailed_progress;
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
  await saveFlow(nodeStore.flow_id, flowPath);
  modalVisibleForSave.value = false;
};

function openFlowAction(inputSelectedFile: FileInfo | null) {
  if (inputSelectedFile) {
    emit("openFlow", {
      message: "Flow opened",
      flowPath: inputSelectedFile.path,
    });
  }
  nodeStore.resetRunResults();
  modalVisibleForOpen.value = false;
}

const openSaveModal = async () => {
  const settings = await getFlowSettings(nodeStore.flow_id);
  if (!settings) return;

  savePath.value = settings.path;
  modalVisibleForSave.value = true;
  await fileBrowserRef.value?.handleInitialFileSelection();
};

const handleCreateAction = async (flowPath: string, _1: string, _2: string) => {
  const pathWithoutExtension = flowPath.replace(/\.[^/.]+$/, "");
  const normalizedPath = `${pathWithoutExtension}.flowfile`;

  const createdFlowId = await createFlow(normalizedPath);
  await saveFlow(createdFlowId, normalizedPath);

  modalVisibleForCreate.value = false;
  nodeStore.flow_id = createdFlowId;

  emit("refreshFlow");
};

const openSettingsModal = () => {
  modalVisibleForSettings.value = true;
};

// Watch for flow ID changes to reload settings
watch(() => nodeStore.flow_id, async (newId, oldId) => {
  if (newId !== oldId && newId > 0) {
    await loadFlowSettings();
  }
});

defineExpose({
  loadFlowSettings,
  openCreateDialog: () => (modalVisibleForCreate.value = true),
  openOpenDialog: () => (modalVisibleForOpen.value = true),
});

onMounted(async () => {
  if (nodeStore.flow_id && nodeStore.flow_id > 0) {
    await loadFlowSettings();
  }
});
</script>

<style scoped>
.action-buttons {
  padding-left: 20px;
  display: flex;
  align-items: center;
  gap: 10px;
  height: 50px;
}

.settings-modal-content {
  padding: 10px;
}

.form-group {
  margin-bottom: 10px;
}
</style>
