<template>
  <div class="action-buttons">
    <button class="action-btn" @click="openSaveModal">
      <span class="material-icons btn-icon">save</span>
      <span class="btn-text">Save</span>
    </button>
    <button class="action-btn" @click="modalVisibleForOpen = true">
      <span class="material-icons btn-icon">folder_open</span>
      <span class="btn-text">Open</span>
    </button>
    <button class="action-btn" @click="modalVisibleForCreate = true">
      <span class="material-icons btn-icon">add_circle_outline</span>
      <span class="btn-text">Create</span>
    </button>
    <button class="action-btn" @click="modalVisibleForQuickCreate = true">
      <span class="material-icons btn-icon">flash_on</span>
      <span class="btn-text">Quick Create</span>
    </button>
    <button class="action-btn" @click="openSettingsModal">
      <span class="material-icons btn-icon">settings</span>
      <span class="btn-text">Settings</span>
    </button>
    <run-button ref="runButton" :flow-id="nodeStore.flow_id" />
    <button
      class="action-btn"
      :class="{ active: nodeStore.showCodeGenerator }"
      title="Generate Python Code (Ctrl+G)"
      @click="toggleCodeGenerator"
    >
      <span class="material-icons btn-icon">code</span>
      <span class="btn-text">Generate code</span>
    </button>
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

  <el-dialog v-model="modalVisibleForCreate" title="Select save location" width="70%">
    <file-browser
      :allowed-file-types="['flowfile']"
      mode="create"
      @create-file="handleCreateAction"
      @overwrite-file="handleCreateAction"
    />
  </el-dialog>

  <el-dialog v-model="modalVisibleForQuickCreate" title="Create New Flow" width="400px">
    <div class="quick-create-modal">
      <div class="form-group">
        <label for="flow-name">Flow Name (optional):</label>
        <el-input
          id="flow-name"
          v-model="quickCreateName"
          placeholder="Leave empty for auto-generated name"
          clearable
        />
      </div>
      <div class="preview-text">
        <strong>File will be created as:</strong><br />
        <code>{{ getPreviewFileName() }}</code>
      </div>
    </div>
    <template #footer>
      <span class="dialog-footer">
        <el-button @click="modalVisibleForQuickCreate = false">Cancel</el-button>
        <el-button type="primary" @click="handleQuickCreateAction">Create Flow</el-button>
      </span>
    </template>
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
          <label>Execution location:</label>
          <el-select
            v-model="flowSettings.execution_location"
            size="small"
            placeholder="Select the execution location"
            style="width: 100%"
            @change="pushFlowSettings"
          >
            <el-option
              v-for="executionLocationOption in executionLocationOptions"
              :key="executionLocationOption.key"
              :label="executionLocationOption.label"
              :value="executionLocationOption.key"
            />
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
  ExecutionLocation,
  updateRunStatus,
} from "../../nodes/nodeLogic";

const nodeStore = useNodeStore();

const modalVisibleForOpen = ref(false);
const modalVisibleForSave = ref(false);
const modalVisibleForCreate = ref(false);
const modalVisibleForQuickCreate = ref(false);
const modalVisibleForSettings = ref(false);

const flowSettings = ref<FlowSettings | null>(null);
const savePath = ref<string | undefined>(undefined);
const runButton = ref<InstanceType<typeof RunButton> | null>(null);
const quickCreateName = ref<string>("");

const executionModes = ref<ExecutionMode[]>(["Development", "Performance"]);

interface ExecutionLocationOption {
  key: ExecutionLocation;
  label: string;
}

const executionLocationOptions = ref<ExecutionLocationOption[]>([
  { key: "local", label: "Local" },
  { key: "remote", label: "Remote" },
]);

const emit = defineEmits(["openFlow", "refreshFlow", "logs-start", "logs-stop"]);

// Generate default filename with current datetime
const generateDefaultFileName = (): string => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const hours = String(now.getHours()).padStart(2, "0");
  const minutes = String(now.getMinutes()).padStart(2, "0");
  const seconds = String(now.getSeconds()).padStart(2, "0");

  return `${year}${month}${day}_${hours}${minutes}${seconds}_flow.flowfile`;
};

// Get preview filename for the modal
const getPreviewFileName = (): string => {
  if (quickCreateName.value.trim()) {
    const name = quickCreateName.value.trim();
    return name.endsWith(".flowfile") ? name : `${name}.flowfile`;
  }
  return generateDefaultFileName();
};

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

const runFlow = () => {
  if (runButton.value) {
    runButton.value?.runFlow();
  }
};

const toggleCodeGenerator = () => {
  nodeStore.toggleCodeGenerator();
};

const handleCreateAction = async (flowPath: string, _1: string, _2: string) => {
  const pathWithoutExtension = flowPath.replace(/\.[^/.]+$/, "");
  const normalizedPath = `${pathWithoutExtension}.flowfile`;

  const createdFlowId = await createFlow(normalizedPath);

  modalVisibleForCreate.value = false;
  nodeStore.flow_id = createdFlowId;

  emit("refreshFlow");
};

const handleQuickCreateAction = async () => {
  const fileName = getPreviewFileName();
  // Use temp storage path - you'll need to replace 'temp://' with your actual temp storage path

  try {
    const createdFlowId = await createFlow(null, fileName);
    modalVisibleForQuickCreate.value = false;
    quickCreateName.value = ""; // Reset the input
    nodeStore.flow_id = createdFlowId;

    emit("refreshFlow");
  } catch (error) {
    console.error("Failed to create quick flow:", error);
    // You might want to show an error message to the user here
  }
};

const openSettingsModal = () => {
  modalVisibleForSettings.value = true;
};

watch(
  () => nodeStore.flow_id,
  async (newId, oldId) => {
    if (newId !== oldId && newId > 0) {
      await loadFlowSettings();
    }
  },
);

defineExpose({
  loadFlowSettings,
  openCreateDialog: () => (modalVisibleForCreate.value = true),
  handleQuickCreateAction,
  openOpenDialog: () => (modalVisibleForOpen.value = true),
  openSaveModal: () => (modalVisibleForSave.value = true),
  runFlow,
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
  gap: 12px;
  height: 50px;
  font-family:
    "Inter",
    "Roboto",
    -apple-system,
    BlinkMacSystemFont,
    sans-serif;
}

.action-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 12px;
  height: 36px;
  background-color: rgba(3, 11, 27, 0.05); /* Slightly darker background */
  border: 1px solid rgba(16, 24, 40, 0.12); /* Darker border */
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s ease;
  color: rgb(2, 27, 45); /* Darker text color */
  font-size: 13px;
  font-weight: 500;
  letter-spacing: 0.01em;
  box-shadow: 0 1px 2px rgba(1, 5, 13, 0.08); /* Slightly darker shadow */
}

.action-btn:hover {
  background-color: rgba(16, 24, 40, 0.08); /* Darker hover background */
  border-color: rgba(16, 24, 40, 0.18); /* Darker hover border */
}

.action-btn:active {
  transform: translateY(1px);
  box-shadow: none;
}

.btn-icon {
  font-size: 16px;
  color: rgb(2, 27, 45); /* Darker icon color */
}

.btn-text {
  white-space: nowrap;
}

.settings-modal-content {
  padding: 16px;
  font-family:
    "Inter",
    "Roboto",
    -apple-system,
    BlinkMacSystemFont,
    sans-serif;
}

.form-group {
  margin-bottom: 16px;
}

.form-group label {
  display: block;
  margin-bottom: 8px;
  font-size: 14px;
  font-weight: 500;
  color: rgba(16, 24, 40, 0.9);
}

.quick-create-modal {
  padding: 16px 0;
}

.quick-create-modal .form-group {
  margin-bottom: 20px;
}

.quick-create-modal label {
  display: block;
  margin-bottom: 8px;
  font-size: 14px;
  font-weight: 500;
  color: rgba(16, 24, 40, 0.9);
}

.preview-text {
  padding: 12px;
  background-color: rgba(3, 11, 27, 0.03);
  border: 1px solid rgba(16, 24, 40, 0.08);
  border-radius: 6px;
  font-size: 13px;
  color: rgba(16, 24, 40, 0.7);
}

.preview-text code {
  background-color: rgba(16, 24, 40, 0.08);
  padding: 2px 6px;
  border-radius: 3px;
  font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace;
  font-size: 12px;
}

.dialog-footer {
  display: flex;
  gap: 8px;
}
</style>
