<template>
  <div class="action-buttons">
    <button class="action-btn" data-tutorial="save-btn" @click="openSaveModal">
      <span class="material-icons btn-icon">save</span>
      <span class="btn-text">Save</span>
    </button>
    <button class="action-btn" data-tutorial="open-btn" @click="modalVisibleForOpen = true">
      <span class="material-icons btn-icon">folder_open</span>
      <span class="btn-text">Open</span>
    </button>
    <button class="action-btn" data-tutorial="create-btn" @click="modalVisibleForCreate = true">
      <span class="material-icons btn-icon">add_circle_outline</span>
      <span class="btn-text">Create</span>
    </button>
    <button class="action-btn" data-tutorial="quick-create-btn" @click="handleQuickCreateClick">
      <span class="material-icons btn-icon">flash_on</span>
      <span class="btn-text">Quick Create</span>
    </button>
    <button class="action-btn" data-tutorial="settings-btn" @click="openSettingsModal">
      <span class="material-icons btn-icon">settings</span>
      <span class="btn-text">Settings</span>
    </button>
    <run-button ref="runButton" :flow-id="nodeStore.flow_id" data-tutorial="run-btn" />
    <el-tooltip content="Generate code (Ctrl+G)" placement="bottom" :show-after="400">
      <button
        class="action-btn action-btn--icon-only"
        :class="{ active: nodeStore.showCodeGenerator }"
        data-tutorial="generate-code-btn"
        @click="toggleCodeGenerator"
      >
        <span class="material-icons btn-icon">code</span>
      </button>
    </el-tooltip>
    <el-tooltip content="Flow Parameters" placement="bottom" :show-after="400">
      <button
        class="action-btn action-btn--icon-only"
        :class="{ active: editorStore.showParametersPanel }"
        @click="editorStore.toggleParametersPanel()"
      >
        <span class="material-icons btn-icon">tune</span>
      </button>
    </el-tooltip>
  </div>

  <el-dialog
    v-model="modalVisibleForOpen"
    title="Select or Enter a Flow File"
    width="70%"
    custom-class="high-z-index-dialog"
  >
    <file-browser
      :allowed-file-types="FLOWFILE_EXTENSIONS"
      mode="open"
      context="flows"
      :is-visible="modalVisibleForOpen"
      @file-selected="openFlowAction"
    />
  </el-dialog>

  <el-dialog
    v-model="modalVisibleForSave"
    title="Select save location"
    width="70%"
    custom-class="high-z-index-dialog"
  >
    <file-browser
      ref="fileBrowserRef"
      :allowed-file-types="ALLOWED_SAVE_EXTENSIONS"
      mode="create"
      context="flows"
      :initial-file-path="savePath"
      :is-visible="modalVisibleForSave"
      @create-file="saveFlowAction"
      @overwrite-file="saveFlowAction"
    />
  </el-dialog>

  <el-dialog
    v-model="modalVisibleForCreate"
    title="Select save location"
    width="70%"
    custom-class="high-z-index-dialog"
  >
    <file-browser
      :allowed-file-types="ALLOWED_SAVE_EXTENSIONS"
      mode="create"
      context="flows"
      :is-visible="modalVisibleForCreate"
      @create-file="handleCreateAction"
      @overwrite-file="handleCreateAction"
    />
  </el-dialog>

  <el-dialog
    v-model="modalVisibleForQuickCreate"
    title="Create New Flow"
    width="400px"
    custom-class="high-z-index-dialog"
  >
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
        <el-button
          type="primary"
          data-tutorial="create-flow-confirm-btn"
          @click="handleQuickCreateAction"
          >Create Flow</el-button
        >
      </span>
    </template>
  </el-dialog>

  <el-dialog
    v-model="modalVisibleForSettings"
    title="Flow Settings"
    width="40%"
    custom-class="high-z-index-dialog"
  >
    <div v-if="flowSettings">
      <div class="settings-modal-content">
        <div class="settings-section">
          <h4 class="settings-section-title">Execution</h4>
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
          <div class="form-group">
            <label>Parallel workers:</label>
            <el-input-number
              v-model="flowSettings.max_parallel_workers"
              :min="1"
              :max="32"
              size="small"
              style="width: 100%"
              @change="pushFlowSettings"
            />
            <span class="form-hint">
              Max threads for running independent nodes in parallel. Only applies when execution
              location is Remote. Local execution always runs sequentially.
            </span>
          </div>
        </div>
        <div class="settings-section">
          <h4 class="settings-section-title">Display</h4>
          <div class="form-group">
            <el-checkbox
              v-model="flowSettings.show_edge_labels"
              label="Show edge labels"
              size="small"
              @change="pushFlowSettings"
            />
            <span class="form-hint"> Display input names on connections between nodes. </span>
          </div>
        </div>
        <div class="settings-section">
          <h4 class="settings-section-title">Parameters</h4>
          <span class="form-hint" style="display: block; margin-bottom: var(--spacing-3)">
            Define flow-level parameters and reference them in node settings using
            <code>${param_name}</code> syntax.
          </span>
          <div v-if="flowSettings.parameters && flowSettings.parameters.length > 0">
            <div v-for="(param, index) in flowSettings.parameters" :key="index" class="param-row">
              <el-input
                v-model="param.name"
                placeholder="Name"
                size="small"
                class="param-name-input"
                @change="pushFlowSettings"
              />
              <el-input
                v-model="param.default_value"
                placeholder="Default value"
                size="small"
                class="param-value-input"
                @change="pushFlowSettings"
              />
              <el-input
                v-model="param.description"
                placeholder="Description (optional)"
                size="small"
                class="param-desc-input"
                @change="pushFlowSettings"
              />
              <el-button
                type="danger"
                size="small"
                :icon="'Delete'"
                circle
                @click="removeParameter(index)"
              />
            </div>
          </div>
          <div v-else class="param-empty">No parameters defined.</div>
          <el-button size="small" style="margin-top: var(--spacing-3)" @click="addParameter">
            + Add Parameter
          </el-button>
        </div>
      </div>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from "vue";
import { ElMessage } from "element-plus";

import { saveFlow } from "./utils";
import RunButton from "./run.vue";
import FileBrowser from "../../common/FileBrowser/fileBrowser.vue";
import { FileInfo } from "../../common/FileBrowser/types";
import { FLOWFILE_EXTENSIONS, ALLOWED_SAVE_EXTENSIONS } from "../../common/FileBrowser/constants";
import { useNodeStore } from "../../../stores/column-store";
import { useEditorStore } from "../../../stores/editor-store";
import { useTutorialStore } from "../../../stores/tutorial-store";
import {
  createFlow,
  getFlowSettings,
  FlowSettings,
  updateFlowSettings,
  ExecutionMode,
  ExecutionLocation,
  updateRunStatus,
} from "../../nodes/nodeLogic";
import type { FlowParameter } from "../../../types/flow.types";

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const tutorialStore = useTutorialStore();

const modalVisibleForOpen = ref(false);
const modalVisibleForSave = ref(false);
const modalVisibleForCreate = ref(false);
const modalVisibleForQuickCreate = ref(false);
const modalVisibleForSettings = ref(false);

const flowSettings = ref<FlowSettings | null>(null);
const savePath = ref<string | undefined>(undefined);
const runButton = ref<InstanceType<typeof RunButton> | null>(null);
const quickCreateName = ref<string>("");

// Handle Quick Create button click - opens modal and advances tutorial if active
function handleQuickCreateClick() {
  modalVisibleForQuickCreate.value = true;
  // Advance tutorial if we're on the "click-quick-create" step
  if (tutorialStore.isActive && tutorialStore.currentStep?.id === "click-quick-create") {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 200);
  }
}

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

const isValidSaveExtension = (filePath: string): boolean => {
  const name = filePath.toLowerCase();
  return ALLOWED_SAVE_EXTENSIONS.some((ext) => name.endsWith(`.${ext}`));
};

// Generate default filename with current datetime
const generateDefaultFileName = (): string => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const hours = String(now.getHours()).padStart(2, "0");
  const minutes = String(now.getMinutes()).padStart(2, "0");
  const seconds = String(now.getSeconds()).padStart(2, "0");

  return `${year}${month}${day}_${hours}${minutes}${seconds}_flow`;
};

// Get preview filename for the modal
const getPreviewFileName = (): string => {
  if (quickCreateName.value.trim()) {
    return quickCreateName.value.trim();
  }
  return generateDefaultFileName();
};

const loadFlowSettings = async () => {
  if (!(nodeStore.flow_id && nodeStore.flow_id > 0)) return;

  flowSettings.value = await getFlowSettings(nodeStore.flow_id);
  if (!flowSettings.value) return;

  flowSettings.value.execution_mode = flowSettings.value.execution_mode || "Development";
  flowSettings.value.show_edge_labels = flowSettings.value.show_edge_labels ?? false;
  flowSettings.value.parameters = flowSettings.value.parameters ?? [];
  editorStore.displayLogViewer = flowSettings.value.show_detailed_progress;
  editorStore.showEdgeLabels = flowSettings.value.show_edge_labels;

  if (!runButton.value) return;

  if (flowSettings.value.is_running) {
    editorStore.isRunning = true;
    runButton.value.startPolling(runButton.value.checkRunStatus);
  } else {
    editorStore.isRunning = false;
    runButton.value.stopPolling();
    updateRunStatus(nodeStore.flow_id, nodeStore);
  }
};

const pushFlowSettings = async () => {
  if (flowSettings.value) {
    await updateFlowSettings(flowSettings.value);
    editorStore.displayLogViewer = flowSettings.value.show_detailed_progress;
    editorStore.showEdgeLabels = flowSettings.value.show_edge_labels;
  }
};

const fileBrowserRef = ref<{
  refresh: () => Promise<void>;
  handleInitialFileSelection: (filePath?: string) => Promise<void>;
  loadCurrentDirectory: () => Promise<void>;
  navigateUpDirectory: () => Promise<void>;
  selectedFile: FileInfo | null;
} | null>(null);

const saveFlowAction = async (flowPath: string) => {
  // Check for deprecated .flowfile extension
  if (flowPath.toLowerCase().endsWith(".flowfile")) {
    ElMessage.error({
      message: "The .flowfile format is deprecated. Please use .yaml or .yml instead.",
      duration: 5000,
    });
    return;
  }

  // Validate extension
  if (!isValidSaveExtension(flowPath)) {
    ElMessage.error({
      message: "Invalid file extension. Please use .yaml or .yml",
      duration: 5000,
    });
    return;
  }

  try {
    await saveFlow(nodeStore.flow_id, flowPath);
    ElMessage.success("Flow saved successfully");
    modalVisibleForSave.value = false;
    // Advance tutorial if we're on the "save-flow" step
    if (tutorialStore.isActive && tutorialStore.currentStep?.id === "save-flow") {
      setTimeout(() => {
        tutorialStore.nextStep();
      }, 300);
    }
  } catch (error: any) {
    ElMessage.error({
      message: error.message || "Failed to save flow",
      duration: 5000,
    });
  }
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
  // Advance tutorial if we're on the "generate-code" step
  if (tutorialStore.isActive && tutorialStore.currentStep?.id === "generate-code") {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 300);
  }
};

const handleCreateAction = async (flowPath: string) => {
  if (!isValidSaveExtension(flowPath)) {
    ElMessage.error({
      message: "Invalid file extension. Please use .yaml or .yml",
      duration: 5000,
    });
    return;
  }

  const createdFlowId = await createFlow(flowPath);

  modalVisibleForCreate.value = false;
  nodeStore.setFlowId(createdFlowId);

  emit("refreshFlow");
};

const handleQuickCreateAction = async () => {
  const fileName = getPreviewFileName();
  // Use temp storage path - you'll need to replace 'temp://' with your actual temp storage path
  console.log("Creating flow with name:", fileName);
  try {
    const createdFlowId = await createFlow(null, fileName);
    modalVisibleForQuickCreate.value = false;
    quickCreateName.value = ""; // Reset the input
    nodeStore.setFlowId(createdFlowId);

    emit("refreshFlow");
  } catch (error) {
    console.error("Failed to create quick flow:", error);
    // You might want to show an error message to the user here
  }
};

const addParameter = () => {
  if (!flowSettings.value) return;
  if (!flowSettings.value.parameters) {
    flowSettings.value.parameters = [];
  }
  flowSettings.value.parameters.push({
    name: "",
    default_value: "",
    description: "",
  } as FlowParameter);
  pushFlowSettings();
};

const removeParameter = (index: number) => {
  if (!flowSettings.value?.parameters) return;
  flowSettings.value.parameters.splice(index, 1);
  pushFlowSettings();
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
  padding-left: var(--spacing-5);
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  height: 50px;
  font-family: var(--font-family-base);
}

.action-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-1-5);
  padding: var(--spacing-2) var(--spacing-3);
  height: 34px;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  transition: all var(--transition-fast);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  box-shadow: var(--shadow-xs);
}

.action-btn:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.action-btn:active {
  transform: translateY(1px);
  box-shadow: none;
}

.action-btn.active {
  background-color: var(--color-accent-subtle);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.btn-icon {
  font-size: 18px;
  color: var(--color-text-secondary);
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-rendering: optimizeLegibility;
}

.action-btn:hover .btn-icon {
  color: var(--color-text-primary);
}

.action-btn.active .btn-icon {
  color: var(--color-accent);
}

.btn-text {
  white-space: nowrap;
}

.action-btn--icon-only {
  padding: var(--spacing-2);
}

.settings-modal-content {
  padding: var(--spacing-4);
  font-family: var(--font-family-base);
}

.form-group {
  margin-bottom: var(--spacing-4);
}

.form-group label {
  display: block;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.settings-section {
  margin-bottom: var(--spacing-5);
  padding: var(--spacing-4);
  background-color: var(--color-background-muted, #f9f9fb);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg, 8px);
}

.settings-section:last-child {
  margin-bottom: 0;
}

.settings-section-title {
  margin: 0 0 var(--spacing-4) 0;
  padding-bottom: var(--spacing-2);
  border-bottom: 2px solid var(--color-border-secondary, #ddd);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold, 600);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary);
}

.form-hint {
  display: block;
  margin-top: var(--spacing-1);
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-muted, #999);
  line-height: 1.4;
}

.quick-create-modal {
  padding: var(--spacing-4) 0;
}

.quick-create-modal .form-group {
  margin-bottom: var(--spacing-5);
}

.quick-create-modal label {
  display: block;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.preview-text {
  padding: var(--spacing-3);
  background-color: var(--color-background-muted);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
}

.preview-text code {
  background-color: var(--color-background-tertiary);
  padding: 2px var(--spacing-1-5);
  border-radius: var(--border-radius-sm);
  font-family: var(--font-family-mono);
  font-size: var(--font-size-sm);
}

.dialog-footer {
  display: flex;
  gap: var(--spacing-2);
}

.param-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
}

.param-name-input {
  flex: 1;
  min-width: 80px;
}

.param-value-input {
  flex: 2;
  min-width: 100px;
}

.param-desc-input {
  flex: 3;
  min-width: 120px;
}

.param-empty {
  font-size: var(--font-size-sm);
  color: var(--color-text-muted, #999);
  font-style: italic;
}
</style>
