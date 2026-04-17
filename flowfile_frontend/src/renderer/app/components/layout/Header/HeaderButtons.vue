<template>
  <div class="action-buttons">
    <div v-if="hasOpenFlow" class="action-btn-split" data-tutorial="save-btn">
      <button class="action-btn action-btn--split-main" @click="openSaveModal">
        <span class="material-icons btn-icon">save</span>
        <span class="btn-text">Save</span>
      </button>
      <el-dropdown trigger="click" placement="bottom-end" :hide-on-click="true">
        <button class="action-btn action-btn--split-caret" aria-label="More save options">
          <span class="material-icons btn-icon">arrow_drop_down</span>
        </button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item data-tutorial="save-as-btn" @click="openSaveAsModal">
              <span class="material-icons save-as-icon">save_as</span>
              Save As…
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
    <button class="action-btn" data-tutorial="open-btn" @click="modalVisibleForOpen = true">
      <span class="material-icons btn-icon">folder_open</span>
      <span class="btn-text">Open</span>
    </button>
    <div class="action-btn-split" data-tutorial="quick-create-btn">
      <button class="action-btn action-btn--split-main" @click="handleQuickCreate">
        <span class="material-icons btn-icon">add_circle_outline</span>
        <span class="btn-text">Create</span>
      </button>
      <el-dropdown trigger="click" placement="bottom-end" :hide-on-click="true">
        <button class="action-btn action-btn--split-caret" aria-label="More create options">
          <span class="material-icons btn-icon">arrow_drop_down</span>
        </button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item data-tutorial="create-at-location-btn" @click="openCreateDialog">
              <span class="material-icons save-as-icon">folder_open</span>
              Create at specific location…
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
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

  <open-dialog
    ref="openDialogRef"
    v-model:visible="modalVisibleForOpen"
    @open-flow="handleOpenFromDialog"
  />

  <save-dialog
    ref="saveDialogRef"
    v-model:visible="modalVisibleForSave"
    :flow-id="nodeStore.flow_id"
    @save-complete="handleSaveDialogComplete"
  />

  <create-dialog
    ref="createDialogRef"
    v-model:visible="modalVisibleForCreate"
    @create-complete="handleCreateComplete"
  />

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
import { ref, onMounted, watch, computed } from "vue";
import { ElMessage } from "element-plus";

import { saveFlowSilent } from "./utils";
import RunButton from "./run.vue";
import SaveDialog from "../../../features/designer/components/SaveDialog.vue";
import OpenDialog from "../../../features/designer/components/OpenDialog.vue";
import CreateDialog from "../../../features/designer/components/CreateDialog.vue";
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
const modalVisibleForSettings = ref(false);

// Save is a no-op when no flow is loaded; hide the button entirely rather
// than leaving a disabled control in the header.
const hasOpenFlow = computed(() => !!nodeStore.flow_id && nodeStore.flow_id > 0);

const flowSettings = ref<FlowSettings | null>(null);
const runButton = ref<InstanceType<typeof RunButton> | null>(null);

// Main "Create" click: instant quick-create — flow lands in
// ~/.flowfile/flows/unnamed_flows/ via the backend's auto-naming, auto-
// registered in the default catalog namespace. Users who want to pick a
// location or namespace use the chevron → "Create at specific location…".
const handleQuickCreate = async () => {
  try {
    const createdFlowId = await createFlow(null, null);
    nodeStore.setFlowId(createdFlowId);
    emit("refreshFlow");
    ElMessage.success("Flow created");
    advanceQuickCreateTutorial();
  } catch (error) {
    console.error("Failed to create flow:", error);
    ElMessage.error("Failed to create flow");
  }
};

const openCreateDialog = () => {
  modalVisibleForCreate.value = true;
};

const advanceQuickCreateTutorial = () => {
  if (!tutorialStore.isActive) return;
  const stepId = tutorialStore.currentStep?.id;
  if (stepId === "click-quick-create" || stepId === "confirm-create-flow") {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 200);
  }
};

const handleCreateComplete = (flowId: number) => {
  modalVisibleForCreate.value = false;
  if (flowId) {
    nodeStore.setFlowId(flowId);
    emit("refreshFlow");
  }
  advanceQuickCreateTutorial();
};

const executionModes = ref<ExecutionMode[]>(["Development", "Performance"]);

interface ExecutionLocationOption {
  key: ExecutionLocation;
  label: string;
}

const executionLocationOptions = ref<ExecutionLocationOption[]>([
  { key: "local", label: "Local" },
  { key: "remote", label: "Remote" },
]);

const emit = defineEmits(["openFlow", "refreshFlow", "flowSaved", "logs-start", "logs-stop"]);

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
    editorStore.bumpGraphVersion();
  }
};

const saveDialogRef = ref<{
  open: () => Promise<void>;
  close: () => void;
} | null>(null);

const openDialogRef = ref<{
  open: () => void;
  close: () => void;
} | null>(null);

const handleSaveDialogComplete = (flowId: number) => {
  modalVisibleForSave.value = false;
  if (flowId && flowId !== nodeStore.flow_id) {
    // "Save As" produced a new flow identity — switch to it
    nodeStore.setFlowId(flowId);
    emit("refreshFlow");
  } else {
    emit("flowSaved", flowId);
  }
  if (tutorialStore.isActive && tutorialStore.currentStep?.id === "save-flow") {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 300);
  }
};

function handleOpenFromDialog(payload: { message: string; flowPath: string }) {
  emit("openFlow", payload);
  nodeStore.resetRunResults();
  modalVisibleForOpen.value = false;
}

const openSaveModal = async () => {
  const settings = await getFlowSettings(nodeStore.flow_id);
  if (!settings) return;

  // Silent-save whenever the flow has any existing path — including
  // quick-created flows living in ~/.flowfile/flows/unnamed_flows/, which
  // are persistent. Relocation is an explicit "Save As…" action.
  if (settings.path) {
    try {
      await saveFlowSilent(nodeStore.flow_id);
      emit("flowSaved", nodeStore.flow_id);
      ElMessage.success("Flow saved successfully");
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
    return;
  }

  // No path at all: open the save dialog for first-time placement.
  if (saveDialogRef.value) {
    await saveDialogRef.value.open();
  } else {
    modalVisibleForSave.value = true;
  }
};

const openSaveAsModal = async () => {
  const settings = await getFlowSettings(nodeStore.flow_id);
  if (!settings) return;
  if (saveDialogRef.value) {
    await saveDialogRef.value.open();
  } else {
    modalVisibleForSave.value = true;
  }
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
  openCreateDialog,
  handleQuickCreate,
  openOpenDialog: () => (modalVisibleForOpen.value = true),
  openSaveModal,
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

/* Split button: primary action (Save) + caret dropdown (Save As…)
   Visually presents as one unified control with a divider between halves. */
.action-btn-split {
  display: inline-flex;
  align-items: stretch;
  height: 34px;
  box-shadow: var(--shadow-xs);
  border-radius: var(--border-radius-lg);
}

.action-btn-split .action-btn {
  box-shadow: none;
  height: 34px;
}

.action-btn--split-main {
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-right: none;
}

.action-btn--split-caret {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 0 var(--spacing-1);
  min-width: 24px;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  cursor: pointer;
  transition: all var(--transition-fast);
  color: var(--color-text-primary);
}

.action-btn--split-caret:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.action-btn--split-caret:focus {
  outline: none;
}

.action-btn--split-caret .btn-icon {
  font-size: 20px;
  color: var(--color-text-secondary);
}

.action-btn--split-caret:hover .btn-icon {
  color: var(--color-text-primary);
}

/* When the el-dropdown is open, give the caret a pressed look. */
.action-btn-split :deep(.el-dropdown--active) .action-btn--split-caret,
.action-btn-split :deep(.el-tooltip__trigger:focus) .action-btn--split-caret {
  background-color: var(--color-background-tertiary);
}

.save-as-icon {
  font-size: 16px;
  margin-right: var(--spacing-2);
  vertical-align: middle;
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
