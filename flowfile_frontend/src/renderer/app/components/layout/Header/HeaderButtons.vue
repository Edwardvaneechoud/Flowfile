<template>
  <div class="action-buttons">
    <div v-if="hasOpenFlow" class="action-btn-split" data-tutorial="save-btn">
      <el-popover
        placement="bottom"
        :width="240"
        trigger="hover"
        :show-after="200"
        popper-class="header-action-popover"
        :show-arrow="true"
      >
        <template #reference>
          <button
            class="action-btn action-btn--split-main"
            data-tutorial="save-main-btn"
            @click="openSaveModal"
          >
            <span class="material-icons btn-icon">save</span>
            <span class="btn-text">Save</span>
          </button>
        </template>
        <div class="header-action-popover-body">
          <div class="header-action-popover-title">
            <span class="material-icons header-action-popover-icon">save</span>
            <span>Save</span>
          </div>
          <p class="header-action-popover-desc">
            Save changes to this flow. Use the ▼ for Save As…
          </p>
          <p class="header-action-popover-shortcut-hint">
            <span class="header-action-popover-shortcut">
              <kbd>{{ MODIFIER_LABEL }}</kbd>
              <kbd>S</kbd>
            </span>
            to save.
          </p>
        </div>
      </el-popover>
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
    <el-popover
      placement="bottom"
      :width="240"
      trigger="hover"
      :show-after="200"
      popper-class="header-action-popover"
      :show-arrow="true"
    >
      <template #reference>
        <button class="action-btn" data-tutorial="open-btn" @click="modalVisibleForOpen = true">
          <span class="material-icons btn-icon">folder_open</span>
          <span class="btn-text">Open</span>
        </button>
      </template>
      <div class="header-action-popover-body">
        <div class="header-action-popover-title">
          <span class="material-icons header-action-popover-icon">folder_open</span>
          <span>Open</span>
        </div>
        <p class="header-action-popover-desc">Open an existing flow.</p>
        <p class="header-action-popover-shortcut-hint">
          <span class="header-action-popover-shortcut">
            <kbd>{{ MODIFIER_LABEL }}</kbd>
            <kbd>O</kbd>
          </span>
          to open the picker.
        </p>
      </div>
    </el-popover>
    <div class="action-btn-split" data-tutorial="quick-create-btn">
      <el-popover
        placement="bottom"
        :width="240"
        trigger="hover"
        :show-after="200"
        popper-class="header-action-popover"
        :show-arrow="true"
      >
        <template #reference>
          <button
            class="action-btn action-btn--split-main"
            data-tutorial="quick-create-main-btn"
            @click="handleQuickCreate"
          >
            <span class="material-icons btn-icon">add_circle_outline</span>
            <span class="btn-text">Create</span>
          </button>
        </template>
        <div class="header-action-popover-body">
          <div class="header-action-popover-title">
            <span class="material-icons header-action-popover-icon">add_circle_outline</span>
            <span>Create</span>
          </div>
          <p class="header-action-popover-desc">
            Start a new flow at the default location. Use the ▼ to pick a folder.
          </p>
          <p class="header-action-popover-shortcut-hint">
            <span class="header-action-popover-shortcut">
              <kbd>{{ MODIFIER_LABEL }}</kbd>
              <kbd>N</kbd>
            </span>
            to create.
          </p>
        </div>
      </el-popover>
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
  </div>

  <!-- Hidden run-button instance kept solely to back loadFlowSettings()'s
       polling-restoration path. The user-visible run-button now lives in
       RightActionCluster; this off-screen instance preserves the existing
       runButton.value.startPolling()/stopPolling() calls below without
       refactoring the polling restore. v-show:false stays in the layout
       tree so the ref resolves. -->
  <run-button v-show="false" ref="runButton" :flow-id="nodeStore.flow_id" aria-hidden="true" />

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
          <parameter-settings v-model="flowSettings.parameters" @change="pushFlowSettings" />
        </div>
      </div>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
// TODO(refactor): ~692 LOC. Plan to extract:
//   - SettingsModal.vue: 4-section settings modal (~lines 90-209)
//   - ExecutionSettings.vue (~lines 99-151), DisplaySettings.vue (~153-164),
//     ParameterSettings.vue (~166-207) as children of SettingsModal
//   - Parameter CRUD logic (~lines 431-448) goes with ParameterSettings
import { ref, onMounted, watch, computed } from "vue";
import { ElMessage } from "element-plus";

import { saveFlowSilent } from "./utils";
import RunButton from "./run.vue";
import ParameterSettings from "./ParameterSettings.vue";
import SaveDialog from "../../../features/designer/components/SaveDialog.vue";
import OpenDialog from "../../../features/designer/components/OpenDialog.vue";
import CreateDialog from "../../../features/designer/components/CreateDialog.vue";
import { useNodeStore } from "../../../stores/column-store";
import { useFlowStore } from "../../../stores/flow-store";
import { useEditorStore } from "../../../stores/editor-store";
import { useTutorialStore } from "../../../stores/tutorial-store";
import { useProjectStore } from "../../../stores/project-store";
import { useCatalogStore } from "../../../stores/catalog-store";
import { useRecentFlows } from "../../../composables/useRecentFlows";
import {
  createFlow,
  getFlowSettings,
  FlowSettings,
  updateFlowSettings,
  ExecutionMode,
  ExecutionLocation,
  updateRunStatus,
} from "../../nodes/nodeLogic";
import { MODIFIER_LABEL } from "../../../utils/shortcuts";

const nodeStore = useNodeStore();
const flowStore = useFlowStore();
const editorStore = useEditorStore();
const tutorialStore = useTutorialStore();
const projectStore = useProjectStore();
const catalogStore = useCatalogStore();
const { recordFlowFromSettings, refreshCatalogRefs } = useRecentFlows();

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
    await recordCurrentFlowAsRecent();
    emit("refreshFlow");
    ElMessage.success("Flow created");
    tutorialStore.notify({ type: "flow-created", flowId: createdFlowId });
  } catch (error) {
    console.error("Failed to create flow:", error);
    ElMessage.error("Failed to create flow");
  }
};

const openCreateDialog = () => {
  modalVisibleForCreate.value = true;
};

const handleCreateComplete = async (flowId: number, catalogRef?: string) => {
  modalVisibleForCreate.value = false;
  if (flowId) {
    nodeStore.setFlowId(flowId);
    await recordCurrentFlowAsRecent(catalogRef);
    emit("refreshFlow");
    tutorialStore.notify({ type: "flow-created", flowId });
  }
};

// Created flows never pass through DesignerView's reloadCanvas (the open-path
// recording seam), so record them here. loadFlowSettings dedupes with the
// flow_id watcher's concurrent call, so this costs no extra request.
const recordCurrentFlowAsRecent = async (catalogRef?: string) => {
  await loadFlowSettings();
  recordFlowFromSettings(flowSettings.value, catalogRef);
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

// Collapse the concurrent watcher-triggered + explicit loadFlowSettings calls fired on
// flow load (setFlowId fires the flow_id watcher while initialSetup also calls explicitly)
// into one getFlowSettings + one run_status. Keyed by flow_id; cleared once settled.
let settingsLoad: { id: number; promise: Promise<void> } | null = null;

const loadFlowSettings = async (): Promise<void> => {
  const flowId = nodeStore.flow_id;
  if (!(flowId && flowId > 0)) return;
  if (settingsLoad && settingsLoad.id === flowId) return settingsLoad.promise;

  const promise = (async () => {
    flowSettings.value = await getFlowSettings(flowId);
    if (!flowSettings.value) return;

    flowSettings.value.execution_mode = flowSettings.value.execution_mode || "Development";
    flowSettings.value.show_edge_labels = flowSettings.value.show_edge_labels ?? false;
    flowSettings.value.parameters = flowSettings.value.parameters ?? [];
    flowStore.setParameters(flowSettings.value.parameters);
    editorStore.displayLogViewer = flowSettings.value.show_detailed_progress;
    editorStore.showEdgeLabels = flowSettings.value.show_edge_labels;

    if (!runButton.value) return;

    if (flowSettings.value.is_running) {
      editorStore.isRunning = true;
      runButton.value.startPolling(runButton.value.checkRunStatus);
    } else {
      editorStore.isRunning = false;
      runButton.value.stopPolling();
      updateRunStatus(flowId, nodeStore);
    }
  })();

  settingsLoad = { id: flowId, promise };
  try {
    await promise;
  } finally {
    if (settingsLoad?.id === flowId) settingsLoad = null;
  }
};

const pushFlowSettings = async () => {
  if (flowSettings.value) {
    await updateFlowSettings(flowSettings.value);
    flowStore.setParameters(flowSettings.value.parameters ?? []);
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
  projectStore.onSourceChanged();
  // A catalog save adds/updates a registration; refresh the cached listing so
  // the Catalog view and recents reflect it without a manual reload.
  catalogStore.loadAllFlows();
  refreshCatalogRefs();
  tutorialStore.notify({ type: "flow-saved", flowId: nodeStore.flow_id });
};

function handleOpenFromDialog(payload: {
  message: string;
  flowPath: string;
  flowName?: string;
  catalogRef?: string;
}) {
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
      projectStore.onSourceChanged();
      ElMessage.success("Flow saved successfully");
      tutorialStore.notify({ type: "flow-saved", flowId: nodeStore.flow_id });
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

const openSettingsModal = () => {
  modalVisibleForSettings.value = true;
  tutorialStore.notify({ type: "flow-settings-opened" });
};

// Allow other components (e.g. the Performance-mode notice in the run results)
// to open the Flow Settings modal via an editor-store signal.
watch(
  () => editorStore.flowSettingsOpenRequest,
  () => openSettingsModal(),
);

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
  openSettings: openSettingsModal,
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
</style>
