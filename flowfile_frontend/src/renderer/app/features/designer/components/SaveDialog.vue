<template>
  <!-- TODO(ux): move the file-browser Save trigger into the dialog footer
       too, so both tabs share a single primary-action location. Requires
       changing the file-browser to a pure-selection mode.
       TODO(ux): surface the catalog-registration namespace more prominently
       on re-save — keep the default-on behavior (users get free run history
       that way), but make the target namespace visible at a glance so users
       understand where their flow is being tracked. -->
  <el-dialog
    v-model="isVisible"
    title="Save Flow"
    width="70%"
    :close-on-click-modal="false"
    custom-class="high-z-index-dialog"
    @closed="handleDialogClosed"
  >
    <div class="save-dialog-body">
      <aside class="save-dialog-sidebar">
        <button
          class="sidebar-btn"
          :class="{ active: saveMode === 'file' }"
          @click="saveMode = 'file'"
        >
          <i class="fa-solid fa-folder"></i>
          <span>File System</span>
        </button>
        <button
          class="sidebar-btn"
          :class="{ active: saveMode === 'catalog' }"
          @click="saveMode = 'catalog'"
        >
          <i class="fa-solid fa-book"></i>
          <span>Catalog</span>
        </button>
      </aside>

      <section class="save-dialog-main">
        <!-- File System tab -->
        <div v-show="saveMode === 'file'" class="save-panel">
          <file-browser
            ref="fileBrowserRef"
            :allowed-file-types="ALLOWED_SAVE_EXTENSIONS"
            mode="create"
            context="flows"
            :initial-file-path="initialPath"
            :is-visible="isVisible && saveMode === 'file'"
            @create-file="handleSaveFlow"
            @overwrite-file="handleSaveFlow"
          />
          <div class="catalog-options">
            <el-checkbox v-model="registerInCatalog"> Also register in catalog </el-checkbox>
            <div v-if="registerInCatalog" class="namespace-section">
              <label class="namespace-label">Namespace</label>
              <catalog-namespace-picker v-model="selectedNamespaceId" />
            </div>
          </div>
        </div>

        <!-- Catalog tab -->
        <div v-show="saveMode === 'catalog'" class="save-panel">
          <div class="catalog-save-form">
            <div class="form-group">
              <label>Browse the catalog</label>
              <catalog-flow-picker
                v-model="selectedRegistrationId"
                @select-flow="handleOverwriteTargetSelected"
                @select-namespace="handleNamespaceSelected"
                @flows-loaded="handleFlowsLoaded"
              />
              <span v-if="overwriteTargetHint" class="form-hint overwrite-hint">
                Will overwrite <code>{{ overwriteTargetHint }}</code>
                <el-button text type="primary" size="small" @click="clearOverwriteSelection">
                  Clear
                </el-button>
              </span>
              <span v-else class="form-hint">
                Pick an existing flow to overwrite, or save as new below.
              </span>
            </div>
            <div v-if="!selectedRegistrationId" class="catalog-new-section">
              <div class="section-divider">
                <span class="divider-label">Or save as new</span>
              </div>
              <div class="form-group">
                <label>Flow name</label>
                <el-input
                  v-model="catalogFlowName"
                  placeholder="my_flow"
                  @input="clearOverwriteSelection"
                />
                <span v-if="nameCollisionFlow" class="form-hint collision-hint">
                  A flow named <code>{{ nameCollisionFlow.name }}</code> already exists here.
                  <el-button text type="primary" size="small" @click="selectCollisionForOverwrite">
                    Overwrite it instead
                  </el-button>
                </span>
                <span v-else class="form-hint"
                  >File will be saved as <code>{{ catalogFilePath }}</code> in
                  <strong>{{ selectedNamespaceName || "the selected namespace" }}</strong></span
                >
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
    <template #footer>
      <div class="dialog-footer">
        <el-button @click="isVisible = false">Cancel</el-button>
        <el-button
          v-if="saveMode === 'catalog'"
          type="primary"
          :disabled="!canSaveToCatalog"
          @click="handleCatalogSave"
        >
          {{ catalogPrimaryLabel }}
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch, computed } from "vue";
import { ElMessage } from "element-plus";
import FileBrowser from "../../../components/common/FileBrowser/fileBrowser.vue";
import { FileInfo } from "../../../components/common/FileBrowser/types";
import {
  saveFlow,
  saveFlowToCatalog,
  overwriteFlowInCatalog,
  isInternalFlowfilePath,
} from "../../../components/layout/Header/utils";
import { getCatalogFlowsDirectory } from "../../../api/file.api";
import { getFlowSettings } from "../../../components/nodes/nodeLogic";
import { ALLOWED_SAVE_EXTENSIONS } from "../../../components/common/FileBrowser/constants";
import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";
import CatalogFlowPicker from "./CatalogFlowPicker.vue";
import type { FlowRegistration } from "../../../types";

const props = defineProps({
  visible: {
    type: Boolean,
    default: false,
  },
  flowId: {
    type: Number,
    required: true,
  },
});

const emit = defineEmits(["save-complete", "save-cancelled", "update:visible"]);

const isVisible = ref(props.visible);
const initialPath = ref("");

// Sidebar tab selection
const saveMode = ref<"file" | "catalog">("file");

// Catalog options
const registerInCatalog = ref(true);
const selectedNamespaceId = ref<number | null>(null);
const selectedNamespaceName = ref<string | null>(null);
const catalogFlowName = ref("");
// Picker-driven catalog state: if ``selectedRegistrationId`` is set, saving
// overwrites that existing flow; otherwise we create a new registration in
// ``selectedNamespaceId`` using ``catalogFlowName``.
const selectedRegistrationId = ref<number | null>(null);
const overwriteTargetHint = ref<string>("");
// Flows in the currently-selected namespace — used to detect name collisions
// before the user hits save so we can offer the overwrite shortcut.
const namespaceFlows = ref<FlowRegistration[]>([]);
// Always points at the backend-managed flows dir (``~/.flowfile/flows`` on
// local, ``/data/user/flows`` in Docker).  Used as the target for Catalog-tab
// saves so they land in the managed location regardless of where the file
// browser was last navigated.
const catalogFlowsDir = ref("");

const fileBrowserRef = ref<{
  refresh: () => Promise<void>;
  handleInitialFileSelection: (filePath?: string) => Promise<void>;
  loadCurrentDirectory: () => Promise<void>;
  navigateUpDirectory: () => Promise<void>;
  selectedFile: FileInfo | null;
} | null>(null);

watch(
  () => props.visible,
  (newValue) => {
    isVisible.value = newValue;
  },
);

// When visibility changes, update the initialPath and file selection
watch(isVisible, async (newValue) => {
  if (newValue !== props.visible) {
    emit("update:visible", newValue);
  }

  if (newValue && props.flowId) {
    await updateInitialPath();
  }
});

// Emit cancelled when closed without saving
watch(isVisible, (newValue) => {
  if (!newValue) {
    emit("save-cancelled", props.flowId);
  }
});

const handleDialogClosed = () => {
  // No extra cleanup needed
};

const updateInitialPath = async () => {
  // Reset picker state each open so nothing leaks between sessions.
  selectedRegistrationId.value = null;
  overwriteTargetHint.value = "";

  // Always refresh the managed catalog flows directory — this is the target
  // for the Catalog tab and must never be influenced by the user's last
  // browsed directory.
  try {
    catalogFlowsDir.value = await getCatalogFlowsDirectory();
  } catch (error) {
    console.error("Error fetching catalog flows directory:", error);
  }

  try {
    const settings = await getFlowSettings(props.flowId);
    if (settings?.path) {
      // Pre-fill catalog flow name from current file name (strip extension)
      const fileName = settings.path.split(/[/\\]/).pop() ?? "";
      catalogFlowName.value = fileName.replace(/\.(yaml|yml|json)$/i, "");

      // Only seed the file-browser initial path from the current flow path
      // when that path lives in a real user directory.  Quick-created flows
      // live under ``~/.flowfile/flows/unnamed_flows`` and we don't want the
      // Save dialog to open inside Flowfile's internal storage.
      if (!isInternalFlowfilePath(settings.path)) {
        initialPath.value = settings.path;
        if (fileBrowserRef.value) {
          await fileBrowserRef.value.handleInitialFileSelection(settings.path);
        }
      } else {
        initialPath.value = "";
      }
    }
  } catch (error) {
    console.error("Error getting flow settings:", error);
  }
};

const isValidSaveExtension = (filePath: string): boolean => {
  const name = filePath.toLowerCase();
  return ALLOWED_SAVE_EXTENSIONS.some((ext) => name.endsWith(`.${ext}`));
};

const handleSaveFlow = async (flowPath: string) => {
  if (flowPath.toLowerCase().endsWith(".flowfile")) {
    ElMessage.error({
      message: "The .flowfile format is deprecated. Please use .yaml or .yml instead.",
      duration: 5000,
    });
    return;
  }
  if (!isValidSaveExtension(flowPath)) {
    ElMessage.error({
      message: "Invalid file extension. Please use .yaml or .yml",
      duration: 5000,
    });
    return;
  }

  try {
    const namespaceId = registerInCatalog.value ? selectedNamespaceId.value : undefined;
    const newFlowId = await saveFlow(props.flowId, flowPath, namespaceId ?? undefined);
    ElMessage.success("Flow saved successfully");
    isVisible.value = false;
    emit("save-complete", newFlowId || props.flowId);
  } catch (error: any) {
    console.error("Error saving flow:", error);
    ElMessage.error({
      message: error?.message || "Failed to save flow",
      duration: 5000,
    });
  }
};

const catalogFilePath = computed(() => {
  // Display-only preview of the final catalog path. Must mirror the backend
  // convention in /save_flow_to_catalog (``{flow_id}_{stem}.yaml`` under the
  // managed flows dir) so the user sees what will actually be written.
  const dir = catalogFlowsDir.value || "~/.flowfile/flows";
  const raw = catalogFlowName.value.trim() || "flow";
  const stem = raw.replace(/\.(yaml|yml|json)$/i, "");
  const sep = dir.includes("\\") ? "\\" : "/";
  return `${dir}${sep}${props.flowId}_${stem}.yaml`;
});

const normalizedCatalogName = computed(() =>
  catalogFlowName.value.trim().replace(/\.(yaml|yml|json)$/i, ""),
);

const nameCollisionFlow = computed<FlowRegistration | null>(() => {
  if (selectedRegistrationId.value !== null) return null;
  const candidate = normalizedCatalogName.value;
  if (!candidate) return null;
  return namespaceFlows.value.find((f) => f.name === candidate) ?? null;
});

const canSaveToCatalog = computed(() => {
  if (selectedRegistrationId.value !== null) {
    return true;
  }
  if (nameCollisionFlow.value) return false;
  return catalogFlowName.value.trim().length > 0 && selectedNamespaceId.value !== null;
});

const catalogPrimaryLabel = computed(() => {
  if (selectedRegistrationId.value !== null && overwriteTargetHint.value) {
    return `Overwrite "${overwriteTargetHint.value}"`;
  }
  if (selectedRegistrationId.value !== null) {
    return "Overwrite flow";
  }
  return "Save to Catalog";
});

const handleOverwriteTargetSelected = (payload: {
  flow: FlowRegistration;
  namespaceName: string | null;
}) => {
  const prefix = payload.namespaceName ? `${payload.namespaceName}/` : "";
  overwriteTargetHint.value = `${prefix}${payload.flow.name}`;
};

const handleNamespaceSelected = (payload: {
  namespaceId: number;
  namespaceName: string | null;
}) => {
  selectedNamespaceId.value = payload.namespaceId;
  selectedNamespaceName.value = payload.namespaceName;
};

const clearOverwriteSelection = () => {
  if (selectedRegistrationId.value !== null) {
    selectedRegistrationId.value = null;
    overwriteTargetHint.value = "";
  }
};

const handleFlowsLoaded = (flows: FlowRegistration[]) => {
  namespaceFlows.value = flows;
};

const selectCollisionForOverwrite = () => {
  const target = nameCollisionFlow.value;
  if (!target) return;
  selectedRegistrationId.value = target.id;
  const prefix = selectedNamespaceName.value ? `${selectedNamespaceName.value}/` : "";
  overwriteTargetHint.value = `${prefix}${target.name}`;
};

const handleCatalogSave = async () => {
  if (!canSaveToCatalog.value) return;
  try {
    let newFlowId: number;
    if (selectedRegistrationId.value !== null) {
      newFlowId = await overwriteFlowInCatalog(props.flowId, selectedRegistrationId.value);
      ElMessage.success("Existing catalog flow overwritten");
    } else {
      newFlowId = await saveFlowToCatalog(
        props.flowId,
        catalogFlowName.value.trim(),
        selectedNamespaceId.value as number,
      );
      ElMessage.success("Flow saved and registered in catalog");
    }
    isVisible.value = false;
    emit("save-complete", newFlowId || props.flowId);
  } catch (error: any) {
    console.error("Error saving flow to catalog:", error);
    const detail = error?.response?.data?.detail;
    ElMessage.error({
      message: detail || error?.message || "Failed to save flow",
      duration: 5000,
    });
  }
};

const open = async () => {
  await updateInitialPath();
  isVisible.value = true;
};

defineExpose({
  open,
  close: () => {
    isVisible.value = false;
  },
});
</script>

<style scoped>
.save-dialog-body {
  display: flex;
  min-height: 420px;
  gap: var(--spacing-4);
}

.save-dialog-sidebar {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  width: 160px;
  padding-right: var(--spacing-3);
  border-right: 1px solid var(--color-border-light);
  flex-shrink: 0;
}

.sidebar-btn {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--border-radius-md);
  text-align: left;
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  transition: all var(--transition-fast);
}

.sidebar-btn:hover {
  background-color: var(--color-background-tertiary);
}

.sidebar-btn.active {
  background-color: var(--color-accent-subtle, #e8f0fe);
  border-color: var(--color-accent, #1976d2);
  color: var(--color-accent, #1976d2);
  font-weight: var(--font-weight-medium);
}

.sidebar-btn i {
  width: 16px;
  text-align: center;
}

.save-dialog-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
}

.save-panel {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.catalog-options {
  padding: var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-muted, #f9f9fb);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.namespace-section {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.namespace-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.catalog-save-form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
  padding: var(--spacing-2);
}

.overwrite-hint {
  color: var(--color-accent, #1976d2);
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.collision-hint {
  color: var(--color-warning, #c77700);
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.catalog-new-section {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.section-divider {
  position: relative;
  text-align: center;
  margin: var(--spacing-2) 0;
}

.section-divider::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 0;
  right: 0;
  height: 1px;
  background-color: var(--color-border-light);
}

.divider-label {
  position: relative;
  display: inline-block;
  padding: 0 var(--spacing-3);
  background-color: var(--color-background-primary);
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.form-group label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.form-hint {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-muted, #999);
}

.form-hint code {
  background-color: var(--color-background-tertiary);
  padding: 1px var(--spacing-1);
  border-radius: var(--border-radius-sm);
  font-family: var(--font-family-mono);
}
</style>
