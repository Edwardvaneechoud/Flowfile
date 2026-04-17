<template>
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
              <label>Flow name</label>
              <el-input v-model="catalogFlowName" placeholder="my_flow" />
              <span class="form-hint"
                >File will be saved as <code>{{ catalogFilePath }}</code></span
              >
            </div>
            <div class="form-group">
              <label>Namespace</label>
              <catalog-namespace-picker v-model="selectedNamespaceId" />
            </div>
            <div class="catalog-save-actions">
              <el-button type="primary" :disabled="!canSaveToCatalog" @click="handleCatalogSave">
                Save to Catalog
              </el-button>
            </div>
          </div>
        </div>
      </section>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch, computed } from "vue";
import { ElMessage } from "element-plus";
import FileBrowser from "../../../components/common/FileBrowser/fileBrowser.vue";
import { FileInfo } from "../../../components/common/FileBrowser/types";
import { saveFlow, isInternalFlowfilePath } from "../../../components/layout/Header/utils";
import { getCatalogFlowsDirectory } from "../../../api/file.api";
import { getFlowSettings } from "../../../components/nodes/nodeLogic";
import { ALLOWED_SAVE_EXTENSIONS } from "../../../components/common/FileBrowser/constants";
import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";

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
const catalogFlowName = ref("");
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
  // Catalog saves always target the backend-managed flows directory, never
  // wherever the file browser happens to be.
  const dir = catalogFlowsDir.value || "~/.flowfile/flows";
  const name = catalogFlowName.value.trim() || "flow";
  const sep = dir.includes("\\") ? "\\" : "/";
  const hasExt = /\.(yaml|yml|json)$/i.test(name);
  return `${dir}${sep}${name}${hasExt ? "" : ".yaml"}`;
});

const canSaveToCatalog = computed(() => {
  return catalogFlowName.value.trim().length > 0 && selectedNamespaceId.value !== null;
});

const handleCatalogSave = async () => {
  if (!canSaveToCatalog.value) return;
  try {
    const newFlowId = await saveFlow(
      props.flowId,
      catalogFilePath.value,
      selectedNamespaceId.value ?? undefined,
    );
    ElMessage.success("Flow saved and registered in catalog");
    isVisible.value = false;
    emit("save-complete", newFlowId || props.flowId);
  } catch (error: any) {
    console.error("Error saving flow to catalog:", error);
    ElMessage.error({
      message: error?.message || "Failed to save flow",
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

.catalog-save-actions {
  display: flex;
  justify-content: flex-end;
}
</style>
