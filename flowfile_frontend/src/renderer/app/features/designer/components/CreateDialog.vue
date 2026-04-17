<template>
  <!-- TODO(ux): add role='tablist' / role='tab' / aria-selected to the
       sidebar tab buttons. Same applies to SaveDialog/OpenDialog. -->
  <el-dialog
    v-model="isVisible"
    title="Create Flow"
    width="70%"
    :close-on-click-modal="false"
    custom-class="high-z-index-dialog"
  >
    <div class="create-dialog-body">
      <aside class="create-dialog-sidebar">
        <button
          class="sidebar-btn"
          :class="{ active: createMode === 'file' }"
          @click="createMode = 'file'"
        >
          <i class="fa-solid fa-folder"></i>
          <span>File System</span>
        </button>
        <button
          class="sidebar-btn"
          :class="{ active: createMode === 'catalog' }"
          @click="createMode = 'catalog'"
        >
          <i class="fa-solid fa-book"></i>
          <span>Catalog</span>
        </button>
      </aside>

      <section class="create-dialog-main">
        <!-- File System tab -->
        <div v-show="createMode === 'file'" class="create-panel">
          <file-browser
            :allowed-file-types="ALLOWED_SAVE_EXTENSIONS"
            mode="create"
            context="flows"
            :is-visible="isVisible && createMode === 'file'"
            @create-file="handleCreateAtPath"
            @overwrite-file="handleCreateAtPath"
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
        <div v-show="createMode === 'catalog'" class="create-panel">
          <div class="catalog-create-form">
            <div class="form-group">
              <label>Flow name</label>
              <el-input
                v-model="catalogFlowName"
                placeholder="my_flow"
                data-tutorial="create-catalog-name"
              />
              <span class="form-hint"
                >File will be created as <code>{{ catalogFilePath }}</code></span
              >
            </div>
            <div class="form-group">
              <label>Namespace</label>
              <catalog-namespace-picker v-model="selectedNamespaceId" />
            </div>
          </div>
        </div>
      </section>
    </div>
    <template #footer>
      <div class="dialog-footer">
        <el-button @click="isVisible = false">Cancel</el-button>
        <el-button
          v-if="createMode === 'catalog'"
          type="primary"
          :disabled="!canCreateInCatalog"
          @click="handleCreateInCatalog"
        >
          Create in Catalog
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch, computed } from "vue";
import { ElMessage } from "element-plus";
import FileBrowser from "../../../components/common/FileBrowser/fileBrowser.vue";
import { saveFlow } from "../../../components/layout/Header/utils";
import { createFlow } from "../../../components/nodes/nodeLogic";
import { getCatalogFlowsDirectory } from "../../../api/file.api";
import { ALLOWED_SAVE_EXTENSIONS } from "../../../components/common/FileBrowser/constants";
import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";

const props = defineProps({
  visible: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["create-complete", "create-cancelled", "update:visible"]);

const isVisible = ref(props.visible);

// Sidebar tab selection
const createMode = ref<"file" | "catalog">("file");

// Catalog options
const registerInCatalog = ref(true);
const selectedNamespaceId = ref<number | null>(null);
const catalogFlowName = ref("");
const catalogFlowsDir = ref("");

watch(
  () => props.visible,
  (newValue) => {
    isVisible.value = newValue;
  },
);

// Emit visibility + refresh catalog dir when opening
watch(isVisible, async (newValue) => {
  if (newValue !== props.visible) {
    emit("update:visible", newValue);
  }
  if (!newValue) {
    emit("create-cancelled");
    return;
  }
  try {
    catalogFlowsDir.value = await getCatalogFlowsDirectory();
  } catch (error) {
    console.error("Error fetching catalog flows directory:", error);
  }
});

const isValidSaveExtension = (filePath: string): boolean => {
  const name = filePath.toLowerCase();
  return ALLOWED_SAVE_EXTENSIONS.some((ext) => name.endsWith(`.${ext}`));
};

const handleCreateAtPath = async (flowPath: string) => {
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
    const newFlowId = await createFlow(flowPath);
    // The create endpoint auto-registers in the default namespace. If the user
    // picked a different namespace, re-register by calling save_flow with
    // namespace_id (same path → backend updates the existing registration).
    if (registerInCatalog.value && selectedNamespaceId.value !== null) {
      try {
        await saveFlow(newFlowId, flowPath, selectedNamespaceId.value);
      } catch (regErr) {
        console.warn("Created flow but failed to set catalog namespace:", regErr);
      }
    }
    ElMessage.success("Flow created successfully");
    isVisible.value = false;
    emit("create-complete", newFlowId);
  } catch (error: any) {
    console.error("Error creating flow:", error);
    ElMessage.error({
      message: error?.message || "Failed to create flow",
      duration: 5000,
    });
  }
};

const catalogFilePath = computed(() => {
  const dir = catalogFlowsDir.value || "~/.flowfile/flows";
  const name = catalogFlowName.value.trim() || "flow";
  const sep = dir.includes("\\") ? "\\" : "/";
  const hasExt = /\.(yaml|yml|json)$/i.test(name);
  return `${dir}${sep}${name}${hasExt ? "" : ".yaml"}`;
});

const canCreateInCatalog = computed(() => {
  return catalogFlowName.value.trim().length > 0 && selectedNamespaceId.value !== null;
});

const handleCreateInCatalog = async () => {
  if (!canCreateInCatalog.value) return;
  try {
    const newFlowId = await createFlow(catalogFilePath.value);
    if (selectedNamespaceId.value !== null) {
      try {
        await saveFlow(newFlowId, catalogFilePath.value, selectedNamespaceId.value);
      } catch (regErr) {
        console.warn("Created flow but failed to set catalog namespace:", regErr);
      }
    }
    ElMessage.success("Flow created and registered in catalog");
    isVisible.value = false;
    // Reset catalog name so the next invocation starts clean
    catalogFlowName.value = "";
    emit("create-complete", newFlowId);
  } catch (error: any) {
    console.error("Error creating flow in catalog:", error);
    ElMessage.error({
      message: error?.message || "Failed to create flow",
      duration: 5000,
    });
  }
};

defineExpose({
  open: () => {
    isVisible.value = true;
  },
  close: () => {
    isVisible.value = false;
  },
});
</script>

<style scoped>
.create-dialog-body {
  display: flex;
  min-height: 420px;
  gap: var(--spacing-4);
}

.create-dialog-sidebar {
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

.create-dialog-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
}

.create-panel {
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

.catalog-create-form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
  padding: var(--spacing-2);
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
