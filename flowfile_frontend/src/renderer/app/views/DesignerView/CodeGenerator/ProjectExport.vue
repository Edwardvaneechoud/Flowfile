<template>
  <div class="project-export">
    <div class="project-toolbar">
      <button class="toolbar-button" :disabled="loading" @click="fetchManifest">
        <span v-if="loading" class="spinner"></span>
        {{ loading ? "Loading..." : "Refresh" }}
      </button>
      <div class="toolbar-actions">
        <button
          class="toolbar-button primary"
          :disabled="loading || !files.length"
          @click="downloadZip"
        >
          Download .zip
        </button>
        <button
          class="toolbar-button primary"
          :disabled="loading || !files.length"
          @click="showSaveDialog = true"
        >
          Save to folder…
        </button>
      </div>
    </div>

    <div v-if="errorMessage" class="project-error">{{ errorMessage }}</div>

    <div v-if="warnings.length" class="project-warnings">
      <div v-for="(warning, index) in warnings" :key="index" class="warning-line">
        ⚠ {{ warning }}
      </div>
    </div>

    <div v-if="files.length" class="project-body">
      <div class="file-list">
        <div
          v-for="file in files"
          :key="file.path"
          :class="[
            'file-entry',
            { active: selectedPath === file.path, nested: file.path.includes('/') },
          ]"
          @click="selectedPath = file.path"
        >
          {{ file.path }}
        </div>
      </div>
      <div class="file-preview">
        <codemirror :model-value="selectedContent" :extensions="extensions" :disabled="true" />
      </div>
    </div>

    <el-dialog
      v-model="showSaveDialog"
      title="Save project to folder"
      width="70%"
      :close-on-click-modal="false"
    >
      <p class="save-hint">
        The project will be written to a new
        <code>{{ projectName }}/</code> directory inside the folder you select.
      </p>
      <FileBrowser
        mode="open"
        :allow-directory-selection="true"
        @directory-selected="saveToDirectory"
      />
    </el-dialog>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted, watch } from "vue";
import axios from "axios";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import { ElMessage, ElMessageBox } from "element-plus";
import FileBrowser from "../../../components/common/FileBrowser/fileBrowser.vue";
import { useNodeStore } from "../../../stores/column-store";

interface ProjectExportFile {
  path: string;
  content: string;
}

const nodeStore = useNodeStore();
const loading = ref(false);
const errorMessage = ref("");
const projectName = ref("");
const files = ref<ProjectExportFile[]>([]);
const warnings = ref<string[]>([]);
const selectedPath = ref("");
const showSaveDialog = ref(false);

const extensions = [
  python(),
  oneDark,
  EditorView.theme({
    "&": { fontSize: "11px" },
    ".cm-content": { padding: "20px" },
    ".cm-focused": { outline: "none" },
  }),
];

const selectedContent = computed(() => {
  const file = files.value.find((f) => f.path === selectedPath.value);
  return file?.content ?? "";
});

// Monotonic token so an out-of-order response (e.g. after switching flows
// mid-request) can't overwrite the state of a newer request.
let fetchSeq = 0;

const fetchManifest = async () => {
  if (nodeStore.flow_id <= 0) return;
  const requestId = ++fetchSeq;
  loading.value = true;
  errorMessage.value = "";
  try {
    const response = await axios.get("/editor/code_to_project", {
      params: { flow_id: nodeStore.flow_id },
    });
    if (requestId !== fetchSeq) return;
    projectName.value = response.data.project_name;
    files.value = response.data.files;
    warnings.value = response.data.warnings ?? [];
    if (!files.value.some((f) => f.path === selectedPath.value)) {
      selectedPath.value = files.value[0]?.path ?? "";
    }
  } catch (error: any) {
    if (requestId !== fetchSeq) return;
    files.value = [];
    warnings.value = [];
    errorMessage.value =
      error?.response?.data?.detail ??
      "Failed to generate the project export. Please check your flow configuration.";
  } finally {
    if (requestId === fetchSeq) {
      loading.value = false;
    }
  }
};

const downloadZip = async () => {
  try {
    const response = await axios.get("/editor/code_to_project/zip", {
      params: { flow_id: nodeStore.flow_id },
      responseType: "blob",
    });
    const url = URL.createObjectURL(new Blob([response.data], { type: "application/zip" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `${projectName.value || "flowfile_project"}.zip`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch (error) {
    console.error("Failed to download project zip:", error);
    ElMessage.error("Failed to download the project zip.");
  }
};

const saveToDirectory = async (directory: string, overwrite = false) => {
  try {
    const response = await axios.post("/editor/code_to_project/save", {
      flow_id: nodeStore.flow_id,
      target_directory: directory,
      overwrite,
    });
    showSaveDialog.value = false;
    ElMessage.success(`Project saved to ${response.data.saved_to}`);
  } catch (error: any) {
    if (error?.response?.status === 409 && !overwrite) {
      try {
        await ElMessageBox.confirm(
          `'${projectName.value}' already exists in this folder. Overwrite its files?`,
          "Warning",
          { confirmButtonText: "Overwrite", cancelButtonText: "Cancel", type: "warning" },
        );
        await saveToDirectory(directory, true);
      } catch {
        // User cancelled
      }
      return;
    }
    ElMessage.error(error?.response?.data?.detail ?? "Failed to save the project.");
  }
};

watch(
  () => nodeStore.flow_id,
  (newFlowId) => {
    if (newFlowId > 0) {
      fetchManifest();
    }
  },
);

onMounted(fetchManifest);
</script>

<style scoped>
.project-export {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.project-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.toolbar-actions {
  display: flex;
  gap: 12px;
}

.toolbar-button {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  background: var(--color-gray-500);
  color: var(--color-text-inverse);
  border: none;
  border-radius: var(--border-radius-md);
  cursor: pointer;
  font-size: var(--font-size-base);
  transition: background var(--transition-fast);
}

.toolbar-button.primary {
  background: var(--color-accent);
}

.toolbar-button.primary:hover:not(:disabled) {
  background: var(--color-accent-hover);
}

.toolbar-button:disabled {
  background: var(--color-gray-500);
  cursor: not-allowed;
}

.project-error {
  padding: 10px 14px;
  border-radius: var(--border-radius-md);
  background: var(--color-danger-bg, rgba(239, 68, 68, 0.1));
  color: var(--color-danger, #ef4444);
  font-size: var(--font-size-sm, 13px);
  white-space: pre-wrap;
}

.project-warnings {
  padding: 10px 14px;
  border-radius: var(--border-radius-md);
  background: var(--color-warning-bg, rgba(245, 158, 11, 0.1));
  color: var(--color-warning, #f59e0b);
  font-size: var(--font-size-sm, 13px);
}

.warning-line + .warning-line {
  margin-top: 4px;
}

.project-body {
  display: flex;
  gap: 12px;
  min-height: 0;
}

.file-list {
  width: 240px;
  flex-shrink: 0;
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius-md);
  overflow-y: auto;
  padding: 6px;
  align-self: flex-start;
  max-height: 60vh;
}

.file-entry {
  padding: 6px 10px;
  border-radius: var(--border-radius-md);
  font-family: var(--font-family-mono, monospace);
  font-size: 12px;
  color: var(--color-text-secondary, #999);
  cursor: pointer;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.file-entry.nested {
  padding-left: 22px;
}

.file-entry:hover {
  background: var(--color-background-tertiary);
}

.file-entry.active {
  background: var(--color-accent-subtle, #e8f0fe);
  color: var(--color-accent);
  font-weight: var(--font-weight-medium);
}

.file-preview {
  flex: 1;
  min-width: 0;
  min-height: 200px;
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.file-preview :deep(.cm-editor) {
  min-height: 200px;
}

.save-hint {
  margin: 0 0 12px;
  font-size: var(--font-size-sm, 13px);
  color: var(--color-text-secondary, #999);
}

.save-hint code {
  background: var(--color-background-secondary);
  padding: 2px 6px;
  border-radius: 4px;
}

.spinner {
  display: inline-block;
  width: 14px;
  height: 14px;
  border: 2px solid var(--color-text-inverse);
  border-radius: 50%;
  border-top-color: transparent;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
