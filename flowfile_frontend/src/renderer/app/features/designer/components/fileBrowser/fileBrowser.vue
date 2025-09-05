//flowfile_frontend/src/renderer/app/pages/designer/components/fileBrowser/fileBrowser.vue

<template>
  <div class="file-browser">
    <!-- Title Section -->
    <div class="browser-header">
      <div class="browser-title">
        <span class="material-icons">folder</span>
        <span>File Browser</span>
      </div>
    </div>
    <div class="browser-content">
      <!-- Navigation and Search Section -->
      <div class="browser-toolbar">
        <div class="path-navigation">
          <button class="nav-button" :disabled="loading" @click="navigateUpDirectory">
            <span class="material-icons">arrow_upward</span>
            <span>Up</span>
          </button>
          <div class="current-path">
            {{ currentPath }}
          </div>
        </div>

        <div class="controls-row">
          <div class="search-container">
            <el-input v-model="searchTerm" placeholder="Search files..." class="search-input">
              <template #prefix>
                <span class="material-icons">search</span>
              </template>
            </el-input>
          </div>

          <div class="sort-controls">
            <el-select v-model="sortBy" placeholder="Sort by" size="small" class="sort-select">
              <el-option label="Name" value="name" />
              <el-option label="Size" value="size" />
              <el-option label="Modified" value="last_modified" />
              <el-option label="Created" value="created_date" />
            </el-select>
            <el-button class="sort-direction-button" size="small" @click="toggleSortDirection">
              <span class="material-icons">
                {{ sortDirection === "asc" ? "arrow_upward" : "arrow_downward" }}
              </span>
            </el-button>
          </div>

          <label class="show-hidden-toggle">
            <el-checkbox v-model="showHidden" @change="loadCurrentDirectory">
              Show hidden files
            </el-checkbox>
          </label>
        </div>
      </div>
      <!-- Main Content Area -->
      <div class="browser-main">
        <!-- Loading State -->
        <div v-if="loading" class="loading-state"></div>

        <!-- Error State -->
        <div v-else-if="error" class="error-state">
          {{ error }}
        </div>

        <!-- File Grid -->
        <div v-else class="grid-container" @click="handleBackgroundClick">
          <div
            v-for="file in filteredFiles"
            :key="file.path"
            class="file-item"
            :class="{
              selected: selectedFile?.path === file.path,
              'is-directory': file.is_directory,
            }"
            @click="handleSingleClick(file)"
            @dblclick="handleDoubleClick(file)"
          >
            <div class="file-item-content">
              <div class="file-icon-wrapper">
                <img
                  v-if="isDataFile(file)"
                  src="/images/sheets.png"
                  alt="Table file"
                  class="file-icon"
                />
                <img
                  v-else-if="isFlowfile(file)"
                  src="/images/flowfile.svg"
                  alt="Flow file"
                  class="file-icon"
                />
                <span
                  v-else
                  class="material-icons file-icon"
                  :class="{ 'hidden-file': file.is_hidden }"
                >
                  {{ file.is_directory ? "folder" : "insert_drive_file" }}
                </span>
              </div>
              <div class="file-details">
                <div class="file-name" :class="{ 'hidden-file': file.is_hidden }">
                  {{ file.name }}
                </div>
                <div class="file-info">
                  {{ formatFileSize(file.size) }} •
                  {{ formatDate(file.last_modified) }}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <!-- Bottom Action Bar -->
      <div class="browser-actions">
        <template v-if="mode === 'create'">
          <el-button
            v-if="selectedFile && !selectedFile.is_directory && showWarningOnOverwrite"
            type="warning"
            :disabled="loading"
            size="small"
            style="background-color: rgb(92, 92, 92); color: white"
            @click="confirmOverwrite"
          >
            <span class="material-icons">save</span>
            Overwrite File
          </el-button>
          <el-button
            v-else
            type="primary"
            :disabled="loading"
            size="small"
            style="background-color: rgb(92, 92, 92); color: white"
            @click="showCreateDialog = true"
          >
            <span class="material-icons">add</span>
            Create New File Here
          </el-button>
        </template>

        <!-- New button for selecting current directory -->
        <el-button
          v-if="allowDirectorySelection"
          type="primary"
          :disabled="loading"
          size="small"
          style="background-color: rgb(92, 92, 92); color: white; margin-right: auto"
          @click="handleSelectDirectory"
        >
          <span class="material-icons">folder</span>
          Select This Directory
        </el-button>

        <el-button
          v-if="mode === 'open' && selectedFile && !selectedFile.is_directory"
          type="primary"
          :disabled="loading"
          size="small"
          style="background-color: rgb(92, 92, 92); color: white"
          @click="handleOpenFile"
        >
          <span class="material-icons">open_in_new</span>
          Open File
        </el-button>
      </div>
    </div>

    <!-- Create File Dialog -->
    <el-dialog
      v-model="showCreateDialog"
      title="Create New File"
      width="30%"
      :close-on-click-modal="false"
      @closed="handleDialogClosed"
    >
      <el-form @submit.prevent="handleCreateFile">
        <el-form-item :error="fileNameError" label="File Name">
          <el-input
            ref="fileNameInput"
            v-model="newFileName"
            placeholder="Enter file name"
            @keyup.enter="handleCreateFile"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="showCreateDialog = false">Cancel</el-button>
          <el-button type="primary" :disabled="!newFileName.trim()" @click="handleCreateFile">
            Create
          </el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from "vue";
import { FileInfo } from "./types";
import { useFileBrowserStore } from "../../../../stores/fileBrowserStore";

import { ElMessage, ElMessageBox } from "element-plus";
import {
  getCurrentDirectoryContents,
  getCurrentPath,
  navigateUp,
  navigateInto,
  navigateTo,
} from "./fileSystemApi";
import path from "path-browserify";

const fileBrowserStore = useFileBrowserStore();

const sortBy = computed<"name" | "size" | "last_modified" | "created_date">({
  get: () => fileBrowserStore.sortBy,
  set: (newValue) => {
    fileBrowserStore.setSortBy(newValue);
  },
});

const sortDirection = computed(() => fileBrowserStore.sortDirection);
const toggleSortDirection = () => fileBrowserStore.toggleSortDirection();

interface Props {
  allowedFileTypes?: string[];
  modelValue?: FileInfo | null;
  mode: "open" | "create";
  initialFilePath?: string;
  allowDirectorySelection?: boolean;
  showWarningOnOverwrite?: boolean;
}

const props = withDefaults(defineProps<Props>(), {
  allowedFileTypes: () => [],
  modelValue: null,
  mode: "open",
  initialFilePath: "",
  allowDirectorySelection: false,
  showWarningOnOverwrite: true,
});

const handleInitialFileSelection = async () => {
  if (!props.initialFilePath) return;

  loading.value = true;
  error.value = null;

  try {
    // Get the directory path from the full file path
    const directoryPath = path.dirname(props.initialFilePath);
    const fileName = path.basename(props.initialFilePath);

    // Navigate to the directory
    await navigateTo(directoryPath);
    await loadCurrentDirectory();

    // Find and select the file
    const fileToSelect = files.value.find((file) => file.name === fileName);
    if (fileToSelect) {
      selectedFile.value = fileToSelect;
      emit("update:modelValue", fileToSelect);
    }
  } catch (err: any) {
    error.value = err.message || "Failed to navigate to initial file";
    ElMessage.error("Failed to navigate to initial file location");
  } finally {
    loading.value = false;
  }
};

// Emits
const emit = defineEmits<{
  (e: "fileSelected", file: FileInfo): void;
  (e: "update:modelValue", value: FileInfo | null): void;
  (e: "createFile", file_path: string, currentPath: string, fileName: string): void;
  (e: "overwriteFile", file_path: string, currentPath: string, fileName: string): void;
  (e: "directorySelected", path: string): void; // New emit
}>();

// State
const files = ref<FileInfo[]>([]);
const currentPath = ref<string>("");
const searchTerm = ref("");
const showHidden = ref(false);
const loading = ref(false);
const error = ref<string | null>(null);

// Create file state
const showCreateDialog = ref(false);
const newFileName = ref("");
const fileNameError = ref("");
const fileNameInput = ref<HTMLInputElement | null>(null);
const selectedFile = ref<FileInfo | null>(null);
const DATA_FILE_TYPES = ["xlsx", "parquet", "csv", "txt"];

// Methods
const isDataFile = (file: FileInfo): boolean => {
  if (file.is_directory) return false;
  return DATA_FILE_TYPES.some((type) => file.name.toLowerCase().endsWith(`.${type.toLowerCase()}`));
};

const isFlowfile = (file: FileInfo): boolean => {
  if (file.is_directory) return false;
  return file.name.toLowerCase().endsWith(".flowfile");
};

const formatFileSize = (bytes: number): string => {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(1) + " GB";
};

const formatDate = (dateString: string | Date): string => {
  const date = new Date(dateString);
  return `${date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  })} ${date.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  })}`;
};
const handleBackgroundClick = (event: MouseEvent) => {
  // Only deselect if clicking directly on the grid container
  if ((event.target as HTMLElement).classList.contains("grid-container")) {
    selectedFile.value = null;
    emit("update:modelValue", null);
  }
};

const handleOpenFile = () => {
  if (selectedFile.value && !selectedFile.value.is_directory) {
    emit("fileSelected", selectedFile.value);
  }
};

const handleSingleClick = (file: FileInfo) => {
  console.log("handleSingleClick", file);
  emit("update:modelValue", file);
  selectedFile.value = file;
};

const handleSelectDirectory = () => {
  emit("directorySelected", currentPath.value);
};

const handleDoubleClick = async (file: FileInfo) => {
  if (file.is_directory) {
    loading.value = true;
    error.value = null;
    selectedFile.value = null;
    try {
      await navigateInto(file.name);
      await loadCurrentDirectory();
      searchTerm.value = "";
    } catch (err: any) {
      error.value = err.message || "Failed to open directory";
    } finally {
      loading.value = false;
    }
  } else {
    emit("fileSelected", file);
  }
};

const loadCurrentDirectory = async () => {
  loading.value = true;
  error.value = null;
  try {
    const [pathResponse, filesResponse] = await Promise.all([
      getCurrentPath(),
      getCurrentDirectoryContents({ include_hidden: showHidden.value }),
    ]);
    currentPath.value = pathResponse;
    files.value = filesResponse;
  } catch (err: any) {
    error.value = err.message || "Failed to load directory";
  } finally {
    loading.value = false;
  }
};

const navigateUpDirectory = async () => {
  loading.value = true;
  error.value = null;
  selectedFile.value = null;
  try {
    await navigateUp();
    await loadCurrentDirectory();
  } catch (err: any) {
    error.value = err.message || "Failed to navigate up";
  } finally {
    loading.value = false;
  }
};

// Create file methods
const handleDialogClosed = () => {
  newFileName.value = "";
  fileNameError.value = "";
};

const validateFileName = (fileName: string): boolean => {
  if (!fileName.trim()) {
    fileNameError.value = "File name cannot be empty";
    return false;
  }

  if (!/^[a-zA-Z0-9-_. ]+$/.test(fileName)) {
    fileNameError.value = "File name contains invalid characters";
    return false;
  }

  if (files.value.some((file) => file.name.toLowerCase() === fileName.toLowerCase())) {
    fileNameError.value = "A file with this name already exists";
    return false;
  }

  return true;
};

const handleCreateFile = () => {
  if (validateFileName(newFileName.value)) {
    const newFilePath = path.join(currentPath.value, newFileName.value);
    emit("createFile", newFilePath, currentPath.value, newFileName.value);
    showCreateDialog.value = false;
  }
};

const confirmOverwrite = () => {
  if (selectedFile.value && !selectedFile.value.is_directory) {
    ElMessageBox.confirm(
      `Are you sure you want to overwrite "${selectedFile.value.name}"?`,
      "Warning",
      {
        confirmButtonText: "OK",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    )
      .then(() => {
        if (selectedFile.value)
          emit(
            "overwriteFile",
            selectedFile.value.path,
            currentPath.value,
            selectedFile.value.name,
          );
      })
      .catch(() => {
        // User cancelled
      });
  }
};
const refresh = async () => {
  await loadCurrentDirectory();
};

defineExpose({
  refresh,
  handleInitialFileSelection,
  loadCurrentDirectory,
  navigateUpDirectory,
  selectedFile: computed(() => selectedFile.value),
});
// Add new sorting state
// const sortBy = ref<"name" | "size" | "last_modified" | "created_date">("name");
// const sortDirection = ref<"asc" | "desc">("asc");

const calculateSortedFiles = () => {
  return files.value
    .filter((file) => {
      // For directories, only apply search and hidden filters
      if (file.is_directory) {
        const matchesSearch = file.name.toLowerCase().includes(searchTerm.value.toLowerCase());
        const matchesHidden = showHidden.value || !file.is_hidden;
        return matchesSearch && matchesHidden;
      }

      // For files, apply all filters
      const matchesSearch = file.name.toLowerCase().includes(searchTerm.value.toLowerCase());
      const matchesHidden = showHidden.value || !file.is_hidden;
      const matchesType =
        props.allowedFileTypes.length === 0 ||
        props.allowedFileTypes.some((type) =>
          file.name.toLowerCase().endsWith(`.${type.toLowerCase()}`),
        );
      return matchesSearch && matchesHidden && matchesType;
    })
    .sort((a, b) => {
      // Sort logic
      let comparison = 0;
      switch (sortBy.value) {
        case "size": {
          // Add block scope with curly braces
          const aSize = a.is_directory ? a.size || 0 : a.size;
          const bSize = b.is_directory ? b.size || 0 : b.size;
          comparison = aSize - bSize;
          break;
        }
        case "last_modified":
          comparison = new Date(a.last_modified).getTime() - new Date(b.last_modified).getTime();
          break;
        case "created_date":
          comparison = new Date(a.created_date).getTime() - new Date(b.created_date).getTime();
          break;
        case "name":
        default:
          // Special handling for name to be case-insensitive
          comparison = a.name.toLowerCase().localeCompare(b.name.toLowerCase());
          break;
      }
      return sortDirection.value === "asc" ? comparison : -comparison;
    });
};

const filteredFiles = computed(() => {
  return calculateSortedFiles();
});

// Watch for changes in allowedFileTypes
watch(
  () => props.allowedFileTypes,
  () => {
    loadCurrentDirectory();
  },
);

watch(
  () => props.initialFilePath,
  async (newPath) => {
    if (newPath) {
      await handleInitialFileSelection();
    }
  },
);

// Initialize
onMounted(async () => {
  if (props.initialFilePath) {
    await handleInitialFileSelection();
  } else {
    await loadCurrentDirectory();
  }
});
</script>

<style scoped>
.browser-title {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 16px;
  font-size: 18px;
  font-weight: 600;
  color: #333;
}

.mode-selector {
  display: flex;
  justify-content: center;
}

.browser-content {
  display: flex;
  flex-direction: column;
  height: calc(100% - 116px); /* Adjust based on header height */
  overflow: hidden;
}

.browser-toolbar {
  padding: 16px;
  background-color: #fff;
  border-bottom: 1px solid #e9ecef;
}

.path-navigation {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
}

.nav-button {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 8px 16px;
  border: 1px solid #dee2e6;
  border-radius: 4px;
  background-color: white;
  color: #495057;
  font-size: 14px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.nav-button:hover:not(:disabled) {
  background-color: #f8f9fa;
  border-color: #ced4da;
}

.current-path {
  flex: 1;
  padding: 8px 12px;
  background-color: #f8f9fa;
  border: 1px solid #dee2e6;
  border-radius: 4px;
  font-family: monospace;
  color: #495057;
}

.search-container {
  display: flex;
  align-items: center;
  gap: 16px;
  flex: 1;
}

.search-input {
  flex: 1;
}

.browser-main {
  flex: 1;
  overflow-y: auto;
  padding: 16px;
}

.file-item {
  border: 1px solid #e9ecef;
  border-radius: 8px;
  overflow: hidden;
  transition: all 0.2s ease;
}

.file-item:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}

.file-item.selected {
  border-color: #3b82f6;
  background-color: #eff6ff;
}

.file-item-content {
  padding: 12px;
  display: flex;
  align-items: center;
  gap: 12px;
}

.file-icon-wrapper {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 30px;
  height: 30px;
}

.file-icon {
  width: 24px;
  height: 24px;
  color: #6b7280;
}

.file-details {
  flex: 1;
  min-width: 0;
}

.file-name {
  font-weight: 500;
  color: #1f2937;
  margin-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.file-info {
  font-size: 12px;
  color: #6b7280;
}

.browser-actions {
  padding: 16px;
  background-color: #f8f9fa;
  border-top: 1px solid #e9ecef;
  display: flex;
  justify-content: flex-end;
  align-items: center;
  height: 72px;
  box-sizing: border-box;
  gap: 12px; /* Add gap between buttons */
}
.selected-file {
  color: #6b7280;
  font-size: 14px;
}

.loading-state,
.error-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 200px;
  color: #6b7280;
}

.error-state {
  color: #dc2626;
}

.hidden-file {
  opacity: 0.5;
}

.file-browser {
  display: flex;
  flex-direction: column;
  height: 70vh; /* Set to 70% of viewport height */
  background-color: white;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.browser-content {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0; /* Enable proper flex child scrolling */
  overflow: hidden;
}

.browser-main {
  flex: 1;
  overflow-y: auto; /* Make this section scrollable */
  padding: 16px;
  min-height: 0; /* Enable proper flex child scrolling */
}

.grid-container {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(205px, 1fr));
  gap: 16px;
  padding: 8px;
}

/* Optional: if you want to maintain icon size consistency */
.file-icon {
  width: 24px;
  height: 24px;
  object-fit: contain;
}

/* Add new styles for sort controls */
.search-and-sort-container {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.sort-controls {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 180px; /* Prevent select from being too narrow */
}

.sort-direction-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 32px;
  padding: 0;
}

.controls-row {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-top: 12px;
}
</style>
