<template>
  <div class="file-browser" :class="{ dense }">
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
          <button class="nav-button" :disabled="loading || isAtRoot" @click="navigateUpDirectory">
            <span class="material-icons">arrow_upward</span>
            <span>Up</span>
          </button>
          <button
            class="nav-button refresh-button"
            :disabled="loading"
            title="Refresh"
            @click="loadCurrentDirectory"
          >
            <span class="material-icons" :class="{ spin: loading }">refresh</span>
          </button>
          <div class="current-path">
            {{ currentPath }}
          </div>
        </div>

        <div class="controls-row">
          <div class="search-container">
            <el-input
              v-model="searchTerm"
              placeholder="Search files..."
              size="small"
              class="search-input"
            >
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
              <el-option
                v-if="provider.capabilities.createdDate"
                label="Created"
                value="created_date"
              />
            </el-select>
            <el-button class="sort-direction-button" size="small" @click="toggleSortDirection">
              <span class="material-icons">
                {{ sortDirection === "asc" ? "arrow_upward" : "arrow_downward" }}
              </span>
            </el-button>
          </div>

          <label v-if="provider.capabilities.hiddenFiles" class="show-hidden-toggle">
            <el-checkbox v-model="showHidden" @change="loadCurrentDirectory">
              Show hidden files
            </el-checkbox>
          </label>
        </div>
      </div>
      <!-- Main Content Area -->
      <div class="browser-main" @click="handleBackgroundClick">
        <!-- Loading State -->
        <div v-if="loading" class="loading-state"></div>

        <!-- Error State -->
        <div v-else-if="error" class="error-state">
          <div class="error-content">
            <span class="material-icons error-icon">error_outline</span>
            <span class="error-message">{{ error }}</span>
            <el-button type="primary" size="small" @click.stop="loadCurrentDirectory">
              <span class="material-icons">refresh</span>
              Retry
            </el-button>
          </div>
        </div>

        <!-- Credentials cannot enumerate containers; ask for one instead of dead-ending -->
        <div v-else-if="rootListingDenied" class="denied-state" @click.stop>
          <span class="material-icons">lock</span>
          <span>This connection is not allowed to list buckets.</span>
          <div class="bucket-entry">
            <el-input
              v-model="bucketInput"
              size="small"
              placeholder="Enter a bucket name to open"
              @keyup.enter="openTypedBucket"
            />
            <el-button
              type="primary"
              size="small"
              :disabled="!bucketInput.trim()"
              @click="openTypedBucket"
            >
              Open
            </el-button>
          </div>
        </div>

        <!-- File Grid -->
        <div v-else class="grid-container">
          <div
            v-for="file in filteredFiles"
            :key="file.path"
            class="file-item"
            :class="{
              selected: selectedFile?.path === file.path,
              'is-directory': file.is_directory,
            }"
            @click.stop="handleSingleClick(file)"
            @dblclick.stop="handleDoubleClick(file)"
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
                  src="/images/flowfile.png"
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
                  {{ formatEntryMeta(file) }}
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Object stores return one page at a time -->
        <div v-if="!loading && !error && truncated" class="truncation-bar" @click.stop>
          <span class="material-icons">more_horiz</span>
          <span
            >Showing the first {{ files.length }} entries. Narrow the path or search to
            refine.</span
          >
          <el-button v-if="nextToken" size="small" :loading="loadingMore" @click="loadMoreEntries">
            Load more
          </el-button>
        </div>

        <!-- Empty state hint when no file is selected -->
        <div
          v-if="!loading && !error && !rootListingDenied && filteredFiles.length === 0"
          class="empty-state"
        >
          <span class="material-icons">folder_open</span>
          <span>No files found</span>
        </div>
      </div>
      <!-- Bottom Action Bar -->
      <div class="browser-actions">
        <template v-if="mode === 'create' && provider.capabilities.createFile">
          <el-button
            v-if="selectedFile && !selectedFile.is_directory && showWarningOnOverwrite"
            type="warning"
            :disabled="loading"
            size="small"
            class="browser-action-btn"
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
            class="browser-action-btn"
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
          class="browser-action-btn"
          style="margin-right: auto"
          @click="handleSelectDirectory"
        >
          <span class="material-icons">folder</span>
          {{ currentIsDeltaTable ? "Select This Delta Table" : "Select This Directory" }}
        </el-button>

        <el-button
          v-if="mode === 'open' && selectedFile && !selectedFile.is_directory"
          type="primary"
          :disabled="loading"
          size="small"
          class="browser-action-btn"
          @click="handleOpenFile"
        >
          <span class="material-icons">open_in_new</span>
          Open File
        </el-button>
      </div>
    </div>

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
            placeholder="Enter file name (e.g., my_flow)"
            @keyup.enter="handleCreateFile"
          />
          <div v-if="newFileName.trim()" class="form-hint">
            <span class="preview-label">Will be saved as:</span>
            <code>{{ previewFileName }}</code>
            <span v-if="!hasValidExtension" class="auto-extension-hint"> </span>
          </div>
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
// TODO(refactor): ~1130 LOC. Plan to extract:
//   - useFileBrowserSort composable: sort/filter (~lines 705-760)
//   - FileBrowserToolbar.vue (~lines 12-69), FileBrowserGrid.vue (~109-154)
//   - FileCreateDialog.vue (~232-262), useFileBrowserNavigation composable (~394-500)
// Storage is already abstracted behind the `location` prop -> ./storage providers;
// the navigation composable should take the provider rather than reach for the API.
import { ref, computed, onMounted, onActivated, watch } from "vue";
import { FileInfo, StorageLocation } from "./types";
import { useFileBrowserStore, type FileBrowserContext } from "../../../stores/fileBrowserStore";

import { ElMessage, ElMessageBox } from "element-plus";
import { resolveStorageProvider, storageScopeKey } from "./storage";
import { basename, dirname, normalizePath } from "../../../utils/storagePath";

import { FLOWFILE_EXTENSIONS, DATA_FILE_EXTENSIONS, ALLOWED_SAVE_EXTENSIONS } from "./constants";

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
  /** External visibility control - when true, triggers auto-refresh */
  isVisible?: boolean;
  /** Context for state management - each context maintains its own path state */
  context?: FileBrowserContext;
  /** When set, browsing is locked to this directory (start dir + no navigating above it). */
  rootPath?: string;
  /** Prefill for the "Create New File" dialog (e.g. the current flow's name on Save As). */
  defaultNewFileName?: string;
  /** Compact row rhythm for height-capped hosts (the Save/Create dialogs). */
  dense?: boolean;
  /** Which storage to browse. Omit for the local filesystem. */
  location?: StorageLocation;
}

const props = withDefaults(defineProps<Props>(), {
  allowedFileTypes: () => [],
  modelValue: null,
  mode: "open",
  initialFilePath: "",
  allowDirectorySelection: false,
  showWarningOnOverwrite: true,
  isVisible: true,
  context: "flows",
  rootPath: undefined,
  defaultNewFileName: "",
  dense: false,
  location: () => ({ kind: "local" }) as StorageLocation,
});

const provider = computed(() => resolveStorageProvider(props.location));
/** Null for local so the existing contexts gain no extra localStorage entry. */
const storedScope = computed(() =>
  props.location.kind === "local" ? null : storageScopeKey(props.location),
);

// Emits
const emit = defineEmits<{
  (e: "fileSelected", file: FileInfo): void;
  (e: "update:modelValue", value: FileInfo | null): void;
  (e: "createFile", file_path: string, currentPath: string, fileName: string): void;
  (e: "overwriteFile", file_path: string, currentPath: string, fileName: string): void;
  (e: "directorySelected", path: string): void;
}>();

// State
const files = ref<FileInfo[]>([]);
const currentPath = ref<string>("");
const searchTerm = ref("");
const showHidden = ref(false);
const loading = ref(false);
const error = ref<string | null>(null);

// Object-store listings arrive one page at a time and may be refused at the
// container-list level; local listings never set any of these.
const truncated = ref(false);
const nextToken = ref<string | null>(null);
const loadingMore = ref(false);
const rootListingDenied = ref(false);
const currentIsDeltaTable = ref(false);
const bucketInput = ref("");

// Create file state
const showCreateDialog = ref(false);
const newFileName = ref("");
const fileNameError = ref("");
const selectedFile = ref<FileInfo | null>(null);

// Seed the filename from the caller's default (e.g. the flow's name on Save As);
// handleDialogClosed resets it, so a reopen re-seeds.
watch(showCreateDialog, (open) => {
  if (open && !newFileName.value && props.defaultNewFileName) {
    newFileName.value = props.defaultNewFileName;
  }
});

const normalizeForCompare = (p: string): string => {
  const n = normalizePath(p);
  return n.length > 1 && n.endsWith("/") ? n.slice(0, -1) : n;
};

const isWithinRoot = (p: string): boolean => {
  if (!props.rootPath) return true;
  const root = normalizeForCompare(props.rootPath);
  const target = normalizeForCompare(p);
  return target === root || target.startsWith(root + "/");
};

const isAtRoot = computed(
  () =>
    (!!props.rootPath &&
      normalizeForCompare(currentPath.value) === normalizeForCompare(props.rootPath)) ||
    provider.value.isRoot(currentPath.value),
);

const loadDirectoryContents = async (directoryPath: string) => {
  loading.value = true;
  error.value = null;
  try {
    const listing = await provider.value.list(directoryPath, {
      includeHidden: showHidden.value,
    });
    files.value = listing.entries;
    currentPath.value = listing.path || directoryPath;
    truncated.value = listing.truncated;
    nextToken.value = listing.nextToken;
    rootListingDenied.value = listing.rootListingDenied;
    currentIsDeltaTable.value = listing.currentIsDeltaTable;
    if (!props.rootPath) {
      fileBrowserStore.setCurrentPath(props.context, currentPath.value, storedScope.value);
    }
  } catch (err: any) {
    error.value = err.message || "Failed to load directory";
  } finally {
    loading.value = false;
  }
};

/** Append the next page in place; object stores cap how much one listing returns. */
const loadMoreEntries = async () => {
  if (!nextToken.value || loadingMore.value) return;
  loadingMore.value = true;
  try {
    const listing = await provider.value.list(currentPath.value, {
      includeHidden: showHidden.value,
      pageToken: nextToken.value,
    });
    files.value = [...files.value, ...listing.entries];
    truncated.value = listing.truncated;
    nextToken.value = listing.nextToken;
  } catch (err: any) {
    ElMessage.warning(err.message || "Could not load more entries");
  } finally {
    loadingMore.value = false;
  }
};

const loadDefaultDirectory = async () => {
  loading.value = true;
  error.value = null;
  try {
    const defaultPath = await provider.value.defaultPath();
    await loadDirectoryContents(defaultPath);
  } catch (err: any) {
    error.value = err.message || "Failed to load directory";
    loading.value = false;
  }
};

const loadCurrentDirectory = async () => {
  if (props.rootPath) {
    await loadDirectoryContents(props.rootPath);
    return;
  }

  const storedPath = fileBrowserStore.getCurrentPath(props.context);
  // A path remembered against a different connection must not be replayed here.
  const sameStorage = fileBrowserStore.getScope(props.context) === storedScope.value;

  if (storedPath && sameStorage) {
    await loadDirectoryContents(storedPath);
    if (error.value) {
      fileBrowserStore.resetContext(props.context);
      await loadDefaultDirectory();
    }
  } else {
    await loadDefaultDirectory();
  }
};

const openTypedBucket = async () => {
  const typed = bucketInput.value.trim();
  if (!typed) return;
  await navigateToPath(provider.value.join(currentPath.value, typed));
  bucketInput.value = "";
};

/**
 * Navigate to a specific directory path.
 * On failure, stays at the current directory and shows a warning instead of an error state.
 */
const navigateToPath = async (directoryPath: string) => {
  if (!isWithinRoot(directoryPath)) return;
  const previousPath = currentPath.value;
  const previousFiles = [...files.value];
  await loadDirectoryContents(directoryPath);
  if (error.value) {
    // Navigation failed — revert to previous state so the user stays where they were
    currentPath.value = previousPath;
    files.value = previousFiles;
    fileBrowserStore.setCurrentPath(props.context, previousPath);
    error.value = null;
    ElMessage.warning("Cannot navigate to this directory");
    return;
  }
  searchTerm.value = "";
  selectedFile.value = null;
};

/**
 * Navigate up one directory level
 */
const navigateUpDirectory = async () => {
  if (isAtRoot.value) return;
  const parent = provider.value.parent(currentPath.value);
  if (parent && parent !== currentPath.value && isWithinRoot(parent)) {
    await navigateToPath(parent);
  }
};

/**
 * Navigate into a subdirectory
 */
const navigateIntoDirectory = async (directoryName: string) => {
  const newPath = provider.value.join(currentPath.value, directoryName);
  await navigateToPath(newPath);
};

const handleInitialFileSelection = async (filePath?: string) => {
  const initialPath = filePath || props.initialFilePath;
  if (!initialPath) return;

  loading.value = true;
  error.value = null;

  try {
    const directoryPath = dirname(initialPath);
    const fileName = basename(initialPath);

    await loadDirectoryContents(directoryPath);

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

const isDataFile = (file: FileInfo): boolean => {
  if (file.is_directory) return false;
  const name = file.name.toLowerCase();
  return DATA_FILE_EXTENSIONS.some((ext) => name.endsWith(`.${ext}`));
};

const isFlowfile = (file: FileInfo): boolean => {
  if (file.is_directory) return false;
  const name = file.name.toLowerCase();
  return FLOWFILE_EXTENSIONS.some((ext) => name.endsWith(`.${ext}`));
};

const formatFileSize = (bytes: number): string => {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(1) + " GB";
};

const formatDate = (dateString: string | Date | null): string => {
  if (!dateString) return "";
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
/** Object stores report no size for a prefix and no timestamp for many entries. */
const formatEntryMeta = (file: FileInfo): string => {
  if (provider.value.kind === "local") {
    return `${formatFileSize(file.size)} • ${formatDate(file.last_modified)}`;
  }
  if (file.is_directory) return "Folder";
  const parts = [formatFileSize(file.size)];
  if (file.last_modified) parts.push(formatDate(file.last_modified));
  return parts.join(" • ");
};

const handleBackgroundClick = () => {
  // Deselect file when clicking on empty area (file item clicks are stopped with @click.stop)
  selectedFile.value = null;
  emit("update:modelValue", null);
};

const handleOpenFile = () => {
  if (selectedFile.value && !selectedFile.value.is_directory) {
    emit("fileSelected", selectedFile.value);
  }
};

const handleSingleClick = (file: FileInfo) => {
  emit("update:modelValue", file);
  selectedFile.value = file;
};

const handleSelectDirectory = () => {
  emit("directorySelected", currentPath.value);
};

const handleDoubleClick = async (file: FileInfo) => {
  if (file.is_directory) {
    selectedFile.value = null;
    await navigateIntoDirectory(file.name);
  } else {
    emit("fileSelected", file);
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

const hasValidExtension = computed(() => {
  const name = newFileName.value.trim().toLowerCase();
  const validExtensions =
    props.allowedFileTypes.length > 0 ? props.allowedFileTypes : ALLOWED_SAVE_EXTENSIONS;
  return validExtensions.some((ext) => name.endsWith(`.${ext}`));
});

const previewFileName = computed(() => {
  const name = newFileName.value.trim();
  if (!name) return "";
  if (hasValidExtension.value) return name;
  const defaultExt = props.allowedFileTypes.length > 0 ? props.allowedFileTypes[0] : "yaml";
  return `${name}.${defaultExt}`;
});

const handleCreateFile = () => {
  if (validateFileName(newFileName.value)) {
    const fileName = previewFileName.value;
    const newFilePath = provider.value.join(currentPath.value, fileName);
    emit("createFile", newFilePath, currentPath.value, fileName);
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
        customClass: "overwrite-confirm-dialog",
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

/** Fall back to name when the active sort key means nothing for this storage. */
const effectiveSortBy = computed(() =>
  sortBy.value === "created_date" && !provider.value.capabilities.createdDate
    ? "name"
    : sortBy.value,
);

const toTime = (value: string | Date | null): number => (value ? new Date(value).getTime() : 0);

const calculateSortedFiles = () => {
  return files.value
    .filter((file) => {
      if (file.is_directory) {
        const matchesSearch = file.name.toLowerCase().includes(searchTerm.value.toLowerCase());
        const matchesHidden = showHidden.value || !file.is_hidden;
        return matchesSearch && matchesHidden;
      }

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
      let comparison = 0;
      switch (effectiveSortBy.value) {
        case "size": {
          const aSize = a.is_directory ? a.size || 0 : a.size;
          const bSize = b.is_directory ? b.size || 0 : b.size;
          comparison = aSize - bSize;
          break;
        }
        case "last_modified":
          comparison = toTime(a.last_modified) - toTime(b.last_modified);
          break;
        case "created_date":
          comparison = toTime(a.created_date) - toTime(b.created_date);
          break;
        case "name":
        default:
          comparison = a.name.toLowerCase().localeCompare(b.name.toLowerCase());
          break;
      }
      return sortDirection.value === "asc" ? comparison : -comparison;
    });
};

const filteredFiles = computed(() => {
  return calculateSortedFiles();
});

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

watch(
  () => props.isVisible,
  async (newVisible, oldVisible) => {
    if (newVisible && !oldVisible) {
      await loadCurrentDirectory();
    }
  },
);

// The caller can swap the connection while the browser is open.
watch(storedScope, async (next, previous) => {
  if (next !== previous) {
    selectedFile.value = null;
    await loadDefaultDirectory();
  }
});

// For keep-alive components, refresh when re-activated
onActivated(async () => {
  await loadCurrentDirectory();
});

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
  gap: 6px;
  margin-bottom: 6px;
  font-size: 13px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.browser-title .material-icons {
  font-size: 16px;
}

.mode-selector {
  display: flex;
  justify-content: center;
}

.browser-content {
  display: flex;
  flex-direction: column;
  height: calc(100% - 72px); /* Header is now ~72px tall (title + compact toolbar) */
  overflow: hidden;
}

.browser-toolbar {
  padding: 8px 12px;
  background-color: var(--color-background-primary);
  border-bottom: 1px solid var(--color-border-primary);
}

.browser-action-btn {
  background-color: var(--color-file-browser-bg);
  color: var(--color-text-inverse);
}

.path-navigation {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 6px;
}

.nav-button {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 8px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background-color: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: 12px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.nav-button .material-icons {
  font-size: 14px;
}

.nav-button:hover:not(:disabled) {
  background-color: var(--color-background-secondary);
  border-color: var(--color-border-secondary);
}

.current-path {
  flex: 1;
  padding: 4px 10px;
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  font-family: var(--font-family-mono);
  font-size: 12px;
  color: var(--color-text-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.search-container {
  display: flex;
  align-items: center;
  gap: 8px;
  flex: 1;
}

.search-input {
  flex: 1;
}

.search-input :deep(.el-input__wrapper) {
  border-radius: 4px;
}

.search-input :deep(.el-input__inner) {
  font-size: 12px;
}

.browser-main {
  flex: 1;
  overflow-y: auto;
  padding: 16px;
}

.file-item {
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  overflow: hidden;
  transition: all 0.2s ease;
}

.file-item:hover {
  transform: translateY(-2px);
  box-shadow: var(--shadow-sm);
}

.file-item.selected {
  border-color: var(--color-info);
  border-width: 2px;
  background-color: var(--color-background-selected);
  box-shadow: var(--shadow-md);
  position: relative;
}

.file-item.selected::after {
  content: "check_circle";
  font-family: "Material Icons";
  position: absolute;
  top: 6px;
  right: 6px;
  color: var(--color-info);
  font-size: 18px;
  background: var(--color-background-primary);
  border-radius: 50%;
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
  color: var(--color-text-tertiary);
}

.file-details {
  flex: 1;
  min-width: 0;
}

.file-name {
  font-weight: 500;
  color: var(--color-text-primary);
  margin-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.file-info {
  font-size: 12px;
  color: var(--color-text-secondary);
}

.browser-actions {
  padding: 16px;
  background-color: var(--color-background-secondary);
  border-top: 1px solid var(--color-border-primary);
  display: flex;
  justify-content: flex-end;
  align-items: center;
  height: 72px;
  box-sizing: border-box;
  gap: 12px;
}
.selected-file {
  color: var(--color-text-secondary);
  font-size: 14px;
}

.loading-state,
.error-state,
.empty-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 200px;
  color: var(--color-text-secondary);
}

.error-state {
  color: var(--color-danger);
}

.error-content {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 12px;
  text-align: center;
}

.error-icon {
  font-size: 48px;
  color: var(--color-danger, #ef4444);
}

.error-message {
  max-width: 300px;
}

.empty-state {
  flex-direction: column;
  gap: 8px;
  color: var(--color-text-secondary);
}

.denied-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  height: 200px;
  color: var(--color-text-secondary);
  text-align: center;
}

.denied-state .material-icons {
  font-size: 40px;
  opacity: 0.5;
}

.bucket-entry {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 320px;
  max-width: 100%;
}

.truncation-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 8px;
  padding: 8px 12px;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  background-color: var(--color-background-secondary);
  font-size: 12px;
  color: var(--color-text-secondary);
}

.truncation-bar .material-icons {
  font-size: 16px;
}

.empty-state .material-icons {
  font-size: 48px;
  opacity: 0.5;
}

/* Refresh button styling */
.refresh-button {
  padding: 8px;
  min-width: auto;
}

.refresh-button .material-icons {
  transition: transform 0.3s ease;
}

.refresh-button .material-icons.spin {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}

.hidden-file {
  opacity: 0.5;
}

.file-browser {
  display: flex;
  flex-direction: column;
  height: 70vh; /* Set to 70% of viewport height */
  background-color: var(--color-background-primary);
  border-radius: 8px;
  box-shadow: var(--shadow-md);
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

/* Opt-in compact mode: the Save/Create dialogs cap the browser's height so the
   catalog options below stay visible, which leaves too little room for the
   default row rhythm. Every other consumer keeps the roomier default. */
.file-browser.dense .browser-main {
  padding: 8px 16px;
}

.file-browser.dense .grid-container {
  gap: 8px;
  padding: 4px;
}

.file-browser.dense .file-item-content {
  padding: 6px 10px;
  gap: 10px;
}

.file-browser.dense .file-icon-wrapper {
  width: 24px;
  height: 24px;
}

.file-browser.dense .file-icon {
  width: 20px;
  height: 20px;
}

.file-browser.dense .file-name {
  font-size: 13px;
  margin-bottom: 1px;
}

.file-browser.dense .file-info {
  font-size: 11px;
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
  gap: 6px;
  min-width: 150px;
}

.sort-select :deep(.el-input__inner) {
  height: 26px;
  font-size: 12px;
}

.sort-direction-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 26px;
  padding: 0;
}

.sort-direction-button .material-icons {
  font-size: 14px;
}

.controls-row {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-top: 0;
}

.show-hidden-toggle :deep(.el-checkbox__label) {
  font-size: 12px;
}

.form-hint {
  font-size: 12px;
  color: var(--color-text-tertiary);
  margin-top: 8px;
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}

.preview-label {
  color: var(--color-text-secondary);
}

.form-hint code {
  background-color: var(--color-background-secondary);
  padding: 2px 6px;
  border-radius: 4px;
  font-family: monospace;
  color: var(--color-text-primary);
}

.auto-extension-hint {
  color: var(--color-text-muted);
  font-style: italic;
}
</style>
