<template>
  <div class="file-manager-container">
    <div class="mb-3">
      <h2 class="page-title">File Manager</h2>
      <p class="page-description">
        Upload and manage data files in the shared directory
      </p>
    </div>

    <!-- Upload Card -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Upload Files</h3>
      </div>
      <div class="card-content">
        <div
          class="upload-zone"
          :class="{ 'upload-zone--active': isDragging }"
          @dragover.prevent="isDragging = true"
          @dragleave.prevent="isDragging = false"
          @drop.prevent="handleDrop"
          @click="openFilePicker"
        >
          <i class="fa-solid fa-cloud-arrow-up upload-zone__icon"></i>
          <p class="upload-zone__text">
            Drag and drop files here, or click to browse
          </p>
          <p class="upload-zone__hint">
            Supported: CSV, Parquet, Excel (.xlsx/.xls), JSON, TXT, TSV
          </p>
          <input
            ref="fileInput"
            type="file"
            multiple
            :accept="acceptString"
            class="upload-zone__input"
            @change="handleFileSelect"
          />
        </div>

        <!-- Upload progress -->
        <div v-if="uploadQueue.length > 0" class="upload-queue">
          <div
            v-for="item in uploadQueue"
            :key="item.name"
            class="upload-queue__item"
          >
            <div class="upload-queue__info">
              <i :class="getFileIcon(item.name)" class="upload-queue__file-icon"></i>
              <span class="upload-queue__name">{{ item.name }}</span>
            </div>
            <div class="upload-queue__status">
              <template v-if="item.status === 'uploading'">
                <div class="upload-queue__progress-bar">
                  <div
                    class="upload-queue__progress-fill"
                    :style="{ width: item.progress + '%' }"
                  ></div>
                </div>
                <span class="upload-queue__percent">{{ item.progress }}%</span>
              </template>
              <i
                v-else-if="item.status === 'done'"
                class="fa-solid fa-check upload-queue__done"
              ></i>
              <span v-else-if="item.status === 'error'" class="upload-queue__error">
                {{ item.error }}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Files List Card -->
    <div class="card">
      <div class="card-header">
        <h3 class="card-title">
          Files ({{ filteredFiles.length }})
        </h3>
        <div v-if="files.length > 0" class="search-container">
          <input
            v-model="searchTerm"
            type="text"
            placeholder="Search files..."
            class="search-input"
            aria-label="Search files"
          />
          <i class="fa-solid fa-search search-icon"></i>
        </div>
      </div>
      <div class="card-content">
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading files...</p>
        </div>

        <div v-else-if="files.length === 0" class="empty-state">
          <i class="fa-solid fa-folder-open"></i>
          <p>No files uploaded yet</p>
          <p>Upload CSV, Parquet, Excel, JSON, or text files to get started</p>
        </div>

        <div v-else-if="filteredFiles.length > 0" class="connections-list">
          <div
            v-for="file in filteredFiles"
            :key="file.name"
            class="connection-item"
          >
            <div class="connection-info">
              <div class="connection-name">
                <i :class="getFileIcon(file.name)"></i>
                <span>{{ file.name }}</span>
                <span class="file-badge">{{ file.file_type.toUpperCase() }}</span>
              </div>
              <span class="file-meta">
                {{ formatSize(file.size) }} &middot; Modified {{ formatDate(file.last_modified) }}
              </span>
            </div>
            <div class="connection-actions">
              <button
                type="button"
                class="btn btn-danger btn-sm"
                :aria-label="`Delete file ${file.name}`"
                @click="handleConfirmDelete(file.name)"
              >
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>
        </div>

        <div v-else class="empty-state">
          <i class="fa-solid fa-search"></i>
          <p>No files found matching "{{ searchTerm }}"</p>
        </div>
      </div>
    </div>

    <!-- Delete Confirmation Modal -->
    <div v-if="showDeleteModal" class="modal-overlay" @click="cancelDelete">
      <div class="modal-container" @click.stop>
        <div class="modal-header">
          <h3 class="modal-title">Delete File</h3>
          <button
            class="modal-close"
            aria-label="Close delete confirmation"
            @click="cancelDelete"
          >
            <i class="fa-solid fa-times"></i>
          </button>
        </div>
        <div class="modal-content">
          <p>
            Are you sure you want to delete
            <strong>{{ fileToDelete }}</strong>?
          </p>
          <p class="warning-text">
            This action cannot be undone. Any flows referencing this file will
            need to be updated.
          </p>
        </div>
        <div class="modal-actions">
          <button class="btn btn-secondary" @click="cancelDelete">
            Cancel
          </button>
          <button
            class="btn btn-danger-filled"
            :disabled="isDeleting"
            @click="handleDeleteFile"
          >
            <i v-if="isDeleting" class="fas fa-spinner fa-spin"></i>
            {{ isDeleting ? "Deleting..." : "Delete File" }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useFileManager } from "./useFileManager";

const {
  files,
  filteredFiles,
  isLoading,
  searchTerm,
  loadFiles,
  uploadFile,
  deleteFile,
} = useFileManager();

// Upload state
const fileInput = ref<HTMLInputElement | null>(null);
const isDragging = ref(false);

interface UploadQueueItem {
  name: string;
  status: "uploading" | "done" | "error";
  progress: number;
  error?: string;
}

const uploadQueue = ref<UploadQueueItem[]>([]);

const acceptString =
  ".csv,.parquet,.xlsx,.xls,.json,.txt,.tsv";

// Delete state
const showDeleteModal = ref(false);
const fileToDelete = ref("");
const isDeleting = ref(false);

function openFilePicker() {
  fileInput.value?.click();
}

function handleFileSelect(event: Event) {
  const input = event.target as HTMLInputElement;
  if (input.files) {
    processFiles(Array.from(input.files));
    input.value = "";
  }
}

function handleDrop(event: DragEvent) {
  isDragging.value = false;
  if (event.dataTransfer?.files) {
    processFiles(Array.from(event.dataTransfer.files));
  }
}

async function processFiles(fileList: File[]) {
  for (const file of fileList) {
    const item: UploadQueueItem = {
      name: file.name,
      status: "uploading",
      progress: 0,
    };
    uploadQueue.value.push(item);

    try {
      await uploadFile(file, (percent: number) => {
        item.progress = percent;
      });
      item.status = "done";
      item.progress = 100;
    } catch (error: any) {
      item.status = "error";
      item.error =
        error.response?.data?.detail || error.message || "Upload failed";
    }
  }

  // Clear completed items after a delay
  setTimeout(() => {
    uploadQueue.value = uploadQueue.value.filter(
      (i) => i.status === "uploading",
    );
  }, 3000);
}

function handleConfirmDelete(filename: string) {
  fileToDelete.value = filename;
  showDeleteModal.value = true;
}

function cancelDelete() {
  showDeleteModal.value = false;
  fileToDelete.value = "";
}

async function handleDeleteFile() {
  if (!fileToDelete.value) return;
  isDeleting.value = true;
  try {
    await deleteFile(fileToDelete.value);
    cancelDelete();
  } catch {
    alert("Failed to delete file. Please try again.");
    cancelDelete();
  } finally {
    isDeleting.value = false;
  }
}

function getFileIcon(filename: string): string {
  const ext = filename.split(".").pop()?.toLowerCase() || "";
  const icons: Record<string, string> = {
    csv: "fa-solid fa-file-csv",
    tsv: "fa-solid fa-file-csv",
    parquet: "fa-solid fa-database",
    xlsx: "fa-solid fa-file-excel",
    xls: "fa-solid fa-file-excel",
    json: "fa-solid fa-file-code",
    txt: "fa-solid fa-file-lines",
  };
  return icons[ext] || "fa-solid fa-file";
}

function formatSize(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + " " + units[i];
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr);
  return d.toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

onMounted(() => {
  loadFiles().catch(() => {
    alert("Failed to load files. Please try again.");
  });
});
</script>

<style scoped>
.file-manager-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

/* Upload zone */
.upload-zone {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: var(--spacing-8) var(--spacing-4);
  border: 2px dashed var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background-color: var(--color-background-secondary);
  cursor: pointer;
  transition: all var(--transition-fast) var(--transition-timing);
}

.upload-zone:hover,
.upload-zone--active {
  border-color: var(--color-accent);
  background-color: var(--color-accent-subtle);
}

.upload-zone__icon {
  font-size: var(--font-size-4xl);
  color: var(--color-accent);
  margin-bottom: var(--spacing-3);
}

.upload-zone__text {
  margin: 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.upload-zone__hint {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.upload-zone__input {
  display: none;
}

/* Upload queue */
.upload-queue {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  margin-top: var(--spacing-4);
}

.upload-queue__item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-2) var(--spacing-3);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  gap: var(--spacing-3);
}

.upload-queue__info {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-width: 0;
}

.upload-queue__file-icon {
  color: var(--color-accent);
  flex-shrink: 0;
}

.upload-queue__name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.upload-queue__status {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-shrink: 0;
}

.upload-queue__progress-bar {
  width: 100px;
  height: 6px;
  background-color: var(--color-border-light);
  border-radius: var(--border-radius-full);
  overflow: hidden;
}

.upload-queue__progress-fill {
  height: 100%;
  background-color: var(--color-accent);
  border-radius: var(--border-radius-full);
  transition: width var(--transition-fast);
}

.upload-queue__percent {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  min-width: 32px;
  text-align: right;
}

.upload-queue__done {
  color: var(--color-success);
}

.upload-queue__error {
  font-size: var(--font-size-xs);
  color: var(--color-danger);
}

/* File list overrides */
.file-badge {
  display: inline-flex;
  align-items: center;
  padding: 1px var(--spacing-2);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-accent);
  background-color: var(--color-accent-subtle);
  border-radius: var(--border-radius-sm);
}

.file-meta {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}
</style>
