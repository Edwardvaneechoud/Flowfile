<template>
  <div class="kernel-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Kernel Manager</h2>
      <p class="page-description">Manage Python execution environments for your flows</p>
    </div>

    <!-- Docker status banners -->
    <div v-if="dockerStatus && !dockerStatus.available" class="status-banner status-banner--error mb-3">
      <i class="fa-solid fa-circle-exclamation"></i>
      <div>
        <strong>Docker is not running.</strong>
        Kernels require Docker to create and run containers. Please start Docker and reload this page.
      </div>
    </div>
    <div
      v-else-if="dockerStatus && dockerStatus.available && !dockerStatus.image_available"
      class="status-banner status-banner--warning mb-3"
    >
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div>
        <strong>Kernel image not found.</strong>
        The <code>flowfile-kernel</code> Docker image is not available. Build or pull the image before
        starting kernels.
      </div>
    </div>

    <!-- API-level error (e.g. network failure) -->
    <div v-if="errorMessage && (!dockerStatus || dockerStatus.available)" class="status-banner status-banner--error mb-3">
      <i class="fa-solid fa-circle-exclamation"></i>
      <span>{{ errorMessage }}</span>
    </div>

    <!-- Create Kernel Form -->
    <CreateKernelForm @create="handleCreate" />

    <!-- Kernel List -->
    <div class="card">
      <div class="card-header">
        <h3 class="card-title">Your Kernels ({{ kernels.length }})</h3>
      </div>
      <div class="card-content">
        <!-- Loading state -->
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading kernels...</p>
        </div>

        <!-- Empty state -->
        <div v-else-if="kernels.length === 0 && !errorMessage" class="empty-state">
          <i class="fa-solid fa-server"></i>
          <p>No kernels configured yet</p>
          <p>Create a kernel above to start running Python code in your flows</p>
        </div>

        <!-- Kernel grid -->
        <div v-else class="kernel-grid">
          <KernelCard
            v-for="kernel in kernels"
            :key="kernel.id"
            :kernel="kernel"
            :busy="isActionInProgress(kernel.id)"
            :memory-info="memoryStats[kernel.id] ?? null"
            @start="handleStart"
            @stop="handleStop"
            @restart="handleRestart"
            @delete="confirmDelete"
          />
        </div>
      </div>
    </div>

    <!-- Delete Confirmation Modal -->
    <div v-if="showDeleteModal" class="modal-overlay" @click="cancelDelete">
      <div class="modal-container" @click.stop>
        <div class="modal-header">
          <h3 class="modal-title">Delete Kernel</h3>
          <button class="modal-close" aria-label="Close" @click="cancelDelete">
            <i class="fa-solid fa-times"></i>
          </button>
        </div>
        <div class="modal-content">
          <p>
            Are you sure you want to delete the kernel
            <strong>{{ deleteTarget.name }}</strong
            >?
          </p>
          <p class="warning-text">
            This will stop and remove the container. This action cannot be undone.
          </p>
        </div>
        <div class="modal-actions">
          <button class="btn btn-secondary" @click="cancelDelete">Cancel</button>
          <button class="btn btn-danger-filled" :disabled="isDeleting" @click="handleDelete">
            <i v-if="isDeleting" class="fas fa-spinner fa-spin"></i>
            {{ isDeleting ? "Deleting..." : "Delete Kernel" }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import type { KernelConfig } from "../../types";
import { useKernelManager } from "./useKernelManager";
import CreateKernelForm from "./CreateKernelForm.vue";
import KernelCard from "./KernelCard.vue";

const {
  kernels,
  isLoading,
  errorMessage,
  dockerStatus,
  memoryStats,
  createKernel,
  startKernel,
  stopKernel,
  restartKernel,
  deleteKernel,
  isActionInProgress,
} = useKernelManager();

// Delete confirmation state
const showDeleteModal = ref(false);
const deleteTarget = ref({ id: "", name: "" });
const isDeleting = ref(false);

const handleCreate = async (config: KernelConfig) => {
  try {
    await createKernel(config);
    alert(`Kernel "${config.name}" created successfully.`);
  } catch (error: any) {
    const msg = error.message || "Failed to create kernel.";
    alert(`Error creating kernel: ${msg}`);
  }
};

const handleStart = async (kernelId: string) => {
  try {
    await startKernel(kernelId);
  } catch (error: any) {
    const msg = error.message || "Failed to start kernel.";
    alert(`Error: ${msg}`);
  }
};

const handleStop = async (kernelId: string) => {
  try {
    await stopKernel(kernelId);
  } catch (error: any) {
    alert(`Error: ${error.message || "Failed to stop kernel."}`);
  }
};

const handleRestart = async (kernelId: string) => {
  try {
    await restartKernel(kernelId);
  } catch (error: any) {
    alert(`Error: ${error.message || "Failed to restart kernel."}`);
  }
};

const confirmDelete = (kernelId: string, kernelName: string) => {
  deleteTarget.value = { id: kernelId, name: kernelName };
  showDeleteModal.value = true;
};

const cancelDelete = () => {
  showDeleteModal.value = false;
  deleteTarget.value = { id: "", name: "" };
};

const handleDelete = async () => {
  if (!deleteTarget.value.id) return;
  isDeleting.value = true;
  try {
    const name = deleteTarget.value.name;
    await deleteKernel(deleteTarget.value.id);
    cancelDelete();
    alert(`Kernel "${name}" deleted successfully.`);
  } catch {
    alert("Failed to delete kernel. Please try again.");
    cancelDelete();
  } finally {
    isDeleting.value = false;
  }
};
</script>

<style scoped>
.kernel-manager-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

.kernel-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
  gap: var(--spacing-3);
}

.status-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
}

.status-banner i {
  margin-top: 2px;
  flex-shrink: 0;
}

.status-banner code {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  background-color: rgba(0, 0, 0, 0.06);
  padding: 1px 4px;
  border-radius: var(--border-radius-sm);
}

.status-banner--error {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  border: 1px solid var(--color-danger);
}

.status-banner--warning {
  background-color: var(--color-warning-light);
  color: var(--color-warning-dark);
  border: 1px solid var(--color-warning);
}
</style>
