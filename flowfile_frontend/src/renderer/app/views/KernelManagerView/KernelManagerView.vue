<template>
  <div class="kernel-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Kernel Manager</h2>
      <p class="page-description">Manage Python execution environments for your flows</p>
    </div>

    <!-- Docker / service error banner -->
    <div v-if="errorMessage" class="error-banner mb-3">
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
            @start="handleStart"
            @stop="handleStop"
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
  createKernel,
  startKernel,
  stopKernel,
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
  max-width: 1000px;
}

.kernel-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
  gap: var(--spacing-3);
}

.error-banner {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  border: 1px solid var(--color-danger);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
}
</style>
