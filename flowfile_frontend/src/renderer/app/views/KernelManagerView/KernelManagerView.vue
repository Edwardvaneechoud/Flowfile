<template>
  <div class="kernel-manager-container">
    <!-- Page header -->
    <div class="mb-3">
      <h2 class="page-title">Kernel Manager</h2>
      <p class="page-description">Manage Python execution environments for your flows</p>
    </div>

    <!-- Docker status banner (only when down) -->
    <div
      v-if="dockerStatus && !dockerStatus.available"
      class="status-banner status-banner--error mb-3"
    >
      <i class="fa-solid fa-circle-exclamation"></i>
      <div>
        <strong>Docker is not running.</strong>
        Kernels require Docker to create and run containers. Please start Docker and reload this
        page.
      </div>
    </div>
    <!-- Network / API error banner — page-level, full width -->
    <div
      v-if="errorMessage && (!dockerStatus || dockerStatus.available)"
      class="status-banner status-banner--error mb-3"
    >
      <i class="fa-solid fa-circle-exclamation"></i>
      <span>{{ errorMessage }}</span>
    </div>

    <!-- Stats overview -->
    <div class="km-stats">
      <div class="km-stat km-stat--total">
        <div class="km-stat__icon">
          <i class="fa-solid fa-server"></i>
        </div>
        <div class="km-stat__body">
          <div class="km-stat__value">{{ totalKernels }}</div>
          <div class="km-stat__label">Total kernels</div>
        </div>
      </div>

      <div class="km-stat km-stat--active">
        <div class="km-stat__icon">
          <i class="fa-solid fa-bolt"></i>
        </div>
        <div class="km-stat__body">
          <div class="km-stat__value">
            {{ activeKernels }}<span class="km-stat__value-suffix">/ {{ totalKernels }}</span>
          </div>
          <div class="km-stat__label">Active now</div>
        </div>
      </div>

      <div class="km-stat km-stat--memory">
        <div class="km-stat__icon">
          <i class="fa-solid fa-memory"></i>
        </div>
        <div class="km-stat__body">
          <div class="km-stat__value">{{ memoryDisplay }}</div>
          <div class="km-stat__label">Memory in use</div>
        </div>
      </div>

      <div class="km-stat" :class="dockerStatus?.available ? 'km-stat--ok' : 'km-stat--down'">
        <div class="km-stat__icon">
          <i
            :class="
              dockerStatus?.available ? 'fa-brands fa-docker' : 'fa-solid fa-plug-circle-xmark'
            "
          ></i>
        </div>
        <div class="km-stat__body">
          <div class="km-stat__value">
            {{ dockerStatus === null ? "…" : dockerStatus.available ? "Connected" : "Unavailable" }}
          </div>
          <div class="km-stat__label">Docker engine</div>
        </div>
      </div>
    </div>

    <!-- Two-column layout: status sidebar (left) + create form (right) -->
    <div class="km-grid">
      <KernelStatusSidebar
        :docker-status="dockerStatus"
        :kernels="kernels"
        :memory-stats="memoryStats"
        :is-action-in-progress="isActionInProgress"
        :is-loading="isLoading"
        @start="handleStart"
        @stop="handleStop"
        @details="openDetails"
        @delete="confirmDelete"
        @refresh-status="checkDockerStatus"
      />

      <main class="km-main">
        <CreateKernelForm
          :flavour-info="flavourInfo"
          :image-statuses="dockerStatus?.images ?? []"
          :on-create="handleCreate"
        />
      </main>
    </div>

    <!-- Details Modal -->
    <KernelDetailsModal
      v-if="detailsKernel"
      :kernel="detailsKernel"
      :flavour-info="flavourInfo"
      :on-save="handleSavePackages"
      @close="closeDetails"
    />

    <!-- Delete Confirmation Modal -->
    <div v-if="showDeleteModal" class="modal-overlay" @click="cancelDelete">
      <div class="modal-container km-modal" @click.stop>
        <div class="modal-header km-modal__header">
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
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import type { KernelConfig, KernelInfo } from "../../types";
import { useKernelManager } from "./useKernelManager";
import CreateKernelForm from "./CreateKernelForm.vue";
import KernelStatusSidebar from "./KernelStatusSidebar.vue";
import KernelDetailsModal from "./KernelDetailsModal.vue";

const {
  kernels,
  isLoading,
  errorMessage,
  dockerStatus,
  memoryStats,
  flavourInfo,
  createKernel,
  updateKernel,
  startKernel,
  stopKernel,
  deleteKernel,
  isActionInProgress,
  checkDockerStatus,
} = useKernelManager();

// Details modal state — keyed by kernel id, kept reactive against the polled list.
const detailsKernelId = ref<string | null>(null);
const detailsKernel = computed<KernelInfo | null>(
  () => kernels.value.find((k) => k.id === detailsKernelId.value) ?? null,
);

const openDetails = (kernelId: string) => {
  detailsKernelId.value = kernelId;
};

const closeDetails = () => {
  detailsKernelId.value = null;
};

const handleSavePackages = async (kernelId: string, packages: string[]): Promise<void> => {
  // Re-throws so the modal can surface the error inline; parent doesn't toast.
  await updateKernel(kernelId, { packages });
};

// ---- stats derivations --------------------------------------------------

const totalKernels = computed(() => kernels.value.length);

const activeKernels = computed(
  () => kernels.value.filter((k) => k.state === "idle" || k.state === "executing").length,
);

const formatBytes = (bytes: number): string => {
  if (!bytes) return "0 B";
  const gb = bytes / (1024 * 1024 * 1024);
  if (gb >= 1) return `${gb.toFixed(1)} GB`;
  const mb = bytes / (1024 * 1024);
  if (mb >= 1) return `${mb.toFixed(0)} MB`;
  return `${bytes} B`;
};

const memoryDisplay = computed(() => {
  const total = Object.values(memoryStats.value).reduce(
    (sum, info) => sum + (info?.used_bytes ?? 0),
    0,
  );
  if (total === 0) return "—";
  return formatBytes(total);
});

// Delete confirmation state
const showDeleteModal = ref(false);
const deleteTarget = ref({ id: "", name: "" });
const isDeleting = ref(false);

const handleCreate = async (config: KernelConfig) => {
  try {
    await createKernel(config);
    ElMessage.success(`Kernel "${config.name}" created`);
  } catch (error: any) {
    const msg = error.message || "Failed to create kernel.";
    ElMessage.error({ message: `Error creating kernel: ${msg}` });
    throw error; // Re-throw so the form keeps the user's input for retry.
  }
};

const handleStart = async (kernelId: string) => {
  try {
    await startKernel(kernelId);
  } catch (error: any) {
    ElMessage.error({ message: error.message || "Failed to start kernel." });
  }
};

const handleStop = async (kernelId: string) => {
  try {
    await stopKernel(kernelId);
  } catch (error: any) {
    ElMessage.error({ message: error.message || "Failed to stop kernel." });
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
    ElMessage.success(`Kernel "${name}" deleted`);
  } catch {
    ElMessage.error({ message: "Failed to delete kernel. Please try again." });
    cancelDelete();
  } finally {
    isDeleting.value = false;
  }
};
</script>

<style scoped>
.kernel-manager-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

/* ─── Stats overview ──────────────────────────────────────────────────── */

.km-stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-4);
}

.km-stat {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-xs);
  transition:
    transform var(--transition-base) var(--transition-timing),
    box-shadow var(--transition-base) var(--transition-timing);
}

.km-stat:hover {
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.km-stat__icon {
  width: 40px;
  height: 40px;
  border-radius: var(--border-radius-md);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 16px;
  flex-shrink: 0;
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
}

.km-stat--down .km-stat__icon {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.km-stat__body {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.km-stat__value {
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  line-height: 1.1;
}

.km-stat__value-suffix {
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  margin-left: var(--spacing-1);
}

.km-stat__label {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  margin-top: var(--spacing-0-5);
}

/* ─── Two-column body ─────────────────────────────────────────────────── */

.km-grid {
  display: grid;
  grid-template-columns: 340px 1fr;
  gap: var(--spacing-4);
  align-items: flex-start;
}

@media (max-width: 900px) {
  .km-grid {
    grid-template-columns: 1fr;
  }
  .km-hero {
    padding: var(--spacing-4);
  }
  .km-hero__title {
    font-size: var(--font-size-2xl);
  }
}

.km-main {
  min-width: 0;
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

.status-banner--error {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  border: 1px solid var(--color-danger);
}

/* ─── Modal polish ────────────────────────────────────────────────────── */

.km-modal {
  border-radius: var(--border-radius-xl);
  box-shadow: var(--shadow-lg);
  overflow: hidden;
}

.km-modal__header {
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.06) 0%, rgba(102, 126, 234, 0.04) 100%);
}

[data-theme="dark"] .km-modal__header {
  background: linear-gradient(135deg, rgba(8, 145, 178, 0.12) 0%, rgba(102, 126, 234, 0.1) 100%);
}
</style>
