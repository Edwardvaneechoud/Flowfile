<template>
  <div class="kernel-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Kernel Manager</h2>
      <p class="page-description">Manage Python execution environments for your flows</p>
    </div>

    <!-- Docker status banners -->
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
    <div
      v-else-if="dockerStatus && dockerStatus.available && visibleMissingImages.length > 0"
      class="status-banner status-banner--warning mb-3"
    >
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div class="missing-images">
        <strong>
          {{
            visibleMissingImages.length === 1
              ? "Kernel image not pulled."
              : "Kernel images not pulled."
          }}
        </strong>
        <p class="missing-images__hint">
          {{ missingHintText }}
        </p>
        <ul class="missing-images__list">
          <li v-for="img in visibleMissingImages" :key="img.image">
            <span class="missing-images__flavour">{{ flavourLabel(img.flavour) }}</span>
            <code class="missing-images__cmd">docker pull {{ img.image }}</code>
            <button
              type="button"
              class="missing-images__copy"
              :title="copiedImage === img.image ? 'Copied!' : 'Copy command'"
              @click="copyPullCommand(img.image)"
            >
              <i
                :class="copiedImage === img.image ? 'fa-solid fa-check' : 'fa-regular fa-copy'"
              ></i>
            </button>
            <button
              v-if="img.flavour !== 'base' || baseAvailable"
              type="button"
              class="missing-images__dismiss"
              title="Hide for this image tag"
              @click="dismissImage(img.image)"
            >
              <i class="fa-solid fa-xmark"></i>
            </button>
          </li>
        </ul>
      </div>
    </div>

    <!-- API-level error (e.g. network failure) -->
    <div
      v-if="errorMessage && (!dockerStatus || dockerStatus.available)"
      class="status-banner status-banner--error mb-3"
    >
      <i class="fa-solid fa-circle-exclamation"></i>
      <span>{{ errorMessage }}</span>
    </div>

    <!-- Create Kernel Form -->
    <CreateKernelForm :flavour-info="flavourInfo" :on-create="handleCreate" />

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
            :flavour-info="flavourInfo"
            @start="handleStart"
            @stop="handleStop"
            @details="openDetails"
            @delete="confirmDelete"
          />
        </div>
      </div>
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
import { computed, ref } from "vue";
import {
  KERNEL_FLAVOURS,
  type ImageFlavour,
  type KernelConfig,
  type KernelImageStatus,
  type KernelInfo,
} from "../../types";
import { useKernelManager } from "./useKernelManager";
import CreateKernelForm from "./CreateKernelForm.vue";
import KernelCard from "./KernelCard.vue";
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
  // Re-throws so the modal can surface the error inline; parent doesn't alert.
  await updateKernel(kernelId, { packages });
};

const missingImages = computed<KernelImageStatus[]>(() => {
  const imgs = dockerStatus.value?.images ?? [];
  return imgs.filter((i) => !i.available);
});

// Persisted dismissals are keyed by the full image tag (e.g.
// edwardvaneechoud/flowfile-kernel-ml:0.3.0) so that bumping the version
// re-surfaces the banner — the new tag is a different key.
const DISMISSED_KEY = "flowfile.kernel.dismissedMissingImages";

const loadDismissed = (): Set<string> => {
  try {
    const raw = localStorage.getItem(DISMISSED_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    return new Set(Array.isArray(parsed) ? parsed : []);
  } catch {
    return new Set();
  }
};

const dismissedImages = ref<Set<string>>(loadDismissed());

const persistDismissed = () => {
  try {
    localStorage.setItem(DISMISSED_KEY, JSON.stringify([...dismissedImages.value]));
  } catch (err) {
    console.warn("Could not persist dismissed kernel image set:", err);
  }
};

const dismissImage = (image: string) => {
  // Reactive Set assignment — replace so Vue picks up the change.
  const next = new Set(dismissedImages.value);
  next.add(image);
  dismissedImages.value = next;
  persistDismissed();
};

const baseAvailable = computed<boolean>(() => {
  const imgs = dockerStatus.value?.images ?? [];
  return imgs.some((i) => i.flavour === "base" && i.available);
});

const visibleMissingImages = computed<KernelImageStatus[]>(() =>
  missingImages.value.filter((i) => !dismissedImages.value.has(i.image)),
);

const missingHintText = computed<string>(() => {
  const total = dockerStatus.value?.images.length ?? 0;
  if (visibleMissingImages.value.length === total && total > 0) {
    return "No kernel images are available locally yet. Pull at least one before creating a kernel:";
  }
  if (baseAvailable.value) {
    return "Optional flavours aren't available locally. Pull what you plan to use, or dismiss what you don't:";
  }
  return "Some kernel flavours are not available locally. Run the matching pull command:";
});

const flavourLabel = (flavour: ImageFlavour): string =>
  KERNEL_FLAVOURS.find((f) => f.value === flavour)?.label ?? flavour;

const copiedImage = ref<string | null>(null);
const copyPullCommand = async (image: string) => {
  const cmd = `docker pull ${image}`;
  try {
    await navigator.clipboard.writeText(cmd);
    copiedImage.value = image;
    setTimeout(() => {
      if (copiedImage.value === image) copiedImage.value = null;
    }, 1800);
  } catch (err) {
    console.error("Clipboard copy failed", err);
  }
};

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
    throw error; // Re-throw so the form keeps the user's input for retry.
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

.missing-images {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.missing-images__hint {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.missing-images__list {
  list-style: none;
  margin: var(--spacing-2) 0 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1-5);
}

.missing-images__list li {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-wrap: wrap;
}

.missing-images__flavour {
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 0 var(--spacing-1);
  border: 1px solid currentColor;
  border-radius: var(--border-radius-sm);
  flex-shrink: 0;
}

.missing-images__cmd {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  background-color: rgba(0, 0, 0, 0.06);
  padding: var(--spacing-0-5) var(--spacing-1-5);
  border-radius: var(--border-radius-sm);
  flex-grow: 1;
  user-select: all;
  word-break: break-all;
}

.missing-images__copy,
.missing-images__dismiss {
  background: transparent;
  border: 1px solid var(--color-warning);
  color: var(--color-warning-dark);
  border-radius: var(--border-radius-sm);
  padding: var(--spacing-0-5) var(--spacing-1-5);
  cursor: pointer;
  font-size: var(--font-size-xs);
  flex-shrink: 0;
}

.missing-images__copy:hover,
.missing-images__dismiss:hover {
  /* Banner bg is constant pale yellow in both themes, so a small dark tint
     reads as a slightly darker yellow patch under either palette. */
  background-color: rgba(0, 0, 0, 0.05);
}

.missing-images__dismiss {
  border-style: dashed;
  opacity: 0.75;
}

.missing-images__dismiss:hover {
  opacity: 1;
}
</style>
