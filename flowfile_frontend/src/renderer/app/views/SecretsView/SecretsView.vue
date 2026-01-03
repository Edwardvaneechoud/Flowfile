<template>
  <div class="secret-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Secret Manager</h2>
      <p class="page-description">Securely store and manage credentials for your integrations</p>
    </div>

    <!-- Add Secret Card -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Add New Secret</h3>
      </div>
      <div class="card-content">
        <form class="form" @submit.prevent="handleAddSecret">
          <div class="form-grid">
            <div class="form-field">
              <label for="secret-name" class="form-label">Secret Name</label>
              <input
                id="secret-name"
                v-model="newSecret.name"
                type="text"
                class="form-input"
                placeholder="api_key, database_password, etc."
                required
              />
            </div>

            <div class="form-field">
              <label for="secret-value" class="form-label">Secret Value</label>
              <div class="password-field">
                <input
                  id="secret-value"
                  v-model="newSecret.value"
                  :type="showNewSecret ? 'text' : 'password'"
                  class="form-input"
                  placeholder="Enter secret value"
                  required
                />
                <button
                  type="button"
                  class="toggle-visibility"
                  aria-label="Toggle new secret visibility"
                  @click="showNewSecret = !showNewSecret"
                >
                  <i :class="showNewSecret ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
                </button>
              </div>
            </div>
          </div>

          <div class="form-actions">
            <button
              type="submit"
              class="btn btn-primary"
              :disabled="!newSecret.name || !newSecret.value || isSubmitting"
            >
              <i class="fa-solid fa-plus"></i>
              {{ isSubmitting ? "Adding..." : "Add Secret" }}
            </button>
          </div>
        </form>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Your Secrets ({{ filteredSecrets.length }})</h3>
        <div v-if="secrets.length > 0" class="search-container">
          <input
            v-model="searchTerm"
            type="text"
            placeholder="Search secrets..."
            class="search-input"
            aria-label="Search secrets"
          />
          <i class="fa-solid fa-search search-icon"></i>
        </div>
      </div>
      <div class="card-content">
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading secrets...</p>
        </div>

        <div v-else-if="!isLoading && secrets.length === 0" class="empty-state">
          <i class="fa-solid fa-lock"></i>
          <p>You haven't added any secrets yet</p>
          <p>Secrets are securely stored and can be used in your flows</p>
        </div>

        <!-- Secrets List -->
        <div v-else-if="filteredSecrets.length > 0" class="secrets-list">
          <div v-for="secret in filteredSecrets" :key="secret.name" class="secret-item">
            <div class="secret-name">
              <i class="fa-solid fa-key"></i>
              <span>{{ secret.name }}</span>
            </div>
            <div class="secret-value">
              <input
                type="password"
                value="••••••••••••••••"
                readonly
                class="form-input"
                aria-label="Masked secret value"
              />
            </div>
            <div class="secret-actions">
              <button
                type="button"
                class="btn btn-danger"
                :aria-label="`Delete secret ${secret.name}`"
                @click="handleConfirmDelete(secret.name)"
              >
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>
        </div>

        <!-- No Results State -->
        <div v-else class="empty-state">
          <i class="fa-solid fa-search"></i>
          <p>No secrets found matching "{{ searchTerm }}"</p>
        </div>
      </div>
    </div>

    <!-- Delete Confirmation Modal -->
    <div v-if="showDeleteModal" class="modal-overlay" @click="cancelDelete">
      <div class="modal-container" @click.stop>
        <div class="modal-header">
          <h3 class="modal-title">Delete Secret</h3>
          <button class="modal-close" aria-label="Close delete confirmation" @click="cancelDelete">
            <i class="fa-solid fa-times"></i>
          </button>
        </div>
        <div class="modal-content">
          <p>
            Are you sure you want to delete the secret <strong>{{ secretToDelete }}</strong
            >?
          </p>
          <p class="warning-text">
            This action cannot be undone and may break any flows that use this secret.
          </p>
        </div>
        <div class="modal-actions">
          <button class="btn btn-secondary" @click="cancelDelete">Cancel</button>
          <button class="btn btn-danger-filled" :disabled="isDeleting" @click="handleDeleteSecret">
            <i v-if="isDeleting" class="fas fa-spinner fa-spin"></i>
            {{ isDeleting ? "Deleting..." : "Delete Secret" }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import type { SecretInput } from "./secretTypes";
import { useSecretManager } from "./useSecretManager";

// Use our secrets composable
const { secrets, filteredSecrets, isLoading, searchTerm, loadSecrets, addSecret, deleteSecret } =
  useSecretManager();

// Local component state
const newSecret = ref<SecretInput>({ name: "", value: "" });
const showNewSecret = ref(false);
const isSubmitting = ref(false);
const isDeleting = ref(false);
const showDeleteModal = ref(false);
const secretToDelete = ref("");

// Add a new secret
const handleAddSecret = async () => {
  if (!newSecret.value.name || !newSecret.value.value) return;

  isSubmitting.value = true;
  try {
    const secretName = await addSecret(newSecret.value);
    newSecret.value = { name: "", value: "" };
    showNewSecret.value = false;
    alert(`Secret "${secretName}" added successfully.`);
  } catch (error: any) {
    const errorMsg = error.message || "An unknown error occurred while adding the secret.";
    alert(`Error adding secret: ${errorMsg}`);
  } finally {
    isSubmitting.value = false;
  }
};

// Confirm deletion of a secret
const handleConfirmDelete = (secretName: string) => {
  secretToDelete.value = secretName;
  showDeleteModal.value = true;
};

// Cancel delete operation
const cancelDelete = () => {
  showDeleteModal.value = false;
  secretToDelete.value = "";
};

// Delete a secret after confirmation
const handleDeleteSecret = async () => {
  if (!secretToDelete.value) return;

  isDeleting.value = true;
  try {
    const nameToDelete = secretToDelete.value;
    await deleteSecret(nameToDelete);
    cancelDelete(); // Close modal and clear state
    alert(`Secret "${nameToDelete}" deleted successfully.`);
  } catch (error) {
    alert("Failed to delete secret. Please try again.");
    cancelDelete();
  } finally {
    isDeleting.value = false;
  }
};

// Load secrets when component mounts
onMounted(() => {
  loadSecrets().catch(() => {
    alert("Failed to load secrets. Please try again.");
  });
});
</script>

<style scoped>
.secrets-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.secret-item {
  display: flex !important;
  flex-direction: row !important;
  align-items: center !important;
  padding: var(--spacing-2) var(--spacing-4);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  gap: var(--spacing-4);
}

.secret-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  flex-shrink: 0;
  white-space: nowrap;
}

.secret-name i {
  color: var(--color-accent);
}

.secret-value {
  flex: 1;
  min-width: 100px;
  max-width: 250px;
}

.secret-value .form-input {
  background-color: var(--color-background-muted);
  height: 32px;
}

.secret-actions {
  flex-shrink: 0;
  margin-left: auto;
}
</style>
