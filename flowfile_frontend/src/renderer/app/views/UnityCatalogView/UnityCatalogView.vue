<template>
  <div class="cloud-connection-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Unity Catalog Connections</h2>
      <p class="description-text">
        Connect to Unity Catalog servers to discover and access tables with automatic credential
        management. Unity Catalog provides a unified governance layer for your data lakehouse.
      </p>
    </div>

    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Your Connections ({{ connections.length }})</h3>
        <button class="btn btn-primary" @click="showAddModal">
          <i class="fa-solid fa-plus"></i> Add Connection
        </button>
      </div>
      <div class="card-content">
        <div class="info-box mb-3">
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <p><strong>What is Unity Catalog?</strong></p>
            <p>
              Unity Catalog is an open-source data catalog that provides a three-level namespace
              (catalog.schema.table) with built-in governance. When you connect Flowfile to a UC
              server, it handles storage credentials automatically via credential vending — you
              only need the UC server URL and an auth token.
            </p>
          </div>
        </div>

        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading connections...</p>
        </div>

        <div v-else-if="connections.length === 0" class="empty-state">
          <i class="fa-solid fa-layer-group"></i>
          <p>No Unity Catalog connections configured yet</p>
          <p class="hint-text">
            Click "Add Connection" to connect to your Unity Catalog server.
          </p>
        </div>

        <div v-else class="connections-list">
          <div
            v-for="conn in connections"
            :key="conn.connectionName"
            class="connection-item"
          >
            <div class="connection-info">
              <div class="connection-name">
                <i class="fa-solid fa-layer-group"></i>
                <span>{{ conn.connectionName }}</span>
                <span class="badge">Unity Catalog</span>
                <span v-if="conn.credentialVendingEnabled" class="badge auth-badge">
                  Credential Vending
                </span>
              </div>
              <div class="connection-details">
                <span>{{ conn.serverUrl }}</span>
                <span v-if="conn.defaultCatalog">
                  <span class="separator">&#8226;</span>
                  Default: {{ conn.defaultCatalog }}
                </span>
              </div>
            </div>
            <div class="connection-actions">
              <button type="button" class="btn btn-secondary" @click="showEditModal(conn)">
                <i class="fa-solid fa-edit"></i>
                <span>Modify</span>
              </button>
              <button
                type="button"
                class="btn btn-danger"
                @click="showDeleteModal(conn.connectionName)"
              >
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Add/Edit Dialog -->
    <el-dialog
      v-model="dialogVisible"
      :title="isEditing ? 'Edit Unity Catalog Connection' : 'Add Unity Catalog Connection'"
      width="600px"
      :before-close="handleCloseDialog"
    >
      <div class="modal-description mb-3">
        <p>
          Enter the URL of your Unity Catalog server and an optional authentication token.
          Credential vending lets UC handle storage credentials automatically.
        </p>
      </div>
      <form class="form" @submit.prevent="handleFormSubmit">
        <div class="form-grid">
          <div class="form-field">
            <label class="form-label">Connection Name</label>
            <input
              v-model="formData.connectionName"
              type="text"
              class="form-input"
              placeholder="my_unity_catalog"
              required
            />
          </div>
          <div class="form-field">
            <label class="form-label">Server URL</label>
            <input
              v-model="formData.serverUrl"
              type="text"
              class="form-input"
              placeholder="http://localhost:8080"
              required
            />
          </div>
          <div class="form-field">
            <label class="form-label">Auth Token (optional)</label>
            <div class="password-field">
              <input
                v-model="formData.authToken"
                :type="showToken ? 'text' : 'password'"
                class="form-input"
                placeholder="Bearer token for UC server"
              />
              <button
                type="button"
                class="toggle-visibility"
                @click="showToken = !showToken"
              >
                <i :class="showToken ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
              </button>
            </div>
          </div>
          <div class="form-field">
            <label class="form-label">Default Catalog (optional)</label>
            <input
              v-model="formData.defaultCatalog"
              type="text"
              class="form-input"
              placeholder="unity"
            />
          </div>
          <div class="form-field">
            <div class="checkbox-container">
              <input
                v-model="formData.credentialVendingEnabled"
                type="checkbox"
                class="checkbox-input"
                id="cred-vending"
              />
              <label for="cred-vending" class="form-label">
                Enable Credential Vending (recommended)
              </label>
            </div>
          </div>
        </div>
        <div class="form-actions">
          <button type="button" class="btn btn-secondary" @click="handleTestConnection">
            <i class="fa-solid fa-plug"></i>
            {{ testingConnection ? 'Testing...' : 'Test Connection' }}
          </button>
          <div>
            <button type="button" class="btn btn-secondary" @click="dialogVisible = false">
              Cancel
            </button>
            <button type="submit" class="btn btn-primary" :disabled="!isFormValid || isSubmitting">
              {{ isSubmitting ? 'Saving...' : (isEditing ? 'Update' : 'Create') }}
            </button>
          </div>
        </div>
      </form>
    </el-dialog>

    <!-- Delete Confirmation -->
    <el-dialog
      v-model="deleteDialogVisible"
      title="Delete Connection"
      width="400px"
    >
      <p>
        Are you sure you want to delete <strong>{{ connectionToDelete }}</strong>?
      </p>
      <p class="warning-text">
        This will remove the connection. Any flows using this connection will need to be updated.
      </p>
      <template #footer>
        <el-button @click="deleteDialogVisible = false">Cancel</el-button>
        <el-button type="danger" :loading="isDeleting" @click="handleDelete">
          Delete
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted } from "vue";
import { ElDialog, ElButton, ElMessage } from "element-plus";
import {
  fetchUcConnections,
  createUcConnection,
  deleteUcConnection,
  testUcConnection,
} from "./api";
import type {
  UnityCatalogConnectionInterface,
  UnityCatalogConnectionInput,
} from "./UnityCatalogTypes";

const connections = ref<UnityCatalogConnectionInterface[]>([]);
const isLoading = ref(true);
const dialogVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditing = ref(false);
const isSubmitting = ref(false);
const isDeleting = ref(false);
const testingConnection = ref(false);
const connectionToDelete = ref("");
const showToken = ref(false);

const defaultForm = (): UnityCatalogConnectionInput => ({
  connectionName: "",
  serverUrl: "",
  authToken: "",
  defaultCatalog: "",
  credentialVendingEnabled: true,
});

const formData = ref<UnityCatalogConnectionInput>(defaultForm());

const isFormValid = computed(() => {
  return !!formData.value.connectionName && !!formData.value.serverUrl;
});

const loadConnections = async () => {
  isLoading.value = true;
  try {
    connections.value = await fetchUcConnections();
  } catch (error) {
    ElMessage.error("Failed to load Unity Catalog connections");
  } finally {
    isLoading.value = false;
  }
};

const showAddModal = () => {
  isEditing.value = false;
  formData.value = defaultForm();
  dialogVisible.value = true;
};

const showEditModal = (conn: UnityCatalogConnectionInterface) => {
  isEditing.value = true;
  formData.value = {
    connectionName: conn.connectionName,
    serverUrl: conn.serverUrl,
    authToken: "",
    defaultCatalog: conn.defaultCatalog || "",
    credentialVendingEnabled: conn.credentialVendingEnabled,
  };
  dialogVisible.value = true;
};

const showDeleteModal = (name: string) => {
  connectionToDelete.value = name;
  deleteDialogVisible.value = true;
};

const handleFormSubmit = async () => {
  isSubmitting.value = true;
  try {
    await createUcConnection(formData.value);
    await loadConnections();
    dialogVisible.value = false;
    ElMessage.success(`Connection ${isEditing.value ? "updated" : "created"} successfully`);
  } catch (error: any) {
    ElMessage.error(error.message || "Failed to save connection");
  } finally {
    isSubmitting.value = false;
  }
};

const handleTestConnection = async () => {
  testingConnection.value = true;
  try {
    const result = await testUcConnection(formData.value);
    if (result.success) {
      ElMessage.success("Connection successful!");
    } else {
      ElMessage.error(`Connection failed: ${result.message}`);
    }
  } catch {
    ElMessage.error("Connection test failed");
  } finally {
    testingConnection.value = false;
  }
};

const handleDelete = async () => {
  isDeleting.value = true;
  try {
    await deleteUcConnection(connectionToDelete.value);
    await loadConnections();
    deleteDialogVisible.value = false;
    ElMessage.success("Connection deleted");
  } catch {
    ElMessage.error("Failed to delete connection");
  } finally {
    isDeleting.value = false;
  }
};

const handleCloseDialog = (done: () => void) => {
  if (!isSubmitting.value) done();
};

onMounted(() => loadConnections());
</script>

<style scoped>
.description-text {
  color: var(--color-text-secondary);
  margin-top: var(--spacing-2);
  font-size: var(--font-size-sm);
}
.info-box {
  display: flex;
  gap: var(--spacing-4);
  padding: var(--spacing-4);
  background-color: var(--color-background-muted);
  border-left: 4px solid var(--color-accent);
  border-radius: var(--border-radius-md);
}
.info-box i { color: var(--color-accent); font-size: var(--font-size-2xl); margin-top: var(--spacing-2); }
.info-box p { margin: 0 0 var(--spacing-2) 0; font-size: var(--font-size-sm); color: var(--color-text-secondary); }
.info-box p:last-child { margin-bottom: 0; }
.info-box p strong { color: var(--color-text-primary); }
.modal-description { color: var(--color-text-secondary); font-size: var(--font-size-sm); }
.badge {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-full);
  padding: var(--spacing-1) var(--spacing-3);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  margin-left: var(--spacing-2);
}
.auth-badge { background-color: var(--color-info-light); color: var(--color-info); }
.connection-details {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  margin-top: var(--spacing-1);
}
.separator { margin: 0 var(--spacing-2); }
.hint-text { color: var(--color-text-tertiary); font-size: var(--font-size-sm); margin-top: var(--spacing-2); }
.form-grid { display: flex; flex-direction: column; gap: 1rem; }
.form-field { display: flex; flex-direction: column; gap: 0.25rem; }
.form-label { font-size: 0.875rem; font-weight: 500; color: var(--color-text-primary); }
.form-input {
  padding: 0.5rem;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  font-size: 0.875rem;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
}
.password-field { display: flex; gap: 0.5rem; align-items: center; }
.password-field .form-input { flex: 1; }
.toggle-visibility {
  background: none; border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm); padding: 0.5rem; cursor: pointer;
  color: var(--color-text-secondary);
}
.checkbox-container { display: flex; align-items: center; gap: 0.5rem; }
.checkbox-input { width: 1rem; height: 1rem; cursor: pointer; }
.form-actions { display: flex; justify-content: space-between; margin-top: 1.5rem; gap: 0.5rem; }
.form-actions > div { display: flex; gap: 0.5rem; }
.warning-text { color: var(--color-text-tertiary); font-size: var(--font-size-sm); margin-top: var(--spacing-2); }
.fa-layer-group { font-size: var(--font-size-xl); color: #6366f1; }
</style>
