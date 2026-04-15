<template>
  <div class="ga-connection-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Google Analytics Connections</h2>
      <p class="description-text">
        Google Analytics connections store the service-account JSON key needed to query
        GA4 properties. Keys are encrypted at rest with your user-derived key and are
        never transmitted to the browser after creation.
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
            <p><strong>What are Google Analytics connections?</strong></p>
            <p>
              A connection is a reusable, per-user bundle that holds a GA4 service-account
              key (and optional default property ID). Use connections inside a Google
              Analytics Reader node to load reports without re-entering credentials.
            </p>
          </div>
        </div>

        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading connections...</p>
        </div>

        <div v-else-if="connections.length === 0" class="empty-state">
          <i class="fa-solid fa-chart-line"></i>
          <p>You haven't added any Google Analytics connections yet</p>
          <p class="hint-text">
            Click "Add Connection" to create your first one.
          </p>
        </div>

        <div v-else class="connections-list">
          <div
            v-for="connection in connections"
            :key="connection.connectionName"
            class="connection-item"
          >
            <div class="connection-info">
              <div class="connection-name">
                <i class="fa-solid fa-chart-line"></i>
                <span>{{ connection.connectionName }}</span>
                <span v-if="connection.defaultPropertyId" class="badge">
                  Property {{ connection.defaultPropertyId }}
                </span>
              </div>
              <div v-if="connection.description" class="connection-details">
                {{ connection.description }}
              </div>
            </div>
            <div class="connection-actions">
              <button type="button" class="btn btn-secondary" @click="showEditModal(connection)">
                <i class="fa-solid fa-edit"></i>
                <span>Modify</span>
              </button>
              <button
                type="button"
                class="btn btn-danger"
                @click="showDeleteModal(connection)"
              >
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <el-dialog
      v-model="dialogVisible"
      :title="isEditing ? 'Edit Google Analytics Connection' : 'Add Google Analytics Connection'"
      width="640px"
      :before-close="handleCloseDialog"
    >
      <GoogleAnalyticsConnectionSettings
        :initial-connection="activeConnection"
        :is-editing="isEditing"
        :is-submitting="isSubmitting"
        @submit="handleFormSubmit"
        @cancel="dialogVisible = false"
      />
    </el-dialog>

    <el-dialog
      v-model="deleteDialogVisible"
      title="Delete Connection"
      width="400px"
      :before-close="handleCloseDeleteDialog"
    >
      <p>
        Are you sure you want to delete the connection
        <strong>{{ connectionToDelete?.connectionName }}</strong
        >?
      </p>
      <p class="warning-text">
        This action cannot be undone and may break any flows that use this connection.
      </p>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="deleteDialogVisible = false">Cancel</el-button>
          <el-button type="danger" :loading="isDeleting" @click="handleDeleteConnection">
            Delete
          </el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted } from "vue";
import { ElDialog, ElButton, ElMessage } from "element-plus";
import {
  fetchGoogleAnalyticsConnections,
  createGoogleAnalyticsConnection,
  updateGoogleAnalyticsConnection,
  deleteGoogleAnalyticsConnection,
} from "./api";
import type {
  GoogleAnalyticsConnection,
  GoogleAnalyticsConnectionInterface,
} from "./GoogleAnalyticsConnectionTypes";
import GoogleAnalyticsConnectionSettings from "./GoogleAnalyticsConnectionSettings.vue";

const connections = ref<GoogleAnalyticsConnectionInterface[]>([]);
const isLoading = ref(true);
const dialogVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditing = ref(false);
const isSubmitting = ref(false);
const isDeleting = ref(false);
const connectionToDelete = ref<GoogleAnalyticsConnectionInterface | null>(null);
const activeConnection = ref<GoogleAnalyticsConnection | undefined>(undefined);

const fetchConnections = async () => {
  isLoading.value = true;
  try {
    connections.value = await fetchGoogleAnalyticsConnections();
  } catch (error) {
    console.error("Error fetching GA connections:", error);
    ElMessage.error("Failed to load Google Analytics connections");
  } finally {
    isLoading.value = false;
  }
};

const showAddModal = () => {
  isEditing.value = false;
  activeConnection.value = undefined;
  dialogVisible.value = true;
};

const showEditModal = (connection: GoogleAnalyticsConnectionInterface) => {
  isEditing.value = true;
  activeConnection.value = {
    connectionName: connection.connectionName,
    description: connection.description ?? "",
    defaultPropertyId: connection.defaultPropertyId ?? "",
    // Secrets are never sent to the browser; leave blank so the backend
    // keeps the stored value unless the user enters a new one.
    serviceAccountJson: "",
  };
  dialogVisible.value = true;
};

const showDeleteModal = (connection: GoogleAnalyticsConnectionInterface) => {
  connectionToDelete.value = connection;
  deleteDialogVisible.value = true;
};

const handleFormSubmit = async (connection: GoogleAnalyticsConnection) => {
  isSubmitting.value = true;
  try {
    if (isEditing.value) {
      await updateGoogleAnalyticsConnection(connection);
    } else {
      await createGoogleAnalyticsConnection(connection);
    }
    await fetchConnections();
    dialogVisible.value = false;
    ElMessage.success(`Connection ${isEditing.value ? "updated" : "created"} successfully`);
  } catch (error: any) {
    ElMessage.error(
      `Failed to ${isEditing.value ? "update" : "create"} connection: ${error.message || "Unknown error"}`,
    );
  } finally {
    isSubmitting.value = false;
  }
};

const handleDeleteConnection = async () => {
  if (!connectionToDelete.value) return;
  isDeleting.value = true;
  try {
    await deleteGoogleAnalyticsConnection(connectionToDelete.value.connectionName);
    await fetchConnections();
    deleteDialogVisible.value = false;
    ElMessage.success("Connection deleted successfully");
  } catch (error) {
    ElMessage.error("Failed to delete connection");
  } finally {
    isDeleting.value = false;
    connectionToDelete.value = null;
  }
};

const handleCloseDialog = (done: () => void) => {
  if (isSubmitting.value) return;
  done();
};

const handleCloseDeleteDialog = (done: () => void) => {
  if (isDeleting.value) return;
  done();
};

onMounted(() => {
  fetchConnections();
});
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

.info-box i {
  color: var(--color-accent);
  font-size: var(--font-size-2xl);
  margin-top: var(--spacing-2);
}

.info-box p {
  margin: 0;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.info-box p:last-child {
  margin-bottom: 0;
}

.info-box p strong {
  color: var(--color-text-primary);
}

.badge {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-full);
  padding: var(--spacing-1) var(--spacing-3);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  margin-left: var(--spacing-2);
}

.connection-details {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  margin-top: var(--spacing-1);
}

.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-sm);
  margin-top: var(--spacing-2);
}

.fa-chart-line {
  color: var(--color-accent);
}
</style>
