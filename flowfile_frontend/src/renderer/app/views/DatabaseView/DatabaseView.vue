//flowfile_frontend/src/renderer/app/pages/DatabaseManager.vue

<template>
  <div class="database-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Database Connections</h2>
      <p class="description-text">
        Database connections allow you to connect to your databases for reading and writing data.
        Create and manage your connections here to use them in your data workflows.
      </p>
    </div>

    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Your Connections ({{ connectionInterfaces.length }})</h3>
        <button class="btn btn-primary" @click="showAddModal">
          <i class="fa-solid fa-plus"></i> Add Connection
        </button>
      </div>
      <div class="card-content">
        <!-- Info box about connections -->
        <div class="info-box mb-3">
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <p><strong>What are database connections?</strong></p>
            <p>
              Database connections store the credentials and configuration needed to securely access
              your databases. Once set up, you can reuse these connections throughout your workflows
              without re-entering credentials.
            </p>
          </div>
        </div>

        <!-- Loading state -->
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading connections...</p>
        </div>

        <!-- Empty state -->
        <div v-else-if="connectionInterfaces.length === 0" class="empty-state">
          <i class="fa-solid fa-database"></i>
          <p>You haven't added any database connections yet</p>
          <p class="hint-text">
            Click the "Add Connection" button to create your first database connection.
          </p>
        </div>

        <!-- List of connections -->
        <div v-else class="connections-list">
          <div
            v-for="connection in connectionInterfaces"
            :key="connection.connectionName"
            class="connection-item"
          >
            <div class="connection-info">
              <div class="connection-name">
                <i class="fa-solid fa-database"></i>
                <span>{{ connection.connectionName }}</span>
                <span class="badge">{{ connection.databaseType }}</span>
              </div>
              <div class="connection-details">
                <span>{{
                  connection.database ? connection.database : "No database specified"
                }}</span>
                <span class="separator">•</span>
                <span>{{ connection.host ? connection.host : "Using connection URL" }}</span>
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
                @click="showDeleteModal(connection.connectionName)"
              >
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Element Plus Modals -->
    <el-dialog
      v-model="dialogVisible"
      :title="isEditing ? 'Edit Database Connection' : 'Add Database Connection'"
      width="500px"
      :before-close="handleCloseDialog"
    >
      <div class="modal-description mb-3">
        <p>
          Configure your database connection details. You can connect using either host/port
          information or a connection URL.
        </p>
      </div>
      <DatabaseConnectionForm
        :initial-connection="activeConnection"
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
        Are you sure you want to delete the connection <strong>{{ connectionToDelete }}</strong
        >?
      </p>
      <p class="warning-text">
        This action cannot be undone and may affect any processes using this connection.
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
  fetchDatabaseConnectionsInterfaces,
  createDatabaseConnectionApi,
  deleteDatabaseConnectionApi,
} from "./api";
import { FullDatabaseConnectionInterface, FullDatabaseConnection } from "./databaseConnectionTypes";
import DatabaseConnectionForm from "./DatabaseConnectionSettings.vue";

// State
const connectionInterfaces = ref<FullDatabaseConnectionInterface[]>([]);
const isLoading = ref(true);
const dialogVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditing = ref(false);
const isSubmitting = ref(false);
const isDeleting = ref(false);
const connectionToDelete = ref("");
const activeConnection = ref<FullDatabaseConnection | undefined>(undefined);

// Fetch connections
const fetchConnections = async () => {
  isLoading.value = true;
  try {
    connectionInterfaces.value = await fetchDatabaseConnectionsInterfaces();
  } catch (error) {
    console.error("Error fetching connections:", error);
    ElMessage.error("Failed to load database connections");
  } finally {
    isLoading.value = false;
  }
};

// Show add connection modal
const showAddModal = () => {
  isEditing.value = false;
  activeConnection.value = undefined;
  dialogVisible.value = true;
};

// Show edit connection modal
const showEditModal = (connection: FullDatabaseConnectionInterface) => {
  isEditing.value = true;
  activeConnection.value = {
    connectionName: connection.connectionName,
    databaseType: connection.databaseType,
    username: connection.username,
    password: "", // Password is not returned from the API
    host: connection.host || "",
    port: connection.port || 5432,
    database: connection.database || "",
    sslEnabled: connection.sslEnabled,
    url: connection.url || "",
  };
  dialogVisible.value = true;
};

// Show delete confirmation modal
const showDeleteModal = (connectionName: string) => {
  connectionToDelete.value = connectionName;
  deleteDialogVisible.value = true;
};

// Handle form submission
const handleFormSubmit = async (connection: FullDatabaseConnection) => {
  isSubmitting.value = true;
  try {
    await createDatabaseConnectionApi(connection);
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

// Handle delete connection
const handleDeleteConnection = async () => {
  if (!connectionToDelete.value) return;

  isDeleting.value = true;
  try {
    await deleteDatabaseConnectionApi(connectionToDelete.value);
    await fetchConnections();
    deleteDialogVisible.value = false;
    ElMessage.success("Connection deleted successfully");
  } catch (error) {
    ElMessage.error("Failed to delete connection");
  } finally {
    isDeleting.value = false;
    connectionToDelete.value = "";
  }
};

// Handle close dialog
const handleCloseDialog = (done: () => void) => {
  if (isSubmitting.value) return;
  done();
};

// Handle close delete dialog
const handleCloseDeleteDialog = (done: () => void) => {
  if (isDeleting.value) return;
  done();
};

// Load connections on mount
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

.modal-description {
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
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

.separator {
  margin: 0 var(--spacing-2);
}

.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-sm);
  margin-top: var(--spacing-2);
}
</style>
