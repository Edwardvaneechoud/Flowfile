<template>
  <div class="kafka-connection-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Kafka Connection Manager</h2>
      <p class="description-text">
        Manage your Kafka and Redpanda connections. Create connections here to use them in Kafka
        Source nodes within your data workflows.
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
        <!-- Info box -->
        <div class="info-box mb-3">
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <p><strong>What are Kafka connections?</strong></p>
            <p>
              Kafka connections store the broker addresses and credentials needed to securely access
              your Kafka or Redpanda clusters. Once set up, you can reuse these connections in Kafka
              Source nodes to read data from topics.
            </p>
          </div>
        </div>

        <!-- Loading state -->
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading connections...</p>
        </div>

        <!-- Empty state -->
        <div v-else-if="connections.length === 0" class="empty-state">
          <i class="fa-solid fa-tower-broadcast"></i>
          <p>You haven't added any Kafka connections yet</p>
          <p class="hint-text">
            Click the "Add Connection" button to create your first Kafka connection.
          </p>
        </div>

        <!-- List of connections -->
        <div v-else class="connections-list">
          <div v-for="connection in connections" :key="connection.id" class="connection-item">
            <div class="connection-info">
              <div class="connection-name">
                <i class="fa-solid fa-tower-broadcast"></i>
                <span>{{ connection.connection_name }}</span>
                <span class="badge">{{ connection.security_protocol }}</span>
                <span v-if="connection.sasl_mechanism" class="badge auth-badge">
                  {{ connection.sasl_mechanism }}
                </span>
              </div>
              <div class="connection-details">
                <span>{{ connection.bootstrap_servers }}</span>
                <span v-if="connection.sasl_username">
                  <span class="separator">&#8226;</span>
                  User: {{ connection.sasl_username }}
                </span>
                <span v-if="connection.schema_registry_url">
                  <span class="separator">&#8226;</span>
                  Schema Registry
                </span>
              </div>
            </div>
            <div class="connection-actions">
              <button
                type="button"
                class="btn btn-secondary"
                :loading="testingConnectionId === connection.id"
                @click="handleTestConnection(connection.id)"
              >
                <i class="fa-solid fa-plug"></i>
                <span>Test</span>
              </button>
              <button type="button" class="btn btn-secondary" @click="showEditModal(connection)">
                <i class="fa-solid fa-edit"></i>
                <span>Modify</span>
              </button>
              <button type="button" class="btn btn-danger" @click="showDeleteModal(connection)">
                <i class="fa-solid fa-trash-alt"></i>
                <span>Delete</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Add/Edit Modal -->
    <el-dialog
      v-model="dialogVisible"
      :title="isEditing ? 'Edit Kafka Connection' : 'Add Kafka Connection'"
      width="600px"
      :before-close="handleCloseDialog"
    >
      <div class="modal-description mb-3">
        <p>
          Configure your Kafka connection details. Choose your security protocol and provide the
          required credentials.
        </p>
      </div>
      <KafkaConnectionSettings
        :initial-connection="activeConnection"
        :is-submitting="isSubmitting"
        :is-editing="isEditing"
        @submit="handleFormSubmit"
        @cancel="dialogVisible = false"
      />
    </el-dialog>

    <!-- Delete Confirmation Modal -->
    <el-dialog
      v-model="deleteDialogVisible"
      title="Delete Connection"
      width="400px"
      :before-close="handleCloseDeleteDialog"
    >
      <p>
        Are you sure you want to delete the connection
        <strong>{{ connectionToDelete?.connection_name }}</strong
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
  fetchKafkaConnections,
  createKafkaConnection,
  updateKafkaConnection,
  deleteKafkaConnection,
  testKafkaConnection,
} from "./api";
import type { KafkaConnectionOut, KafkaConnectionCreate } from "./KafkaConnectionTypes";
import KafkaConnectionSettings from "./KafkaConnectionSettings.vue";

// State
const connections = ref<KafkaConnectionOut[]>([]);
const isLoading = ref(true);
const dialogVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditing = ref(false);
const isSubmitting = ref(false);
const isDeleting = ref(false);
const testingConnectionId = ref<number | null>(null);
const connectionToDelete = ref<KafkaConnectionOut | null>(null);
const activeConnection = ref<KafkaConnectionCreate | undefined>(undefined);
const editingConnectionId = ref<number | null>(null);

// Fetch connections
const fetchConnections = async () => {
  isLoading.value = true;
  try {
    connections.value = await fetchKafkaConnections();
  } catch (error) {
    console.error("Error fetching connections:", error);
    ElMessage.error("Failed to load Kafka connections");
  } finally {
    isLoading.value = false;
  }
};

// Show add modal
const showAddModal = () => {
  isEditing.value = false;
  editingConnectionId.value = null;
  activeConnection.value = undefined;
  dialogVisible.value = true;
};

// Show edit modal
const showEditModal = (connection: KafkaConnectionOut) => {
  isEditing.value = true;
  editingConnectionId.value = connection.id;
  activeConnection.value = {
    connection_name: connection.connection_name,
    bootstrap_servers: connection.bootstrap_servers,
    security_protocol: connection.security_protocol as KafkaConnectionCreate["security_protocol"],
    sasl_mechanism: connection.sasl_mechanism,
    sasl_username: connection.sasl_username,
    sasl_password: null,
    ssl_ca_location: null,
    ssl_cert_location: null,
    ssl_key_pem: null,
    schema_registry_url: connection.schema_registry_url,
    extra_config: null,
  };
  dialogVisible.value = true;
};

// Show delete confirmation
const showDeleteModal = (connection: KafkaConnectionOut) => {
  connectionToDelete.value = connection;
  deleteDialogVisible.value = true;
};

// Handle form submit
const handleFormSubmit = async (connection: KafkaConnectionCreate) => {
  isSubmitting.value = true;
  try {
    if (isEditing.value && editingConnectionId.value !== null) {
      await updateKafkaConnection(editingConnectionId.value, connection);
    } else {
      await createKafkaConnection(connection);
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

// Handle test connection
const handleTestConnection = async (connectionId: number) => {
  testingConnectionId.value = connectionId;
  try {
    const result = await testKafkaConnection(connectionId);
    if (result.success) {
      ElMessage.success(`Connection successful! Found ${result.topics_found} topics.`);
    } else {
      ElMessage.error(`Connection failed: ${result.message}`);
    }
  } catch (error) {
    ElMessage.error("Failed to test connection");
  } finally {
    testingConnectionId.value = null;
  }
};

// Handle delete connection
const handleDeleteConnection = async () => {
  if (!connectionToDelete.value) return;

  isDeleting.value = true;
  try {
    await deleteKafkaConnection(connectionToDelete.value.id);
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

// Dialog close handlers
const handleCloseDialog = (done: () => void) => {
  if (isSubmitting.value) return;
  done();
};

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

.auth-badge {
  background-color: var(--color-info-light);
  color: var(--color-info);
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

.fa-tower-broadcast {
  color: var(--color-accent);
}
</style>
