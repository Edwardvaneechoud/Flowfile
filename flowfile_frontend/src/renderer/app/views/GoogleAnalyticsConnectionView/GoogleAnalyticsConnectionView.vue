<template>
  <div class="ga-connection-manager-container">
    <div class="mb-3">
      <h2 class="page-title">Google Analytics Connections</h2>
      <p class="description-text">
        Google Analytics connections store the OAuth refresh token minted when you sign in with
        Google. Tokens are encrypted at rest with your user-derived key and are never transmitted
        back to the browser after creation.
      </p>
    </div>

    <GoogleOAuthClientCard />

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
            <p><strong>How it works</strong></p>
            <p>
              Click <em>Add Connection</em>, give it a name, then click
              <em>Connect Google Account</em>. You'll sign in with the Google account that has
              Viewer access to your GA4 property. Flowfile stores a refresh token (encrypted at rest
              with your user-derived key) so it can read GA4 on your behalf — no service-account key
              is ever required.
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
          <p class="hint-text">Click "Add Connection" to create your first one.</p>
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
              <div v-if="connection.oauthUserEmail" class="connection-details">
                Connected as {{ connection.oauthUserEmail }}
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
              <button type="button" class="btn btn-danger" @click="showDeleteModal(connection)">
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
        :is-connecting="isConnecting"
        @save-metadata="handleMetadataSave"
        @connect-oauth="handleConnectOAuth"
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
import { ref, onMounted, onBeforeUnmount } from "vue";
import { ElDialog, ElButton, ElMessage } from "element-plus";
import {
  deleteGoogleAnalyticsConnection,
  fetchGoogleAnalyticsConnections,
  startGoogleAnalyticsOAuth,
  updateGoogleAnalyticsConnectionMetadata,
} from "./api";
import type {
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionMetadata,
} from "./GoogleAnalyticsConnectionTypes";
import GoogleAnalyticsConnectionSettings from "./GoogleAnalyticsConnectionSettings.vue";
import GoogleOAuthClientCard from "./GoogleOAuthClientCard.vue";

const connections = ref<GoogleAnalyticsConnectionInterface[]>([]);
const isLoading = ref(true);
const dialogVisible = ref(false);
const deleteDialogVisible = ref(false);
const isEditing = ref(false);
const isSubmitting = ref(false);
const isDeleting = ref(false);
const isConnecting = ref(false);
const connectionToDelete = ref<GoogleAnalyticsConnectionInterface | null>(null);
const activeConnection = ref<GoogleAnalyticsConnectionInterface | undefined>(undefined);

let oauthPopup: Window | null = null;

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
  activeConnection.value = { ...connection };
  dialogVisible.value = true;
};

const showDeleteModal = (connection: GoogleAnalyticsConnectionInterface) => {
  connectionToDelete.value = connection;
  deleteDialogVisible.value = true;
};

const handleMetadataSave = async (metadata: GoogleAnalyticsConnectionMetadata) => {
  isSubmitting.value = true;
  try {
    await updateGoogleAnalyticsConnectionMetadata(metadata);
    await fetchConnections();
    dialogVisible.value = false;
    ElMessage.success("Connection updated");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Failed to update connection: ${message}`);
  } finally {
    isSubmitting.value = false;
  }
};

const handleConnectOAuth = async (metadata: GoogleAnalyticsConnectionMetadata) => {
  if (!metadata.connectionName.trim()) {
    ElMessage.error("Connection name is required before connecting");
    return;
  }
  isConnecting.value = true;
  try {
    const { authUrl } = await startGoogleAnalyticsOAuth(metadata);
    oauthPopup = window.open(authUrl, "flowfile-ga-oauth", "width=520,height=720");
    if (!oauthPopup) {
      ElMessage.error("Popup blocked — allow popups for this site and try again");
      isConnecting.value = false;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Could not start OAuth: ${message}`);
    isConnecting.value = false;
  }
};

const handleOAuthMessage = async (event: MessageEvent) => {
  const data = event.data;
  if (!data || data.source !== "flowfile-ga-oauth") return;
  isConnecting.value = false;
  if (data.status === "ok") {
    await fetchConnections();
    dialogVisible.value = false;
    ElMessage.success(data.message || "Connected");
  } else {
    ElMessage.error(data.message || "Google sign-in failed");
  }
  oauthPopup = null;
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
    console.error(error);
    ElMessage.error("Failed to delete connection");
  } finally {
    isDeleting.value = false;
    connectionToDelete.value = null;
  }
};

const handleCloseDialog = (done: () => void) => {
  if (isSubmitting.value || isConnecting.value) return;
  done();
};

const handleCloseDeleteDialog = (done: () => void) => {
  if (isDeleting.value) return;
  done();
};

onMounted(() => {
  fetchConnections();
  window.addEventListener("message", handleOAuthMessage);
});

onBeforeUnmount(() => {
  window.removeEventListener("message", handleOAuthMessage);
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
