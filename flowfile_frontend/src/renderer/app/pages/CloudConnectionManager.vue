<template>
    <div class="cloud-connection-manager-container">
      <div class="mb-3">
        <h2 class="page-title">Cloud Storage Connections</h2>
        <p class="description-text">
          Cloud storage connections allow you to connect to your cloud storage services like AWS S3 and Azure Data Lake Storage.
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
              <p><strong>What are cloud storage connections?</strong></p>
              <p>
                Cloud storage connections store the credentials and configuration needed to securely access
                your cloud storage services. Once set up, you can reuse these connections throughout your workflows
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
            <i class="fa-solid fa-cloud"></i>
            <p>You haven't added any cloud storage connections yet</p>
            <p class="hint-text">
              Click the "Add Connection" button to create your first cloud storage connection.
            </p>
          </div>
  
          <!-- List of connections -->
          <div v-else class="flex-col gap-2">
            <div
              v-for="connection in connectionInterfaces"
              :key="connection.connectionName"
              class="secret-item"
            >
              <div class="secret-info">
                <div class="secret-name">
                  <i :class="getStorageIcon(connection.storageType)"></i>
                  <span>{{ connection.connectionName }}</span>
                  <span class="badge">{{ getStorageLabel(connection.storageType) }}</span>
                  <span class="badge auth-badge">{{ getAuthMethodLabel(connection.authMethod) }}</span>
                </div>
                <div class="connection-details">
                  <span v-if="connection.storageType === 's3' && connection.awsRegion">
                    Region: {{ connection.awsRegion }}
                  </span>
                  <span v-else-if="connection.storageType === 'adls' && connection.azureAccountName">
                    Account: {{ connection.azureAccountName }}
                  </span>
                  <span v-if="connection.endpointUrl">
                    <span class="separator">•</span>
                    Custom endpoint
                  </span>
                  <span v-if="!connection.verifySsl">
                    <span class="separator">•</span>
                    SSL verification disabled
                  </span>
                </div>
              </div>
              <div class="secret-actions">
                <button type="button" class="btn btn-secondary" @click="showEditModal(connection)">
                  <i class="fa-solid fa-edit"></i>
                  <span>Modify</span>
                </button>
                <button v-if="connection.connectionName"
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
        :title="isEditing ? 'Edit Cloud Storage Connection' : 'Add Cloud Storage Connection'"
        width="600px"
        :before-close="handleCloseDialog"
      >
        <div class="modal-description mb-3">
          <p>
            Configure your cloud storage connection details. Choose your storage provider and authentication method,
            then provide the required credentials.
          </p>
        </div>
        <CloudConnectionSettings
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
          Are you sure you want to delete the connection <strong>{{ connectionToDelete }}</strong>?
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
    fetchCloudStorageConnectionsInterfaces,
    createCloudStorageConnectionApi,
    deleteCloudStorageConnectionApi,
    convertConnectionInterfacePytoTs,
  } from "./cloudConnectionManager/api";
  import {
    FullCloudStorageConnectionInterface,
    FullCloudStorageConnection,
    CloudStorageType,
    AuthMethod,
  } from "./cloudConnectionManager/CloudConnectionTypes";
  import CloudConnectionSettings from "./cloudConnectionManager/CloudConnectionSettings.vue";
import { connect } from "http2";
  
  // State
  const connectionInterfaces = ref<FullCloudStorageConnectionInterface[]>([]);
  const isLoading = ref(true);
  const dialogVisible = ref(false);
  const deleteDialogVisible = ref(false);
  const isEditing = ref(false);
  const isSubmitting = ref(false);
  const isDeleting = ref(false);
  const connectionToDelete = ref("");
  const activeConnection = ref<FullCloudStorageConnection | undefined>(undefined);
  
  // Helper functions
  const getStorageIcon = (storageType: string) => {
    switch (storageType) {
      case "s3":
        return "fa-brands fa-aws";
      case "adls":
        return "fa-brands fa-microsoft";
      default:
        return "fa-solid fa-cloud";
    }
  };
  
  const getStorageLabel = (storageType: string) => {
    switch (storageType) {
      case "s3":
        return "AWS S3";
      case "adls":
        return "Azure ADLS";
      default:
        return storageType.toUpperCase();
    }
  };
  
  const getAuthMethodLabel = (authMethod: string) => {
    switch (authMethod) {
      case "access_key":
        return "Access Key";
      case "iam_role":
        return "IAM Role";
      case "service_principal":
        return "Service Principal";
      case "managed_identity":
        return "Managed Identity";
      case "sas_token":
        return "SAS Token";
      case "aws-cli":
        return "AWS CLI";
      case "auto":
        return "Auto";
      default:
        return authMethod;
    }
  };
  
  // Fetch connections
  const fetchConnections = async () => {
    isLoading.value = true;
    try {
      connectionInterfaces.value = await fetchCloudStorageConnectionsInterfaces();
    } catch (error) {
      console.error("Error fetching connections:", error);
      ElMessage.error("Failed to load cloud storage connections");
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
  const showEditModal = (connection: FullCloudStorageConnectionInterface) => {
    isEditing.value = true;
    activeConnection.value = {
      connectionName: connection.connectionName,
      storageType: connection.storageType,
      authMethod: connection.authMethod,
      
      // AWS fields
      awsRegion: connection.awsRegion || "",
      awsAccessKeyId: connection.awsAccessKeyId || "",
      awsSecretAccessKey: "", // Password is not returned from the API
      awsRoleArn: connection.awsRoleArn || "",
      awsAllowUnsafeHtml: connection.awsAllowUnsafeHtml,
      
      // Azure fields
      azureAccountName: connection.azureAccountName || "",
      azureAccountKey: "", // Password is not returned from the API
      azureTenantId: connection.azureTenantId || "",
      azureClientId: connection.azureClientId || "",
      azureClientSecret: "", // Password is not returned from the API
      
      // Common fields
      endpointUrl: connection.endpointUrl || "",
      verifySsl: connection.verifySsl,
    };
    dialogVisible.value = true;
  };
  
  // Show delete confirmation modal
  const showDeleteModal = (connectionName: string) => {
    connectionToDelete.value = connectionName;
    deleteDialogVisible.value = true;
  };
  
  // Handle form submission
  const handleFormSubmit = async (connection: FullCloudStorageConnection) => {
    isSubmitting.value = true;
    try {
      await createCloudStorageConnectionApi(connection);
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
      await deleteCloudStorageConnectionApi(connectionToDelete.value);
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
    color: #6c757d;
    margin-top: 0.5rem;
    font-size: 0.95rem;
  }
  
  .info-box {
    display: flex;
    gap: 1rem;
    padding: 1rem;
    background-color: #f8f9fa;
    border-left: 4px solid #17a2b8;
    border-radius: 4px;
  }
  
  .info-box i {
    color: #17a2b8;
    font-size: 1.5rem;
    margin-top: 0.5rem;
  }
  
  .info-box p {
    margin: 0;
    margin-bottom: 0.5rem;
  }
  
  .info-box p:last-child {
    margin-bottom: 0;
  }
  
  .modal-description {
    color: #6c757d;
    font-size: 0.9rem;
  }
  
  .badge {
    background-color: #e9ecef;
    border-radius: 1rem;
    padding: 0.25rem 0.75rem;
    font-size: 0.75rem;
    margin-left: 0.5rem;
  }
  
  .auth-badge {
    background-color: #d1ecf1;
    color: #0c5460;
  }
  
  .warning-text {
    color: #dc3545;
    font-size: 0.875rem;
    margin-top: 0.5rem;
  }
  
  .dialog-footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
    margin-top: 1rem;
  }
  
  .connection-details {
    font-size: 0.85rem;
    color: #6c757d;
    margin-top: 0.25rem;
  }
  
  .separator {
    margin: 0 0.5rem;
  }
  
  .hint-text {
    color: #6c757d;
    font-size: 0.875rem;
    margin-top: 0.5rem;
  }
  
  .mb-3 {
    margin-bottom: 1rem;
  }
  
  .flex-col {
    display: flex;
    flex-direction: column;
  }
  
  .gap-2 {
    gap: 0.5rem;
  }
  
  /* Cloud-specific styles */
  .fa-brands {
    font-size: 1.2rem;
  }
  
  .fa-aws {
    color: #ff9900;
  }
  
  .fa-microsoft {
    color: #0078d4;
  }
  </style>