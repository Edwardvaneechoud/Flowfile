<template>
  <div v-if="dataLoaded && nodeCloudStorageReader" class="cloud-storage-container">
    <generic-node-settings v-model="nodeCloudStorageReader">
      <!-- Connection Selection -->
      <div class="listbox-wrapper">
        <div class="form-group">
          <label for="connection-select">Cloud Storage Connection</label>
          <div v-if="connectionsAreLoading" class="loading-state">
            <div class="loading-spinner"></div>
            <p>Loading connections...</p>
          </div>
          <div v-else>
            <select
              id="connection-select"
              v-model="selectedConnection"
              class="form-control minimal-select"
              @change="resetFields"
            >
              <option :value="null">No connection (use local credentials)</option>
              <option v-for="conn in connectionInterfaces" :key="conn.connectionName" :value="conn">
                {{ conn.connectionName }} ({{ getStorageTypeLabel(conn.storageType) }} -
                {{ getAuthMethodLabel(conn.authMethod) }})
              </option>
            </select>
            <div
              v-if="!nodeCloudStorageReader.cloud_storage_settings.connection_name"
              class="helper-text"
            >
              <i class="fa-solid fa-info-circle"></i>
              Will use local AWS CLI credentials or environment variables
            </div>
          </div>
        </div>
      </div>
      <!-- File Path and Scan Settings -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">File Settings</h4>

        <!-- File Path -->
        <div class="form-group">
          <label for="file-path">File Path</label>
          <input
            id="file-path"
            v-model="nodeCloudStorageReader.cloud_storage_settings.resource_path"
            type="text"
            class="form-control"
            placeholder="e.g., bucket-name/folder/file.csv or bucket-name/folder/"
            @input="resetFields"
          />
        </div>

        <!-- File Format -->
        <div class="form-group">
          <label for="file-format">File Format</label>
          <select
            id="file-format"
            v-model="nodeCloudStorageReader.cloud_storage_settings.file_format"
            class="form-control"
            @change="handleFileFormatChange"
          >
            <option value="csv">CSV</option>
            <option value="parquet">Parquet</option>
            <option value="json">JSON</option>
            <option value="delta">Delta Lake</option>
          </select>
        </div>

        <!-- Scan Mode -->
        <div
          v-if="nodeCloudStorageReader.cloud_storage_settings.file_format !== 'delta'"
          class="form-group"
        >
          <label for="scan-mode">Scan Mode</label>
          <select
            id="scan-mode"
            v-model="nodeCloudStorageReader.cloud_storage_settings.scan_mode"
            class="form-control"
          >
            <option value="single_file">Single File</option>
            <option value="directory">Directory</option>
          </select>
        </div>

        <!-- CSV-specific options -->
        <div
          v-if="nodeCloudStorageReader.cloud_storage_settings.file_format === 'csv'"
          class="format-options"
        >
          <h5 class="subsection-title">CSV Options</h5>

          <div class="form-group">
            <div class="checkbox-container">
              <input
                id="csv-has-header"
                v-model="nodeCloudStorageReader.cloud_storage_settings.csv_has_header"
                type="checkbox"
                class="checkbox-input"
              />
              <label for="csv-has-header" class="checkbox-label">First row contains headers</label>
            </div>
          </div>

          <div class="form-row">
            <div class="form-group half">
              <label for="csv-delimiter">Delimiter</label>
              <input
                id="csv-delimiter"
                v-model="nodeCloudStorageReader.cloud_storage_settings.csv_delimiter"
                type="text"
                class="form-control"
                placeholder=","
                maxlength="1"
              />
            </div>

            <div class="form-group half">
              <label for="csv-encoding">Encoding</label>
              <select
                id="csv-encoding"
                v-model="nodeCloudStorageReader.cloud_storage_settings.csv_encoding"
                class="form-control"
              >
                <option value="utf8">UTF-8</option>
                <option value="utf8-lossy">UTF-8 Lossy</option>
              </select>
            </div>
          </div>
        </div>

        <!-- Delta-specific options -->
        <div
          v-if="nodeCloudStorageReader.cloud_storage_settings.file_format === 'delta'"
          class="format-options"
        >
          <h5 class="subsection-title">Delta Lake Options</h5>

          <div class="form-group">
            <label for="delta-version">Version (optional)</label>
            <input
              id="delta-version"
              v-model.number="nodeCloudStorageReader.cloud_storage_settings.delta_version"
              type="number"
              class="form-control"
              placeholder="Latest version"
              min="0"
            />
          </div>
        </div>

        <!-- Info message for scan mode -->
        <div
          v-if="nodeCloudStorageReader.cloud_storage_settings.scan_mode === 'directory'"
          class="info-box"
        >
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <p>
              Directory scan will read all files matching the selected format in the specified path.
            </p>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref } from "vue";
import { NodeCloudStorageReader } from "../../../baseNode/nodeInput";
import { createNodeCloudStorageReader } from "./utils";
import { useNodeStore } from "../../../../../stores/column-store";
import { useNodeSettings } from "../../../../../composables";
import { fetchCloudStorageConnectionsInterfaces } from "../../../../../views/CloudConnectionView/api";
import { FullCloudStorageConnectionInterface } from "../../../../../views/CloudConnectionView/CloudConnectionTypes";
import { ElMessage } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

interface Props {
  nodeId: number;
}

defineProps<Props>();
const nodeStore = useNodeStore();
const dataLoaded = ref<boolean>(false);
const nodeCloudStorageReader = ref<NodeCloudStorageReader | null>(null);
const connectionInterfaces = ref<FullCloudStorageConnectionInterface[]>([]);
const connectionsAreLoading = ref(false);
const selectedConnection = ref<FullCloudStorageConnectionInterface | null>(null);

const getStorageTypeLabel = (storageType: string) => {
  switch (storageType) {
    case "s3":
      return "AWS S3";
    case "adls":
      return "Azure ADLS";
    case "gcs":
      return "Google Cloud Storage";
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

const handleFileFormatChange = () => {
  resetFields();
  // Set default values for format-specific options
  if (nodeCloudStorageReader.value) {
    const format = nodeCloudStorageReader.value.cloud_storage_settings.file_format;

    if (format === "csv") {
      // Set CSV defaults
      if (nodeCloudStorageReader.value.cloud_storage_settings.csv_has_header === undefined) {
        nodeCloudStorageReader.value.cloud_storage_settings.csv_has_header = true;
      }
      if (!nodeCloudStorageReader.value.cloud_storage_settings.csv_delimiter) {
        nodeCloudStorageReader.value.cloud_storage_settings.csv_delimiter = ",";
      }
      if (!nodeCloudStorageReader.value.cloud_storage_settings.csv_encoding) {
        nodeCloudStorageReader.value.cloud_storage_settings.csv_encoding = "utf8";
      }
    } else {
      // Clear CSV-specific settings
      nodeCloudStorageReader.value.cloud_storage_settings.csv_has_header = undefined;
      nodeCloudStorageReader.value.cloud_storage_settings.csv_delimiter = undefined;
      nodeCloudStorageReader.value.cloud_storage_settings.csv_encoding = undefined;
    }

    if (format !== "delta") {
      // Clear Delta-specific settings
      nodeCloudStorageReader.value.cloud_storage_settings.delta_version = undefined;
    }
  }
};

const resetFields = () => {
  if (nodeCloudStorageReader.value) {
    nodeCloudStorageReader.value.fields = [];
    if (!selectedConnection.value) {
      nodeCloudStorageReader.value.cloud_storage_settings.auth_mode = "aws-cli";
      nodeCloudStorageReader.value.cloud_storage_settings.connection_name = undefined;
    } else {
      nodeCloudStorageReader.value.cloud_storage_settings.auth_mode =
        selectedConnection.value.authMethod;
      nodeCloudStorageReader.value.cloud_storage_settings.connection_name =
        selectedConnection.value.connectionName;
    }
  }
};

const setConnectionOnConnectionName = async (connectionName: string | null) => {
  selectedConnection.value =
    connectionInterfaces.value.find(
      (connectionInterface) => connectionInterface.connectionName === connectionName, // Use '===' for strict equality
    ) || null;
};

const loadNodeData = async (nodeId: number) => {
  try {
    const [nodeData] = await Promise.all([
      nodeStore.getNodeData(nodeId, false),
      fetchConnections(),
    ]);
    if (nodeData) {
      const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
      nodeCloudStorageReader.value = hasValidSetup
        ? nodeData.setting_input
        : createNodeCloudStorageReader(nodeStore.flow_id, nodeId);
      if (nodeCloudStorageReader.value?.cloud_storage_settings.connection_name) {
        await setConnectionOnConnectionName(
          nodeCloudStorageReader.value.cloud_storage_settings.connection_name,
        );
      } else {
        selectedConnection.value = null;
      }
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = false;
  }
};

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeData: nodeCloudStorageReader,
  beforeSave: () => {
    if (!nodeCloudStorageReader.value || !nodeCloudStorageReader.value.cloud_storage_settings) {
      throw new Error("Cannot save: cloud storage settings not available");
    }
    nodeCloudStorageReader.value.is_setup = true;
  },
  afterSave: () => {
    dataLoaded.value = false;
  },
});

const fetchConnections = async () => {
  connectionsAreLoading.value = true;
  try {
    connectionInterfaces.value = await fetchCloudStorageConnectionsInterfaces();
  } catch (error) {
    console.error("Error fetching connections:", error);
    ElMessage.error("Failed to load cloud storage connections");
  } finally {
    connectionsAreLoading.value = false;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.cloud-storage-container {
  font-family: var(--font-family-base);
  max-width: 100%;
  color: var(--color-text-primary);
}

.section-subtitle {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: #4a5568;
}

.subsection-title {
  margin: 0.5rem 0 0.5rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: #718096;
}

.format-options {
  margin-top: 1rem;
  padding: 1rem;
  background-color: #f7fafc;
  border-radius: 4px;
  border: 1px solid #e2e8f0;
}

.form-row {
  display: flex;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
  width: 100%;
  box-sizing: border-box;
}

.half {
  flex: 1;
  min-width: 0;
  max-width: calc(50% - 0.375rem);
}

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
}

.form-group {
  margin-bottom: 0.75rem;
  width: 100%;
}

label {
  display: block;
  margin-bottom: 0.25rem;
  font-size: 0.875rem;
  font-weight: 500;
  color: #4a5568;
}

select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234a5568' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}

.checkbox-container {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.checkbox-input {
  width: 1rem;
  height: 1rem;
  cursor: pointer;
}

.checkbox-label {
  margin: 0;
  cursor: pointer;
}

.info-box {
  display: flex;
  gap: 0.75rem;
  padding: 0.75rem;
  background-color: #e6f7ff;
  border-left: 4px solid #1890ff;
  border-radius: 4px;
  margin-top: 1rem;
  font-size: 0.875rem;
}

.info-box i {
  color: #1890ff;
  font-size: 1.25rem;
  flex-shrink: 0;
}

.info-box p {
  margin: 0;
  color: #4a5568;
}

.helper-text {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
  font-size: 0.8125rem;
  color: #718096;
}

.helper-text i {
  color: #4299e1;
  font-size: 0.875rem;
}

.loading-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.5rem;
  padding: 1rem;
}

.loading-state p {
  margin: 0;
  color: #718096;
  font-size: 0.875rem;
}

.loading-spinner {
  width: 2rem;
  height: 2rem;
  border: 2px solid #e2e8f0;
  border-top-color: #4299e1;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 640px) {
  .form-row {
    flex-direction: column;
    gap: 0.5rem;
  }

  .half {
    max-width: 100%;
  }
}
</style>
