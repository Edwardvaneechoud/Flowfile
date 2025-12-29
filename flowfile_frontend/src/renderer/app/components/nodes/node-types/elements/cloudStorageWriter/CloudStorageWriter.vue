<template>
  <div v-if="dataLoaded && nodeCloudStorageWriter" class="cloud-storage-container">
    <generic-node-settings v-model="nodeCloudStorageWriter">
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
              class="form-control"
              @change="updateConnection"
            >
              <option :value="null">No connection (use local credentials)</option>
              <option v-for="conn in connectionInterfaces" :key="conn.connectionName" :value="conn">
                {{ conn.connectionName }} ({{ getStorageTypeLabel(conn.storageType) }} -
                {{ getAuthMethodLabel(conn.authMethod) }})
              </option>
            </select>
            <div
              v-if="!nodeCloudStorageWriter.cloud_storage_settings.connection_name"
              class="helper-text"
            >
              <i class="fa-solid fa-info-circle"></i>
              Will use local AWS CLI credentials or environment variables
            </div>
          </div>
        </div>
      </div>

      <div class="listbox-wrapper">
        <h4 class="section-subtitle">File Settings</h4>
        <div class="form-group">
          <label for="file-path">File Path</label>
          <input
            id="file-path"
            v-model="nodeCloudStorageWriter.cloud_storage_settings.resource_path"
            type="text"
            class="form-control"
            placeholder="e.g., bucket-name/folder/file.parquet"
          />
        </div>

        <div class="form-group">
          <label for="file-format">File Format</label>
          <select
            id="file-format"
            v-model="nodeCloudStorageWriter.cloud_storage_settings.file_format"
            class="form-control"
            @change="handleFileFormatChange"
          >
            <option value="parquet">Parquet</option>
            <option value="csv">CSV</option>
            <option value="json">JSON</option>
            <option value="delta">Delta Lake</option>
          </select>
        </div>

        <div class="form-group">
          <label for="write-mode">Write Mode</label>
          <select
            id="write-mode"
            v-model="nodeCloudStorageWriter.cloud_storage_settings.write_mode"
            class="form-control"
          >
            <option value="overwrite">Overwrite</option>
            <option
              v-if="nodeCloudStorageWriter.cloud_storage_settings.file_format === 'delta'"
              value="append"
            >
              Append
            </option>
          </select>
        </div>

        <div
          v-if="nodeCloudStorageWriter.cloud_storage_settings.file_format === 'csv'"
          class="format-options"
        >
          <h5 class="subsection-title">CSV Options</h5>
          <div class="form-row">
            <div class="form-group half">
              <label for="csv-delimiter">Delimiter</label>
              <input
                id="csv-delimiter"
                v-model="nodeCloudStorageWriter.cloud_storage_settings.csv_delimiter"
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
                v-model="nodeCloudStorageWriter.cloud_storage_settings.csv_encoding"
                class="form-control"
              >
                <option value="utf8">UTF-8</option>
                <option value="utf8-lossy">UTF-8 Lossy</option>
              </select>
            </div>
          </div>
        </div>

        <div
          v-if="nodeCloudStorageWriter.cloud_storage_settings.file_format === 'parquet'"
          class="format-options"
        >
          <h5 class="subsection-title">Parquet Options</h5>
          <div class="form-group">
            <label for="parquet-compression">Compression</label>
            <select
              id="parquet-compression"
              v-model="nodeCloudStorageWriter.cloud_storage_settings.parquet_compression"
              class="form-control"
            >
              <option value="snappy">Snappy</option>
              <option value="gzip">Gzip</option>
              <option value="brotli">Brotli</option>
              <option value="lz4">LZ4</option>
              <option value="zstd">Zstd</option>
            </select>
          </div>
        </div>

        <div
          v-if="nodeCloudStorageWriter.cloud_storage_settings.write_mode === 'overwrite'"
          class="info-box info-warn"
        >
          <i class="fa-solid fa-triangle-exclamation"></i>
          <div>
            <p>
              <strong>Overwrite mode:</strong> If a file or data at the target path exists, it will
              be replaced.
            </p>
          </div>
        </div>
        <div
          v-if="nodeCloudStorageWriter.cloud_storage_settings.write_mode === 'append'"
          class="info-box"
        >
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <p>
              <strong>Append mode:</strong> New data will be added. The schema of the new data must
              match the existing data.
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
import { NodeCloudStorageWriter } from "../../../baseNode/nodeInput";
import { createNodeCloudStorageWriter } from "./utils"; // Import the new utility function
import { useNodeStore } from "../../../../../stores/column-store";
import { fetchCloudStorageConnectionsInterfaces } from "../../../../../views/CloudConnectionView/api";
import { FullCloudStorageConnectionInterface } from "../../../../../views/CloudConnectionView/CloudConnectionTypes";
import { ElMessage } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

interface Props {
  nodeId: number;
}

const props = defineProps<Props>();
const nodeStore = useNodeStore();
const dataLoaded = ref<boolean>(false);
const nodeCloudStorageWriter = ref<NodeCloudStorageWriter | null>(null); // Use the writer type
const connectionInterfaces = ref<FullCloudStorageConnectionInterface[]>([]);
const connectionsAreLoading = ref(false);
const selectedConnection = ref<FullCloudStorageConnectionInterface | null>(null);

// --- Reusable Helper Functions (from reader) ---
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
  if (nodeCloudStorageWriter.value) {
    const settings = nodeCloudStorageWriter.value.cloud_storage_settings;
    const format = settings.file_format;

    // If the newly selected format is NOT delta, force write_mode to 'overwrite'
    if (format !== "delta") {
      settings.write_mode = "overwrite";
    }

    // Set defaults for newly selected formats
    if (format === "parquet" && !settings.parquet_compression) {
      settings.parquet_compression = "snappy";
    } else if (format === "csv" && !settings.csv_delimiter) {
      settings.csv_delimiter = ",";
      settings.csv_encoding = "utf8";
    }

    if (format !== "parquet") {
      settings.parquet_compression = "snappy";
    }
    if (format !== "csv") {
      settings.csv_delimiter = ";";
      settings.csv_encoding = "utf8-lossy";
    }
  }
};

const updateConnection = () => {
  if (nodeCloudStorageWriter.value) {
    if (!selectedConnection.value) {
      nodeCloudStorageWriter.value.cloud_storage_settings.auth_mode = "aws-cli";
      nodeCloudStorageWriter.value.cloud_storage_settings.connection_name = undefined;
    } else {
      nodeCloudStorageWriter.value.cloud_storage_settings.auth_mode =
        selectedConnection.value.authMethod;
      nodeCloudStorageWriter.value.cloud_storage_settings.connection_name =
        selectedConnection.value.connectionName;
    }
  }
};

const setConnectionOnConnectionName = async (connectionName: string | null) => {
  selectedConnection.value =
    connectionInterfaces.value.find((ci) => ci.connectionName === connectionName) || null;
};

const loadNodeData = async (nodeId: number) => {
  try {
    const [nodeData] = await Promise.all([
      nodeStore.getNodeData(nodeId, false),
      fetchConnections(),
    ]);
    if (nodeData) {
      const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
      nodeCloudStorageWriter.value = hasValidSetup
        ? nodeData.setting_input
        : createNodeCloudStorageWriter(nodeStore.flow_id, nodeId);

      if (nodeCloudStorageWriter.value?.cloud_storage_settings.connection_name) {
        await setConnectionOnConnectionName(
          nodeCloudStorageWriter.value.cloud_storage_settings.connection_name,
        );
      } else {
        selectedConnection.value = null;
      }
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    ElMessage.error("Failed to load node settings.");
    dataLoaded.value = false;
  }
};

const pushNodeData = async () => {
  if (!nodeCloudStorageWriter.value || !nodeCloudStorageWriter.value.cloud_storage_settings) {
    return;
  }
  console.log(nodeCloudStorageWriter);
  nodeCloudStorageWriter.value.is_setup = true;
  nodeStore.updateSettings(nodeCloudStorageWriter);
  dataLoaded.value = false;
};

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
});
</script>

<style scoped>
/* Copied styles from the reader component for consistency */
.cloud-storage-container {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  max-width: 100%;
  color: #333;
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

.info-box.info-warn {
  background-color: #fffbe6;
  border-left-color: #faad14;
}

.info-box.info-warn i {
  color: #faad14;
}

.info-box i {
  color: #1890ff;
  font-size: 1.25rem;
  flex-shrink: 0;
  padding-top: 2px;
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
