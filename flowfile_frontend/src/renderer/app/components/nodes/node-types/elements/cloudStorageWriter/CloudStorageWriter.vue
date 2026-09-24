<template>
  <div v-if="loadView === 'form' && nodeCloudStorageWriter" class="cloud-storage-container">
    <generic-node-settings
      v-model="nodeCloudStorageWriter"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <CloudConnectionPicker
          v-model="selectedConnection"
          :connections="connectionInterfaces"
          :unavailable-connection="unavailableConnection"
          :loading="connectionsAreLoading"
          :resource-path="nodeCloudStorageWriter.cloud_storage_settings.resource_path"
          ambient-credentials
          @change="updateConnection"
        />
      </div>

      <div class="listbox-wrapper">
        <h4 class="section-subtitle">File Settings</h4>
        <div class="form-group">
          <label for="file-path">File Path</label>
          <div class="path-row">
            <input
              id="file-path"
              v-model="nodeCloudStorageWriter.cloud_storage_settings.resource_path"
              type="text"
              class="form-control"
              placeholder="s3://bucket/folder/output.parquet"
            />
            <el-button
              size="small"
              :disabled="!!browseDisabledReason"
              :title="browseDisabledReason ?? 'Browse cloud storage'"
              @click="showBrowser = true"
            >
              Browse
            </el-button>
          </div>
          <p v-if="pathWarning" class="field-warning" data-testid="cloud-path-warning">
            {{ pathWarning }}
          </p>
          <p class="field-hint">
            Full URI including the scheme &mdash; <code>s3://</code>, <code>az://</code> or
            <code>gs://</code>.
          </p>
          <p v-if="browseDisabledReason" class="field-hint">{{ browseDisabledReason }}</p>
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
            <template v-if="isDelta">
              <option value="error">Error if exists</option>
              <option value="append">Append</option>
              <option value="upsert" :disabled="isGcs">Upsert</option>
              <option value="update" :disabled="isGcs">Update</option>
              <option value="delete" :disabled="isGcs">Delete</option>
            </template>
          </select>
          <p v-if="isDelta && isGcs" class="field-hint">
            Upsert, update, delete and change tracking are not supported on Google Cloud Storage
            yet.
          </p>
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

        <div v-if="isDelta" class="format-options">
          <h5 class="subsection-title">Delta Options</h5>
          <DeltaTableStatusLine
            v-if="tableProbe"
            :exists="tableProbe.exists"
            :version="tableProbe.current_version"
            :partition-columns="tableProbe.partition_columns"
          />
          <p v-if="probeHint" class="field-warning" data-testid="delta-probe-hint">
            {{ probeHint }}
          </p>

          <div v-if="needsMergeKeys(writeMode)" class="form-group">
            <label for="merge-keys">Key columns</label>
            <MergeKeysSelect
              id="merge-keys"
              v-model="nodeCloudStorageWriter.cloud_storage_settings.merge_keys"
              :columns="availableColumns"
            />
          </div>

          <div v-if="canPartition(writeMode)" class="form-group">
            <label for="partition-by">Partition by (optional)</label>
            <el-select
              id="partition-by"
              v-model="nodeCloudStorageWriter.cloud_storage_settings.partition_by"
              multiple
              filterable
              size="small"
              placeholder="Select partition columns"
            >
              <el-option v-for="col in availableColumns" :key="col" :label="col" :value="col" />
            </el-select>
            <p class="field-hint">
              Set at table creation; writes to an existing table must match its partitioning. Use
              low-cardinality columns.
            </p>
          </div>

          <div class="form-group">
            <el-tooltip
              :disabled="trackChangesDisabled === null"
              :content="trackChangesDisabled ?? ''"
              placement="top"
            >
              <span>
                <el-checkbox
                  v-model="nodeCloudStorageWriter.cloud_storage_settings.track_changes"
                  size="small"
                  :disabled="trackChangesDisabled !== null"
                >
                  Track changes
                </el-checkbox>
              </span>
            </el-tooltip>
            <p class="field-hint">
              Records every insert, update and delete so a reader can read only what changed.
              Turning it on never turns it off again.
            </p>
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
        <div v-if="isDelta && otherModeDescription" class="info-box">
          <i class="fa-solid fa-info-circle"></i>
          <div>
            <p>{{ otherModeDescription }}</p>
          </div>
        </div>
      </div>
    </generic-node-settings>

    <CloudPathPicker
      v-model="showBrowser"
      :connection="selectedConnection"
      :mode="writeTargetsDirectory ? 'open' : 'create'"
      :allowed-file-types="writeFileTypes"
      :allow-directory-selection="writeTargetsDirectory"
      :initial-file-path="nodeCloudStorageWriter.cloud_storage_settings.resource_path"
      :default-new-file-name="defaultWriteFileName"
      context="cloudWrite"
      :title="writeTargetsDirectory ? 'Select a table directory' : 'Choose where to write'"
      @select="applyBrowsedPath"
    />
  </div>
  <div v-else-if="loadView === 'error'" class="load-error" data-testid="node-load-error">
    <p>{{ loadError }}</p>
    <el-button size="small" @click="loadNodeData(requestedNodeId)">Retry</el-button>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { computed, onUnmounted, ref, watch } from "vue";
import { NodeCloudStorageWriter } from "../../../baseNode/nodeInput";
import { createNodeCloudStorageWriter } from "./utils";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { fetchCloudStorageConnectionsInterfaces } from "../../../../../views/CloudConnectionView/api";
import { FullCloudStorageConnectionInterface } from "../../../../../views/CloudConnectionView/CloudConnectionTypes";
import { ElMessage } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { CloudConnectionPicker, DeltaTableStatusLine, MergeKeysSelect } from "../../../../common";
import {
  resolveConnection,
  unavailableConnectionName,
} from "../../../../common/CloudConnectionPicker/connectionOptions";
import CloudPathPicker from "../../../../common/FileBrowser/CloudPathPicker.vue";
import { browseUnsupportedReason } from "../../../../common/FileBrowser/browseSupport";
import {
  extensionForFormat,
  suggestedWriteFileName,
  targetsDirectory,
} from "../../../../common/FileBrowser/cloudPathMapping";
import { isCloudUri, storageTypeForUri } from "../../../../../utils/storagePath";
import { cloudPathWarning } from "../../../../../utils/cloudPathWarning";
import { settingsLoadView } from "../../../../../utils/settingsLoadView";
import { deltaProbeHint } from "../../../../../utils/cloudDeltaProbe";
import {
  canPartition,
  modeDescription,
  needsMergeKeys,
  trackChangesDisabledReason,
} from "../../../../../utils/deltaWriteModes";
import { CloudDeltaApi, type CloudDeltaInfo } from "../../../../../api/cloudDelta.api";
import type { WriteMode } from "../../../../../types/node.types";

interface Props {
  nodeId: number;
}

defineProps<Props>();
const nodeStore = useNodeStore();
const dataLoaded = ref<boolean>(false);
const loadError = ref<string | null>(null);
const requestedNodeId = ref(-1);
let loadSeq = 0;
const nodeCloudStorageWriter = ref<NodeCloudStorageWriter | null>(null);
const loadView = computed(() =>
  settingsLoadView(dataLoaded.value && nodeCloudStorageWriter.value !== null, loadError.value),
);
const availableColumns = ref<string[]>([]);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeCloudStorageWriter,
  onBeforeSave: () => {
    const settings = nodeCloudStorageWriter.value?.cloud_storage_settings;
    if (settings && needsMergeKeys(settings.write_mode) && !settings.merge_keys?.length) {
      ElMessage.error(`Select the key columns to ${settings.write_mode} on.`);
      return false;
    }
  },
});
const connectionInterfaces = ref<FullCloudStorageConnectionInterface[]>([]);
const connectionsAreLoading = ref(false);
const selectedConnection = ref<FullCloudStorageConnectionInterface | null>(null);
const unavailableConnection = ref<string | null>(null);
const showBrowser = ref(false);

/** Delta and Iceberg write to the directory itself, so no filename is chosen. */
const writeTargetsDirectory = computed(() =>
  targetsDirectory(nodeCloudStorageWriter.value?.cloud_storage_settings.file_format),
);

// A computed, not an inline literal: fileBrowser.vue watches this prop by identity.
const writeFileTypes = computed(() =>
  writeTargetsDirectory.value
    ? []
    : [extensionForFormat(nodeCloudStorageWriter.value?.cloud_storage_settings.file_format)],
);

const defaultWriteFileName = computed(() =>
  suggestedWriteFileName(
    nodeCloudStorageWriter.value?.cloud_storage_settings.file_format,
    nodeCloudStorageWriter.value?.cloud_storage_settings.resource_path,
  ),
);

const browseDisabledReason = computed<string | null>(() => {
  if (connectionsAreLoading.value) return "Loading connections…";
  if (selectedConnection.value) {
    return browseUnsupportedReason(
      selectedConnection.value.storageType,
      selectedConnection.value.authMethod,
    );
  }
  const currentPath = nodeCloudStorageWriter.value?.cloud_storage_settings.resource_path ?? "";
  if (storageTypeForUri(currentPath)) return null;
  return "Pick a connection, or type a path starting with s3://, az:// or gs:// to browse.";
});

const pathWarning = computed(() =>
  cloudPathWarning(nodeCloudStorageWriter.value?.cloud_storage_settings.resource_path, "writer"),
);

const isDelta = computed(
  () => nodeCloudStorageWriter.value?.cloud_storage_settings.file_format === "delta",
);

const writeMode = computed<WriteMode>(
  () => nodeCloudStorageWriter.value?.cloud_storage_settings.write_mode ?? "overwrite",
);

/** Delta merges and change tracking go through delta-rs, which takes no gcsfs-style options. */
const isGcs = computed(
  () =>
    storageTypeForUri(nodeCloudStorageWriter.value?.cloud_storage_settings.resource_path ?? "") ===
      "gcs" || selectedConnection.value?.storageType === "gcs",
);

const trackChangesDisabled = computed(() =>
  isGcs.value
    ? "Change tracking is not supported on Google Cloud Storage yet."
    : trackChangesDisabledReason(writeMode.value),
);

// Overwrite and append keep their own info boxes above.
const otherModeDescription = computed(() =>
  writeMode.value === "overwrite" || writeMode.value === "append"
    ? null
    : modeDescription(writeMode.value),
);

// Existing-table probe; null while unknown, when it failed, or for gs:// (unsupported there).
const tableProbe = ref<CloudDeltaInfo | null>(null);
const probeHint = ref<string | null>(null);
let probeTimer: ReturnType<typeof setTimeout> | null = null;
let probeSeq = 0;

async function probeTable() {
  const settings = nodeCloudStorageWriter.value?.cloud_storage_settings;
  if (!settings) return;
  const seq = probeSeq;
  try {
    const info = await CloudDeltaApi.getInfo({
      resource_path: settings.resource_path,
      connection_name: settings.connection_name ?? null,
      auth_mode: settings.auth_mode,
    });
    if (seq === probeSeq) tableProbe.value = info;
  } catch (error) {
    // Show no status rather than a wrong "new table", but say why when the run would fail too.
    if (seq === probeSeq) probeHint.value = deltaProbeHint(error);
  }
}

function scheduleProbe() {
  if (probeTimer) clearTimeout(probeTimer);
  probeSeq += 1;
  tableProbe.value = null;
  probeHint.value = null;
  const path = nodeCloudStorageWriter.value?.cloud_storage_settings.resource_path ?? "";
  if (!isDelta.value || isGcs.value || !isCloudUri(path)) return;
  probeTimer = setTimeout(probeTable, 350);
}

onUnmounted(() => {
  if (probeTimer) clearTimeout(probeTimer);
});

watch(
  () => {
    const settings = nodeCloudStorageWriter.value?.cloud_storage_settings;
    return [settings?.file_format, settings?.resource_path, settings?.connection_name];
  },
  () => scheduleProbe(),
);

watch([writeMode, isGcs], ([mode, gcs]) => {
  const settings = nodeCloudStorageWriter.value?.cloud_storage_settings;
  if (!settings) return;
  if (!needsMergeKeys(mode)) settings.merge_keys = [];
  if (!canPartition(mode)) settings.partition_by = [];
  if (mode === "overwrite" || gcs) settings.track_changes = false;
});

const applyBrowsedPath = (selectedPath: string) => {
  const settings = nodeCloudStorageWriter.value?.cloud_storage_settings;
  if (settings) settings.resource_path = selectedPath;
};

const handleFileFormatChange = () => {
  if (nodeCloudStorageWriter.value) {
    const settings = nodeCloudStorageWriter.value.cloud_storage_settings;
    const format = settings.file_format;

    if (format !== "delta") {
      settings.write_mode = "overwrite";
      settings.partition_by = [];
      settings.merge_keys = [];
      settings.track_changes = false;
    }

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
    unavailableConnection.value = null;
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

const setConnectionOnConnectionName = (connectionName: string | null) => {
  selectedConnection.value = resolveConnection(connectionInterfaces.value, connectionName ?? "");
  unavailableConnection.value = unavailableConnectionName(
    connectionInterfaces.value,
    connectionName,
  );
};

// Nothing stale stays behind to save; the drawer shows a retry instead of a skeleton.
const failLoad = () => {
  nodeCloudStorageWriter.value = null;
  dataLoaded.value = false;
  loadError.value =
    "Could not load this node's settings. Check that Flowfile is running, then retry.";
};

const loadNodeData = async (nodeId: number) => {
  const seq = ++loadSeq;
  requestedNodeId.value = nodeId;
  loadError.value = null;
  try {
    const [nodeData] = await Promise.all([
      nodeStore.getNodeData(nodeId, false),
      fetchConnections(),
    ]);
    if (seq !== loadSeq) return;
    if (!nodeData) {
      failLoad();
      return;
    }
    const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
    nodeCloudStorageWriter.value = hasValidSetup
      ? nodeData.setting_input
      : createNodeCloudStorageWriter(nodeStore.flow_id, nodeId);

    availableColumns.value = nodeData.main_input?.columns ?? [];
    // Backfill fields for nodes saved before partitioning / merge support
    const settings = nodeCloudStorageWriter.value!.cloud_storage_settings;
    if (!settings.partition_by) settings.partition_by = [];
    settings.merge_keys = settings.merge_keys ?? [];
    settings.track_changes = settings.track_changes ?? false;

    setConnectionOnConnectionName(
      nodeCloudStorageWriter.value?.cloud_storage_settings.connection_name ?? null,
    );
    dataLoaded.value = true;
  } catch (error) {
    if (seq !== loadSeq) return;
    console.error("Error loading node data:", error);
    ElMessage.error("Failed to load node settings.");
    failLoad();
  }
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
  saveSettings,
  // A failed load has nothing to apply; the drawer hides its Apply footer.
  canApply: computed(() => loadError.value === null),
});
</script>

<style scoped>
/* Copied styles from the reader component for consistency */
.cloud-storage-container {
  font-family: var(--font-family-base);
  max-width: 100%;
  color: var(--color-text-primary);
}

.section-subtitle {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.subsection-title {
  margin: 0.5rem 0 0.5rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--color-text-tertiary);
}

.field-hint {
  margin: 4px 0 0;
  font-size: 11px;
  color: var(--color-text-tertiary, #718096);
}

.field-warning {
  margin: 0.25rem 0 0 0;
  font-size: 0.75rem;
  color: var(--color-warning-dark);
}

.load-error {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.5rem;
  padding: 1rem;
  font-size: 0.875rem;
  color: var(--color-danger);
}

.load-error p {
  margin: 0;
}

.format-options {
  margin-top: 1rem;
  padding: 1rem;
  background-color: var(--color-background-secondary);
  border-radius: 4px;
  border: 1px solid var(--color-border-primary);
}

.table-status {
  margin-bottom: 0.75rem;
  background-color: var(--color-background-primary);
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
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
}

.form-group {
  margin-bottom: 0.75rem;
  width: 100%;
}

.path-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.path-row .form-control {
  flex: 1;
  min-width: 0;
}

.field-hint code {
  background-color: var(--color-background-tertiary);
  padding: 0 0.25rem;
  border-radius: 3px;
}

label {
  display: block;
  margin-bottom: 0.25rem;
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--color-text-secondary);
}

select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%2394a3b8' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}

.info-box {
  display: flex;
  gap: 0.75rem;
  padding: 0.75rem;
  background-color: var(--color-accent-subtle);
  border-left: 4px solid var(--color-accent);
  border-radius: 4px;
  margin-top: 1rem;
  font-size: 0.875rem;
}

.info-box.info-warn {
  background-color: var(--color-warning-light);
  border-left-color: var(--color-warning);
}

.info-box.info-warn i {
  color: var(--color-warning);
}

.info-box i {
  color: var(--color-accent-dark);
  font-size: 1.25rem;
  flex-shrink: 0;
  padding-top: 2px;
}

.info-box p {
  margin: 0;
  color: var(--color-text-secondary);
}

.helper-text {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
  font-size: 0.8125rem;
  color: var(--color-text-tertiary);
}

.helper-text i {
  color: var(--color-accent-dark);
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
  color: var(--color-text-tertiary);
  font-size: 0.875rem;
}

.loading-spinner {
  width: 2rem;
  height: 2rem;
  border: 2px solid var(--color-border-primary);
  border-top-color: var(--color-accent);
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
