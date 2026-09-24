<template>
  <div v-if="dataLoaded && nodeCloudStorageReader" class="cloud-storage-container">
    <generic-node-settings
      v-model="nodeCloudStorageReader"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <!-- Connection Selection -->
      <div class="listbox-wrapper">
        <CloudConnectionPicker
          v-model="selectedConnection"
          :connections="connectionInterfaces"
          :unavailable-connection="unavailableConnection"
          :loading="connectionsAreLoading"
          ambient-credentials
          @change="updateConnection"
        />
      </div>
      <!-- File Path and Scan Settings -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">File Settings</h4>

        <!-- File Path -->
        <div class="form-group">
          <label for="file-path">File Path</label>
          <div class="path-row">
            <input
              id="file-path"
              v-model="nodeCloudStorageReader.cloud_storage_settings.resource_path"
              type="text"
              class="form-control"
              placeholder="s3://bucket/folder/file.parquet"
              @input="resetFields"
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
          <p v-if="formatWarning" class="field-warning">{{ formatWarning }}</p>
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
        <div v-if="isDelta" class="format-options">
          <h5 class="subsection-title">Delta Lake Options</h5>

          <div class="form-group">
            <ChangeFeedReadSection
              :model-value="cdcSettings"
              :node-id="nodeCloudStorageReader.node_id"
              :status="cdcStatus"
              :version-options="versionOptions"
              :disabled-reason="readChangesDisabledReason"
              :can-enable="!!deltaInfo?.exists"
              :enabling="enablingCdc"
              :allow-last-run="false"
              persist-key="cloudReader.cdc"
              @update:model-value="applyCdcSettings"
              @enable="enableTracking"
            />
          </div>

          <div class="form-group">
            <label for="delta-version">Version (optional)</label>
            <el-select
              v-if="versionOptions.length > 0"
              id="delta-version"
              v-model="nodeCloudStorageReader.cloud_storage_settings.delta_version"
              size="small"
              placeholder="Latest"
              clearable
            >
              <el-option
                v-for="v in versionOptions"
                :key="v.version"
                :label="v.label"
                :value="v.version"
              />
            </el-select>
            <input
              v-else
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

    <CloudPathPicker
      v-model="showBrowser"
      :connection="selectedConnection"
      :allowed-file-types="browseFileTypes"
      :initial-file-path="nodeCloudStorageReader.cloud_storage_settings.resource_path"
      context="cloudRead"
      allow-directory-selection
      title="Select a cloud file or folder to read"
      @select="applyBrowsedPath"
    />
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { computed, onUnmounted, ref, watch } from "vue";
import { NodeCloudStorageReader } from "../../../baseNode/nodeInput";
import { createNodeCloudStorageReader } from "./utils";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { fetchCloudStorageConnectionsInterfaces } from "../../../../../views/CloudConnectionView/api";
import { FullCloudStorageConnectionInterface } from "../../../../../views/CloudConnectionView/CloudConnectionTypes";
import { ElMessage } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { ChangeFeedReadSection, CloudConnectionPicker } from "../../../../common";
import {
  resolveConnection,
  unavailableConnectionName,
} from "../../../../common/CloudConnectionPicker/connectionOptions";
import CloudPathPicker from "../../../../common/FileBrowser/CloudPathPicker.vue";
import { browseUnsupportedReason } from "../../../../common/FileBrowser/browseSupport";
import {
  browseFileTypesForFormat,
  formatMismatchWarning,
  scanModeForSelection,
} from "../../../../common/FileBrowser/cloudPathMapping";
import { isCloudUri, storageTypeForUri } from "../../../../../utils/storagePath";
import { cloudPathWarning } from "../../../../../utils/cloudPathWarning";
import { cdcFieldErrors, deltaVersionOptions } from "../../../../../utils/catalogCdc";
import {
  CloudDeltaApi,
  type CloudDeltaInfo,
  type CloudDeltaTarget,
} from "../../../../../api/cloudDelta.api";
import type { DeltaVersionCommit, TableCdcStatus } from "../../../../../types/catalog.types";
import {
  DEFAULT_CDC_SETTINGS,
  type CdcReaderSettings,
  type CloudStorageReadSettings,
} from "../../../../../types/node.types";

interface Props {
  nodeId: number;
}

defineProps<Props>();
const nodeStore = useNodeStore();
const dataLoaded = ref<boolean>(false);
const nodeCloudStorageReader = ref<NodeCloudStorageReader | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeCloudStorageReader,
  onBeforeSave: () => {
    if (!isDelta.value) return;
    const errors = cdcFieldErrors(cdcSettings.value);
    const error = errors.version ?? errors.timestamp;
    if (error) {
      ElMessage.error(error);
      return false;
    }
  },
});
const connectionInterfaces = ref<FullCloudStorageConnectionInterface[]>([]);
const connectionsAreLoading = ref(false);
const selectedConnection = ref<FullCloudStorageConnectionInterface | null>(null);
const unavailableConnection = ref<string | null>(null);
const showBrowser = ref(false);

// Must be a computed: fileBrowser.vue watches this prop by identity, so an inline
// array literal would re-list on every parent render — a paid request per render.
const browseFileTypes = computed(() =>
  browseFileTypesForFormat(nodeCloudStorageReader.value?.cloud_storage_settings.file_format),
);

const pathWarning = computed(() =>
  cloudPathWarning(nodeCloudStorageReader.value?.cloud_storage_settings.resource_path, "reader"),
);

const formatWarning = computed(() =>
  formatMismatchWarning(
    nodeCloudStorageReader.value?.cloud_storage_settings.file_format,
    nodeCloudStorageReader.value?.cloud_storage_settings.resource_path,
  ),
);

/** Browsing needs to know which storage to root at, and that the auth method supports it. */
const browseDisabledReason = computed<string | null>(() => {
  if (connectionsAreLoading.value) return "Loading connections…";
  if (selectedConnection.value) {
    return browseUnsupportedReason(
      selectedConnection.value.storageType,
      selectedConnection.value.authMethod,
    );
  }
  const currentPath = nodeCloudStorageReader.value?.cloud_storage_settings.resource_path ?? "";
  if (storageTypeForUri(currentPath)) return null;
  return "Pick a connection, or type a path starting with s3://, az:// or gs:// to browse.";
});

const readSettings = computed(() => nodeCloudStorageReader.value?.cloud_storage_settings ?? null);

const isDelta = computed(() => readSettings.value?.file_format === "delta");

/** Change reads go through delta-rs, which takes no gcsfs-style GCS options. */
const isGcs = computed(
  () =>
    storageTypeForUri(readSettings.value?.resource_path ?? "") === "gcs" ||
    selectedConnection.value?.storageType === "gcs",
);

const cdcSettings = computed<CdcReaderSettings>(() => ({
  cdc_mode: readSettings.value?.cdc_mode ?? "off",
  cdc_from_version: readSettings.value?.cdc_from_version ?? null,
  cdc_from_timestamp: readSettings.value?.cdc_from_timestamp ?? null,
  cdc_include_preimage: readSettings.value?.cdc_include_preimage ?? false,
}));

function applyCdcSettings(next: CdcReaderSettings) {
  const settings = readSettings.value;
  if (!settings) return;
  // The section never offers since_last_run without allowLastRun.
  settings.cdc_mode = next.cdc_mode as NonNullable<CloudStorageReadSettings["cdc_mode"]>;
  settings.cdc_from_version = next.cdc_from_version;
  settings.cdc_from_timestamp = next.cdc_from_timestamp;
  settings.cdc_include_preimage = next.cdc_include_preimage;
}

const readChangesDisabledReason = computed<string | null>(() => {
  if (isGcs.value) return "Reading changes is not supported on Google Cloud Storage yet.";
  if (typeof readSettings.value?.delta_version === "number") {
    return "Reading changes and time travel are mutually exclusive — clear the version to read changes.";
  }
  return null;
});

// A disabled Read selector must not keep a change mode the backend would reject.
watch(readChangesDisabledReason, (reason) => {
  if (reason !== null && cdcSettings.value.cdc_mode !== "off") {
    applyCdcSettings(DEFAULT_CDC_SETTINGS);
  }
});

// Table state at the path: null while unknown, when the probe failed, or for gs://.
const deltaInfo = ref<CloudDeltaInfo | null>(null);
const deltaHistory = ref<DeltaVersionCommit[]>([]);
const enablingCdc = ref(false);
let probeTimer: ReturnType<typeof setTimeout> | null = null;
let probeSeq = 0;

const cdcStatus = computed<TableCdcStatus | null>(() =>
  deltaInfo.value?.exists
    ? {
        cdc_enabled: deltaInfo.value.cdc_enabled,
        cdc_enabled_version: deltaInfo.value.cdc_enabled_version,
        current_version: deltaInfo.value.current_version,
        cursors: [],
      }
    : null,
);

const versionOptions = computed(() => deltaVersionOptions(deltaHistory.value));

function deltaTarget(): CloudDeltaTarget | null {
  const settings = readSettings.value;
  if (!settings || !isDelta.value || isGcs.value || !isCloudUri(settings.resource_path)) {
    return null;
  }
  return {
    resource_path: settings.resource_path,
    connection_name: settings.connection_name ?? null,
    auth_mode: settings.auth_mode,
  };
}

async function loadDeltaState() {
  const target = deltaTarget();
  if (!target) return;
  const seq = probeSeq;
  try {
    const info = await CloudDeltaApi.getInfo(target);
    if (seq !== probeSeq) return;
    deltaInfo.value = info;
    if (!info.exists) return;
    const history = await CloudDeltaApi.getHistory(target, 100);
    if (seq === probeSeq) deltaHistory.value = history;
  } catch {
    // Unknown state: no tracking warning, and the version fields fall back to plain inputs.
  }
}

function scheduleDeltaProbe() {
  if (probeTimer) clearTimeout(probeTimer);
  probeSeq += 1;
  deltaInfo.value = null;
  deltaHistory.value = [];
  if (deltaTarget()) probeTimer = setTimeout(loadDeltaState, 350);
}

onUnmounted(() => {
  if (probeTimer) clearTimeout(probeTimer);
});

async function enableTracking() {
  const target = deltaTarget();
  if (!target) return;
  const seq = probeSeq;
  enablingCdc.value = true;
  try {
    const info = await CloudDeltaApi.enableCdc(target);
    if (seq !== probeSeq) return;
    deltaInfo.value = info;
    ElMessage.success("Change tracking enabled");
    // Enabling is itself a commit, so the version list moved.
    const history = await CloudDeltaApi.getHistory(target, 100).catch(() => null);
    if (history && seq === probeSeq) deltaHistory.value = history;
  } catch (e: any) {
    const detail = e?.response?.data?.detail;
    ElMessage.error(detail?.message ?? e?.message ?? "Could not enable change tracking");
  } finally {
    enablingCdc.value = false;
  }
}

const applyBrowsedPath = (selectedPath: string, isDirectory: boolean) => {
  const settings = nodeCloudStorageReader.value?.cloud_storage_settings;
  if (!settings) return;
  settings.resource_path = selectedPath;
  // Only an explicit pick is authoritative about file-vs-prefix; the scan-mode
  // select stays visible below so the value is always shown and overridable.
  if (settings.file_format !== "delta") {
    settings.scan_mode = scanModeForSelection(isDirectory);
  }
  resetFields();
};

const handleFileFormatChange = () => {
  resetFields();
  if (nodeCloudStorageReader.value) {
    const format = nodeCloudStorageReader.value.cloud_storage_settings.file_format;

    if (format === "csv") {
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
      nodeCloudStorageReader.value.cloud_storage_settings.csv_has_header = undefined;
      nodeCloudStorageReader.value.cloud_storage_settings.csv_delimiter = undefined;
      nodeCloudStorageReader.value.cloud_storage_settings.csv_encoding = undefined;
    }

    if (format !== "delta") {
      nodeCloudStorageReader.value.cloud_storage_settings.delta_version = undefined;
      applyCdcSettings(DEFAULT_CDC_SETTINGS);
    }
  }
};

// Only invalidates cached state (schema, Delta table probe) — must not touch the connection,
// or editing the file path would wipe a connection the picker merely failed to resolve.
const resetFields = () => {
  if (nodeCloudStorageReader.value) {
    nodeCloudStorageReader.value.fields = [];
  }
  scheduleDeltaProbe();
};

const updateConnection = () => {
  resetFields();
  if (nodeCloudStorageReader.value) {
    unavailableConnection.value = null;
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

const setConnectionOnConnectionName = (connectionName: string | null) => {
  selectedConnection.value = resolveConnection(connectionInterfaces.value, connectionName ?? "");
  unavailableConnection.value = unavailableConnectionName(
    connectionInterfaces.value,
    connectionName,
  );
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
      // Backfill change-feed fields for nodes saved before change reads
      applyCdcSettings(cdcSettings.value);
      setConnectionOnConnectionName(
        nodeCloudStorageReader.value?.cloud_storage_settings.connection_name ?? null,
      );
      scheduleDeltaProbe();
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = false;
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
  color: var(--color-text-secondary);
}

.subsection-title {
  margin: 0.5rem 0 0.5rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--color-text-tertiary);
}

.format-options {
  margin-top: 1rem;
  padding: 1rem;
  background-color: var(--color-background-secondary);
  border-radius: 4px;
  border: 1px solid var(--color-border-primary);
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

.field-hint {
  margin: 0.25rem 0 0 0;
  font-size: 0.75rem;
  color: var(--color-text-tertiary);
}

.field-hint code {
  background-color: var(--color-background-tertiary);
  padding: 0 0.25rem;
  border-radius: 3px;
}

.field-warning {
  margin: 0.25rem 0 0 0;
  font-size: 0.75rem;
  color: var(--color-warning-dark);
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
  background-color: var(--color-accent-subtle);
  border-left: 4px solid var(--color-accent);
  border-radius: 4px;
  margin-top: 1rem;
  font-size: 0.875rem;
}

.info-box i {
  color: var(--color-accent-dark);
  font-size: 1.25rem;
  flex-shrink: 0;
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
