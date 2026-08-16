<template>
  <div v-if="dataLoaded && nodeRead" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodeRead"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="file-path-row">
          <el-input
            v-model="pathInput"
            placeholder="Path or ${param_name}/file.csv"
            clearable
            class="file-path-input"
            @change="
              (val: string) => {
                handleManualPathChange(val);
                saveSettings();
              }
            "
          >
            <template #prefix>
              <i class="fas fa-table" style="font-size: 14px" />
            </template>
          </el-input>
          <el-button title="Browse files" @click="modalVisibleForOpen = true">
            <span class="material-icons" style="font-size: 16px; line-height: 1">folder_open</span>
          </el-button>
        </div>
      </div>
      <div
        v-if="receivedTable && isDirectoryCapable(receivedTable.file_type)"
        class="listbox-wrapper"
      >
        <div class="listbox-subtitle">Source</div>
        <div class="scan-row">
          <label class="scan-label">Read</label>
          <el-select
            :model-value="scanMode"
            size="small"
            class="scan-control"
            @change="handleScanModeChange"
          >
            <el-option label="Single file" value="single_file" />
            <el-option label="Directory" value="directory" />
          </el-select>
        </div>
        <template v-if="scanMode === 'directory'">
          <div class="scan-row">
            <label class="scan-label">File format</label>
            <el-select
              :model-value="receivedTable.file_type"
              size="small"
              class="scan-control"
              @change="handleDirectoryFileTypeChange"
            >
              <el-option label="CSV" value="csv" />
              <el-option label="Parquet" value="parquet" />
              <el-option label="Arrow (IPC)" value="ipc" />
            </el-select>
          </div>
          <div class="scan-row">
            <label class="scan-label">File path column</label>
            <el-input
              v-model="includeFilePathsInput"
              size="small"
              class="scan-control"
              clearable
              placeholder="Optional: column name for the source file path"
              @change="handleIncludeFilePathsChange"
            />
          </div>
        </template>
      </div>
      <div v-if="receivedTable">
        <div class="listbox-wrapper">
          <div class="listbox-subtitle">File Specs</div>
          <ExcelTableConfig
            v-if="isInputExcelTable(receivedTable.table_settings)"
            v-model="receivedTable.table_settings"
            :path="receivedTable.path"
          />
          <CsvTableConfig
            v-if="isInputCsvTable(receivedTable.table_settings)"
            v-model="receivedTable.table_settings"
          />
          <ParquetTableConfig
            v-if="isInputParquetTable(receivedTable.table_settings)"
            v-model="receivedTable.table_settings"
          />
          <IpcTableConfig
            v-if="isInputIpcTable(receivedTable.table_settings)"
            v-model="receivedTable.table_settings"
          />
          <NdjsonTableConfig
            v-if="isInputNdjsonTable(receivedTable.table_settings)"
            v-model="receivedTable.table_settings"
          />
          <AvroTableConfig
            v-if="isInputAvroTable(receivedTable.table_settings)"
            v-model="receivedTable.table_settings"
          />
        </div>
      </div>

      <el-dialog
        v-model="modalVisibleForOpen"
        title="Select a file or folder to read"
        width="70%"
        append-to-body
        :close-on-click-modal="false"
      >
        <file-browser
          :allowed-file-types="[
            'csv',
            'txt',
            'tsv',
            'parquet',
            'xlsx',
            'xls',
            'ipc',
            'arrow',
            'feather',
            'ndjson',
            'jsonl',
            'avro',
          ]"
          mode="open"
          context="dataFiles"
          :is-visible="modalVisibleForOpen"
          :allow-directory-selection="true"
          @file-selected="handleFileChange"
          @directory-selected="handleDirectorySelected"
        />
      </el-dialog>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { computed, ref, watch } from "vue";
import ExcelTableConfig from "./readExcel.vue";
import CsvTableConfig from "./readCsv.vue";
import ParquetTableConfig from "./readParquet.vue";
import IpcTableConfig from "./readIpc.vue";
import NdjsonTableConfig from "./readNdjson.vue";
import AvroTableConfig from "./readAvro.vue";
import {
  ReceivedTable,
  NodeRead,
  ScanMode,
  isInputCsvTable,
  isInputExcelTable,
  isInputParquetTable,
  isInputIpcTable,
  isInputNdjsonTable,
  isInputAvroTable,
} from "../../../baseNode/nodeInput";
import {
  baseNameOf,
  createDefaultSettings,
  detectFileType,
  extensionOf,
  inferScanModeFromPath,
  isDirectoryCapable,
  type ReadFileType,
} from "../../../../../utils/readFileTypes";
import { scanModeForSelection } from "../../../../common/FileBrowser/cloudPathMapping";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import FileBrowser from "../../../../common/FileBrowser/fileBrowser.vue";
import { FileInfo } from "../../../../common/FileBrowser/types";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeRead = ref<null | NodeRead>(null);
const receivedTable = ref<ReceivedTable | null>(null);
const dataLoaded = ref(false);
const modalVisibleForOpen = ref(false);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeRead,
  onBeforeSave: () => {
    if (!nodeRead.value || !receivedTable.value) {
      console.warn("No node read value available");
      return false;
    }
    nodeRead.value.received_file = receivedTable.value;
    return true;
  },
});

// Use a plain ref so the input is always editable (writable computed resets
// on every keystroke when receivedTable is null, making the field unusable)
const pathInput = ref<string>("");

// Keep pathInput in sync when receivedTable changes externally (e.g. file browser)
watch(
  () => receivedTable.value?.path,
  (newPath) => {
    if (newPath !== undefined && newPath !== pathInput.value) {
      pathInput.value = newPath;
    }
  },
);

const scanMode = computed<ScanMode>(() => receivedTable.value?.scan_mode ?? "single_file");

const includeFilePathsInput = ref<string>("");

watch(
  () => receivedTable.value?.include_file_paths,
  (value) => {
    includeFilePathsInput.value = value ?? "";
  },
);

// Seed only: the scan-mode select below stays the authoritative override, so a
// path edit may promote to directory but never silently demotes an explicit pick.
function applyScanModeForPath(table: ReceivedTable, path: string) {
  if (!isDirectoryCapable(table.file_type)) {
    table.scan_mode = "single_file";
  } else if (inferScanModeFromPath(path) === "directory") {
    table.scan_mode = "directory";
  } else {
    table.scan_mode = table.scan_mode ?? "single_file";
  }
  if (table.scan_mode === "single_file") {
    table.include_file_paths = null;
  }
}

function handleManualPathChange(path: string) {
  const detectedType = detectFileType(path);
  const fileName = path.split(/[/\\]/).pop() || path;

  if (receivedTable.value) {
    receivedTable.value.path = path;
    receivedTable.value.name = fileName;
    // Swap settings when file type changes (e.g. user edits .csv → .parquet)
    if (detectedType && detectedType !== receivedTable.value.file_type) {
      receivedTable.value.file_type = detectedType;
      receivedTable.value.table_settings = createDefaultSettings(detectedType, extensionOf(path));
    }
    applyScanModeForPath(receivedTable.value, path);
  } else if (detectedType) {
    // Bootstrap a new receivedTable from a typed path
    const table: ReceivedTable = {
      name: fileName,
      path,
      file_type: detectedType,
      table_settings: createDefaultSettings(detectedType, extensionOf(path)),
      scan_mode: "single_file",
      include_file_paths: null,
    };
    applyScanModeForPath(table, path);
    receivedTable.value = table;
  }
}

function handleScanModeChange(value: string) {
  if (!receivedTable.value) return;
  receivedTable.value.scan_mode = value === "directory" ? "directory" : "single_file";
  if (receivedTable.value.scan_mode === "single_file") {
    receivedTable.value.include_file_paths = null;
  }
  // The schema differs per mode, so the cached one can never be reused.
  receivedTable.value.fields = [];
  saveSettings();
}

// A bare directory has no extension to sniff, so the format is picked explicitly.
function handleDirectoryFileTypeChange(value: string) {
  if (!receivedTable.value || !isDirectoryCapable(value)) return;
  receivedTable.value.file_type = value;
  receivedTable.value.table_settings = createDefaultSettings(value);
  receivedTable.value.fields = [];
  saveSettings();
}

function handleIncludeFilePathsChange(value: string) {
  if (!receivedTable.value) return;
  const columnName = value.trim();
  includeFilePathsInput.value = columnName;
  receivedTable.value.include_file_paths = columnName === "" ? null : columnName;
  receivedTable.value.fields = [];
  saveSettings();
}

const handleDirectorySelected = (directoryPath: string) => {
  const currentType = receivedTable.value?.file_type;
  const fileType: ReadFileType = isDirectoryCapable(currentType) ? currentType : "csv";
  const tableSettings =
    receivedTable.value && receivedTable.value.file_type === fileType
      ? receivedTable.value.table_settings
      : createDefaultSettings(fileType);

  receivedTable.value = {
    name: baseNameOf(directoryPath) || directoryPath,
    path: directoryPath,
    file_type: fileType,
    table_settings: tableSettings,
    // Only an explicit pick is authoritative about file-vs-directory; the select stays the override.
    scan_mode: scanModeForSelection(true),
    include_file_paths: receivedTable.value?.include_file_paths ?? null,
    fields: [],
  };

  modalVisibleForOpen.value = false;
};

const handleFileChange = (fileInfo: FileInfo) => {
  try {
    if (!fileInfo) {
      console.warn("No file info provided");
      return;
    }

    const fileType = detectFileType(fileInfo.name);
    if (!fileType) {
      console.warn("Unsupported file type:", fileInfo.name);
      return;
    }

    // Preserve the user's table_settings when re-selecting a file of the same type;
    // only rebuild defaults when the type actually changes (mirrors handleManualPathChange).
    const tableSettings =
      receivedTable.value && receivedTable.value.file_type === fileType
        ? receivedTable.value.table_settings
        : createDefaultSettings(fileType, extensionOf(fileInfo.name));

    receivedTable.value = {
      name: fileInfo.name,
      path: fileInfo.path,
      file_type: fileType,
      table_settings: tableSettings,
      // A file pick is authoritative about file-vs-directory; the select stays the override.
      scan_mode: scanModeForSelection(false),
      include_file_paths: null,
    };

    modalVisibleForOpen.value = false;
  } catch (error) {
    console.error("Error handling file change:", error);
  }
};

const loadNodeData = async (nodeId: number) => {
  try {
    const nodeResult = await nodeStore.getNodeData(nodeId, false);

    if (!nodeResult) {
      console.warn("No node result received");
      dataLoaded.value = true;
      return;
    }

    nodeRead.value = nodeResult.setting_input;

    if (nodeResult.setting_input?.is_setup && nodeResult.setting_input.received_file) {
      receivedTable.value = nodeResult.setting_input.received_file;
      pathInput.value = nodeResult.setting_input.received_file.path;
    }

    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = true;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.file-path-row {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
}

.file-path-input {
  flex: 1;
  min-width: 0;
}

.scan-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}

.scan-label {
  flex: 0 0 120px;
  font-size: 12px;
}

.scan-control {
  flex: 1;
  min-width: 0;
}
</style>
