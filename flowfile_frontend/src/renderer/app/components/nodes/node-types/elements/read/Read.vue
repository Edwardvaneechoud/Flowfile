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
            @change="(val: string) => { handleManualPathChange(val); saveSettings(); }"
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
        </div>
      </div>

      <el-dialog v-model="modalVisibleForOpen" title="Select a file to Read" width="70%">
        <file-browser
          :allowed-file-types="['csv', 'txt', 'parquet', 'xlsx']"
          mode="open"
          context="dataFiles"
          :is-visible="modalVisibleForOpen"
          @file-selected="handleFileChange"
        />
      </el-dialog>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref, watch } from "vue";
import ExcelTableConfig from "./readExcel.vue";
import CsvTableConfig from "./readCsv.vue";
import ParquetTableConfig from "./readParquet.vue";
import {
  ReceivedTable,
  NodeRead,
  isInputCsvTable,
  isInputExcelTable,
  isInputParquetTable,
  InputCsvTable,
  InputExcelTable,
  InputParquetTable,
} from "../../../baseNode/nodeInput";
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

// Use the standardized node settings composable
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

// ---------------------------------------------------------------------------
// Manual path input
// ---------------------------------------------------------------------------

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

function detectFileType(path: string): "csv" | "excel" | "parquet" | null {
  // Strip ${...} references so we can read the real extension
  const cleaned = path.replace(/\$\{[^}]*\}/g, "");
  const ext = cleaned.split(".").pop()?.toLowerCase();
  if (ext === "xlsx") return "excel";
  if (ext === "csv" || ext === "txt") return "csv";
  if (ext === "parquet") return "parquet";
  return null;
}

function createDefaultSettings(
  fileType: "csv" | "excel" | "parquet",
): InputCsvTable | InputExcelTable | InputParquetTable {
  if (fileType === "excel") return createDefaultExcelSettings();
  if (fileType === "parquet") return createDefaultParquetSettings();
  return createDefaultCsvSettings();
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
      receivedTable.value.table_settings = createDefaultSettings(detectedType);
    }
  } else if (detectedType) {
    // Bootstrap a new receivedTable from a typed path
    receivedTable.value = {
      name: fileName,
      path,
      file_type: detectedType,
      table_settings: createDefaultSettings(detectedType),
    };
  }
}

// Default table settings factories
function createDefaultCsvSettings(): InputCsvTable {
  return {
    file_type: "csv",
    reference: "",
    starting_from_line: 0,
    delimiter: ",",
    has_headers: true,
    encoding: "utf-8",
    row_delimiter: "\n",
    quote_char: '"',
    infer_schema_length: 1000,
    truncate_ragged_lines: false,
    ignore_errors: false,
  };
}

function createDefaultExcelSettings(): InputExcelTable {
  return {
    file_type: "excel",
    sheet_name: "",
    start_row: 0,
    start_column: 0,
    end_row: 0,
    end_column: 0,
    has_headers: true,
    type_inference: false,
  };
}

function createDefaultParquetSettings(): InputParquetTable {
  return {
    file_type: "parquet",
  };
}

const handleFileChange = (fileInfo: FileInfo) => {
  try {
    if (!fileInfo) {
      console.warn("No file info provided");
      return;
    }

    const ext = fileInfo.name.split(".").pop()?.toLowerCase();
    if (!ext) {
      console.warn("No file type detected");
      return;
    }

    let fileType: "csv" | "excel" | "parquet";
    let tableSettings: InputCsvTable | InputExcelTable | InputParquetTable;

    switch (ext) {
      case "xlsx":
        fileType = "excel";
        tableSettings = createDefaultExcelSettings();
        break;
      case "csv":
      case "txt":
        fileType = "csv";
        tableSettings = createDefaultCsvSettings();
        break;
      case "parquet":
        fileType = "parquet";
        tableSettings = createDefaultParquetSettings();
        break;
      default:
        console.warn("Unsupported file type:", ext);
        return;
    }

    receivedTable.value = {
      name: fileInfo.name,
      path: fileInfo.path,
      file_type: fileType,
      table_settings: tableSettings,
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
</style>
