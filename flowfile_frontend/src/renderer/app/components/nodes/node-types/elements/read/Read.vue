<template>
  <div v-if="dataLoaded && nodeRead" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodeRead"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveNodeData"
    >
      <div class="listbox-wrapper">
        <div class="file-upload-container">
          <div class="file-upload-wrapper" @click="modalVisibleForOpen = true">
            <label for="file-upload" class="file-upload-label">
              <i class="fas fa-table file-icon"></i>
              <span class="file-label-text">
                {{ getDisplayFileName }}
              </span>
            </label>
          </div>
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
import { ref, computed } from "vue";
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
import { useEditorStore } from "../../../../../stores/editor-store";
import FileBrowser from "../../../../common/FileBrowser/fileBrowser.vue";
import { FileInfo } from "../../../../common/FileBrowser/types";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { useGenericNodeSettings } from "../../../../../composables/useGenericNodeSettings";

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const selectedFile = ref<FileInfo | null>(null);
const nodeRead = ref<null | NodeRead>(null);
const receivedTable = ref<ReceivedTable | null>(null);
const dataLoaded = ref(false);
const modalVisibleForOpen = ref(false);

// Use composable for automatic NodeBase property syncing
const { handleGenericSettingsUpdate } = useGenericNodeSettings(nodeRead);

const getDisplayFileName = computed(() => {
  if (selectedFile.value?.name) {
    return selectedFile.value.name;
  }
  if (receivedTable.value?.name) {
    return receivedTable.value.name;
  }
  return "Choose a file...";
});

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

    selectedFile.value = fileInfo;
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
    }

    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = true;
  }
};

const saveNodeData = async () => {
  if (!nodeRead.value || !receivedTable.value) {
    console.warn("No node read value available");
    return;
  }

  nodeRead.value.is_setup = true;
  nodeRead.value.received_file = receivedTable.value;

  await nodeStore.updateSettings(nodeRead);
};

const pushNodeData = async () => {
  try {
    await saveNodeData();
    // Trigger drawer close via editor store
    editorStore.pushNodeData();
  } catch (error) {
    console.error("Error pushing node data:", error);
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: var(--color-background-primary);
  padding: 8px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  border-radius: 4px;
}

.context-menu button {
  display: block;
  background: none;
  border: none;
  padding: 4px 8px;
  text-align: left;
  width: 100%;
  cursor: pointer;
}

.context-menu button:hover {
  background-color: var(--color-background-secondary);
}

.file-upload-wrapper {
  position: relative;
  width: 100%;
}

.file-upload-label {
  display: flex;
  align-items: center;
  background-color: var(--color-background-secondary);
  border: 1px solid #ddd;
  border-radius: 4px;
  padding: 10px 15px;
  color: var(--color-text-primary);
  font-size: 16px;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.3s ease;
}

.file-upload-label:hover {
  background-color: var(--color-background-primary);
}

.file-icon {
  margin-right: 10px;
  font-size: 20px;
}

.file-label-text {
  flex-grow: 1;
  margin-left: 10px;
}

input[type="text"] {
  width: 100%;
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
  transition: border-color 0.2s ease;
}

input[type="text"]:focus {
  border-color: #3498db;
  outline: none;
}
</style>
