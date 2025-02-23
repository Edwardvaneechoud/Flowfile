<template>
  <div v-if="dataLoaded" class="listbox-wrapper">
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
    <div v-if="isCsvFile || isExcelFile || isParquetFile">
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">File Specs</div>
        <ExcelTableConfig v-if="isExcelFile" v-model="receivedExcelTable" />
        <CsvTableConfig v-if="isCsvFile" v-model="receivedCsvTable" />
        <ParquetTableConfig v-if="isParquetFile" v-model="receivedParquetTable" />
      </div>
    </div>

    <el-dialog v-model="modalVisibleForOpen" title="Select a file to Read" width="70%">
      <file-browser
        :allowed-file-types="['csv', 'txt', 'parquet', 'xlsx']"
        mode="open"
        @file-selected="handleFileChange"
      />
    </el-dialog>
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
  ReceivedExcelTable,
  ReceivedCsvTable,
  ReceivedParquetTable,
  NodeRead,
} from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/column-store";
import FileBrowser from "../../../components/fileBrowser/fileBrowser.vue";
import { FileInfo } from "../../../components/fileBrowser/types";

const nodeStore = useNodeStore();
const selectedFile = ref<FileInfo | null>(null);
const isExcelFile = ref(false);
const isCsvFile = ref(false);
const isParquetFile = ref(false);
const nodeRead = ref<null | NodeRead>(null);
const dataLoaded = ref(false);
const selectedPath = ref("");
const modalVisibleForOpen = ref(false);

// Computed property to safely handle file name display
const getDisplayFileName = computed(() => {
  if (selectedFile.value?.name) {
    return selectedFile.value.name;
  }
  if (nodeRead.value?.received_file?.name) {
    return nodeRead.value.received_file.name;
  }
  return "Choose a file...";
});

const receivedExcelTable = ref<ReceivedExcelTable>({
  name: "",
  path: "",
  file_type: "excel",
  sheet_name: "",
  start_row: 0,
  start_column: 0,
  end_row: 0,
  end_column: 0,
  has_headers: true,
  type_inference: false,
});

const receivedCsvTable = ref<ReceivedCsvTable>({
  name: "",
  path: "",
  file_type: "csv",
  reference: "",
  starting_from_line: 0,
  delimiter: ",",
  has_headers: true,
  encoding: "utf-8",
  row_delimiter: "",
  quote_char: "",
  infer_schema_length: 1000,
  truncate_ragged_lines: false,
  ignore_errors: false,
});

const receivedParquetTable = ref<ReceivedParquetTable>({
  name: "",
  path: "",
  file_type: "parquet",
});

const handleFileChange = (fileInfo: FileInfo) => {
  try {
    if (!fileInfo) {
      console.warn("No file info provided");
      return;
    }

    const fileType = fileInfo.name.split(".").pop()?.toLowerCase();
    if (!fileType) {
      console.warn("No file type detected");
      return;
    }

    // Reset all file type flags
    isExcelFile.value = false;
    isCsvFile.value = false;
    isParquetFile.value = false;

    // Set appropriate flag based on file type
    switch (fileType) {
      case "xlsx":
        isExcelFile.value = true;
        receivedExcelTable.value.path = fileInfo.path;
        receivedExcelTable.value.name = fileInfo.name;
        break;
      case "csv":
      case "txt":
        isCsvFile.value = true;
        receivedCsvTable.value.path = fileInfo.path;
        receivedCsvTable.value.name = fileInfo.name;
        break;
      case "parquet":
        isParquetFile.value = true;
        receivedParquetTable.value.path = fileInfo.path;
        receivedParquetTable.value.name = fileInfo.name;
        break;
      default:
        console.warn("Unsupported file type:", fileType);
        return;
    }

    selectedFile.value = fileInfo;
    selectedPath.value = fileInfo.path;
    modalVisibleForOpen.value = false;
  } catch (error) {
    console.error("Error handling file change:", error);
  }
};

const loadNodeData = async (nodeId: number) => {
  try {
    const nodeResult = await nodeStore.getNodeData(1, nodeId, false);

    if (!nodeResult) {
      console.warn("No node result received");
      dataLoaded.value = true;
      return;
    }

    nodeRead.value = nodeResult.setting_input;

    if (nodeResult.setting_input?.is_setup && nodeResult.setting_input.received_file) {
      const { file_type } = nodeResult.setting_input.received_file;

      // Reset all flags
      isExcelFile.value = false;
      isCsvFile.value = false;
      isParquetFile.value = false;

      switch (file_type) {
        case "excel":
          isExcelFile.value = true;
          receivedExcelTable.value = nodeResult.setting_input.received_file;
          break;
        case "csv":
          isCsvFile.value = true;
          receivedCsvTable.value = nodeResult.setting_input.received_file;
          break;
        case "parquet":
          isParquetFile.value = true;
          receivedParquetTable.value = nodeResult.setting_input.received_file;
          break;
      }

      selectedPath.value = nodeResult.setting_input.received_file.path;
    }

    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = true;
  }
};

const pushNodeData = async () => {
  try {
    dataLoaded.value = false;

    if (!nodeRead.value) {
      console.warn("No node read value available");
      dataLoaded.value = true;
      return;
    }

    nodeRead.value.is_setup = true;

    if (isExcelFile.value) {
      // nodeRead.value.cache_results = true;
      nodeRead.value.received_file = receivedExcelTable.value;
    } else if (isCsvFile.value) {
      // nodeRead.value.cache_results = true;
      nodeRead.value.received_file = receivedCsvTable.value;
    } else if (isParquetFile.value) {
      nodeRead.value.cache_results = false;
      nodeRead.value.received_file = receivedParquetTable.value;
    }

    await nodeStore.updateSettings(nodeRead);
  } catch (error) {
    console.error("Error pushing node data:", error);
  } finally {
    dataLoaded.value = true;
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
  background-color: white;
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
  background-color: #f0f0f0;
}

.file-upload-wrapper {
  position: relative;
  width: 100%;
}

.file-upload-label {
  display: flex;
  align-items: center;
  background-color: #f5f5f5;
  border: 1px solid #ddd;
  border-radius: 4px;
  padding: 10px 15px;
  color: #333;
  font-size: 16px;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.3s ease;
}

.file-upload-label:hover {
  background-color: #e4e4e4;
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
