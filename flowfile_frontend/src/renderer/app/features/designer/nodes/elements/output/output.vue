<template>
  <div v-if="dataLoaded && nodeOutput && nodeOutput.output_settings" class="listbox-wrapper">
    <div class="main-part">
      <div class="file-upload-row">
        <label class="file-upload-label" @click="showFileSelectionModal = true">
          <i class="file-icon fas fa-upload"></i>
          <span class="file-label-text">Folder</span>
        </label>
        <el-input
          v-if="nodeOutput.output_settings"
          v-model="nodeOutput.output_settings.directory"
          size="small"
          @change="handleFolderChange"
        />
        <span v-if="selectedDirectoryExists === false" class="warning-message">
          <el-icon><WarningFilled /></el-icon>
        </span>
      </div>

      <el-autocomplete
        v-model="nodeOutput.output_settings.name"
        :fetch-suggestions="querySearch"
        clearable
        class="inline-input w-50"
        placeholder="Select file or create file"
        :trigger-on-focus="false"
        @change="handleFileNameChange"
        @select="handleFileNameChange"
      />
    </div>

    <div class="main-part">
      <div class="file-type-row">
        File type:
        <el-select
          v-model="nodeOutput.output_settings.file_type"
          class="m-2"
          placeholder="Select"
          size="small"
          :disabled="hasFileExtension"
          @change="handleFileTypeChange"
        >
          <el-option
            v-for="type in ['csv', 'excel', 'parquet']"
            :key="type"
            :label="type"
            :value="type"
          />
        </el-select>
      </div>

      Writing option:
      <el-select
        v-model="nodeOutput.output_settings.write_mode"
        class="m-2"
        placeholder="Select output option"
        size="small"
        :disabled="!nodeOutput.output_settings.file_type"
      >
        <el-option
          v-for="option in getWriteOptions(nodeOutput.output_settings.file_type)"
          :key="option"
          :label="option"
          :value="option"
        />
      </el-select>

      <CsvTableConfig
        v-if="nodeOutput.output_settings.file_type === 'csv'"
        v-model="nodeOutput.output_settings.output_csv_table"
      />
      <ExcelTableConfig
        v-if="nodeOutput.output_settings.file_type === 'excel'"
        v-model="nodeOutput.output_settings.output_excel_table"
      />
      <ParquetTableConfig
        v-if="nodeOutput.output_settings.file_type === 'parquet'"
        v-model="nodeOutput.output_settings.output_parquet_table"
      />
    </div>

    <el-dialog
      v-model="showFileSelectionModal"
      title="Select directory or file to write to"
      width="70%"
    >
      <file-browser
        :allowed-file-types="['csv', 'xlsx', 'parquet']"
        :allow-directory-selection="true"
        mode="create"
        @directory-selected="handleDirectorySelected"
        @overwrite-file="handleFileSelected"
        @create-file="handleFileSelected"
      />
    </el-dialog>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { NodeOutput } from "../../../baseNode/nodeInput";
import { createDefaultOutputSettings } from "./defaultValues";
import { useNodeStore } from "../../../../../stores/column-store";
import axios, { AxiosError } from "axios";
import CsvTableConfig from "./outputCsv.vue";
import ExcelTableConfig from "./outputExcel.vue";
import ParquetTableConfig from "./outputParquet.vue";
import FileBrowser from "../../../components/fileBrowser/fileBrowser.vue";
import { WarningFilled } from "@element-plus/icons-vue";

interface LocalFileInfo {
  file_name: string;
  path: string;
  file_type: string;
  value: string;
}

const nodeStore = useNodeStore();
const nodeOutput = ref<NodeOutput | null>(null);
const dataLoaded = ref(false);
const showFileSelectionModal = ref(false);
const selectedDirectoryExists = ref<boolean | null>(null);
const localFileInfos = ref<LocalFileInfo[]>([]);

const hasFileExtension = computed(() => {
  return nodeOutput.value?.output_settings.name?.includes(".") ?? false;
});

function getWriteOptions(fileType: string): string[] {
  return fileType === "csv" ? ["overwrite", "new file", "append"] : ["overwrite", "new file"];
}

async function fetchFiles() {
  try {
    const response = await axios.get("/files/files_in_local_directory/", {
      params: { directory: nodeOutput.value?.output_settings.directory },
      headers: { accept: "application/json" },
    });
    localFileInfos.value = response.data;
    selectedDirectoryExists.value = true;
  } catch (err) {
    const axiosError = err as AxiosError;
    if (axiosError.response?.status === 404) {
      localFileInfos.value = [];
      selectedDirectoryExists.value = false;
    }
  }
}

function detectFileType(fileName: string) {
  if (!fileName) return;

  const extension = fileName.split(".").pop()?.toLowerCase();
  if (!extension || !["csv", "xlsx", "xls", "parquet"].includes(extension)) {
    return;
  }
  const verifiedExtension = extension as "csv" | "xlsx" | "xls" | "parquet";
  const fileTypeMap: Record<"csv" | "xlsx" | "xls" | "parquet", "csv" | "excel" | "parquet"> = {
    csv: "csv",
    xlsx: "excel",
    xls: "excel",
    parquet: "parquet",
  };

  if (nodeOutput.value && fileTypeMap[verifiedExtension]) {
    nodeOutput.value.output_settings.file_type = fileTypeMap[verifiedExtension];
    nodeOutput.value.output_settings.write_mode = "overwrite";
  }
}

function handleFileNameChange() {
  if (nodeOutput.value?.output_settings.name) {
    detectFileType(nodeOutput.value.output_settings.name);
  }
}

function handleFileTypeChange() {
  if (!nodeOutput.value) return;

  const fileExtMap: Record<string, string> = {
    csv: ".csv",
    excel: ".xlsx",
    parquet: ".parquet",
  };

  const baseName = nodeOutput.value.output_settings.name.split(".")[0];
  nodeOutput.value.output_settings.name =
    baseName + (fileExtMap[nodeOutput.value.output_settings.file_type] || "");

  if (!nodeOutput.value.output_settings.write_mode) {
    nodeOutput.value.output_settings.write_mode = "overwrite";
  }
}

function handleDirectorySelected(directoryPath: string) {
  if (!nodeOutput.value) return;

  nodeOutput.value.output_settings.directory = directoryPath;
  showFileSelectionModal.value = false;
  fetchFiles();
}

function handleFileSelected(filePath: string, currentPath: string, fileName: string) {
  if (!nodeOutput.value) return;

  nodeOutput.value.output_settings.name = fileName;
  nodeOutput.value.output_settings.directory = currentPath;
  showFileSelectionModal.value = false;
  detectFileType(fileName);
}

function handleFolderChange() {
  fetchFiles();
}

const querySearch = (queryString: string, cb: (suggestions: LocalFileInfo[]) => void) => {
  const results = queryString
    ? localFileInfos.value.filter((item) =>
        item.file_name.toLowerCase().startsWith(queryString.toLowerCase()),
      )
    : localFileInfos.value;
  cb(results);
};

async function loadNodeData(nodeId: number) {
  const nodeResult = await nodeStore.getNodeData(1, nodeId, false);
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    nodeOutput.value = nodeResult.setting_input;
    console.log("this is all good", nodeResult?.setting_input);
  } else {
    // Provide a default configuration that matches NodeBase interface
    nodeOutput.value = {
      output_settings: createDefaultOutputSettings(),
      flow_id: 1,
      node_id: nodeId,
      cache_results: false,
      pos_x: 0,
      pos_y: 0,
      is_setup: false,
      description: "",
    };
  }
  dataLoaded.value = true;
}

async function pushNodeData() {
  if (nodeOutput.value?.output_settings) {
    await nodeStore.updateSettings(nodeOutput);
    dataLoaded.value = false;
  }
}

defineExpose({
  loadNodeData,
  pushNodeData,
});
</script>

<style scoped>
.main-part {
  display: flex;
  flex-direction: column;
  padding: 20px;
  border: 1px solid #ddd;
  border-radius: 8px;
  background-color: #fff;
  margin-top: 20px;
}

.file-upload-row {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 20px;
}

.file-upload-label {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background-color: #3498db;
  border: 1px solid transparent;
  border-radius: 4px;
  padding: 10px 20px;
  color: white;
  font-size: 16px;
  cursor: pointer;
  transition: all 0.3s ease;
}

.file-upload-label:hover {
  background-color: #2980b9;
}

.warning-message {
  color: #e74c3c;
  display: flex;
  align-items: center;
}
</style>
