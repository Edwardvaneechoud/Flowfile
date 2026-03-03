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
        v-if="isOutputCsvTable(nodeOutput.output_settings.table_settings)"
        v-model="nodeOutput.output_settings.table_settings"
      />
      <ExcelTableConfig
        v-if="isOutputExcelTable(nodeOutput.output_settings.table_settings)"
        v-model="nodeOutput.output_settings.table_settings"
      />
      <ParquetTableConfig
        v-if="isOutputParquetTable(nodeOutput.output_settings.table_settings)"
        v-model="nodeOutput.output_settings.table_settings"
      />
    </div>

    <!-- Publish to Catalog -->
    <div class="main-part">
      <div class="catalog-publish-row">
        <el-checkbox
          v-model="nodeOutput.output_settings.publish_to_catalog"
          label="Publish to Catalog"
        />
      </div>
      <div v-if="nodeOutput.output_settings.publish_to_catalog" class="catalog-options">
        <div class="catalog-field">
          <label class="catalog-label">Table name</label>
          <el-input
            v-model="nodeOutput.output_settings.catalog_table_name"
            size="small"
            placeholder="Defaults to file name"
          />
        </div>
        <div class="catalog-field">
          <label class="catalog-label">Catalog / Schema</label>
          <el-select
            v-model="nodeOutput.output_settings.catalog_namespace_id"
            size="small"
            placeholder="Default namespace"
            clearable
          >
            <el-option
              v-for="ns in catalogNamespaces"
              :key="ns.id"
              :label="ns.label"
              :value="ns.id"
            />
          </el-select>
        </div>
      </div>
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
        context="dataFiles"
        :is-visible="showFileSelectionModal"
        @directory-selected="handleDirectorySelected"
        @overwrite-file="handleFileSelected"
        @create-file="handleFileSelected"
      />
    </el-dialog>
  </div>
</template>
<script lang="ts" setup>
import { ref, computed, onMounted } from "vue";
import {
  NodeOutput,
  isOutputCsvTable,
  isOutputParquetTable,
  isOutputExcelTable,
} from "../../../baseNode/nodeInput";
import {
  createDefaultOutputSettings,
  createCsvTableSettings,
  createParquetTableSettings,
  createExcelTableSettings,
} from "./defaultValues";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import axios, { AxiosError } from "axios";
import CsvTableConfig from "./outputCsv.vue";
import ExcelTableConfig from "./outputExcel.vue";
import ParquetTableConfig from "./outputParquet.vue";
import FileBrowser from "../../../../common/FileBrowser/fileBrowser.vue";
import { WarningFilled } from "@element-plus/icons-vue";
import { CatalogApi } from "../../../../../api/catalog.api";

interface LocalFileInfo {
  file_name: string;
  path: string;
  file_type: string;
  value: string;
}

const nodeStore = useNodeStore();
const nodeOutput = ref<NodeOutput | null>(null);
const dataLoaded = ref(false);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeOutput,
});
const showFileSelectionModal = ref(false);
const selectedDirectoryExists = ref<boolean | null>(null);
const localFileInfos = ref<LocalFileInfo[]>([]);

// Catalog namespace options for the publish dropdown
const catalogNamespaces = ref<{ id: number; label: string }[]>([]);

onMounted(async () => {
  try {
    const tree = await CatalogApi.getNamespaceTree();
    for (const catalog of tree) {
      for (const schema of catalog.children ?? []) {
        catalogNamespaces.value.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
      }
    }
  } catch {
    // Catalog not available — leave empty
  }
});

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

    // Update table_settings to match the new file type
    updateTableSettings(fileTypeMap[verifiedExtension]);
  }
}

function updateTableSettings(fileType: "csv" | "excel" | "parquet") {
  if (!nodeOutput.value) return;

  switch (fileType) {
    case "csv":
      nodeOutput.value.output_settings.table_settings = createCsvTableSettings();
      break;
    case "parquet":
      nodeOutput.value.output_settings.table_settings = createParquetTableSettings();
      break;
    case "excel":
      nodeOutput.value.output_settings.table_settings = createExcelTableSettings();
      break;
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

  // Update table_settings when file type changes
  updateTableSettings(nodeOutput.value.output_settings.file_type);
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
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    nodeOutput.value = nodeResult.setting_input;
  } else {
    // Provide a default configuration that matches NodeBase interface
    nodeOutput.value = {
      output_settings: createDefaultOutputSettings(),
      flow_id: nodeStore.flow_id,
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

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.main-part {
  display: flex;
  flex-direction: column;
  padding: 20px;
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  background-color: var(--color-background-primary);
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
  background-color: var(--color-button-secondary);
  border: 1px solid transparent;
  border-radius: 4px;
  padding: 10px 20px;
  color: var(--color-text-inverse);
  font-size: 16px;
  cursor: pointer;
  transition: all 0.3s ease;
}

.file-upload-label:hover {
  background-color: var(--color-button-secondary-hover);
}

.warning-message {
  color: var(--color-danger);
  display: flex;
  align-items: center;
}

.catalog-publish-row {
  margin-bottom: 8px;
}

.catalog-options {
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding-left: 4px;
}

.catalog-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.catalog-label {
  font-size: 12px;
  color: var(--color-text-secondary);
}
</style>
