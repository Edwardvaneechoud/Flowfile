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

    <el-dialog
      v-model="showFileSelectionModal"
      title="Select directory or file to write to"
      width="70%"
      append-to-body
      :close-on-click-modal="false"
    >
      <file-browser
        :allowed-file-types="['csv', 'xlsx', 'parquet']"
        :allow-directory-selection="true"
        mode="create"
        context="output"
        :is-visible="showFileSelectionModal"
        @directory-selected="handleDirectorySelected"
        @overwrite-file="handleFileSelected"
        @create-file="handleFileSelected"
      />
    </el-dialog>
  </div>
</template>
<script lang="ts" setup>
import { ref } from "vue";
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
import { useFileBrowserStore } from "../../../../../stores/fileBrowserStore";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { getDefaultPath } from "@/api/file.api";
import axios, { AxiosError } from "axios";
import CsvTableConfig from "./outputCsv.vue";
import ExcelTableConfig from "./outputExcel.vue";
import ParquetTableConfig from "./outputParquet.vue";
import FileBrowser from "../../../../common/FileBrowser/fileBrowser.vue";
import { WarningFilled } from "@element-plus/icons-vue";

interface LocalFileInfo {
  file_name: string;
  path: string;
  file_type: string;
  value: string;
}

const nodeStore = useNodeStore();
const fileBrowserStore = useFileBrowserStore();
const nodeOutput = ref<NodeOutput | null>(null);
const dataLoaded = ref(false);

/**
 * Remember the chosen output directory so the next new output node defaults to
 * it (persisted to localStorage via the "output" file-browser context).
 */
function rememberOutputDirectory(directory: string | undefined) {
  if (directory) fileBrowserStore.setCurrentPath("output", directory);
}

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeRef: nodeOutput,
  onBeforeSave: () => {
    rememberOutputDirectory(nodeOutput.value?.output_settings.directory);
  },
});
const showFileSelectionModal = ref(false);
const selectedDirectoryExists = ref<boolean | null>(null);
const localFileInfos = ref<LocalFileInfo[]>([]);

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

  const currentName = nodeOutput.value.output_settings.name;
  const lastDot = currentName.lastIndexOf(".");
  const baseName = lastDot > 0 ? currentName.slice(0, lastDot) : currentName;
  nodeOutput.value.output_settings.name =
    baseName + (fileExtMap[nodeOutput.value.output_settings.file_type] || "");

  if (!nodeOutput.value.output_settings.write_mode) {
    nodeOutput.value.output_settings.write_mode = "overwrite";
  }

  updateTableSettings(nodeOutput.value.output_settings.file_type);
}

function handleDirectorySelected(directoryPath: string) {
  if (!nodeOutput.value) return;

  nodeOutput.value.output_settings.directory = directoryPath;
  rememberOutputDirectory(directoryPath);
  showFileSelectionModal.value = false;
  fetchFiles();
}

function handleFileSelected(filePath: string, currentPath: string, fileName: string) {
  if (!nodeOutput.value) return;

  nodeOutput.value.output_settings.name = fileName;
  nodeOutput.value.output_settings.directory = currentPath;
  rememberOutputDirectory(currentPath);
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

/**
 * A directory is "resolved" only if it's an absolute path the user can actually
 * see the target of. A relative value like "." is not — the backend resolves it
 * against its own working directory, so the UI can't tell where the file lands.
 */
function isResolvedDirectory(directory: string | undefined): boolean {
  if (!directory || directory === ".") return false;
  // Absolute: POSIX (/...), Windows drive (C:\ or C:/), or UNC (\\...).
  return /^([a-zA-Z]:[\\/]|\\\\|\/)/.test(directory);
}

/**
 * Pick a concrete output directory: the last directory the user wrote to
 * (remembered in the "output" file-browser context), else the user's home
 * location from the backend. Falls back to "." only if the backend is
 * unreachable.
 */
async function resolveDefaultOutputDirectory(): Promise<string> {
  const remembered = fileBrowserStore.getLastUsedPath("output");
  if (isResolvedDirectory(remembered)) return remembered;
  try {
    return (await getDefaultPath()) || ".";
  } catch {
    return ".";
  }
}

/**
 * On open, make sure the directory field shows a real, writable location rather
 * than an unresolved "." — otherwise the user can't tell where the file will be
 * written. Resolves to the last-used output dir (else home) and remembers it.
 * For an already-configured node we persist the conversion immediately so the
 * saved flow stops carrying "." for good.
 */
async function ensureResolvedDirectory() {
  const settings = nodeOutput.value?.output_settings;
  if (!settings || isResolvedDirectory(settings.directory)) return;

  const resolved = await resolveDefaultOutputDirectory();
  if (!isResolvedDirectory(resolved)) return; // backend unreachable — leave as-is

  const wasConfigured = nodeOutput.value?.is_setup === true;
  settings.directory = resolved;
  rememberOutputDirectory(resolved);

  if (wasConfigured) await saveSettings();
}

async function loadNodeData(nodeId: number) {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    nodeOutput.value = nodeResult.setting_input;
  } else {
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
  await ensureResolvedDirectory();
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
  padding: 5px 12px;
  color: var(--color-text-inverse);
  font-size: 13px;
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
</style>
