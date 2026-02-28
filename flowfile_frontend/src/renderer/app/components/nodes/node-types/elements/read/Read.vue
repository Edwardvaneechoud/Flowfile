<template>
  <div v-if="dataLoaded && nodeRead" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodeRead"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
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
          <button class="catalog-browse-btn" title="Browse Catalog" @click="catalogDialogVisible = true">
            <i class="fa-solid fa-folder-tree"></i>
            <span>Browse Catalog</span>
          </button>
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

      <el-dialog v-model="catalogDialogVisible" title="Select a Catalog Table" width="50%">
        <div v-if="catalogTablesLoading" class="catalog-loading">Loading tables...</div>
        <div v-else-if="catalogTables.length === 0" class="catalog-empty">
          No tables registered in the catalog yet.
        </div>
        <div v-else class="catalog-table-list">
          <div
            v-for="table in catalogTables"
            :key="table.id"
            class="catalog-table-item"
            :class="{ selected: selectedCatalogTableId === table.id }"
            @click="selectedCatalogTableId = table.id"
            @dblclick="handleCatalogTableSelect(table)"
          >
            <i class="fa-solid fa-table catalog-table-icon"></i>
            <div class="catalog-table-info">
              <span class="catalog-table-name">{{ table.name }}</span>
              <span class="catalog-table-meta">
                {{ table.row_count?.toLocaleString() ?? "?" }} rows,
                {{ table.column_count ?? "?" }} columns
              </span>
            </div>
          </div>
        </div>
        <template #footer>
          <el-button @click="catalogDialogVisible = false">Cancel</el-button>
          <el-button
            type="primary"
            :disabled="selectedCatalogTableId === null"
            @click="confirmCatalogTableSelect"
          >
            Select
          </el-button>
        </template>
      </el-dialog>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref, computed, watch } from "vue";
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
import { CatalogApi } from "../../../../../api/catalog.api";
import type { CatalogTable } from "../../../../../types";

const nodeStore = useNodeStore();
const selectedFile = ref<FileInfo | null>(null);
const nodeRead = ref<null | NodeRead>(null);
const receivedTable = ref<ReceivedTable | null>(null);
const dataLoaded = ref(false);
const modalVisibleForOpen = ref(false);

// Catalog browse state
const catalogDialogVisible = ref(false);
const catalogTables = ref<CatalogTable[]>([]);
const catalogTablesLoading = ref(false);
const selectedCatalogTableId = ref<number | null>(null);

watch(catalogDialogVisible, async (visible) => {
  if (visible) {
    catalogTablesLoading.value = true;
    selectedCatalogTableId.value = null;
    try {
      catalogTables.value = await CatalogApi.getTables();
    } catch {
      catalogTables.value = [];
    } finally {
      catalogTablesLoading.value = false;
    }
  }
});

function handleCatalogTableSelect(table: CatalogTable) {
  // Table is materialized as Parquet – set up the read node with its file_path
  receivedTable.value = {
    name: table.name,
    path: table.file_path,
    file_type: "parquet",
    table_settings: createDefaultParquetSettings(),
  };
  selectedFile.value = {
    name: table.name,
    path: table.file_path,
    is_directory: false,
    size: table.size_bytes ?? 0,
    file_type: "parquet",
    last_modified: new Date(),
    created_date: new Date(),
    is_hidden: false,
  };
  catalogDialogVisible.value = false;
}

function confirmCatalogTableSelect() {
  if (selectedCatalogTableId.value === null) return;
  const table = catalogTables.value.find((t) => t.id === selectedCatalogTableId.value);
  if (table) handleCatalogTableSelect(table);
}

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

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
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

/* ========== Catalog Browse Button ========== */
.catalog-browse-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
  margin-top: 6px;
  padding: 8px 12px;
  background: transparent;
  border: 1px dashed var(--color-border-primary, #ddd);
  border-radius: 4px;
  color: var(--color-text-secondary, #666);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.catalog-browse-btn:hover {
  border-color: var(--color-primary, #3b82f6);
  color: var(--color-primary, #3b82f6);
  background: rgba(59, 130, 246, 0.04);
}

/* ========== Catalog Table Dialog ========== */
.catalog-loading,
.catalog-empty {
  padding: 24px;
  text-align: center;
  color: var(--color-text-muted, #999);
  font-size: 14px;
}

.catalog-table-list {
  max-height: 400px;
  overflow-y: auto;
}

.catalog-table-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 14px;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.15s ease;
}

.catalog-table-item:hover {
  background: var(--color-background-hover, #f5f5f5);
}

.catalog-table-item.selected {
  background: rgba(59, 130, 246, 0.1);
  outline: 2px solid var(--color-primary, #3b82f6);
}

.catalog-table-icon {
  font-size: 18px;
  color: #10b981;
  flex-shrink: 0;
}

.catalog-table-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.catalog-table-name {
  font-weight: 500;
  font-size: 14px;
  color: var(--color-text-primary, #333);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.catalog-table-meta {
  font-size: 12px;
  color: var(--color-text-muted, #999);
}
</style>
