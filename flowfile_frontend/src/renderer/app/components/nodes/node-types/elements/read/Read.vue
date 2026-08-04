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
        title="Select a file to Read"
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
import IpcTableConfig from "./readIpc.vue";
import NdjsonTableConfig from "./readNdjson.vue";
import AvroTableConfig from "./readAvro.vue";
import {
  ReceivedTable,
  NodeRead,
  isInputCsvTable,
  isInputExcelTable,
  isInputParquetTable,
  isInputIpcTable,
  isInputNdjsonTable,
  isInputAvroTable,
} from "../../../baseNode/nodeInput";
import {
  createDefaultSettings,
  detectFileType,
  extensionOf,
} from "../../../../../utils/readFileTypes";
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
  } else if (detectedType) {
    // Bootstrap a new receivedTable from a typed path
    receivedTable.value = {
      name: fileName,
      path,
      file_type: detectedType,
      table_settings: createDefaultSettings(detectedType, extensionOf(path)),
    };
  }
}

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
