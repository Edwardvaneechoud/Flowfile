<template>
  <div v-if="dataLoaded && nodeListFiles" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeListFiles"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Folder</div>
        <FilePathInput
          v-model="nodeListFiles.path"
          mode="open"
          allow-directory-selection
          dialog-title="Select a folder to list"
          placeholder="Folder path or ${param_name}/subfolder"
          @update:model-value="saveSettings"
        />
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Filter</div>
        <el-row align="middle" class="setting-row">
          <el-col :span="10" class="grid-content">File types</el-col>
          <el-col :span="14">
            <el-select
              v-model="nodeListFiles.file_types"
              multiple
              filterable
              allow-create
              default-first-option
              size="small"
              class="full-width"
              placeholder="All types"
              @change="saveSettings"
            >
              <el-option v-for="t in commonFileTypes" :key="t" :label="t" :value="t" />
            </el-select>
          </el-col>
        </el-row>
        <div class="hint">Leave empty to list every file. Extensions are matched without the dot.</div>

        <el-row align="middle" class="setting-row">
          <el-col :span="10" class="grid-content">Include</el-col>
          <el-col :span="14">
            <el-checkbox
              v-model="nodeListFiles.include_files"
              label="Files"
              size="small"
              @change="saveSettings"
            />
            <el-checkbox
              v-model="nodeListFiles.include_directories"
              label="Folders"
              size="small"
              @change="saveSettings"
            />
          </el-col>
        </el-row>
        <el-row align="middle" class="setting-row">
          <el-col :span="10" class="grid-content">Hidden files</el-col>
          <el-col :span="14">
            <el-switch v-model="nodeListFiles.include_hidden" size="small" @change="saveSettings" />
          </el-col>
        </el-row>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Subfolders</div>
        <el-row align="middle" class="setting-row">
          <el-col :span="10" class="grid-content">Search subfolders</el-col>
          <el-col :span="14">
            <el-switch v-model="nodeListFiles.recursive" size="small" @change="saveSettings" />
          </el-col>
        </el-row>
        <el-row v-if="nodeListFiles.recursive" align="middle" class="setting-row">
          <el-col :span="10" class="grid-content">Max depth</el-col>
          <el-col :span="8">
            <el-input
              v-model.number="nodeListFiles.max_depth"
              size="small"
              type="number"
              :min="1"
              :max="50"
              @change="saveSettings"
            />
          </el-col>
        </el-row>
        <el-row align="middle" class="setting-row">
          <el-col :span="10" class="grid-content">Max rows</el-col>
          <el-col :span="8">
            <el-input
              v-model.number="maxFilesInput"
              size="small"
              type="number"
              :min="1"
              placeholder="no limit"
              @change="saveSettings"
            />
          </el-col>
        </el-row>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { ElMessage } from "element-plus";
import type { NodeListFiles } from "@/types/node.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "@/stores/node-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";
import FilePathInput from "@/components/common/FileBrowser/FilePathInput.vue";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeListFiles = ref<null | NodeListFiles>(null);
const nodeData = ref<null | NodeData>(null);
const maxFilesInput = ref<number | null>(null);

const commonFileTypes = ["csv", "parquet", "xlsx", "json", "ndjson", "txt", "ipc", "avro"];

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeListFiles,
  onBeforeSave: () => {
    if (!nodeListFiles.value) return false;
    if (!nodeListFiles.value.include_files && !nodeListFiles.value.include_directories) {
      ElMessage.error("Select at least files or folders");
      return false;
    }
    const n = Number(maxFilesInput.value);
    nodeListFiles.value.max_files = Number.isFinite(n) && n >= 1 ? n : null;
    return true;
  },
});

const defaults = (): Partial<NodeListFiles> => ({
  path: "",
  file_types: [],
  recursive: false,
  max_depth: 5,
  include_hidden: false,
  include_files: true,
  include_directories: false,
  max_files: null,
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeListFiles.value = nodeData.value?.setting_input;
  if (nodeListFiles.value) {
    if (!nodeListFiles.value.is_setup) {
      Object.assign(nodeListFiles.value, defaults());
    }
    nodeListFiles.value.file_types = nodeListFiles.value.file_types ?? [];
    maxFilesInput.value = nodeListFiles.value.max_files ?? null;
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
.setting-row {
  margin-top: 6px;
}
.full-width {
  width: 100%;
}
.hint {
  margin-top: 4px;
  font-size: 11px;
  color: #64748b;
}
</style>
