<template>
  <div v-if="dataLoaded && nodeDynamicRename" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeDynamicRename"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="section">
        <div class="listbox-subtitle">Rename mode</div>
        <el-radio-group v-model="settings.rename_mode" size="small">
          <el-radio-button value="prefix">Prefix</el-radio-button>
          <el-radio-button value="suffix">Suffix</el-radio-button>
          <el-radio-button value="formula">Formula</el-radio-button>
        </el-radio-group>
      </div>

      <div class="section">
        <div v-if="settings.rename_mode === 'prefix'">
          <div class="listbox-subtitle">Prefix</div>
          <el-input v-model="settings.prefix" placeholder="e.g. src_" size="small" clearable />
        </div>
        <div v-else-if="settings.rename_mode === 'suffix'">
          <div class="listbox-subtitle">Suffix</div>
          <el-input v-model="settings.suffix" placeholder="e.g. _raw" size="small" clearable />
        </div>
        <div v-else>
          <div class="listbox-subtitle">Formula</div>
          <div class="formula-editor">
            <FunctionEditor
              :editor-string="settings.formula"
              :columns="formulaColumns"
              @update-editor-string="handleFormulaChange"
            />
          </div>
          <div class="hint">
            Use <code>[column_name]</code> to reference the current column name.
          </div>
        </div>
      </div>

      <div class="section">
        <div class="listbox-subtitle">Apply to</div>
        <el-radio-group v-model="settings.selection_mode" size="small">
          <el-radio-button value="all">All columns</el-radio-button>
          <el-radio-button value="list">Specific columns</el-radio-button>
          <el-radio-button value="data_type">By data type</el-radio-button>
        </el-radio-group>

        <div v-if="settings.selection_mode === 'list'" class="selection-body">
          <el-select
            v-model="settings.selected_columns"
            multiple
            filterable
            collapse-tags
            collapse-tags-tooltip
            placeholder="Select columns..."
            size="small"
            style="width: 100%"
          >
            <el-option
              v-for="col in incomingColumns"
              :key="col.name"
              :label="col.name"
              :value="col.name"
            >
              <span>{{ col.name }}</span>
              <span class="column-type">{{ col.data_type }}</span>
            </el-option>
          </el-select>
        </div>

        <div v-else-if="settings.selection_mode === 'data_type'" class="selection-body">
          <el-select
            v-model="settings.selected_data_type"
            placeholder="Select data type..."
            size="small"
            clearable
            style="width: 100%"
          >
            <el-option
              v-for="bucket in dataTypeBuckets"
              :key="bucket"
              :label="bucket"
              :value="bucket"
            />
          </el-select>
        </div>
      </div>

      <div class="section">
        <div class="listbox-subtitle">Preview</div>
        <div v-if="previewError" class="preview-error">{{ previewError }}</div>
        <div v-else-if="previewLoading" class="preview-loading">Computing preview...</div>
        <div v-else-if="previewRows.length === 0" class="preview-empty">
          No columns will be renamed.
        </div>
        <div v-else class="preview-table">
          <div class="preview-header">
            <span>Original</span>
            <span>&rarr;</span>
            <span>Renamed</span>
          </div>
          <div v-for="row in previewRows" :key="row.oldName" class="preview-row">
            <span class="preview-old">{{ row.oldName }}</span>
            <span class="preview-arrow">&rarr;</span>
            <span class="preview-new">{{ row.newName }}</span>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { computed, ref, watch } from "vue";
import { CodeLoader } from "vue-content-loader";
import axios from "../../../../../services/axios.config";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import FunctionEditor from "../../../../../features/designer/editor/FunctionEditor.vue";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import type {
  DataTypeBucket,
  DynamicRenameInput,
  NodeDynamicRename,
} from "../../../../../types/node.types";
import { createDynamicRenameNode } from "./dynamicRename";

interface PreviewRow {
  oldName: string;
  newName: string;
}

interface PreviewResponse {
  rename_map: Record<string, string>;
  error: string | null;
}

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeDynamicRename = ref<NodeDynamicRename | null>(null);
const nodeData = ref<NodeData | null>(null);
const previewRows = ref<PreviewRow[]>([]);
const previewError = ref<string | null>(null);
const previewLoading = ref(false);

const dataTypeBuckets: DataTypeBucket[] = ["numeric", "string", "date"];
const formulaColumns: string[] = ["column_name"];

const handleFormulaChange = (value: string) => {
  if (nodeDynamicRename.value) {
    nodeDynamicRename.value.dynamic_rename_input.formula = value;
  }
};

const matchesBucket = (dtype: string, bucket: DataTypeBucket | null): boolean => {
  if (!bucket) return false;
  if (bucket === "numeric") return /^(Int|UInt|Float|Decimal)/.test(dtype);
  if (bucket === "string") {
    return (
      dtype === "String" ||
      dtype === "Utf8" ||
      dtype.startsWith("Categorical") ||
      dtype.startsWith("Enum")
    );
  }
  if (bucket === "date") return /^(Date|Datetime|Time|Duration)/.test(dtype);
  return false;
};

const settings = computed<DynamicRenameInput>(() => {
  return (
    nodeDynamicRename.value?.dynamic_rename_input ?? {
      rename_mode: "prefix",
      prefix: "",
      suffix: "",
      formula: "",
      selection_mode: "all",
      selected_columns: [],
      selected_data_type: null,
    }
  );
});

const incomingColumns = computed(() => {
  return nodeData.value?.main_input?.table_schema ?? [];
});

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeDynamicRename,
});

const resolveClientPreview = (): PreviewRow[] => {
  const s = settings.value;
  const cols = incomingColumns.value;
  let targets: string[] = [];
  if (s.selection_mode === "all") {
    targets = cols.map((c) => c.name);
  } else if (s.selection_mode === "list") {
    const available = new Set(cols.map((c) => c.name));
    targets = s.selected_columns.filter((n) => available.has(n));
  } else {
    targets = cols
      .filter((c) => matchesBucket(c.data_type, s.selected_data_type))
      .map((c) => c.name);
  }

  const mapped: PreviewRow[] = [];
  if (s.rename_mode === "prefix") {
    for (const name of targets) {
      const newName = `${s.prefix}${name}`;
      if (newName !== name) mapped.push({ oldName: name, newName });
    }
  } else if (s.rename_mode === "suffix") {
    for (const name of targets) {
      const newName = `${name}${s.suffix}`;
      if (newName !== name) mapped.push({ oldName: name, newName });
    }
  }
  return mapped;
};

let previewTimer: ReturnType<typeof setTimeout> | null = null;

const refreshPreview = () => {
  previewError.value = null;
  const s = settings.value;
  if (s.rename_mode !== "formula") {
    previewRows.value = resolveClientPreview();
    previewLoading.value = false;
    return;
  }
  if (!s.formula.trim()) {
    previewRows.value = [];
    previewLoading.value = false;
    return;
  }
  previewLoading.value = true;
  if (previewTimer) clearTimeout(previewTimer);
  previewTimer = setTimeout(async () => {
    try {
      const response = await axios.post<PreviewResponse>("/dynamic_rename/preview", {
        settings: s,
        incoming_columns: incomingColumns.value.map((c) => ({
          name: c.name,
          data_type: c.data_type,
        })),
      });
      if (response.data.error) {
        previewError.value = response.data.error;
        previewRows.value = [];
      } else {
        const rows: PreviewRow[] = [];
        const map = response.data.rename_map;
        for (const key in map) {
          if (Object.prototype.hasOwnProperty.call(map, key)) {
            rows.push({ oldName: key, newName: map[key] });
          }
        }
        previewRows.value = rows;
      }
    } catch (err) {
      previewError.value = err instanceof Error ? err.message : "Preview failed";
      previewRows.value = [];
    } finally {
      previewLoading.value = false;
    }
  }, 300);
};

watch(
  () => [
    settings.value.rename_mode,
    settings.value.prefix,
    settings.value.suffix,
    settings.value.formula,
    settings.value.selection_mode,
    settings.value.selected_columns,
    settings.value.selected_data_type,
    incomingColumns.value,
  ],
  () => refreshPreview(),
  { deep: true },
);

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  if (nodeData.value?.setting_input?.is_setup) {
    nodeDynamicRename.value = nodeData.value.setting_input as NodeDynamicRename;
    if (!nodeDynamicRename.value.dynamic_rename_input) {
      nodeDynamicRename.value.dynamic_rename_input = {
        rename_mode: "prefix",
        prefix: "",
        suffix: "",
        formula: "",
        selection_mode: "all",
        selected_columns: [],
        selected_data_type: null,
      };
    }
  } else {
    nodeDynamicRename.value = createDynamicRenameNode(
      Number(nodeStore.flow_id),
      Number(nodeStore.node_id ?? nodeId),
    );
    nodeDynamicRename.value.depending_on_id = nodeData.value?.main_input?.node_id;
  }
  dataLoaded.value = true;
  refreshPreview();
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.section {
  margin-bottom: 16px;
}

.hint {
  color: #909399;
  font-size: 0.75rem;
  margin-top: 4px;
}

.hint code {
  background: #f4f4f5;
  padding: 1px 4px;
  border-radius: 3px;
}

.selection-body {
  margin-top: 8px;
}

.formula-editor {
  border: 1px solid #dcdfe6;
  border-radius: 4px;
  overflow: hidden;
}

.column-type {
  font-size: 0.75rem;
  color: #909399;
  margin-left: 8px;
}

.preview-error {
  color: #f56c6c;
  font-size: 0.8rem;
  padding: 4px 8px;
  background: #fef0f0;
  border-radius: 3px;
}

.preview-loading,
.preview-empty {
  color: #909399;
  font-size: 0.8rem;
  font-style: italic;
}

.preview-table {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 220px;
  overflow-y: auto;
  border: 1px solid #ebeef5;
  border-radius: 3px;
  padding: 4px 0;
}

.preview-header,
.preview-row {
  display: grid;
  grid-template-columns: 1fr 20px 1fr;
  align-items: center;
  padding: 2px 8px;
  font-size: 0.8rem;
  gap: 4px;
}

.preview-header {
  color: #909399;
  font-weight: 600;
  border-bottom: 1px solid #ebeef5;
  padding-bottom: 4px;
  margin-bottom: 2px;
}

.preview-old {
  color: #606266;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preview-new {
  color: #303133;
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preview-arrow {
  color: #c0c4cc;
  text-align: center;
}
</style>
