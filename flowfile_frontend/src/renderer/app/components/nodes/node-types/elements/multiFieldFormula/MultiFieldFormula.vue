<template>
  <div v-if="dataLoaded && nodeMultiFieldFormula" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeMultiFieldFormula"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
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
            <el-option v-for="group in dataTypeGroups" :key="group" :label="group" :value="group" />
          </el-select>
        </div>
      </div>

      <div class="section">
        <div class="listbox-subtitle">Formula</div>
        <div class="formula-editor">
          <FunctionEditor
            :editor-string="settings.formula"
            :columns="formulaColumns"
            @update-editor-string="handleFormulaChange"
          />
        </div>
        <div class="hint">
          Use <code>[_CurrentField_]</code> for the value, <code>[_CurrentFieldName_]</code> for the
          name and <code>[_CurrentFieldType_]</code> for the type of each selected column.
        </div>
      </div>

      <div class="section">
        <div class="listbox-subtitle">Output</div>
        <el-radio-group v-model="settings.output_mode" size="small">
          <el-radio-button value="replace">Overwrite selected columns</el-radio-button>
          <el-radio-button value="new">Write to new columns</el-radio-button>
        </el-radio-group>

        <div v-if="settings.output_mode === 'new'" class="selection-body affix-row">
          <div class="affix-field">
            <div class="listbox-subtitle">Prefix</div>
            <el-input v-model="settings.output_prefix" placeholder="e.g. New_" size="small" />
          </div>
          <div class="affix-field">
            <div class="listbox-subtitle">Suffix</div>
            <el-input v-model="settings.output_suffix" placeholder="e.g. _pct" size="small" />
          </div>
        </div>
        <div v-if="missingAffix" class="affix-warning">
          Writing to new columns requires a prefix or a suffix.
        </div>

        <div class="selection-body">
          <div class="listbox-subtitle">Data type</div>
          <el-select
            v-model="settings.output_data_type"
            placeholder="Select data type..."
            size="small"
            style="width: 100%"
          >
            <el-option v-for="dtype in dataTypes" :key="dtype" :label="dtype" :value="dtype" />
          </el-select>
          <div class="hint">"Auto" keeps the type the expression produces.</div>
        </div>
      </div>

      <div class="section">
        <div class="listbox-subtitle">Preview</div>
        <div v-if="previewRows.length === 0" class="preview-empty">
          No columns are selected, so the data passes through unchanged.
        </div>
        <div v-else class="preview-table">
          <div class="preview-header">
            <span>Column</span>
            <span>&rarr;</span>
            <span>Output</span>
          </div>
          <div v-for="row in previewRows" :key="row.source" class="preview-row">
            <span class="preview-old">{{ row.source }}</span>
            <span class="preview-arrow">&rarr;</span>
            <span class="preview-new">{{ row.output }}</span>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import FunctionEditor from "../../../../../features/designer/editor/FunctionEditor.vue";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import type {
  MultiFieldFormulaInput,
  NodeMultiFieldFormula,
  ReadableDataTypeGroup,
} from "../../../../../types/node.types";
import {
  createMultiFieldFormulaInput,
  createMultiFieldFormulaNode,
  normalizeMultiFieldFormulaInput,
  previewOutputColumns,
  resolveTargetColumns,
} from "./multiFieldFormula";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeMultiFieldFormula = ref<NodeMultiFieldFormula | null>(null);
const nodeData = ref<NodeData | null>(null);

const dataTypeGroups: ReadableDataTypeGroup[] = [
  "Numeric",
  "String",
  "Date",
  "Boolean",
  "Binary",
  "Complex",
  "Other",
];
const dataTypes = [...nodeStore.getDataTypes(), "Auto"];
const PLACEHOLDERS = ["_CurrentField_", "_CurrentFieldName_", "_CurrentFieldType_"];

const settings = computed<MultiFieldFormulaInput>(
  () => nodeMultiFieldFormula.value?.multi_field_formula_input ?? createMultiFieldFormulaInput(),
);

const incomingColumns = computed(() => nodeData.value?.main_input?.table_schema ?? []);

// The placeholders lead so `[_Cur` autocompletes to them before any real column.
const formulaColumns = computed(() => [
  ...PLACEHOLDERS,
  ...incomingColumns.value.map((c) => c.name),
]);

const previewRows = computed(() =>
  previewOutputColumns(resolveTargetColumns(incomingColumns.value, settings.value), settings.value),
);

const missingAffix = computed(
  () =>
    settings.value.output_mode === "new" &&
    !settings.value.output_prefix &&
    !settings.value.output_suffix,
);

const handleFormulaChange = (value: string) => {
  if (nodeMultiFieldFormula.value) {
    nodeMultiFieldFormula.value.multi_field_formula_input.formula = value;
  }
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeMultiFieldFormula,
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  if (nodeData.value?.setting_input?.is_setup) {
    nodeMultiFieldFormula.value = nodeData.value.setting_input as NodeMultiFieldFormula;
    nodeMultiFieldFormula.value.multi_field_formula_input = normalizeMultiFieldFormulaInput(
      nodeMultiFieldFormula.value.multi_field_formula_input,
    );
  } else {
    nodeMultiFieldFormula.value = createMultiFieldFormulaNode(
      Number(nodeStore.flow_id),
      Number(nodeStore.node_id ?? nodeId),
    );
    nodeMultiFieldFormula.value.depending_on_id = nodeData.value?.main_input?.node_id;
  }
  dataLoaded.value = true;
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.section {
  margin-bottom: 16px;
}

.hint {
  color: var(--color-text-tertiary);
  font-size: 0.75rem;
  margin-top: 4px;
}

.hint code {
  background: var(--color-background-tertiary);
  padding: 1px 4px;
  border-radius: 3px;
}

.selection-body {
  margin-top: 8px;
}

.affix-row {
  display: flex;
  gap: 8px;
}

.affix-field {
  flex: 1;
}

.affix-warning {
  margin-top: 8px;
  color: var(--color-danger);
  font-size: 0.8rem;
  padding: 4px 8px;
  background: var(--color-danger-light);
  border-radius: 3px;
}

.formula-editor {
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
}

.column-type {
  font-size: 0.75rem;
  color: var(--color-text-tertiary);
  margin-left: 8px;
}

.preview-empty {
  color: var(--color-text-tertiary);
  font-size: 0.8rem;
  font-style: italic;
}

.preview-table {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 220px;
  overflow-y: auto;
  border: 1px solid var(--color-border-light);
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
  color: var(--color-text-tertiary);
  font-weight: 600;
  border-bottom: 1px solid var(--color-border-light);
  padding-bottom: 4px;
  margin-bottom: 2px;
}

.preview-old {
  color: var(--color-text-secondary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preview-new {
  color: var(--color-text-primary);
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preview-arrow {
  color: var(--color-text-muted);
  text-align: center;
}
</style>
