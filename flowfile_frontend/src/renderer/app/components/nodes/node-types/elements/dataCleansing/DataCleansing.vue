<template>
  <div v-if="dataLoaded && nodeDataCleansing" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeDataCleansing"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Remove null data</div>
        <el-checkbox
          v-model="cleansingInput.remove_null_rows"
          label="Remove rows that are null in every field"
          size="small"
        />
        <el-checkbox
          v-model="cleansingInput.remove_null_columns"
          label="Remove columns that are null in every row"
          size="small"
        />
        <div class="option-hint">
          Only rows or columns that are null everywhere are removed. Empty text ("") is not null, so
          a field holding "" keeps its row and its column.
        </div>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Columns to cleanse</div>
        <el-radio-group
          v-model="cleansingInput.selection_mode"
          class="mode-group"
          @change="onSelectionModeChange"
        >
          <el-radio label="all">All columns</el-radio>
          <el-radio label="list">Selected columns</el-radio>
        </el-radio-group>
        <div class="option-hint">
          The rules below apply to text columns, and null replacement also to numeric columns. Other
          data types pass through unchanged.
        </div>
      </div>

      <select-dynamic
        v-if="cleansingInput.selection_mode === 'list'"
        :select-inputs="selection"
        :show-keep-option="true"
        :show-data-type="false"
        :show-new-columns="false"
        :show-old-columns="true"
        :show-headers="true"
        :show-title="false"
        title="Select data"
        original-column-header="Column"
        @update-select-inputs="calculateSelects"
      />

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Replace nulls</div>
        <el-checkbox
          v-model="cleansingInput.replace_nulls_with_blank"
          label="With blank text, in text columns"
          size="small"
        />
        <el-checkbox
          v-model="cleansingInput.replace_nulls_with_zero"
          label="With 0, in numeric columns"
          size="small"
        />
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Remove unwanted characters</div>
        <el-checkbox
          v-model="cleansingInput.trim_whitespace"
          :disabled="cleansingInput.remove_all_whitespace"
          label="Leading and trailing whitespace"
          size="small"
        />
        <el-checkbox
          v-model="cleansingInput.normalize_whitespace"
          :disabled="cleansingInput.remove_all_whitespace"
          label="Tabs, line breaks and repeated spaces"
          size="small"
        />
        <el-checkbox
          v-model="cleansingInput.remove_all_whitespace"
          label="All whitespace"
          size="small"
        />
        <el-checkbox v-model="cleansingInput.remove_letters" label="Letters" size="small" />
        <el-checkbox v-model="cleansingInput.remove_numbers" label="Numbers" size="small" />
        <el-checkbox v-model="cleansingInput.remove_punctuation" label="Punctuation" size="small" />
        <div class="option-hint">
          Characters are removed first, then whitespace is cleaned up. Removing all whitespace
          supersedes the two options above it.
        </div>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Modify case</div>
        <el-select v-model="cleansingInput.case_mode" size="small" style="width: 100%">
          <el-option
            v-for="option in caseOptions"
            :key="option.value"
            :label="option.label"
            :value="option.value"
          />
        </el-select>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import type {
  CleansingCaseMode,
  DataCleansingInput,
  NodeDataCleansing,
  NodeData,
  SelectInput,
} from "@/types/node.types";
import { createSelectInputFromName } from "@/types/node.types";
import { useNodeStore } from "@/stores/node-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import selectDynamic from "@/components/nodes/baseNode/selectComponents/selectDynamic.vue";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeDataCleansing = ref<null | NodeDataCleansing>(null);
const nodeData = ref<null | NodeData>(null);
const selection = ref<SelectInput[]>([]);

/** Mirrors DataCleansingInput's Pydantic defaults; a freshly dropped node arrives
 * as a NodePromise with no cleansing_input at all. */
const createDefaultCleansingInput = (): DataCleansingInput => ({
  remove_null_rows: false,
  remove_null_columns: false,
  selection_mode: "all",
  selected_columns: [],
  replace_nulls_with_blank: true,
  replace_nulls_with_zero: true,
  trim_whitespace: true,
  normalize_whitespace: false,
  remove_all_whitespace: false,
  remove_letters: false,
  remove_numbers: false,
  remove_punctuation: false,
  case_mode: "none",
});

const cleansingInput = ref<DataCleansingInput>(createDefaultCleansingInput());

const caseOptions: { value: CleansingCaseMode; label: string }[] = [
  { value: "none", label: "Leave unchanged" },
  { value: "uppercase", label: "UPPERCASE" },
  { value: "lowercase", label: "lowercase" },
  { value: "titlecase", label: "Title Case" },
];

const availableColumns = (): string[] => nodeData.value?.main_input?.columns ?? [];

const loadSelection = (columnsToKeep: string[]) => {
  selection.value = availableColumns().map((column) =>
    createSelectInputFromName(column, columnsToKeep.includes(column)),
  );
};

const setSelectedColumns = () => {
  cleansingInput.value.selected_columns = selection.value
    .filter((input) => input.keep)
    .map((input) => input.old_name);
};

const calculateSelects = (updatedInputs: SelectInput[]) => {
  selection.value = updatedInputs;
  setSelectedColumns();
};

// Switching to an empty list would silently cleanse nothing, so seed it with everything.
const onSelectionModeChange = () => {
  if (cleansingInput.value.selection_mode === "list" && selection.value.every((i) => !i.keep)) {
    loadSelection(availableColumns());
    setSelectedColumns();
  }
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeDataCleansing,
  onBeforeSave: () => {
    if (!nodeDataCleansing.value) return false;
    if (cleansingInput.value.selection_mode === "list") {
      setSelectedColumns();
    }
    nodeDataCleansing.value.cleansing_input = cleansingInput.value;
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeDataCleansing.value = nodeData.value?.setting_input;
  if (nodeDataCleansing.value) {
    if (nodeDataCleansing.value.cleansing_input) {
      cleansingInput.value = nodeDataCleansing.value.cleansing_input;
    } else {
      cleansingInput.value = createDefaultCleansingInput();
      nodeDataCleansing.value.cleansing_input = cleansingInput.value;
    }
    loadSelection(cleansingInput.value.selected_columns);
  }
  dataLoaded.value = true;
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.mode-group {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
}
.option-hint {
  margin-top: 4px;
  font-size: 11px;
  color: #64748b;
}
</style>
