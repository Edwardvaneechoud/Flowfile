<template>
  <div v-if="isLoaded && nodeFilter">
    <generic-node-settings
      v-model="nodeFilter"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div style="border-radius: 20px">
          <el-switch
            v-model="isAdvancedFilter"
            class="mb-2"
            active-text="Advanced filter options"
            inactive-text="Basic filter"
          />
        </div>
        <div v-if="isAdvancedFilter">
          Advanced filter
          <mainEditorRef ref="editorChild" :editor-string="editorString" />
        </div>
        <div v-if="!isAdvancedFilter">
          <div class="filter-section">
            <div v-if="nodeFilter?.filter_input.basic_filter" class="filter-row">
              <!-- Column Selector -->
              <div class="filter-field">
                <label class="filter-label">Column</label>
                <column-selector
                  v-model="nodeFilter.filter_input.basic_filter.field"
                  :value="nodeFilter.filter_input.basic_filter.field"
                  :column-options="nodeData?.main_input?.columns"
                  @update:value="(value: string) => handleFieldChange(value)"
                />
              </div>

              <!-- Operator Selector -->
              <div class="filter-field">
                <label class="filter-label">Operator</label>
                <column-selector
                  v-model="operatorDisplayValue"
                  :value="getOperatorLabel(nodeFilter.filter_input.basic_filter.operator)"
                  :column-options="operatorLabels"
                  @update:value="(value: string) => handleOperatorChange(value)"
                />
              </div>

              <!-- Value Input (shown for most operators) -->
              <div v-if="showValueInput" class="filter-field">
                <label class="filter-label">Value</label>
                <input
                  v-model="nodeFilter.filter_input.basic_filter.value"
                  type="text"
                  class="input-field"
                  :placeholder="valuePlaceholder"
                />
              </div>

              <!-- Second Value Input (for BETWEEN) -->
              <div v-if="showValue2Input" class="filter-field">
                <label class="filter-label">And</label>
                <input
                  v-model="nodeFilter.filter_input.basic_filter.value2"
                  type="text"
                  class="input-field"
                  placeholder="End value"
                />
              </div>
            </div>

            <!-- Help text for special operators -->
            <div v-if="operatorHelpText" class="help-text">
              {{ operatorHelpText }}
            </div>
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { CodeLoader } from "vue-content-loader";

import ColumnSelector from "../../../baseNode/page_objects/dropDown.vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import mainEditorRef from "../../../../../features/designer/editor/fullEditor.vue";
import { NodeFilter } from "../../../baseNode/nodeInput";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import {
  FilterOperator,
  FILTER_OPERATOR_LABELS,
  getFilterOperatorLabel,
  OPERATORS_NO_VALUE,
  OPERATORS_WITH_VALUE2,
} from "../../../../../types/node.types";

const editorString = ref<string>("");
const isLoaded = ref<boolean>(false);
const isAdvancedFilter = ref<boolean>(false);
const nodeStore = useNodeStore();
const nodeFilter = ref<NodeFilter | null>(null);
const nodeData = ref<NodeData | null>(null);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFilter,
  onBeforeSave: () => {
    // Prepare filter data before saving
    if (nodeFilter.value) {
      if (isAdvancedFilter.value) {
        updateAdvancedFilter();
        nodeFilter.value.filter_input.mode = "advanced";
        nodeFilter.value.filter_input.filter_type = "advanced";
      } else {
        nodeFilter.value.filter_input.mode = "basic";
        nodeFilter.value.filter_input.filter_type = "basic";
      }
    }
    return true;
  },
});

interface EditorChildType {
  showHideOptions: () => void;
  showTools: boolean;
}
const editorChild = ref<EditorChildType | null>(null);

const operatorLabels = Object.keys(FILTER_OPERATOR_LABELS);

const currentOperator = computed((): FilterOperator => {
  const op = nodeFilter.value?.filter_input?.basic_filter?.operator;
  if (!op) return "equals";
  if (typeof op === "string") {
    if (Object.values(FILTER_OPERATOR_LABELS).includes(op as FilterOperator)) {
      return op as FilterOperator;
    }
    return convertLegacyOperator(op);
  }
  return op as FilterOperator;
});

const operatorDisplayValue = computed({
  get: () => getOperatorLabel(nodeFilter.value?.filter_input?.basic_filter?.operator),
  set: (val: string) => handleOperatorChange(val),
});

function convertLegacyOperator(symbol: string): FilterOperator {
  const legacyMapping: Record<string, FilterOperator> = {
    "=": "equals",
    "==": "equals",
    "!=": "not_equals",
    "<>": "not_equals",
    ">": "greater_than",
    ">=": "greater_than_or_equals",
    "<": "less_than",
    "<=": "less_than_or_equals",
    contains: "contains",
    not_contains: "not_contains",
    starts_with: "starts_with",
    ends_with: "ends_with",
    is_null: "is_null",
    is_not_null: "is_not_null",
    in: "in",
    not_in: "not_in",
    between: "between",
    not_equals: "not_equals",
    greater_than: "greater_than",
    greater_than_or_equals: "greater_than_or_equals",
    less_than: "less_than",
    less_than_or_equals: "less_than_or_equals",
  };
  return legacyMapping[symbol] || "equals";
}

function getOperatorLabel(operator: FilterOperator | string | undefined): string {
  if (!operator) return "Equals";
  const op = typeof operator === "string" ? convertLegacyOperator(operator) : operator;
  return getFilterOperatorLabel(op);
}

const showValueInput = computed((): boolean => {
  return !OPERATORS_NO_VALUE.includes(currentOperator.value);
});

const showValue2Input = computed((): boolean => {
  return OPERATORS_WITH_VALUE2.includes(currentOperator.value);
});

const valuePlaceholder = computed((): string => {
  switch (currentOperator.value) {
    case "in":
    case "not_in":
      return "value1, value2, value3";
    case "between":
      return "Start value";
    default:
      return "Enter value";
  }
});

const operatorHelpText = computed((): string => {
  switch (currentOperator.value) {
    case "in":
    case "not_in":
      return "Enter comma-separated values";
    case "between":
      return "Enter the range boundaries (inclusive)";
    case "is_null":
      return "Filters rows where the column value is null";
    case "is_not_null":
      return "Filters rows where the column value is not null";
    default:
      return "";
  }
});

const handleFieldChange = (newValue: string) => {
  if (nodeFilter.value?.filter_input.basic_filter) {
    nodeFilter.value.filter_input.basic_filter.field = newValue;
  }
};

const handleOperatorChange = (newLabel: string) => {
  if (nodeFilter.value?.filter_input.basic_filter) {
    const operator = FILTER_OPERATOR_LABELS[newLabel];
    if (operator) {
      nodeFilter.value.filter_input.basic_filter.operator = operator;
      if (!OPERATORS_WITH_VALUE2.includes(operator)) {
        nodeFilter.value.filter_input.basic_filter.value2 = undefined;
      }
      if (OPERATORS_NO_VALUE.includes(operator)) {
        nodeFilter.value.filter_input.basic_filter.value = "";
      }
    }
  }
};

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  if (nodeData.value) {
    nodeFilter.value = nodeData.value.setting_input;

    if (nodeFilter.value?.filter_input.advanced_filter) {
      editorString.value = nodeFilter.value?.filter_input.advanced_filter;
    }

    const mode = nodeFilter.value?.filter_input.mode || nodeFilter.value?.filter_input.filter_type;
    isAdvancedFilter.value = mode === "advanced";

    // Migrate legacy basic_filter fields
    if (nodeFilter.value?.filter_input.basic_filter) {
      const bf = nodeFilter.value.filter_input.basic_filter;
      if (bf.filter_type && !bf.operator) {
        bf.operator = convertLegacyOperator(bf.filter_type);
      }
      if (bf.filter_value && !bf.value) {
        bf.value = bf.filter_value;
      }
      if (!bf.operator) {
        bf.operator = "equals";
      }
    }
  }
  isLoaded.value = true;
};

const updateAdvancedFilter = () => {
  if (nodeFilter.value) {
    nodeFilter.value.filter_input.advanced_filter = nodeStore.inputCode;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style lang="scss" scoped>
.filter-section {
  padding: 10px 0;
}

.filter-row {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: flex-end;
}

.filter-field {
  flex: 1;
  min-width: 150px;
  max-width: 250px;
}

.filter-label {
  display: block;
  font-size: 12px;
  font-weight: 500;
  color: #606266;
  margin-bottom: 4px;
}

.input-field {
  width: 100%;
  padding: 8px 12px;
  font-size: 14px;
  line-height: 1.4;
  border: 1px solid #dcdfe6;
  border-radius: 4px;
  box-shadow: none;
  outline: none;
  transition:
    border-color 0.2s,
    box-shadow 0.2s;

  &:focus {
    border-color: #409eff;
    box-shadow: 0 0 0 2px rgba(64, 158, 255, 0.1);
  }

  &::placeholder {
    color: #c0c4cc;
  }
}

.help-text {
  margin-top: 8px;
  font-size: 12px;
  color: #909399;
  font-style: italic;
}

.x-flip {
  transform: scaleX(-100%);
}
</style>
