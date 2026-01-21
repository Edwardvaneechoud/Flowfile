<template>
  <div class="settings-wrapper">
    <el-tabs v-model="activeTab">
      <el-tab-pane label="Main Settings" name="main">
        <slot></slot>
      </el-tab-pane>

      <el-tab-pane label="General Settings" name="general">
        <div class="settings-section">
          <div class="setting-group">
            <div class="setting-header">
              <span class="setting-title">Cache Results</span>
              <div class="setting-description-wrapper">
                <span class="setting-description">
                  Store results on disk to speed up subsequent executions and verify results.
                  <el-tooltip
                    effect="dark"
                    content="Caching is only active when the flow is executed in performance mode"
                    placement="top"
                  >
                    <el-icon class="info-icon">
                      <InfoFilled />
                    </el-icon>
                  </el-tooltip>
                </span>
              </div>
            </div>
            <el-switch v-model="localSettings.cache_results" @change="handleSettingChange" />
          </div>

          <div class="setting-group">
            <div class="setting-header">
              <span class="setting-title">Node Description</span>
              <span class="setting-description">
                Add a description to document this node's purpose
              </span>
            </div>
            <el-input
              v-model="localSettings.description"
              type="textarea"
              :rows="4"
              placeholder="Add a description for this node..."
              @change="handleDescriptionChange"
            />
          </div>
        </div>
      </el-tab-pane>

      <el-tab-pane label="Output Schema" name="output-schema">
        <div class="settings-section">
          <div class="setting-group">
            <div class="setting-header">
              <span class="setting-title">Enable Output Field Configuration</span>
              <span class="setting-description">
                Define and enforce the output schema for predictable dataframe outputs
              </span>
            </div>
            <el-switch v-model="outputFieldConfig.enabled" @change="handleOutputConfigChange" />
          </div>

          <template v-if="outputFieldConfig.enabled">
            <div class="setting-group">
              <div class="setting-header">
                <span class="setting-title">Validation Mode</span>
                <span class="setting-description">
                  How to handle output fields
                </span>
              </div>
              <el-select
                v-model="outputFieldConfig.validation_mode_behavior"
                @change="handleOutputConfigChange"
                style="width: 100%"
              >
                <el-option
                  label="Select Only - Keep only specified fields"
                  value="select_only"
                />
                <el-option
                  label="Add Missing - Add missing fields with defaults"
                  value="add_missing"
                />
                <el-option
                  label="Raise on Missing - Error if fields are missing"
                  value="raise_on_missing"
                />
              </el-select>
            </div>

            <div class="setting-group">
              <div class="setting-header">
                <span class="setting-title">Validate Data Types</span>
                <span class="setting-description">
                  Raise an error if actual data types don't match the configured types (does not cast)
                </span>
              </div>
              <el-switch
                v-model="outputFieldConfig.validate_data_types"
                @change="handleOutputConfigChange"
              />
            </div>

            <div class="setting-group">
              <div class="setting-header">
                <span class="setting-title">Output Fields</span>
                <div style="display: flex; gap: 0.5rem; margin-top: 0.5rem">
                  <el-button size="small" @click="loadFieldsFromSchema">
                    Load from Schema
                  </el-button>
                  <el-button size="small" type="primary" @click="addField">
                    Add Field
                  </el-button>
                </div>
              </div>

              <div v-if="outputFieldConfig.fields.length === 0" class="no-fields">
                No output fields configured. Click "Add Field" or "Load from Schema" to get started.
              </div>

              <el-table
                v-else
                :data="outputFieldConfig.fields"
                style="width: 100%; margin-top: 1rem"
                size="small"
              >
                <el-table-column width="50">
                  <template #default="{ $index }">
                    <el-icon style="cursor: move">
                      <DCaret />
                    </el-icon>
                  </template>
                </el-table-column>

                <el-table-column label="Field Name" prop="name">
                  <template #default="{ row }">
                    <el-input
                      v-model="row.name"
                      size="small"
                      @change="handleOutputConfigChange"
                    />
                  </template>
                </el-table-column>

                <el-table-column label="Data Type" prop="data_type" width="150">
                  <template #default="{ row }">
                    <el-select
                      v-model="row.data_type"
                      size="small"
                      @change="handleOutputConfigChange"
                    >
                      <el-option label="String" value="String" />
                      <el-option label="Int64" value="Int64" />
                      <el-option label="Int32" value="Int32" />
                      <el-option label="Float64" value="Float64" />
                      <el-option label="Float32" value="Float32" />
                      <el-option label="Boolean" value="Boolean" />
                      <el-option label="Date" value="Date" />
                      <el-option label="Datetime" value="Datetime" />
                      <el-option label="Time" value="Time" />
                      <el-option label="List" value="List" />
                      <el-option label="Decimal" value="Decimal" />
                    </el-select>
                  </template>
                </el-table-column>

                <el-table-column label="Default Value" prop="default_value">
                  <template #default="{ row }">
                    <el-input
                      v-model="row.default_value"
                      size="small"
                      placeholder="null or expression"
                      @change="handleOutputConfigChange"
                    />
                  </template>
                </el-table-column>

                <el-table-column width="60">
                  <template #default="{ $index }">
                    <el-button
                      type="danger"
                      size="small"
                      text
                      @click="removeField($index)"
                    >
                      <el-icon><Delete /></el-icon>
                    </el-button>
                  </template>
                </el-table-column>
              </el-table>

              <el-alert
                v-if="outputFieldConfig.fields.length > 0"
                type="info"
                :closable="false"
                style="margin-top: 1rem"
              >
                <strong>Tip:</strong> Default values can be literals (e.g., "0", "Unknown") or
                Polars expressions (e.g., "pl.lit(0)").
              </el-alert>
            </div>
          </template>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch, reactive } from "vue";
import type { NodeBase, OutputFieldConfig, OutputFieldInfo } from "./nodeInput";
import { useNodeStore } from "../../../stores/column-store";
import { InfoFilled, DCaret, Delete } from "@element-plus/icons-vue";

const nodeStore = useNodeStore();

const props = defineProps<{
  modelValue: NodeBase;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: NodeBase): void;
  (e: "requestSave"): void;
}>();

const activeTab = ref("main");

// Watch for tab changes to trigger save when switching to Output Schema
watch(activeTab, (newTab, oldTab) => {
  // When switching to Output Schema tab from any other tab, request a save
  if (newTab === "output-schema" && oldTab !== "output-schema") {
    emit("requestSave");
  }
});

const localSettings = ref<Pick<NodeBase, "cache_results" | "description">>({
  cache_results: props.modelValue?.cache_results ?? false,
  description: props.modelValue?.description ?? "",
});

const outputFieldConfig = reactive<OutputFieldConfig>(
  props.modelValue?.output_field_config ?? {
    enabled: false,
    validation_mode_behavior: "select_only",
    fields: [],
    validate_data_types: false,
  }
);

watch(
  () => props.modelValue,
  (newValue) => {
    if (newValue) {
      localSettings.value = {
        cache_results: newValue.cache_results,
        description: newValue.description ?? "",
      };

      // Update output field config if it exists
      if (newValue.output_field_config) {
        Object.assign(outputFieldConfig, newValue.output_field_config);
      }
    }
  },
  { deep: true },
);

const handleSettingChange = () => {
  emit("update:modelValue", {
    ...props.modelValue,
    cache_results: localSettings.value.cache_results,
    description: localSettings.value.description,
    output_field_config: outputFieldConfig.enabled ? outputFieldConfig : null,
  });
};

const handleDescriptionChange = (value: string) => {
  nodeStore.updateNodeDescription(props.modelValue.node_id, value);
  handleSettingChange();
};

const handleOutputConfigChange = () => {
  handleSettingChange();
};

const addField = () => {
  outputFieldConfig.fields.push({
    name: "",
    data_type: "String",
    default_value: null,
  });
  handleOutputConfigChange();
};

const removeField = (index: number) => {
  outputFieldConfig.fields.splice(index, 1);
  handleOutputConfigChange();
};

const loadFieldsFromSchema = async () => {
  try {
    // Validate that we have a valid node to work with
    if (!props.modelValue || !props.modelValue.node_id) {
      console.error("Cannot load schema: Invalid or missing node data");
      return;
    }

    // Request parent component to save current state
    emit("requestSave");

    // Give the backend a moment to process and update the schema
    await new Promise(resolve => setTimeout(resolve, 150));

    // Get the node data from the store with updated schema
    const nodeData = await nodeStore.getNodeData(props.modelValue.node_id);

    if (nodeData?.main_output?.table_schema) {
      // Load fields from the schema
      outputFieldConfig.fields = nodeData.main_output.table_schema.map((col: any) => ({
        name: col.name,
        data_type: col.data_type,
        default_value: null,
      }));
      handleOutputConfigChange();
    }
  } catch (error) {
    console.error("Error loading schema:", error);
  }
};
</script>

<style scoped>
.settings-wrapper {
  width: 100%;
}

.settings-section {
  background-color: var(--el-bg-color-page);
  border-radius: 8px;
  padding: 1.25rem;
  margin-top: 1rem;
}

.setting-group {
  margin-bottom: 1.5rem;
  padding-bottom: 1.25rem;
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.setting-group:last-child {
  border-bottom: none;
  margin-bottom: 0;
  padding-bottom: 0;
}

.setting-header {
  margin-bottom: 0.75rem;
}

.setting-title {
  display: block;
  font-weight: 500;
  margin-bottom: 0.25rem;
}

.setting-description {
  display: block;
  font-size: 0.875rem;
  color: var(--el-text-color-secondary);
}
.setting-description-wrapper {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.info-icon {
  color: var(--el-text-color-secondary);
  cursor: help;
  font-size: 1rem;
}

.setting-description {
  flex-grow: 1;
}
</style>
