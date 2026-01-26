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
              <span class="setting-title">Node Reference</span>
              <div class="setting-description-wrapper">
                <span class="setting-description">
                  A unique identifier used as the variable name in code generation.
                  <el-tooltip
                    effect="dark"
                    content="Must be lowercase with no spaces. Leave empty to use the default (df_node_id)"
                    placement="top"
                  >
                    <el-icon class="info-icon">
                      <InfoFilled />
                    </el-icon>
                  </el-tooltip>
                </span>
              </div>
            </div>
            <el-input
              v-model="localSettings.node_reference"
              :placeholder="defaultReference"
              :class="{ 'is-error': referenceError }"
              @input="handleReferenceInput"
              @blur="handleReferenceBlur"
            />
            <div v-if="referenceError" class="validation-error">
              {{ referenceError }}
            </div>
            <div v-else-if="isValidatingReference" class="validation-loading">
              Checking...
            </div>
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
                  label="Add Missing (Keep Extra) - Add missing, keep all incoming"
                  value="add_missing_keep_extra"
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
                  <el-button size="small" :disabled="hasSchema" :loading="isLoadingSchema" @click="loadFieldsFromSchema">
                    Load from Schema
                  </el-button>
                  <el-button size="small" type="primary" @click="addField">
                    Add Field
                  </el-button>
                </div>
              </div>

              <div v-if="outputFieldConfig.fields.length === 0" class="no-fields">
                <template v-if="isLoadingSchema">
                  Loading schema...
                </template>
                <template v-else>
                  No output fields configured. Click "Add Field" or "Load from Schema" to get started.
                </template>
              </div>

              <el-table
                v-else
                :data="outputFieldConfig.fields"
                style="width: 100%; margin-top: 1rem"
                size="small"
              >
                <el-table-column width="50">
                  <template #default>
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
                      placeholder="null"
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
                <strong>Tip:</strong> Default values can be any static value.
              </el-alert>
            </div>
          </template>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script lang="ts" setup generic="T extends NodeBase">
/* eslint-disable no-undef */
import { computed, ref, watch, reactive } from "vue";
import type { NodeBase, OutputFieldConfig } from "./nodeInput";
import { useNodeStore } from "../../../stores/node-store";
import { InfoFilled, DCaret, Delete } from "@element-plus/icons-vue";

const nodeStore = useNodeStore();

const props = defineProps<{
  modelValue: T;
}>();

const emit = defineEmits<{
  (e: "update:model-value", value: T): void;
  (e: "request-save"): Promise<boolean> | boolean | void;
}>();
/* eslint-enable no-undef */

// Loading state for async operations
const isLoadingSchema = ref(false);

const activeTab = ref("main");
const referenceError = ref<string | null>(null);
const isValidatingReference = ref(false);
let validationTimeout: ReturnType<typeof setTimeout> | null = null;

const defaultReference = computed(() => `df_${props.modelValue?.node_id ?? ""}`);

// Watch for tab changes to trigger save when switching to Output Schema
watch(activeTab, (newTab, oldTab) => {
  // When switching to Output Schema tab from any other tab, request a save
  if (newTab === "output-schema" && oldTab !== "output-schema") {
    emit("request-save");
  }
});

const localSettings = ref<Pick<NodeBase, "cache_results" | "description" | "node_reference">>({
  cache_results: props.modelValue?.cache_results ?? false,
  description: props.modelValue?.description ?? "",
  node_reference: props.modelValue?.node_reference ?? "",
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
        node_reference: newValue.node_reference ?? "",
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
  emit("update:model-value", {
    ...props.modelValue,
    cache_results: localSettings.value.cache_results,
    description: localSettings.value.description,
    node_reference: localSettings.value.node_reference,
    output_field_config: outputFieldConfig.enabled ? outputFieldConfig : null,
  });
};

const handleDescriptionChange = (value: string) => {
  nodeStore.updateNodeDescription(props.modelValue.node_id, value);
  handleSettingChange();
};

const validateReferenceLocally = (value: string): string | null => {
  if (!value || value === "") {
    return null; // Empty is valid (uses default)
  }
  if (value !== value.toLowerCase()) {
    return "Reference must be lowercase";
  }
  if (/\s/.test(value)) {
    return "Reference cannot contain spaces";
  }
  if (!/^[a-z][a-z0-9_]*$/.test(value)) {
    return "Reference must start with a letter and contain only lowercase letters, numbers, and underscores";
  }
  return null;
};

const handleReferenceInput = (value: string) => {
  // Clear any pending validation
  if (validationTimeout) {
    clearTimeout(validationTimeout);
  }

  // Run local validation immediately
  const localError = validateReferenceLocally(value);
  if (localError) {
    referenceError.value = localError;
    return;
  }

  // If local validation passes, debounce server-side uniqueness check
  referenceError.value = null;
  if (value && value !== "") {
    isValidatingReference.value = true;
    validationTimeout = setTimeout(async () => {
      try {
        const result = await nodeStore.validateNodeReference(props.modelValue.node_id, value);
        if (!result.valid) {
          referenceError.value = result.error;
        } else {
          referenceError.value = null;
        }
      } catch (error) {
        console.error("Error validating reference:", error);
      } finally {
        isValidatingReference.value = false;
      }
    }, 300);
  }
};

const handleReferenceBlur = async () => {
  // Clear any pending validation
  if (validationTimeout) {
    clearTimeout(validationTimeout);
  }

  const value = localSettings.value.node_reference || "";

  // Run local validation
  const localError = validateReferenceLocally(value);
  if (localError) {
    referenceError.value = localError;
    return;
  }

  // If non-empty and passes local validation, do final server validation and save
  if (value !== "") {
    isValidatingReference.value = true;
    try {
      const result = await nodeStore.validateNodeReference(props.modelValue.node_id, value);
      if (!result.valid) {
        referenceError.value = result.error;
        return;
      }
    } catch (error) {
      console.error("Error validating reference:", error);
      return;
    } finally {
      isValidatingReference.value = false;
    }
  }

  // Save the reference if validation passed
  if (!referenceError.value) {
    try {
      await nodeStore.setNodeReference(props.modelValue.node_id, value);
      handleSettingChange();
    } catch (error: any) {
      referenceError.value = error.message || "Failed to save reference";
    }
  }
};

const handleOutputConfigChange = () => {
  handleSettingChange();
};

const hasSchema = computed(() => {
  return outputFieldConfig.fields.length > 0;
});

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

    isLoadingSchema.value = true;

    // Request parent component to save current state and wait for completion
    // The parent component's saveSettings() should return a promise
    const saveResult = emit("request-save");

    // Wait for the save to complete if it returns a promise
    if (saveResult instanceof Promise) {
      await saveResult;
    }

    // Give the backend a moment to process and update the schema
    await new Promise(resolve => setTimeout(resolve, 100));

    // Get the node data from the store with updated schema
    const nodeData = await nodeStore.getNodeData(props.modelValue.node_id, false);

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
  } finally {
    isLoadingSchema.value = false;
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

.validation-error {
  margin-top: 0.5rem;
  font-size: 0.75rem;
  color: var(--el-color-danger);
}

.validation-loading {
  margin-top: 0.5rem;
  font-size: 0.75rem;
  color: var(--el-text-color-secondary);
}

:deep(.el-input.is-error .el-input__wrapper) {
  box-shadow: 0 0 0 1px var(--el-color-danger) inset;
}

.no-fields {
  padding: 1rem;
  text-align: center;
  color: var(--el-text-color-secondary);
  background-color: var(--el-fill-color-lighter);
  border-radius: 4px;
  margin-top: 1rem;
}
</style>
