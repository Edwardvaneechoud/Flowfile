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
              <span class="setting-title">Node Reference</span>
              <div class="setting-description-wrapper">
                <span class="setting-description">
                  Custom variable name used when generating code. Must be a valid Python identifier.
                  <el-tooltip
                    effect="dark"
                    content="If not set, the default 'df_{node_id}' pattern will be used in generated code"
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
              placeholder="e.g., customers_filtered, sales_data"
              :class="{ 'is-error': nodeReferenceError }"
              @input="validateNodeReference"
              @change="handleSettingChange"
            />
            <span v-if="nodeReferenceError" class="error-message">{{ nodeReferenceError }}</span>
          </div>

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
    </el-tabs>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
import type { NodeBase } from "./nodeInput";
import { useNodeStore } from "../../../stores/column-store";
import { InfoFilled } from "@element-plus/icons-vue";

const nodeStore = useNodeStore();

const props = defineProps<{
  modelValue: NodeBase;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: NodeBase): void;
}>();

const activeTab = ref("main");
const nodeReferenceError = ref<string | null>(null);

// Python reserved keywords
const PYTHON_KEYWORDS = new Set([
  "False", "None", "True", "and", "as", "assert", "async", "await",
  "break", "class", "continue", "def", "del", "elif", "else", "except",
  "finally", "for", "from", "global", "if", "import", "in", "is",
  "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try",
  "while", "with", "yield"
]);

const RESERVED_PREFIXES = ["df_", "pl", "polars"];

const localSettings = ref<Pick<NodeBase, "cache_results" | "description" | "node_reference">>({
  cache_results: props.modelValue?.cache_results ?? false,
  description: props.modelValue?.description ?? "",
  node_reference: props.modelValue?.node_reference ?? "",
});

watch(
  () => props.modelValue,
  (newValue) => {
    if (newValue) {
      localSettings.value = {
        cache_results: newValue.cache_results,
        description: newValue.description ?? "",
        node_reference: newValue.node_reference ?? "",
      };
      // Re-validate when value changes externally
      if (newValue.node_reference) {
        validateNodeReference(newValue.node_reference);
      } else {
        nodeReferenceError.value = null;
      }
    }
  },
  { deep: true },
);

const validateNodeReference = (value: string) => {
  // Empty is valid (will use default)
  if (!value || value.trim() === "") {
    nodeReferenceError.value = null;
    return true;
  }

  // Check if valid Python identifier
  const identifierRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
  if (!identifierRegex.test(value)) {
    nodeReferenceError.value = "Must be a valid Python identifier (letters, digits, underscores; cannot start with a digit)";
    return false;
  }

  // Check if not a reserved keyword
  if (PYTHON_KEYWORDS.has(value)) {
    nodeReferenceError.value = `'${value}' is a Python reserved keyword`;
    return false;
  }

  // Check if not starting with reserved prefixes
  for (const prefix of RESERVED_PREFIXES) {
    if (value.startsWith(prefix)) {
      nodeReferenceError.value = `Cannot start with reserved prefix '${prefix}'`;
      return false;
    }
  }

  nodeReferenceError.value = null;
  return true;
};

const handleSettingChange = () => {
  // Only emit if node_reference is valid or empty
  const refValue = localSettings.value.node_reference?.trim() || "";
  if (refValue && !validateNodeReference(refValue)) {
    return;
  }

  emit("update:modelValue", {
    ...props.modelValue,
    cache_results: localSettings.value.cache_results,
    description: localSettings.value.description,
    node_reference: refValue || undefined,
  });
};

const handleDescriptionChange = (value: string) => {
  nodeStore.updateNodeDescription(props.modelValue.node_id, value);
  handleSettingChange();
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

.error-message {
  display: block;
  color: var(--el-color-danger);
  font-size: 0.75rem;
  margin-top: 0.25rem;
}

:deep(.el-input.is-error .el-input__wrapper) {
  box-shadow: 0 0 0 1px var(--el-color-danger) inset;
}
</style>
