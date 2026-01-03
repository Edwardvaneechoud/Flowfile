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

const localSettings = ref<Pick<NodeBase, "cache_results" | "description">>({
  cache_results: props.modelValue?.cache_results ?? false,
  description: props.modelValue?.description ?? "",
});

watch(
  () => props.modelValue,
  (newValue) => {
    if (newValue) {
      localSettings.value = {
        cache_results: newValue.cache_results,
        description: newValue.description ?? "",
      };
    }
  },
  { deep: true },
);

const handleSettingChange = () => {
  emit("update:modelValue", {
    ...props.modelValue,
    cache_results: localSettings.value.cache_results,
    description: localSettings.value.description,
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
</style>
