<template>
  <div v-if="dataLoaded && nodeFlowInput" class="flow-input-root">
    <generic-node-settings
      v-model="nodeFlowInput"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <p>
        Placeholder for data coming from a parent flow. When this flow runs inside another flow via
        a "Run flow" node, the input connected there replaces this node's data.
      </p>
      <div class="flow-input-field">
        <label class="flow-input-label">Input name</label>
        <el-input
          v-model="nodeFlowInput.input_name"
          size="small"
          placeholder="e.g. orders"
          @change="saveSettings"
        />
      </div>
      <div class="sample-section">
        <div class="sample-heading">Sample data (optional)</div>
        <p class="sample-hint">
          Used when the flow runs standalone (e.g. while designing it); replaced by the real input
          data when the flow runs inside another flow.
        </p>
        <raw-data-editor v-model="sampleData" />
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref } from "vue";
import type { NodeData, NodeFlowInput, RawDataFormat } from "../../../../../types/node.types";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import RawDataEditor from "../manualInput/RawDataEditor.vue";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeData = ref<null | NodeData>(null);
const nodeFlowInput = ref<NodeFlowInput | null>(null);
// v-model target for the shared editor; copied into the settings on save.
const sampleData = ref<RawDataFormat | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFlowInput,
  onBeforeSave: () => {
    if (nodeFlowInput.value && sampleData.value) {
      nodeFlowInput.value.raw_data_format = sampleData.value;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeFlowInput.value = nodeData.value?.setting_input as NodeFlowInput;
  if (nodeFlowInput.value) {
    if (!nodeFlowInput.value.input_name) {
      nodeFlowInput.value.input_name = "input";
    }
    // The backend coerces a missing sample to empty columns/data; hand the
    // editor null in that case so it starts with its placeholder table.
    const rawDataFormat = nodeFlowInput.value.raw_data_format;
    sampleData.value =
      rawDataFormat && rawDataFormat.columns && rawDataFormat.columns.length > 0
        ? rawDataFormat
        : null;
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
.flow-input-root {
  height: 100%;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.flow-input-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-top: 8px;
  margin-bottom: 12px;
}

.flow-input-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}

.sample-section {
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.sample-heading {
  font-size: 13px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.sample-hint {
  margin: 2px 0 4px 0;
  font-size: 12px;
  color: var(--color-text-muted);
}
</style>
