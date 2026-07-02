<template>
  <div v-if="dataLoaded && nodeManualInput" class="manual-input-root">
    <generic-node-settings
      v-model="nodeManualInput"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <raw-data-editor v-model="editorData" :legacy-records="legacyRecords" />
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { createManualInput } from "./manualInputLogic";
import type { NodeManualInput, RawDataFormat } from "../../../baseNode/nodeInput";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import RawDataEditor from "./RawDataEditor.vue";

const nodeStore = useNodeStore();

const dataLoaded = ref(false);
const nodeManualInput = ref<NodeManualInput | null>(null);
// v-model target for the shared editor; copied into the settings on save.
const editorData = ref<RawDataFormat | null>(null);
const legacyRecords = ref<Record<string, unknown>[] | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeManualInput,
  onBeforeSave: () => {
    if (nodeManualInput.value && editorData.value) {
      nodeManualInput.value.raw_data_format = editorData.value;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);

  legacyRecords.value = null;
  if (nodeResult?.setting_input) {
    nodeManualInput.value = nodeResult.setting_input;
    const rawDataFormat = nodeResult.setting_input.raw_data_format;
    if (rawDataFormat && rawDataFormat.columns && rawDataFormat.data) {
      editorData.value = rawDataFormat;
    } else {
      editorData.value = null;
      legacyRecords.value = nodeResult.setting_input.raw_data ?? null;
    }
  } else {
    nodeManualInput.value = createManualInput(nodeStore.flow_id, nodeStore.node_id).value;
    editorData.value = null;
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
/* The drawer + genericNodeSettings flex chain provides the height; this just
   needs to be a flex column so the editor can flex-fill below. */
.manual-input-root {
  height: 100%;
  display: flex;
  flex-direction: column;
  min-height: 0;
}
</style>
