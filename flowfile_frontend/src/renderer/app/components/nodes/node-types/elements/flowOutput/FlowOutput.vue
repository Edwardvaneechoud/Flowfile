<template>
  <div v-if="dataLoaded && nodeFlowOutput" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeFlowOutput"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <p>
        Marks this point as a named output of the flow. When this flow runs inside another flow via
        a "Run flow" node, the data arriving here is exposed as an output with this name.
      </p>
      <div class="flow-output-field">
        <label class="flow-output-label">Output name</label>
        <el-input
          v-model="nodeFlowOutput.output_name"
          size="small"
          placeholder="e.g. result"
          @change="saveSettings"
        />
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, nextTick } from "vue";
import type { NodeData, NodeFlowOutput } from "../../../../../types/node.types";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeData = ref<null | NodeData>(null);
const nodeFlowOutput = ref<NodeFlowOutput | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFlowOutput,
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeFlowOutput.value = nodeData.value?.setting_input as NodeFlowOutput;
  if (nodeFlowOutput.value) {
    if (!nodeFlowOutput.value.output_name) {
      nodeFlowOutput.value.output_name = "output";
    }
    dataLoaded.value = true;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});

onMounted(async () => {
  await nextTick();
});
</script>

<style scoped>
.flow-output-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-top: 8px;
}

.flow-output-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}
</style>
