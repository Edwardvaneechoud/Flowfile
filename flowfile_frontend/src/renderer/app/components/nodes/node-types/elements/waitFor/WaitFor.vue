<template>
  <div v-if="dataLoaded && nodeWaitFor" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeWaitFor"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Wait For</div>
        <p class="explainer">
          The <strong>left</strong> input flows through unchanged. The <strong>right</strong> input
          only enforces ordering — its data is discarded; we just wait for that node to complete.
        </p>
        <p class="explainer">
          Use this to make Apply Model wait for Train Model, or to gate any downstream node on a
          side-effect node finishing first.
        </p>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref } from "vue";
import type { NodeMultiInput } from "../../../../../types/node.types";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeWaitFor = ref<NodeMultiInput | null>(null);
const dataLoaded = ref(false);
const nodeData = ref<NodeData | null>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeWaitFor,
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeWaitFor.value = nodeData.value?.setting_input as NodeMultiInput;
  if (nodeWaitFor.value) {
    dataLoaded.value = true;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.explainer {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.4;
  margin: var(--spacing-2) 0;
}
</style>
