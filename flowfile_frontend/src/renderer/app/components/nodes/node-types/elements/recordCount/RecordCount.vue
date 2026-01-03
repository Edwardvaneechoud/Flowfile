<template>
  <div v-if="dataLoaded && nodeRecordCount" class="listbox-wrapper">
    <generic-node-settings v-model="nodeRecordCount">
      <p>
        This node helps you quickly retrieve the total number of records from the selected table.
        It's a simple yet powerful tool to keep track of the data volume as you work through your
        tasks.
      </p>
      <p>This node does not need a setup</p>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, nextTick } from "vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { NodeBase } from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/column-store";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeData = ref<null | NodeData>(null);
const nodeRecordCount = ref<NodeBase | null>(null);

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeRecordCount.value = nodeData.value?.setting_input as NodeBase;
  dataLoaded.value = true;
};

const pushNodeData = async () => {
  if (nodeRecordCount.value) {
    nodeStore.updateSettings(nodeRecordCount);
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
});

onMounted(async () => {
  await nextTick();
});
</script>
