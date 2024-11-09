<template>
  <div v-if="dataLoaded" class="listbox-wrapper">
    <p>
      This node helps you quickly retrieve the total number of records from the
      selected table. It's a simple yet powerful tool to keep track of the data
      volume as you work through your tasks.
    </p>
    <p>This node does not need a setup</p>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, nextTick, defineProps } from "vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { NodeBase } from "../../../baseNode/nodeInput";
import { useNodeStore } from "../../../../../stores/column-store";
const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeData = ref<null | NodeData>(null);
const nodeRecordCount = ref<NodeBase | null>(null);

const props = defineProps({ nodeId: { type: Number, required: true } });

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(1, nodeId, false);
  nodeRecordCount.value = nodeData.value?.setting_input as NodeBase;
  dataLoaded.value = true;
  nodeStore.isDrawerOpen = true;
};

const pushNodeData = async () => {
  if (nodeRecordCount.value) {
    nodeStore.updateSettings(nodeRecordCount);
  }
  nodeStore.isDrawerOpen = false;
};

defineExpose({
  loadNodeData,
  pushNodeData,
});

onMounted(async () => {
  await nextTick();
});
</script>
