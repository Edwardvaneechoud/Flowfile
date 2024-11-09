<template id="functionNode">
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="sample.png"
      :title="`${nodeId}: Sample`"
      @click="openDrawer"
    />
    <teleport v-if="drawer" to="#nodesettings">
      <nodeTitle
        title="Sample"
        intro="When taking a sample, the most crucial factor to consider is the size of the sample. The size refers to the number of items or observations included in the sample from a larger population. This number is pivotal because it influences the accuracy and reliability of the results obtained from the sample. By carefully determining the sample size, we ensure that our findings are representative of the larger group, allowing us to make informed decisions or predictions based on the sample data"
      />
      <readInput ref="childComp" :node-id="nodeId"> </readInput>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import readInput from "./elements/sample/sample.vue";
import { useNodeStore } from "../../../stores/column-store";
import NodeButton from "../baseNode/nodeButton.vue";
import nodeTitle from "../baseNode/nodeTitle.vue";
const nodeStore = useNodeStore(); // Use the nodeStore

interface ChildComponentRef {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void;
  // You can add other methods or properties here as needed
}
const childComp = ref<ChildComponentRef | null>(null);

const props = defineProps({
  nodeId: {
    type: Number,
    required: true,
  },
});

const drawer = ref(false);

const closeOnDrawer = () => {
  childComp.value?.pushNodeData();
  drawer.value = false;
  nodeStore.isDrawerOpen = false;
};

const openDrawer = async () => {
  if (nodeStore.node_id === props.nodeId) {
    return;
  }
  nodeStore.closeDrawer();
  drawer.value = true;
  const drawerOpen = nodeStore.isDrawerOpen;
  nodeStore.isDrawerOpen = true;
  await nextTick();
  if (nodeStore.node_id === props.nodeId && drawerOpen) {
    return;
  }
  console.log("test", childComp.value);
  if (childComp.value) {
    childComp.value.loadNodeData(props.nodeId);
    nodeStore.openDrawer(closeOnDrawer);
  }
};

onMounted(async () => {
  await nextTick();
});
</script>
