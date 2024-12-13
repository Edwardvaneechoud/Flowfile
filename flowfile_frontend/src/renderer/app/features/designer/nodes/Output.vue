<template id="functionNode">
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="output.png"
      :title="`${nodeId}: Write data`"
      @click="openDrawer"
    />

    <teleport v-if="drawer" to="#nodesettings">
      <NodeTitle title="Write data" intro="Write data to a file or database"> </NodeTitle>
      <outputPage ref="childComp" :node-id="nodeId"> </outputPage>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import outputPage from "./elements/output/output.vue";
import { useNodeStore } from "../../../stores/column-store";
import NodeButton from "../baseNode/nodeButton.vue";
import NodeTitle from "../baseNode/nodeTitle.vue";
const props = defineProps({
  nodeId: {
    type: Number,
    required: true,
  },
});

const nodeStore = useNodeStore(); // Use the nodeStore
const success = ref<boolean>(false);
interface ChildComponentRef {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void;
  // You can add other methods or properties here as needed
}
const childComp = ref<ChildComponentRef | null>(null);

const closeOnDrawer = () => {
  console.log("closeOnDrawer");
  if (drawer.value) {
    childComp.value?.pushNodeData();
    drawer.value = false;
  }
};

const drawer = ref(false);

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
  if (childComp.value) {
    childComp.value.loadNodeData(props.nodeId);
    nodeStore.openDrawer(closeOnDrawer);
  }
};

onMounted(async () => {
  await nextTick();
});
</script>
