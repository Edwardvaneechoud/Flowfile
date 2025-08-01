<template id="functionNode">
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="cloud_storage_reader.png"
      :title="`${nodeId}: Cloud storage reader`"
      @click="openDrawer"
    />
    <teleport v-if="drawer" to="#nodesettings">
      <NodeTitle title="Read data from cloud provider" intro="Read data from cloud provider">
      </NodeTitle>
      <cloud-storage-reader ref="childComp" :node-id="nodeId"> </cloud-storage-reader>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import CloudStorageReader from "./elements/cloudStorageReader/CloudStorageReader.vue";
import { useNodeStore } from "../../../stores/column-store";
import NodeButton from "../baseNode/nodeButton.vue";
import NodeTitle from "../baseNode/nodeTitle.vue";

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
  drawer.value = false;
  childComp.value?.pushNodeData();
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
  if (childComp.value) {
    childComp.value.loadNodeData(props.nodeId);
    nodeStore.openDrawer(closeOnDrawer);
  }
};

onMounted(async () => {
  await nextTick();
});
</script>
