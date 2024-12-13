<template id="functionNode">
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="airbyte.png"
      :title="`${nodeId}: External source`"
      @click="openDrawer"
    />
    <teleport v-if="drawer" to="#nodesettings">
      <NodeTitle title="External source" intro="Import data from an external source" />
      <airbyteSource ref="childComp" :node-id="nodeId"> </airbyteSource>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import airbyteSource from "./elements/airbyteReader/airbyteReader.vue";
import { useNodeStore } from "../../../stores/column-store";
import NodeButton from "../baseNode/nodeButton.vue";
import NodeTitle from "../baseNode/nodeTitle.vue";

const nodeStore = useNodeStore(); // Use the nodeStore
const props = defineProps({
  nodeId: {
    type: Number,
    required: true,
  },
});
interface ChildComponentRef {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void;
  // You can add other methods or properties here as needed
}
const childComp = ref<ChildComponentRef | null>(null);

const el: any = ref(null);
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
  console.log("openDrawer");
  drawer.value = true;
  const drawerOpen = nodeStore.isDrawerOpen;
  nodeStore.isDrawerOpen = true;
  await nextTick();
  if (nodeStore.node_id === props.nodeId && drawerOpen) {
    console.log("No need to load data");
    return;
  }
  if (childComp.value) {
    await childComp.value.loadNodeData(props.nodeId);
    nodeStore.openDrawer(closeOnDrawer);
  }
};

onMounted(async () => {
  await nextTick();
});
</script>
