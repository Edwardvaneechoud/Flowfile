<template id="functionNode">
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="formula.png"
      :title="`${nodeId}: Formula`"
      @click="openDrawer"
    />
    <teleport v-if="drawer" to="#nodesettings">
      <NodeTitle title="Formula" intro="Create a new column with a formula"> </NodeTitle>
      <editorInput ref="childComp"> </editorInput>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import editorInput from "./elements/formula/formula.vue";
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

const drawer = ref(false);
const props = defineProps({
  nodeId: {
    type: Number,
    required: true,
  },
});
const closeOnDrawer = () => {
  childComp.value?.pushNodeData();
  drawer.value = false;
  nodeStore.isDrawerOpen = false;
};

const openDrawer = async () => {
  if (nodeStore.node_id === props.nodeId) {
    // nodeStore.openDrawer(closeOnDrawer);
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
  //node_id = df.value.nodeId
});
</script>
