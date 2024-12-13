<template id="functionNode">
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="graph_solver.png"
      :title="`${nodeId}: Graph Solver`"
      @click="openDrawer"
    />
    <teleport v-if="drawer" to="#nodesettings">
      <NodeTitle title="Graph Solver" intro="Find groups in your data"> </NodeTitle>
      <readInput ref="childComp" :node-id="nodeId"> </readInput>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import readInput from "./elements/graphSolver/graphSolver.vue";
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
    nodeStore.openDrawer(closeOnDrawer);
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
