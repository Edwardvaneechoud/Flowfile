<template id="functionNode">
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="group_by.png"
      :title="`${nodeId}: Group by`"
      @click="openDrawer"
    />
    <teleport v-if="drawer" to="#nodesettings">
      <NodeTitle title="Group by" intro="Group data by a column"> </NodeTitle>
      <readInput ref="childComp" :node-id="nodeId"> </readInput>
    </teleport>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import readInput from "./elements/groupBy/groupBy.vue";
import { useNodeStore } from "../../../stores/column-store";
import NodeButton from "../baseNode/nodeButton.vue";
import NodeTitle from "../baseNode/nodeTitle.vue";
const nodeStore = useNodeStore(); // Use the nodeStore

interface ChildComponentRef {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void;
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
  nodeStore.closeDrawer();
  drawer.value = true;
  const drawerOpen = nodeStore.isDrawerOpen;
  nodeStore.isDrawerOpen = true;
  await nextTick();

  if (childComp.value) {
    childComp.value.loadNodeData(props.nodeId);
    nodeStore.openDrawer(closeOnDrawer);
  }
};

onMounted(async () => {
  await nextTick();
});
</script>
