<script setup lang="ts">
import { ref, onMounted, nextTick } from "vue";
import graphicWalker from "./elements/exploreData/exploreData.vue";
import { useNodeStore } from "../../../stores/column-store";
import NodeButton from "../baseNode/nodeButton.vue";
import NodeTitle from "../baseNode/nodeTitle.vue";

const nodeStore = useNodeStore();

interface ChildComponentRef {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void;
}
const props = defineProps({
  nodeId: {
    type: Number,
    required: true,
  },
});
const childComp = ref<ChildComponentRef | null>(null);
const el: any = ref(null);
const drawer = ref(false);

const closeOnDrawer = () => {
  if(childComp.value) {
      childComp.value.pushNodeData();
  }
  drawer.value = false;
};

const openDrawer = async () => {
  const isDifferentNode = nodeStore.node_id !== props.nodeId;

  if (nodeStore.isDrawerOpen && isDifferentNode) {
       nodeStore.closeDrawer();
       await nextTick();
  } else if (nodeStore.isDrawerOpen && !isDifferentNode) {
       nodeStore.requestFullScreen('nodeSettings');
       return;
  }

  drawer.value = true;
  nodeStore.openDrawer(closeOnDrawer);
  nodeStore.node_id = props.nodeId;
  nodeStore.requestFullScreen('nodeSettings');

  await nextTick();

  if (childComp.value) {
    childComp.value.loadNodeData(props.nodeId);
  }
};

onMounted(async () => {
});
</script>

<template>
  <div ref="el">
    <NodeButton
      ref="nodeButton"
      :node-id="nodeId"
      image-src="explore_data.png"
      :title="`${nodeId}: Explore data`"
      @click="openDrawer"
    />
    <teleport v-if="drawer" to="#nodesettings">
       <div style="display: flex; flex-direction: column; height: 100%;">
           <NodeTitle title="explore data" intro="Explore data based on Tableau like experience" />
           <graphicWalker ref="childComp" :node-id="nodeId" style="flex-grow: 1; overflow: auto;"> </graphicWalker>
       </div>
    </teleport>
  </div>
</template>