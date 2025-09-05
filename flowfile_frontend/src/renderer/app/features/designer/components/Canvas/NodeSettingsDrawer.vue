<template>
  <NodeTitle :title="nodeStore.drawerProps.title" :intro="nodeStore.drawerProps.intro" />
  <component
    :is="nodeStore.activeDrawerComponent"
    v-bind="nodeStore.drawerProps"
    ref="drawerComponentInstance"
  />
</template>

<script setup lang="ts">
import { ref, watch, nextTick } from 'vue';
import { useNodeStore } from "../../../../stores/column-store";
import NodeTitle from "../../baseNode/nodeTitle.vue";

interface DrawerComponentInstance {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void;
}

const nodeStore = useNodeStore();
const drawerComponentInstance = ref<DrawerComponentInstance | null>(null);
const nodeTitle = ref<string>("");
const nodeIntro = ref<string>("");

// Track last executed values to prevent double execution
const lastExecutedState = ref({
  nodeId: -1,
  componentInstance: null as DrawerComponentInstance | null
});

// Single function to handle cleanup
const executeCleanup = async () => {
  // Check if we actually need to execute cleanup
  if (lastExecutedState.value.componentInstance) {
    await nodeStore.executeDrawCloseFunction();
  }
};

// Single function to handle setup
const setupNewNode = () => {
  if (drawerComponentInstance.value?.loadNodeData && nodeStore.node_id !== -1) {
    drawerComponentInstance.value.loadNodeData(nodeStore.node_id);
    nodeStore.setCloseFunction(drawerComponentInstance.value.pushNodeData);
    
    // Update tracked state
    lastExecutedState.value = {
      nodeId: nodeStore.node_id,
      componentInstance: drawerComponentInstance.value
    };
  }
};

// Combined watcher for both drawerComponentInstance and node_id
watch(
  [() => drawerComponentInstance.value, () => nodeStore.node_id],
  async ([newInstance, newNodeId], [oldInstance, oldNodeId]) => {
    // Determine if we need to execute cleanup
    const instanceChanged = newInstance !== oldInstance;
    const nodeIdChanged = newNodeId !== oldNodeId;
    
    // Execute cleanup only once if either changed
    if ((instanceChanged || nodeIdChanged) && 
        (oldInstance || oldNodeId !== -1) &&
        lastExecutedState.value.componentInstance) {
      await executeCleanup();
      // Reset the tracked state after cleanup
      lastExecutedState.value = {
        nodeId: -1,
        componentInstance: null
      };
    }
    
    // Handle node deselection
    if (newNodeId === -1) {
      nodeStore.isDrawerOpen = false;
      return;
    }
    
    // Setup new node if we have a valid instance
    if (newInstance) {
      await nextTick(); // Ensure component is fully mounted
      setupNewNode();
    }
  },
  { immediate: true }
);

// Watch for drawer component changes to update title/intro
watch(
  () => nodeStore.activeDrawerComponent,
  (newComponent) => {
    console.log("doing it under the watcher", nodeStore.drawerProps)
    if (newComponent) {

    }
  }
);
</script>