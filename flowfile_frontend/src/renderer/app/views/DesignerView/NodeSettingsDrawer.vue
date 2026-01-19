<template>
  <NodeTitle :title="nodeStore.drawerProps.title" :intro="nodeStore.drawerProps.intro" />
  <component
    :is="nodeStore.activeDrawerComponent"
    v-bind="nodeStore.drawerProps"
    ref="drawerComponentInstance"
    :node-id="nodeStore.node_id"
  />
</template>
<script setup lang="ts">
import { ref, watch, nextTick } from "vue";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import NodeTitle from "../../components/nodes/baseNode/nodeTitle.vue";

interface DrawerComponentInstance {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void;
}

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const drawerComponentInstance = ref<DrawerComponentInstance | null>(null);

// Track last executed values to prevent double execution
const lastExecutedState = ref({
  nodeId: -1,
  componentInstance: null as DrawerComponentInstance | null,
});

// Single function to handle cleanup
const executeCleanup = async () => {
  // Check if we actually need to execute cleanup
  if (lastExecutedState.value.componentInstance) {
    console.log(`[NodeSettingsDrawer] Executing cleanup for node ${lastExecutedState.value.nodeId}`);
    console.log("[NodeSettingsDrawer] Stack trace:", new Error().stack);
    await nodeStore.executeDrawCloseFunction();
    console.log("[NodeSettingsDrawer] Cleanup completed");
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
      componentInstance: drawerComponentInstance.value,
    };
  }
};

// Combined watcher for both drawerComponentInstance and node_id
watch(
  [() => drawerComponentInstance.value, () => nodeStore.node_id],
  async ([newInstance, newNodeId], [, oldNodeId]) => {
    console.log(`[NodeSettingsDrawer] Watcher triggered: newNodeId=${newNodeId}, oldNodeId=${oldNodeId}, hasInstance=${!!newInstance}`);
    // Only execute cleanup when node ID changes (not on initial mount or instance changes)
    const nodeIdChanged = newNodeId !== oldNodeId;
    // Execute cleanup only when node ID changes and we have a previous valid state
    if (nodeIdChanged && oldNodeId !== -1 && lastExecutedState.value.componentInstance) {
      console.log("[NodeSettingsDrawer] Watcher: Executing cleanup due to node change");
      await executeCleanup();
      // Reset the tracked state after cleanup
      lastExecutedState.value = {
        nodeId: -1,
        componentInstance: null,
      };
    }

    // Handle node deselection
    if (newNodeId === -1) {
      console.log("[NodeSettingsDrawer] Watcher: Node deselected, closing drawer");
      editorStore.isDrawerOpen = false;
      return;
    }

    // Setup new node if we have a valid instance
    if (newInstance) {
      console.log("[NodeSettingsDrawer] Watcher: Setting up new node");
      await nextTick(); // Ensure component is fully mounted
      setupNewNode();
    }
  },
  { immediate: true },
);
</script>
