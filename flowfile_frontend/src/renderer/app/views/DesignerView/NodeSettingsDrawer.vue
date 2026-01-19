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

const executeCleanup = async () => {
  if (lastExecutedState.value.componentInstance) {
    await nodeStore.executeDrawCloseFunction();
  }
};

const setupNewNode = () => {
  if (drawerComponentInstance.value?.loadNodeData && nodeStore.node_id !== -1) {
    drawerComponentInstance.value.loadNodeData(nodeStore.node_id);
    nodeStore.setCloseFunction(drawerComponentInstance.value.pushNodeData);
    lastExecutedState.value = {
      nodeId: nodeStore.node_id,
      componentInstance: drawerComponentInstance.value,
    };
  }
};

watch(
  [() => drawerComponentInstance.value, () => nodeStore.node_id],
  async ([newInstance, newNodeId], [, oldNodeId]) => {
    const nodeIdChanged = newNodeId !== oldNodeId;
    if (nodeIdChanged && oldNodeId !== -1 && lastExecutedState.value.componentInstance) {
      await executeCleanup();
      lastExecutedState.value = {
        nodeId: -1,
        componentInstance: null,
      };
    }

    if (newNodeId === -1) {
      editorStore.isDrawerOpen = false;
      return;
    }

    if (newInstance) {
      await nextTick();
      setupNewNode();
    }
  },
  { immediate: true },
);
</script>
