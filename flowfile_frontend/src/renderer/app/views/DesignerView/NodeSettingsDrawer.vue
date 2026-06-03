<template>
  <div class="node-settings-drawer">
    <NodeTitle :title="nodeStore.drawerProps.title" :intro="nodeStore.drawerProps.intro" />
    <div class="node-settings-body">
      <component
        :is="nodeStore.activeDrawerComponent"
        v-bind="nodeStore.drawerProps"
        ref="drawerComponentInstance"
        :node-id="nodeStore.node_id"
      />
    </div>
    <div v-if="nodeStore.node_id !== -1" class="node-settings-footer">
      <el-button type="primary" :loading="isApplying" @click="applySettings">
        {{ justApplied ? "Applied ✓" : "Apply" }}
      </el-button>
    </div>
  </div>
</template>
<script setup lang="ts">
import { ref, watch, nextTick } from "vue";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import NodeTitle from "../../components/nodes/baseNode/nodeTitle.vue";

interface DrawerComponentInstance {
  loadNodeData: (nodeId: number) => void;
  pushNodeData: () => void | Promise<void>;
}

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const drawerComponentInstance = ref<DrawerComponentInstance | null>(null);

// Universal Apply: every drawer-entry node component exposes pushNodeData (the
// same save that runs on drawer-close), so this works for all node types,
// including the ones that don't use genericNodeSettings. Saves without closing.
const isApplying = ref(false);
const justApplied = ref(false);
let appliedTimer: ReturnType<typeof setTimeout> | null = null;

const applySettings = async () => {
  if (!drawerComponentInstance.value?.pushNodeData) return;
  isApplying.value = true;
  try {
    await drawerComponentInstance.value.pushNodeData();
    justApplied.value = true;
    if (appliedTimer) clearTimeout(appliedTimer);
    appliedTimer = setTimeout(() => {
      justApplied.value = false;
    }, 1500);
  } finally {
    isApplying.value = false;
  }
};

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
    if (nodeIdChanged) {
      justApplied.value = false;
    }
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

<style scoped>
.node-settings-drawer {
  height: 100%;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

/* Body holds the active node component. It has an explicit (flex-allocated)
   height, so a node whose root opts in with `height: 100%` (or flex: 1) will
   fill the panel; nodes with natural-height forms still render at the top
   with the remaining space below them. */
.node-settings-body {
  flex: 1;
  min-height: 0;
}

/* Sticky footer holding the universal Apply button, present for every node. */
.node-settings-footer {
  flex-shrink: 0;
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  padding: 10px 12px;
  border-top: 1px solid var(--el-border-color-lighter);
  background-color: var(--el-bg-color);
}
</style>
