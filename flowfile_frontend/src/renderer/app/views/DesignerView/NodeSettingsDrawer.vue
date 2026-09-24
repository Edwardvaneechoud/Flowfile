<template>
  <div class="node-settings-drawer nokey">
    <NodeTitle
      :title="nodeStore.drawerProps.title"
      :intro="nodeStore.drawerProps.intro"
      :docs-url="nodeStore.drawerProps.docsUrl"
    />
    <div
      class="node-settings-body"
      @pointerdown.capture="noteEdit"
      @keydown.capture="noteEdit"
      @input.capture="noteEdit"
      @change.capture="noteEdit"
      @paste.capture="noteEdit"
      @cut.capture="noteEdit"
      @drop.capture="noteEdit"
    >
      <component
        :is="nodeStore.activeDrawerComponent"
        v-bind="componentProps"
        ref="drawerComponentInstance"
        :node-id="nodeStore.node_id"
      />
    </div>
    <div v-if="nodeStore.node_id !== -1 && canApply" class="node-settings-footer">
      <el-button type="primary" size="small" :loading="isApplying" @click="applySettings">
        {{ justApplied ? "Applied ✓" : "Apply" }}
      </el-button>
    </div>
  </div>
</template>
<script setup lang="ts">
import { ref, computed, watch, nextTick, onBeforeUnmount } from "vue";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import { useFlowStore } from "../../stores/flow-store";
import NodeTitle from "../../components/nodes/baseNode/nodeTitle.vue";
import { IS_MAC } from "../../utils/shortcuts";
import {
  createDrawerSession,
  isDrawerEdit,
  type DrawerSession,
} from "../../composables/settingsDrawerSession";

interface DrawerComponentInstance {
  loadNodeData: (nodeId: number) => void | Promise<void>;
  pushNodeData: () => void | boolean | Promise<void | boolean>;
  // Opt-in: a component that can have nothing to save (ExploreData before its
  // data is fetched) exposes this to hide the Apply footer. Undefined keeps
  // Apply visible, which is what every other node settings component wants.
  canApply?: boolean;
}

const nodeStore = useNodeStore();

// Header-only keys must not reach the node component: they would fall through
// as DOM attributes, and a native `title` tooltip then follows the pointer
// anywhere inside the drawer.
const HEADER_ONLY_KEYS = new Set(["title", "intro", "docsUrl"]);
const componentProps = computed(() =>
  Object.fromEntries(
    Object.entries(nodeStore.drawerProps).filter(([key]) => !HEADER_ONLY_KEYS.has(key)),
  ),
);
const editorStore = useEditorStore();
const flowStore = useFlowStore();
const drawerComponentInstance = ref<DrawerComponentInstance | null>(null);

// The shown node's pending save; the drawer's close function is its conditional close.
let session: DrawerSession | null = null;
let stopLoadedWatch: (() => void) | null = null;
onBeforeUnmount(() => stopLoadedWatch?.());

const noteEdit = (event: Event) => {
  if (isDrawerEdit(event, IS_MAC)) session?.noteEdit();
};

// Universal Apply: every drawer-entry node component exposes pushNodeData (the
// same save that runs on drawer-close), so this works for all node types,
// including the ones that don't use genericNodeSettings. Saves without closing.
const isApplying = ref(false);
const justApplied = ref(false);
let appliedTimer: ReturnType<typeof setTimeout> | null = null;

const canApply = computed(() => drawerComponentInstance.value?.canApply !== false);

const applySettings = async () => {
  const instance = drawerComponentInstance.value;
  if (!instance?.pushNodeData || !canApply.value) return;
  isApplying.value = true;
  try {
    // A component that reports a refused save keeps the button on "Apply".
    const result = session ? await session.save() : await instance.pushNodeData();
    if (result === false) return;
    editorStore.disarmRefusedSave();
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

// Fallback for unconditional closes (flow switch); a failed save must not block the next load.
const executeCleanup = async () => {
  if (!lastExecutedState.value.componentInstance) return;
  await editorStore.executeDrawCloseFunctionOnce();
};

const setupNewNode = () => {
  const instance = drawerComponentInstance.value;
  const nodeId = nodeStore.node_id;
  if (instance?.loadNodeData && nodeId !== -1) {
    const current = createDrawerSession({
      push: () => instance.pushNodeData(),
      nodeExists: () => !!flowStore.vueFlowInstance?.findNode(String(nodeId)),
    });
    session = current;
    const previous = nodeStore.nodeData;
    // is_setup comes from the first fresh response for this node, not the draft or the load promise.
    stopLoadedWatch?.();
    stopLoadedWatch = watch(
      () => nodeStore.nodeData,
      (loaded) => {
        if (loaded && loaded !== previous && Number(loaded.node_id) === nodeId) {
          current.loaded(loaded.is_setup);
          stopLoadedWatch?.();
          stopLoadedWatch = null;
        }
      },
    );
    Promise.resolve(instance.loadNodeData(nodeId)).catch((error) =>
      console.error("Loading the node settings failed:", error),
    );
    editorStore.setCloseFunction(current.close, current.hasPendingEdits);
    lastExecutedState.value = {
      nodeId,
      componentInstance: instance,
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

/* Body holds the active node component. It scrolls internally so tall forms
   stay contained instead of overflowing onto the Apply footer; the title above
   and the footer below stay pinned. A node whose root opts in with
   `height: 100%` still fills the body; shorter forms render at the top. */
.node-settings-body {
  flex: 1;
  min-height: 0;
  overflow: auto;
}

/* Sticky footer holding the universal Apply button, present for every node. */
.node-settings-footer {
  flex-shrink: 0;
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  padding: 6px 12px;
  border-top: 1px solid var(--el-border-color-lighter);
  background-color: var(--el-bg-color);
}
</style>
