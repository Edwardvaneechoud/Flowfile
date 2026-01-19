<template>
  <div class="undo-redo-controls">
    <button
      class="control-btn"
      :disabled="!flowStore.canUndo"
      :title="flowStore.undoDescription ? `Undo: ${flowStore.undoDescription}` : 'Nothing to undo'"
      @click="handleUndo"
    >
      <span class="material-icons">undo</span>
    </button>
    <button
      class="control-btn"
      :disabled="!flowStore.canRedo"
      :title="flowStore.redoDescription ? `Redo: ${flowStore.redoDescription}` : 'Nothing to redo'"
      @click="handleRedo"
    >
      <span class="material-icons">redo</span>
    </button>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, watch } from "vue";
import { FlowApi } from "../../api";
import { useFlowStore } from "../../stores/flow-store";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";

const flowStore = useFlowStore();
const nodeStore = useNodeStore();
const editorStore = useEditorStore();

const emit = defineEmits<{
  (e: "refreshFlow"): void;
}>();

// Fetch history state from API (used on mount and flow change)
const fetchHistoryState = async () => {
  if (!(flowStore.flowId && flowStore.flowId > 0)) return;

  try {
    const state = await FlowApi.getHistoryStatus(flowStore.flowId);
    flowStore.updateHistoryState(state);
  } catch (error) {
    flowStore.resetHistoryState();
  }
};

const closeOpenPanelsWithoutSave = () => {
  nodeStore.clearCloseFunction();
  nodeStore.nodeId = -1;
  editorStore.activeDrawerComponent = null;
};

const handleUndo = async () => {
  if (!flowStore.canUndo || !flowStore.flowId) return;

  try {
    closeOpenPanelsWithoutSave();
    const result = await FlowApi.undo(flowStore.flowId);
    if (result.success) {
      emit("refreshFlow");
      await fetchHistoryState();
    }
  } catch (error: any) {
    console.error("Failed to undo:", error);
  }
};

const handleRedo = async () => {
  if (!flowStore.canRedo || !flowStore.flowId) return;

  try {
    closeOpenPanelsWithoutSave();
    const result = await FlowApi.redo(flowStore.flowId);
    if (result.success) {
      emit("refreshFlow");
      await fetchHistoryState();
    }
  } catch (error: any) {
    console.error("Failed to redo:", error);
  }
};

// Keyboard shortcut handler
const handleKeyDown = (event: KeyboardEvent) => {
  // Skip if typing in an input field or code editor
  const target = event.target as HTMLElement;
  const isInputElement =
    target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable;
  const isInCodeMirror = target.closest(".cm-editor") !== null;

  if (isInputElement || isInCodeMirror) return;

  // Check for Ctrl+Z (Windows/Linux) or Cmd+Z (Mac)
  const isMac = navigator.platform.toUpperCase().indexOf("MAC") >= 0;
  const ctrlOrCmd = isMac ? event.metaKey : event.ctrlKey;

  if (ctrlOrCmd && event.key.toLowerCase() === "z") {
    if (event.shiftKey) {
      // Ctrl+Shift+Z or Cmd+Shift+Z = Redo
      event.preventDefault();
      handleRedo();
    } else {
      // Ctrl+Z or Cmd+Z = Undo
      event.preventDefault();
      handleUndo();
    }
  } else if (ctrlOrCmd && event.key.toLowerCase() === "y") {
    // Ctrl+Y = Redo (Windows convention)
    event.preventDefault();
    handleRedo();
  }
};

// Watch for flowId changes
watch(
  () => flowStore.flowId,
  async (newId, oldId) => {
    if (newId !== oldId && newId > 0) {
      await fetchHistoryState();
    }
  },
);

onMounted(async () => {
  window.addEventListener("keydown", handleKeyDown);
  if (flowStore.flowId && flowStore.flowId > 0) {
    await fetchHistoryState();
  }
});

onUnmounted(() => {
  window.removeEventListener("keydown", handleKeyDown);
});
</script>

<style scoped>
.undo-redo-controls {
  position: absolute;
  top: 12px;
  left: 192px; /* Position after the node list panel */
  display: flex;
  gap: 4px;
  z-index: 1000;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  padding: 4px;
  box-shadow: var(--shadow-md);
}

.control-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  background: transparent;
  border: none;
  border-radius: var(--border-radius-md);
  cursor: pointer;
  color: var(--color-text-secondary);
  transition: all var(--transition-fast);
}

.control-btn:hover:not(:disabled) {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.control-btn:active:not(:disabled) {
  transform: scale(0.95);
}

.control-btn:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

.control-btn .material-icons {
  font-size: 20px;
}
</style>
