<template>
  <div class="undo-redo-controls">
    <button
      class="control-btn"
      :disabled="!historyState.can_undo"
      :title="historyState.undo_description ? `Undo: ${historyState.undo_description}` : 'Nothing to undo'"
      @click="handleUndo"
    >
      <span class="material-icons">undo</span>
    </button>
    <button
      class="control-btn"
      :disabled="!historyState.can_redo"
      :title="historyState.redo_description ? `Redo: ${historyState.redo_description}` : 'Nothing to redo'"
      @click="handleRedo"
    >
      <span class="material-icons">redo</span>
    </button>
  </div>
</template>

<script setup lang="ts">
import { reactive, onMounted, onUnmounted, watch } from "vue";
import { FlowApi } from "../../api";
import type { HistoryState } from "../../types";
import { useNodeStore } from "../../stores/column-store";

const nodeStore = useNodeStore();

const emit = defineEmits<{
  (e: "refreshFlow"): void;
}>();

// History state for undo/redo buttons
const historyState = reactive<HistoryState>({
  can_undo: false,
  can_redo: false,
  undo_description: null,
  redo_description: null,
  undo_count: 0,
  redo_count: 0,
});

const refreshHistoryState = async () => {
  if (!(nodeStore.flow_id && nodeStore.flow_id > 0)) return;

  try {
    const state = await FlowApi.getHistoryStatus(nodeStore.flow_id);
    Object.assign(historyState, state);
  } catch (error) {
    // Reset to default if error
    Object.assign(historyState, {
      can_undo: false,
      can_redo: false,
      undo_description: null,
      redo_description: null,
      undo_count: 0,
      redo_count: 0,
    });
  }
};

const handleUndo = async () => {
  if (!historyState.can_undo || !nodeStore.flow_id) return;

  try {
    const result = await FlowApi.undo(nodeStore.flow_id);
    if (result.success) {
      emit("refreshFlow");
      await refreshHistoryState();
    }
  } catch (error: any) {
    console.error("Failed to undo:", error);
  }
};

const handleRedo = async () => {
  if (!historyState.can_redo || !nodeStore.flow_id) return;

  try {
    const result = await FlowApi.redo(nodeStore.flow_id);
    if (result.success) {
      emit("refreshFlow");
      await refreshHistoryState();
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

// Watch for flow_id changes
watch(
  () => nodeStore.flow_id,
  async (newId, oldId) => {
    if (newId !== oldId && newId > 0) {
      await refreshHistoryState();
    }
  },
);

onMounted(async () => {
  window.addEventListener("keydown", handleKeyDown);
  if (nodeStore.flow_id && nodeStore.flow_id > 0) {
    await refreshHistoryState();
  }
});

onUnmounted(() => {
  window.removeEventListener("keydown", handleKeyDown);
});

// Expose refresh method so parent can call it after operations
defineExpose({
  refreshHistoryState,
  handleUndo,
  handleRedo,
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
