<template>
  <div class="undo-redo-controls">
    <el-popover
      placement="bottom"
      :width="240"
      trigger="hover"
      :show-after="200"
      popper-class="header-action-popover"
      :show-arrow="true"
    >
      <template #reference>
        <button class="control-btn" :disabled="!flowStore.canUndo" @click="handleUndo">
          <span class="material-icons">undo</span>
        </button>
      </template>
      <div class="header-action-popover-body">
        <div class="header-action-popover-title">
          <span class="material-icons header-action-popover-icon">undo</span>
          <span>Undo</span>
        </div>
        <p class="header-action-popover-desc">
          {{
            flowStore.canUndo && flowStore.undoDescription
              ? `Undo: ${flowStore.undoDescription}`
              : "Nothing to undo yet."
          }}
        </p>
        <p class="header-action-popover-shortcut-hint">
          <span class="header-action-popover-shortcut">
            <kbd>{{ MODIFIER_LABEL }}</kbd>
            <kbd>Z</kbd>
          </span>
          to undo.
        </p>
      </div>
    </el-popover>
    <el-popover
      placement="bottom"
      :width="240"
      trigger="hover"
      :show-after="200"
      popper-class="header-action-popover"
      :show-arrow="true"
    >
      <template #reference>
        <button class="control-btn" :disabled="!flowStore.canRedo" @click="handleRedo">
          <span class="material-icons">redo</span>
        </button>
      </template>
      <div class="header-action-popover-body">
        <div class="header-action-popover-title">
          <span class="material-icons header-action-popover-icon">redo</span>
          <span>Redo</span>
        </div>
        <p class="header-action-popover-desc">
          {{
            flowStore.canRedo && flowStore.redoDescription
              ? `Redo: ${flowStore.redoDescription}`
              : "Nothing to redo."
          }}
        </p>
        <p class="header-action-popover-shortcut-hint">
          <span class="header-action-popover-shortcut">
            <kbd>{{ MODIFIER_LABEL }}</kbd>
            <kbd>{{ SHIFT_LABEL }}</kbd>
            <kbd>Z</kbd>
          </span>
          to redo.
        </p>
      </div>
    </el-popover>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted } from "vue";
import { ElMessage } from "element-plus";
import { FlowApi } from "../../api";
import { useFlowStore } from "../../stores/flow-store";
import { useNodeStore } from "../../stores/column-store";
import { useEditorStore } from "../../stores/editor-store";
import { useResultsStore } from "../../stores/results-store";
import { useDrawerStore } from "../../stores/drawer-store";
import { whenMutationsIdle } from "../../services/axios.config";
import { flushPendingEdits } from "../../services/mutationChannel";
import { recoverFromFailedMutation } from "../../services/mutationFailure";
import { historyShortcutFor } from "../../composables/useFlowHotkeys";
import { IS_MAC, MODIFIER_LABEL, SHIFT_LABEL } from "../../utils/shortcuts";
import type { UndoRedoResult } from "../../types";
import { prepareHistoryAction, type HistoryAction } from "./historyAction";

const flowStore = useFlowStore();
const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const resultsStore = useResultsStore();
const drawerStore = useDrawerStore();

// Presses run one after another, so each sees the history its predecessor left.
let pressChain: Promise<void> = Promise.resolve();

/**
 * Close the drawer, saving only the user's edits there (they become the step undone); a
 * refused save reports itself and keeps the drawer open.
 */
const closeSettingsDrawer = async (): Promise<boolean> => {
  if ((await editorStore.executeDrawCloseFunction({ userEditsOnly: true }, true)) === false) {
    return false;
  }
  nodeStore.nodeId = -1;
  editorStore.activeDrawerComponent = null;
  return true;
};

const isNothingToDo = (result: UndoRedoResult) =>
  /^nothing to (undo|redo)/i.test(result.error_message ?? "");

const runHistoryAction = async (action: HistoryAction) => {
  const flowId = await prepareHistoryAction(action, {
    flowId: () => flowStore.flowId,
    canRun: (candidate) => (candidate === "undo" ? flowStore.canUndo : flowStore.canRedo),
    drawerHasPendingEdits: () => editorStore.hasPendingDrawerEdits(),
    closeDrawer: closeSettingsDrawer,
    flushPendingEdits,
    whenMutationsIdle,
  });
  if (flowId === null) return;
  let result: UndoRedoResult;
  try {
    result = action === "undo" ? await FlowApi.undo(flowId) : await FlowApi.redo(flowId);
  } catch (error) {
    recoverFromFailedMutation(error, `Could not ${action}`);
    return;
  }
  if (!result.success) {
    if (isNothingToDo(result)) return;
    // A restore that failed part-way may still have changed the graph.
    ElMessage.error(result.error_message || `Could not ${action}`);
    flowStore.requestReload();
    return;
  }
  // Restore rebuilds the nodes, so the flow's run results no longer describe them.
  if (flowStore.flowId !== flowId) return;
  resultsStore.clearFlowResults(flowId);
  drawerStore.clearPreview();
  flowStore.requestReload();
};

const enqueue = (action: HistoryAction) => {
  pressChain = pressChain
    .then(() => runHistoryAction(action))
    .catch((error) => {
      console.error(`Failed to ${action}:`, error);
    });
};

const handleUndo = () => enqueue("undo");
const handleRedo = () => enqueue("redo");

const handleKeyDown = (event: KeyboardEvent) => {
  const action = historyShortcutFor(event, IS_MAC);
  if (!action) return;
  event.preventDefault();
  enqueue(action);
};

onMounted(() => window.addEventListener("keydown", handleKeyDown));
onUnmounted(() => window.removeEventListener("keydown", handleKeyDown));
</script>

<style scoped>
/* Sits inside the page header alongside HeaderButtons. Matches the same
   .action-btn--icon-only visual language so undo/redo read as part of the
   same toolbar group. */
.undo-redo-controls {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-left: var(--spacing-2);
  font-family: var(--font-family-base);
}

.control-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 34px;
  height: 34px;
  padding: var(--spacing-2);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  cursor: pointer;
  color: var(--color-text-primary);
  transition: all var(--transition-fast);
  box-shadow: var(--shadow-xs);
}

.control-btn:hover:not(:disabled) {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.control-btn:active:not(:disabled) {
  transform: translateY(1px);
  box-shadow: none;
}

.control-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.control-btn .material-icons {
  font-size: 18px;
  color: var(--color-text-secondary);
}

.control-btn:hover:not(:disabled) .material-icons {
  color: var(--color-text-primary);
}
</style>
