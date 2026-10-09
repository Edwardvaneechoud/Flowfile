<template>
  <PopoutWindowFrame
    :title="title"
    :state="state"
    test-id="notebook-window"
    @return="returnToDesigner"
    @close="closeThisWindow"
  >
    <NotebookPanel :key="flowId" :flow-id="flowId" host="window" />
  </PopoutWindowFrame>
</template>

<script lang="ts" setup>
// One flow's canvas notebook in its own window. Unpushed cell edits die with the window, so
// "Return to designer" asks first.
import { onMounted } from "vue";
import { ElMessageBox } from "element-plus";
import NotebookPanel from "../CatalogView/NotebookPanel.vue";
import PopoutWindowFrame from "./PopoutWindowFrame.vue";
import { usePopoutWindowHost } from "./usePopoutWindowHost";
import { useEditorStore } from "../../stores/editor-store";
import { flowNeedsSync, useNotebookStore } from "../../stores/notebook-store";

const editorStore = useEditorStore();
const notebookStore = useNotebookStore();

const { flowId, state, title, returnToDesigner, closeThisWindow } = usePopoutWindowHost({
  kind: "notebook",
  // Without a canvas, a reload request becomes the graph-version bump the panel re-renders on.
  onReload: () => editorStore.bumpGraphVersion(),
  onRekey: (moved) => {
    const old = notebookStore.openNotebooks.find((n) => n.flowId === moved.from);
    if (old) notebookStore.closeTab(old.tabId);
  },
  beforeReturn: confirmUnpushedEdits,
});

function hasUnpushedEdits(): boolean {
  const nb = notebookStore.openNotebooks.find((n) => n.flowId === flowId.value);
  return !!nb && flowNeedsSync(nb);
}

async function confirmUnpushedEdits(): Promise<boolean> {
  if (!hasUnpushedEdits()) return true;
  try {
    await ElMessageBox.confirm(
      "Cells edited here were not pushed to the canvas yet; returning discards them.",
      "Return to the designer?",
      { confirmButtonText: "Return", cancelButtonText: "Keep open", type: "warning" },
    );
    return true;
  } catch {
    return false;
  }
}

onMounted(() => {
  // This window's notebook store must not restore, nor write over, the designer's catalog tabs.
  notebookStore.setPersistence(false);
});
</script>
