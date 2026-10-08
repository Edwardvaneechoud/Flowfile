/**
 * The code dock's notebook pop-out. Popping out is a move: the dock closes and shows a stub for that
 * flow until the window comes back, so one window hosts a flow's notebook at a time (two hosts would
 * share one kernel session). The window itself is the shell's or the browser's (`lib/desktop.ts`).
 */
import { ElMessage } from "element-plus";
import { desktop } from "../../lib/desktop";
import router from "../router";
import { notebookWindowName, notebookWindowUrl } from "../services/popoutWindow";
import { useEditorStore } from "../stores/editor-store";
import { useFlowStore } from "../stores/flow-store";

let installed = false;

/** A window's "Return to designer": the dock reopens on that flow, switching to it when needed. */
function adoptReturnedNotebook(flowId: number): void {
  const editorStore = useEditorStore();
  editorStore.clearNotebookPoppedOut(flowId);
  useFlowStore().setFlowId(flowId);
  editorStore.openCodePane("notebook");
  if (router.currentRoute.value.name !== "designer") void router.push({ name: "designer" });
}

/** Once per window: follow closed and returned notebook windows, adopt the ones already open. */
function install(): void {
  if (installed) return;
  installed = true;
  void desktop.onNotebookWindowClosed((flowId) => useEditorStore().clearNotebookPoppedOut(flowId));
  void desktop.onNotebookWindowReturned(adoptReturnedNotebook);
  void desktop
    .listNotebookWindows()
    .then((flowIds) => flowIds.forEach((id) => useEditorStore().markNotebookPoppedOut(id)))
    .catch(() => undefined);
}

export function _resetForTests(): void {
  installed = false;
}

export function useNotebookPopout() {
  const editorStore = useEditorStore();
  install();

  async function popOut(flowId: number): Promise<boolean> {
    try {
      await desktop.openNotebookWindow(flowId, {
        url: notebookWindowUrl(flowId, window.location),
        name: notebookWindowName(flowId),
      });
    } catch (error) {
      console.error("Could not open the notebook window:", error);
      ElMessage.error({ message: "Could not open the notebook window.", showClose: true });
      return false;
    }
    editorStore.markNotebookPoppedOut(flowId);
    editorStore.setCodeGeneratorVisibility(false);
    return true;
  }

  async function focus(flowId: number): Promise<void> {
    await desktop.focusNotebookWindow(flowId).catch(() => undefined);
  }

  async function bringBack(flowId: number): Promise<void> {
    await desktop.closeNotebookWindow(flowId).catch(() => undefined);
    editorStore.clearNotebookPoppedOut(flowId);
    editorStore.openCodePane("notebook");
  }

  return {
    popOut,
    focus,
    bringBack,
    isPoppedOut: (flowId: number) => editorStore.isNotebookPoppedOut(flowId),
  };
}
