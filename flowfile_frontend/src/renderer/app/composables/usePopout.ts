/**
 * Pop-out windows for the designer's panels: a flow's notebook, data preview, logs or AI assistant
 * in its own window (`lib/desktop.ts`: the shell's or the browser's). Popping out is a move: the
 * designer shows a stub or hides the tab for that flow until the window comes back, so one window
 * hosts a flow's panel at a time. Each kind says here what the designer does around the move.
 */
import { ElMessage } from "element-plus";
import { desktop, type PopoutRef } from "../../lib/desktop";
import router from "../router";
import {
  POPOUT_TITLES,
  popoutWindowName,
  popoutWindowUrl,
  type PopoutKind,
} from "../../lib/popoutWindow";
import { useEditorStore } from "../stores/editor-store";
import { useFlowStore } from "../stores/flow-store";

interface PopoutKindDef {
  /** The designer's side of the move, once the window opened. */
  afterPopOut?: (flowId: number) => void;
  /** Reopen the panel in the designer after its window was closed on request. */
  bringBack: (flowId: number) => void;
  /** A window's "Return to designer": reopen the panel on that flow, switching to it when needed. */
  adoptReturn: (flowId: number) => void;
}

// `null` marks a kind that has no window yet; the record keeps the table exhaustive.
const KINDS: Record<PopoutKind, PopoutKindDef | null> = {
  notebook: {
    afterPopOut: () => useEditorStore().setCodeGeneratorVisibility(false),
    bringBack: () => useEditorStore().openCodePane("notebook"),
    adoptReturn: (flowId) => {
      useFlowStore().setFlowId(flowId);
      useEditorStore().openCodePane("notebook");
      if (router.currentRoute.value.name !== "designer") void router.push({ name: "designer" });
    },
  },
  table: null,
  logs: null,
  ai: null,
};

let installed = false;

function onClosed({ kind, flowId }: PopoutRef): void {
  useEditorStore().clearPoppedOut(kind, flowId);
}

function onReturned({ kind, flowId }: PopoutRef): void {
  useEditorStore().clearPoppedOut(kind, flowId);
  KINDS[kind]?.adoptReturn(flowId);
}

/** Once per window, from `AppLayout`: follow closed and returned pop-outs, adopt the ones already open. */
export function installPopoutListeners(): void {
  if (installed) return;
  installed = true;
  void desktop.onPopoutWindowClosed(onClosed);
  void desktop.onPopoutWindowReturned(onReturned);
  void desktop
    .listPopoutWindows()
    .then((open) => {
      for (const { kind, flowId } of open) useEditorStore().markPoppedOut(kind, flowId);
    })
    .catch(() => undefined);
}

export function _resetForTests(): void {
  installed = false;
}

export function usePopout(kind: PopoutKind) {
  const editorStore = useEditorStore();
  const def = KINDS[kind];
  const title = POPOUT_TITLES[kind];

  async function popOut(flowId: number): Promise<boolean> {
    if (!def) {
      console.error(`No ${title} window exists yet`);
      return false;
    }
    try {
      await desktop.openPopoutWindow(kind, flowId, {
        url: popoutWindowUrl(kind, flowId, window.location),
        name: popoutWindowName(kind, flowId),
      });
    } catch (error) {
      console.error(`Could not open the ${title} window:`, error);
      ElMessage.error({ message: `Could not open the ${title} window.`, showClose: true });
      return false;
    }
    editorStore.markPoppedOut(kind, flowId);
    def.afterPopOut?.(flowId);
    return true;
  }

  async function focus(flowId: number): Promise<void> {
    await desktop.focusPopoutWindow(kind, flowId).catch(() => undefined);
  }

  async function bringBack(flowId: number): Promise<void> {
    await desktop.closePopoutWindow(kind, flowId).catch(() => undefined);
    editorStore.clearPoppedOut(kind, flowId);
    def?.bringBack(flowId);
  }

  return {
    popOut,
    focus,
    bringBack,
    isPoppedOut: (flowId: number) => editorStore.isPoppedOut(kind, flowId),
  };
}
