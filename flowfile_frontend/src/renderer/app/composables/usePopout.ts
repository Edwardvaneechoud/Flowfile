/**
 * Pop-out windows for the designer's panels: a flow's notebook, data preview, logs or AI assistant
 * in its own window (`lib/desktop.ts`: the shell's or the browser's). Popping out is a move: the
 * designer shows a stub or hides the tab for that flow until the window comes back, so one window
 * hosts a flow's panel at a time. Each kind says here what the designer does around the move.
 */
import { ElMessage } from "element-plus";
import { desktop, type PopoutMove, type PopoutRef } from "../../lib/desktop";
import router from "../router";
import {
  POPOUT_TITLES,
  popoutWindowHash,
  popoutWindowName,
  type PopoutKind,
  type SelectionMessage,
} from "../../lib/popoutWindow";
import { useDrawerStore, type WindowPreview } from "../stores/drawer-store";
import { useEditorStore } from "../stores/editor-store";
import { useFlowStore } from "../stores/flow-store";

interface PopoutKindDef {
  /** Extra query for the window's route (the Data window's `node`). */
  windowQuery?: (flowId: number) => Record<string, string | number>;
  /** The designer's side of the move, once the window opened. */
  afterPopOut?: (flowId: number) => void;
  /** Reopen the panel in the designer after its window was closed on request. */
  bringBack: (flowId: number) => void;
  /** A window's "Return to designer": reopen the panel on that flow, switching to it when needed. */
  adoptReturn: (flowId: number) => void;
  /** The window went away without a Return or a "Bring back": the panel stays closed. */
  onClosed?: (flowId: number) => void;
  /** A tracked window's flow moved to a new id (a Save As). */
  onRekey?: (from: number, to: number) => void;
  /** The window is told the node the designer sent it (`selectionFor`). */
  followsSelection?: boolean;
}

/** The designer shows that flow, on its own page. */
function showFlow(flowId: number): void {
  useFlowStore().setFlowId(flowId);
  if (router.currentRoute.value.name !== "designer") void router.push({ name: "designer" });
}

/** What the flow's Data window shows, as the designer last sent it. */
function windowPreview(flowId: number): WindowPreview | undefined {
  return useDrawerStore().popoutPreview[flowId];
}

/** The node the flow's Data window showed, taken back: what a Return reopens the dock on. */
function takeWindowNode(flowId: number): number | undefined {
  const nodeId = windowPreview(flowId)?.nodeId;
  useDrawerStore().forgetPopoutPreview(flowId);
  return nodeId;
}

// `null` marks a kind that has no window yet; the record keeps the table exhaustive.
const KINDS: Record<PopoutKind, PopoutKindDef | null> = {
  notebook: {
    afterPopOut: () => useEditorStore().setCodeGeneratorVisibility(false),
    bringBack: () => useEditorStore().openCodePane("notebook"),
    adoptReturn: (flowId) => {
      showFlow(flowId);
      useEditorStore().openCodePane("notebook");
    },
  },
  table: {
    followsSelection: true,
    windowQuery: (): Record<string, number> => {
      const nodeId = useDrawerStore().previewNodeId;
      return nodeId === null ? {} : { node: nodeId };
    },
    afterPopOut: (flowId) => useDrawerStore().divertPreviewToWindow(flowId),
    bringBack: (flowId) =>
      useDrawerStore().requestDock({ flowId, tab: "data", nodeId: takeWindowNode(flowId) }),
    adoptReturn: (flowId) => {
      const nodeId = takeWindowNode(flowId);
      showFlow(flowId);
      useDrawerStore().requestDock({ flowId, tab: "data", nodeId });
    },
    onClosed: (flowId) => useDrawerStore().forgetPopoutPreview(flowId),
    onRekey: (from, to) => useDrawerStore().movePopoutPreview(from, to),
  },
  logs: {
    afterPopOut: () => useEditorStore().hideLogViewer(),
    bringBack: (flowId) => useDrawerStore().requestDock({ flowId, tab: "logs" }),
    adoptReturn: (flowId) => {
      showFlow(flowId);
      useDrawerStore().requestDock({ flowId, tab: "logs" });
    },
    // A run while the window was out set the viewer flag; the dock must not spring open on a close.
    onClosed: () => useEditorStore().hideLogViewer(),
  },
  ai: null,
};

let installed = false;

/**
 * A window went away. After a Return or a "Bring back" the mark is already clear and the dock
 * request filed, so the shell's later close report changes nothing.
 */
function onClosed({ kind, flowId }: PopoutRef): void {
  const editorStore = useEditorStore();
  if (!editorStore.isPoppedOut(kind, flowId)) return;
  editorStore.clearPoppedOut(kind, flowId);
  KINDS[kind]?.onClosed?.(flowId);
}

function onReturned({ kind, flowId }: PopoutRef): void {
  useEditorStore().clearPoppedOut(kind, flowId);
  KINDS[kind]?.adoptReturn(flowId);
}

/** A tracked window's flow moved (a Save As): the mark follows; an untracked window stays untracked. */
function moveMark({ kind, from, to }: PopoutMove): void {
  const editorStore = useEditorStore();
  if (!editorStore.isPoppedOut(kind, from)) return;
  editorStore.clearPoppedOut(kind, from);
  editorStore.markPoppedOut(kind, to);
  KINDS[kind]?.onRekey?.(from, to);
}

/** What a window following the canvas is told: the node the designer sent it, and how often. */
export function selectionFor(flowId: number): SelectionMessage {
  const preview = windowPreview(flowId);
  return {
    type: "selection",
    previewNodeId: preview?.nodeId ?? null,
    previewToken: preview?.token ?? 0,
  };
}

function sendSelection({ kind, flowId }: PopoutRef): void {
  void desktop
    .postToPopoutWindow(kind, flowId, selectionFor(flowId))
    .catch((error) => console.warn(`[popout] selection not delivered to the ${kind} window:`, error));
}

/** Tell every popped-out window of the flow that follows the canvas; `Canvas` calls this on a change. */
export function broadcastSelection(flowId: number): void {
  if (flowId <= 0) return;
  const editorStore = useEditorStore();
  for (const kind of Object.keys(KINDS) as PopoutKind[]) {
    if (KINDS[kind]?.followsSelection && editorStore.isPoppedOut(kind, flowId)) {
      sendSelection({ kind, flowId });
    }
  }
}

// A window listens now (it just opened or reloaded): a following kind gets the current selection.
function onReady(ref: PopoutRef): void {
  if (KINDS[ref.kind]?.followsSelection) sendSelection(ref);
}

/**
 * Once per window, from `AppLayout`: follow closed, returned, rekeyed and ready pop-outs, adopt the
 * ones already open. The marks follow only what the windows report (the shell's registry, the
 * opener's handle map), never this window's own feed, so they cannot run ahead of a pop-out that
 * has not followed a Save As yet.
 */
export function installPopoutListeners(): void {
  if (installed) return;
  installed = true;
  void desktop.onPopoutWindowClosed(onClosed);
  void desktop.onPopoutWindowReturned(onReturned);
  void desktop.onPopoutWindowRekeyed(moveMark);
  void desktop.onPopoutWindowReady(onReady);
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
        hash: popoutWindowHash(kind, flowId, def.windowQuery?.(flowId) ?? {}),
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
