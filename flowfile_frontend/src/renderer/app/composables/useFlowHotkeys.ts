// Canvas-level keyboard shortcuts (one window listener, owned by Canvas.vue).
//
// Cmd/Ctrl+C and Cmd/Ctrl+V are deliberately NOT here: on macOS desktop the
// Tauri Edit-menu accelerators consume those keystrokes before any keydown
// handler sees them, but they still emit DOM ClipboardEvents — so copy/paste
// live on document-level `copy`/`paste` listeners (see useFlowClipboard).
//
// Guarding is a per-shortcut policy, not a blanket rule: canvas-scoped keys
// (select-all, open file, AI drawer) must not hijack typing in an input or
// CodeMirror, while app-global actions (new/save/run/codegen/flow settings)
// stay live even while editing — Cmd+S in the Formula editor must save the
// flow, not fall through to the browser's save-page dialog.
import { onMounted, onUnmounted } from "vue";

export interface FlowHotkeyActions {
  flowId: () => number;
  selectAll: () => void;
  newFlow: () => void;
  save: () => void;
  run: () => void;
  toggleCodeGenerator: () => void;
  openFlowSettings: () => void;
  openFile: () => void;
  toggleAiDrawer: () => void;
}

export const isEditableKeydownTarget = (target: EventTarget | null): boolean => {
  if (!(target instanceof Element)) return false;
  const el = target as HTMLElement;
  if (el.tagName === "INPUT" || el.tagName === "TEXTAREA" || el.isContentEditable) return true;
  return typeof el.closest === "function" && !!el.closest(".cm-editor");
};

// Focus inside any of these owns Cmd/Ctrl+Z itself (text/list controls and overlays).
const HISTORY_SHORTCUT_EXCLUDED = [
  "select",
  "[role='listbox']",
  "[role='combobox']",
  "[role='textbox']",
  ".cm-editor",
  ".el-overlay",
  ".el-dialog",
  ".el-message-box",
  ".el-popper",
  ".context-menu",
  "[role='dialog']",
  "[aria-modal='true']",
].join(",");

/**
 * The one rule for the canvas undo/redo shortcut: Cmd/Ctrl+Z undoes, Shift+Cmd/Ctrl+Z
 * and Cmd/Ctrl+Y redo, unless the event was already handled or focus is in an editable
 * control or an overlay. The settings drawer is deliberately not excluded: Ctrl+Z on a
 * non-editable spot there saves the drawer and then undoes.
 */
export const historyShortcutFor = (
  event: KeyboardEvent,
  isMac: boolean,
): "undo" | "redo" | null => {
  if (event.defaultPrevented || event.altKey) return null;
  if (!(isMac ? event.metaKey : event.ctrlKey)) return null;
  const key = event.key.toLowerCase();
  const action = key === "z" ? (event.shiftKey ? "redo" : "undo") : key === "y" ? "redo" : null;
  if (!action) return null;
  // Inside a shadow root (GraphicWalker) event.target is the host; the path holds the real target.
  const target = event.composedPath?.()[0] ?? event.target;
  if (isEditableKeydownTarget(target)) return null;
  if (target instanceof Element && target.closest(HISTORY_SHORTCUT_EXCLUDED)) return null;
  return action;
};

export const createFlowHotkeysHandler = (actions: FlowHotkeyActions) => {
  return (event: KeyboardEvent): void => {
    if (!(event.ctrlKey || event.metaKey)) return;
    // Normalize to lowercase to handle Caps Lock being on.
    const key = event.key.toLowerCase();
    const inEditable = isEditableKeydownTarget(event.target);

    switch (key) {
      case "a":
        // Select all nodes (prevent the browser from selecting all page text).
        if (inEditable) return;
        event.preventDefault();
        actions.selectAll();
        return;
      case "n":
        event.preventDefault();
        actions.newFlow();
        return;
      case "s":
        if (actions.flowId() > 0) {
          event.preventDefault();
          actions.save();
        }
        return;
      case "e":
        if (actions.flowId() > 0) {
          event.preventDefault();
          actions.run();
        }
        return;
      case "g":
        if (actions.flowId() > 0) {
          event.preventDefault();
          actions.toggleCodeGenerator();
        }
        return;
      case ",":
        if (actions.flowId() > 0) {
          event.preventDefault();
          actions.openFlowSettings();
        }
        return;
      case "o":
        // Guarded so typing "o" with a stuck modifier doesn't open the dialog.
        if (inEditable) return;
        event.preventDefault();
        actions.openFile();
        return;
      case "k":
        // Toggles the AI assistant drawer (rewired from the command palette).
        if (inEditable) return;
        if (actions.flowId() > 0) {
          event.preventDefault();
          actions.toggleAiDrawer();
        }
        return;
    }
  };
};

export const useFlowHotkeys = (actions: FlowHotkeyActions) => {
  const handler = createFlowHotkeysHandler(actions);
  onMounted(() => window.addEventListener("keydown", handler));
  onUnmounted(() => window.removeEventListener("keydown", handler));
  return { handler };
};
