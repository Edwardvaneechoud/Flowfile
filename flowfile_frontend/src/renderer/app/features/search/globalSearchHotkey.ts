/** A key event that belongs to an input method composing text (IME), never a shortcut. */
export function isComposingKey(event: KeyboardEvent): boolean {
  return event.isComposing || event.keyCode === 229;
}

// Cmd/Ctrl+K is the AI assistant on the canvas, so search gets Shift+Cmd/Ctrl+F ("find everywhere").
export function isGlobalSearchShortcut(event: KeyboardEvent, isMac: boolean): boolean {
  if (event.defaultPrevented || isComposingKey(event) || event.altKey || !event.shiftKey) {
    return false;
  }
  if (isMac ? !event.metaKey || event.ctrlKey : !event.ctrlKey || event.metaKey) return false;
  return event.key.toLowerCase() === "f" || event.code === "KeyF";
}

export function globalSearchShortcutLabel(isMac: boolean): string {
  return isMac ? "⇧⌘F" : "Ctrl+Shift+F";
}

// Element Plus overlays, the app's own `.modal-overlay` modals, and anything marked modal.
const OTHER_DIALOGS = '.el-overlay, .modal-overlay, [aria-modal="true"]';

/**
 * Whether a dialog other than the search palette is showing, in which case
 * the shortcut must not open the palette over it. Element Plus keeps a closed dialog's overlay in the DOM hidden, so
 * only a rendered one counts.
 */
export function anotherDialogIsOpen(root: ParentNode, palette: Element | null): boolean {
  for (const element of Array.from(root.querySelectorAll(OTHER_DIALOGS))) {
    if (palette?.contains(element)) continue;
    if (element instanceof HTMLElement && element.getClientRects().length > 0) return true;
  }
  return false;
}

/** Whether ``node`` sits in a dialog other than the search palette (``palette``), e.g. one that just opened. */
export function inAnotherDialog(node: unknown, palette: Element | null): boolean {
  if (!(node instanceof Element)) return false;
  const dialog = node.closest(OTHER_DIALOGS);
  return dialog !== null && !palette?.contains(dialog);
}
