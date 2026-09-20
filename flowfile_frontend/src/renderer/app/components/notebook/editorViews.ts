// Live CodeMirror views keyed by owner id + cell id, so an insert into a visible cell can go
// through the editor (correct cursor + focus) instead of replacing the whole doc.
import type { EditorView } from "@codemirror/view";

// A stack per cell: the node editor mounts inline AND in a fullscreen dialog over the same
// cells, so the last-registered view wins and unmounting it reveals the other one.
const views = new Map<string, Map<string, EditorView[]>>();

export function ownerIdForNotebook(tabId: string): string {
  return tabId;
}

export function ownerIdForNode(flowId: number, nodeId: number): string {
  return `node:${flowId}:${nodeId}`;
}

export function registerCellView(ownerId: string, cellId: string, view: EditorView): void {
  let owner = views.get(ownerId);
  if (!owner) {
    owner = new Map();
    views.set(ownerId, owner);
  }
  const stack = owner.get(cellId);
  if (stack) stack.push(view);
  else owner.set(cellId, [view]);
}

export function unregisterCellView(ownerId: string, cellId: string, view: EditorView): void {
  const owner = views.get(ownerId);
  const stack = owner?.get(cellId);
  if (!owner || !stack) return;
  const idx = stack.lastIndexOf(view);
  if (idx < 0) return;
  stack.splice(idx, 1);
  if (!stack.length) owner.delete(cellId);
  if (!owner.size) views.delete(ownerId);
}

export function getCellView(ownerId: string, cellId: string): EditorView | undefined {
  const stack = views.get(ownerId)?.get(cellId);
  return stack?.length ? stack[stack.length - 1] : undefined;
}

function escapeId(cellId: string): string {
  const cssApi = globalThis.CSS;
  if (cssApi && typeof cssApi.escape === "function") return cssApi.escape(cellId);
  return cellId.replace(/["\\]/g, "\\$&");
}

/** The `[data-cell-id="..."]` selector for a cell root, safe for querySelector. */
export function cellSelector(cellId: string): string {
  return `[data-cell-id="${escapeId(cellId)}"]`;
}

/** Focus a cell: its editor if visible, else the markdown textarea, else the cell root. */
export function focusCell(ownerId: string, cellId: string, host?: HTMLElement | null): boolean {
  const view = getCellView(ownerId, cellId);
  // A collapsed (display:none) editor swallows focus() silently, so fall through to the root.
  if (view && view.dom.offsetParent !== null) {
    view.focus();
    view.dispatch({ selection: { anchor: view.state.doc.length } });
    return true;
  }
  if (!host) return false;
  const selector = cellSelector(cellId);
  const textarea = host.querySelector(`${selector} textarea`);
  if (textarea) {
    (textarea as HTMLTextAreaElement).focus();
    return true;
  }
  const root = host.querySelector(selector);
  if (root) {
    (root as HTMLElement).focus();
    return true;
  }
  return false;
}

export function disposeOwnerViews(ownerId: string): void {
  views.delete(ownerId);
}
