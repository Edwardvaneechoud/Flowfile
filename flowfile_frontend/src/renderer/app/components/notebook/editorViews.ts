// Live CodeMirror views keyed by cell id, so an insert into a visible cell can go
// through the editor (correct cursor + focus) instead of replacing the whole doc.
import type { EditorView } from "@codemirror/view";

const views = new Map<string, EditorView>();

export function registerCellView(cellId: string, view: EditorView) {
  views.set(cellId, view);
}

export function unregisterCellView(cellId: string) {
  views.delete(cellId);
}

export function getCellView(cellId: string): EditorView | undefined {
  return views.get(cellId);
}
