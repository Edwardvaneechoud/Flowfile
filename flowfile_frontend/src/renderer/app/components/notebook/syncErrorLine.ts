// Marks the line a refused canvas notebook sync points at; the cell output carries the message.
import { StateEffect, StateField, type EditorState } from "@codemirror/state";
import { Decoration, EditorView, type DecorationSet } from "@codemirror/view";

export interface SyncErrorMark {
  /** 1-based within the cell; null when the refusal names no line. */
  line: number | null;
  message: string;
}

export const setSyncErrorMark = StateEffect.define<SyncErrorMark | null>();

function decorate(state: EditorState, mark: SyncErrorMark | null): DecorationSet {
  if (mark?.line == null || mark.line < 1) return Decoration.none;
  const line = state.doc.line(Math.min(mark.line, state.doc.lines));
  return Decoration.set(
    Decoration.line({
      class: "nb-sync-error-line",
      attributes: { title: mark.message },
    }).range(line.from),
  );
}

/** Holds the marked line; set or clear it with `setSyncErrorMark`. */
export const syncErrorLineField = StateField.define<DecorationSet>({
  create: () => Decoration.none,
  update(marks, tr) {
    for (const effect of tr.effects) {
      if (effect.is(setSyncErrorMark)) return decorate(tr.state, effect.value);
    }
    return marks.map(tr.changes);
  },
  provide: (field) => EditorView.decorations.from(field),
});
