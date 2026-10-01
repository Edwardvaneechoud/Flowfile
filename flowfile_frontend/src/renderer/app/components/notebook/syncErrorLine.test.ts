import { describe, expect, it } from "vitest";
import { EditorState } from "@codemirror/state";
import { setSyncErrorMark, syncErrorLineField, type SyncErrorMark } from "./syncErrorLine";

const CODE = "import flowfile as ff\nx = ff.col('a')\nprint(x)";

function marked(state: EditorState): { line: number; title: string }[] {
  const out: { line: number; title: string }[] = [];
  state.field(syncErrorLineField).between(0, state.doc.length, (from, _to, deco) => {
    out.push({ line: state.doc.lineAt(from).number, title: deco.spec.attributes?.title });
  });
  return out;
}

function withMark(mark: SyncErrorMark | null, code = CODE): EditorState {
  const state = EditorState.create({ doc: code, extensions: [syncErrorLineField] });
  return state.update({ effects: setSyncErrorMark.of(mark) }).state;
}

describe("sync error line", () => {
  it("marks nothing until a refusal arrives", () => {
    const state = EditorState.create({ doc: CODE, extensions: [syncErrorLineField] });
    expect(marked(state)).toEqual([]);
  });

  it("marks the refused line and carries the message as its title", () => {
    const message = "print is not part of the notebook's flow code; this needs a kernel";
    expect(marked(withMark({ line: 3, message }))).toEqual([{ line: 3, title: message }]);
  });

  it("marks no line when the refusal names none", () => {
    expect(marked(withMark({ line: null, message: "LazyFrame nodes are refused" }))).toEqual([]);
    expect(marked(withMark({ line: 0, message: "bad" }))).toEqual([]);
  });

  it("keeps a line past the end on the last line", () => {
    expect(marked(withMark({ line: 9, message: "m" }))).toEqual([{ line: 3, title: "m" }]);
  });

  it("clears the mark", () => {
    const state = withMark({ line: 2, message: "m" });
    const cleared = state.update({ effects: setSyncErrorMark.of(null) }).state;
    expect(marked(cleared)).toEqual([]);
  });

  it("follows its line through an edit above it", () => {
    const state = withMark({ line: 3, message: "m" });
    const edited = state.update({ changes: { from: 0, insert: "# note\n" } }).state;
    expect(marked(edited)).toEqual([{ line: 4, title: "m" }]);
  });
});
