import { describe, it, expect } from "vitest";
import { EditorState } from "@codemirror/state";
import { CompletionContext } from "@codemirror/autocomplete";
import { selfParamSource, insideDefParams } from "./usePolarsAutocompletion";

// Build a CompletionContext with the cursor at the (single) `|` marker in `doc`.
function ctxAt(doc: string, explicit = false): CompletionContext {
  const pos = doc.indexOf("|");
  // Excise exactly the marker at `pos` (not String.replace, which drops only the
  // first `|` and would leave a stray marker if a doc ever had two).
  const state = EditorState.create({ doc: doc.slice(0, pos) + doc.slice(pos + 1) });
  return new CompletionContext(state, pos, explicit);
}

function labels(doc: string, explicit = false): string[] | null {
  const result = selfParamSource(ctxAt(doc, explicit));
  return result ? result.options.map((o) => o.label) : null;
}

describe("selfParamSource", () => {
  it("offers self on an explicit trigger inside empty def parens", () => {
    expect(labels("    def my_function(|", true)).toEqual(["self"]);
  });

  it("does not auto-open in empty parens without a typed char", () => {
    expect(labels("    def my_function(|", false)).toBeNull();
  });

  it("offers self while typing a prefix of it", () => {
    expect(labels("    def my_function(s|")).toEqual(["self"]);
    expect(labels("    def my_function(sel|")).toEqual(["self"]);
  });

  it("does not offer self for a non-prefix word", () => {
    expect(labels("    def my_function(row|")).toBeNull();
  });

  it("only offers self as the FIRST parameter", () => {
    expect(labels("    def my_function(a, s|")).toBeNull();
    expect(labels("    def my_function(a, |", true)).toBeNull();
  });

  it("works across a multi-line parameter list", () => {
    expect(labels("    def f(\n        s|")).toEqual(["self"]);
  });

  it("does not fire in the method body or after the params close", () => {
    expect(labels("    x = 1\n    lf.s|")).toBeNull();
    expect(labels("    def process(self, *inputs):\n    lf.|")).toBeNull();
  });
});

describe("insideDefParams", () => {
  it("detects the param list and captures text before the cursor", () => {
    expect(insideDefParams(ctxAt("    def f(a, b|"))).toEqual({ before: "a, b" });
    expect(insideDefParams(ctxAt("    lf.filter(|"))).toBeNull(); // a call, not a def
    expect(insideDefParams(ctxAt("    def f(a):\n    x|"))).toBeNull(); // params closed
  });
});
