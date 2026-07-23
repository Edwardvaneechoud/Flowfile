import { describe, it, expect } from "vitest";
import { EditorState } from "@codemirror/state";
import { CompletionContext, type CompletionResult } from "@codemirror/autocomplete";

import { createScopeCompletions } from "./flowfileCompletions";

function ctxFor(code: string, pos: number, explicit = false): CompletionContext {
  return new CompletionContext(EditorState.create({ doc: code }), pos, explicit);
}

describe("createScopeCompletions", () => {
  it("offers prior-cell symbols on a bare identifier prefix", () => {
    const source = createScopeCompletions(() => ["foo = 1\ndef bar():\n    pass"]);
    const result = source(ctxFor("fo", 2)) as CompletionResult;
    expect(result.options.map((o) => o.label).sort()).toEqual(["bar", "foo"]);
  });

  it("returns null in attribute position (identifier after a dot)", () => {
    const source = createScopeCompletions(() => ["foo = 1"]);
    expect(source(ctxFor("df.fo", 5))).toBeNull();
    expect(source(ctxFor("df.foo", 6, true))).toBeNull();
  });

  it("returns null with no prior cells", () => {
    const source = createScopeCompletions(() => []);
    expect(source(ctxFor("fo", 2))).toBeNull();
  });
});
