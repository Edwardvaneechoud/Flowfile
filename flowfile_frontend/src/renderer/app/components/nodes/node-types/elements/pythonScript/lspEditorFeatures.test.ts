import { describe, it, expect, vi } from "vitest";
import { EditorState, Text } from "@codemirror/state";
import { CompletionContext } from "@codemirror/autocomplete";
import type { Tooltip } from "@codemirror/view";

// The hover/signature modules reach the API layer, which boots axios + auth on import.
vi.mock("@/api/lsp.api", () => ({ LspApi: { capabilities: vi.fn(), hover: vi.fn() } }));

import { insideCall, lspDiagnosticToRange, notInAsBinding } from "./lspPositions";
import { signatureCoversHover } from "./lspHover";
import { setSigTooltip, sigTooltipField } from "./lspSignature";
import type { LspDiagnostic } from "@/api/lsp.api";

function stateFor(code: string): EditorState {
  return EditorState.create({ doc: code });
}

function ctxFor(code: string, pos: number): CompletionContext {
  return new CompletionContext(stateFor(code), pos, false);
}

describe("insideCall (signature-help trigger)", () => {
  it("is true with an unclosed call paren before the cursor", () => {
    const code = "pl.col(";
    expect(insideCall(stateFor(code), code.length)).toBe(true);
  });

  it("is true partway through arguments", () => {
    const code = "f(a, b";
    expect(insideCall(stateFor(code), code.length)).toBe(true);
  });

  it("is false once the call is closed", () => {
    const code = "f()";
    expect(insideCall(stateFor(code), code.length)).toBe(false);
  });

  it("is false with no call at all", () => {
    const code = "x = 1";
    expect(insideCall(stateFor(code), code.length)).toBe(false);
  });

  it("is true inside the outer call when a nested call is closed", () => {
    const code = "outer(inner(), ";
    expect(insideCall(stateFor(code), code.length)).toBe(true);
  });

  it("is false after balanced nested calls", () => {
    const code = "outer(inner())";
    expect(insideCall(stateFor(code), code.length)).toBe(false);
  });
});

describe("lspDiagnosticToRange (coordinate -> offset mapping)", () => {
  const base: LspDiagnostic = {
    line: 1,
    column: 0,
    end_line: 1,
    end_column: 0,
    message: "",
    severity: "error",
    source: "jedi",
  };

  it("maps a 1-based line / 0-based column range to absolute offsets", () => {
    const doc = Text.of(["import os", "x = 1"]);
    // second line "x = 1", columns 0..1 -> offsets at start of line 2
    const r = lspDiagnosticToRange(doc, {
      ...base,
      line: 2,
      column: 0,
      end_line: 2,
      end_column: 1,
    });
    expect(r.from).toBe(10); // "import os\n" = 10 chars
    expect(r.to).toBe(11);
  });

  it("widens a zero-width point to the identifier under it (pyflakes-style)", () => {
    const doc = Text.of(["import os"]);
    // point at column 7 (start of "os"), zero width -> widen to end of "os"
    const r = lspDiagnosticToRange(doc, {
      ...base,
      line: 1,
      column: 7,
      end_line: 1,
      end_column: 7,
    });
    expect(r.from).toBe(7);
    expect(r.to).toBe(9); // "os" spans 7..9
  });

  it("clamps an out-of-range line/column into the document", () => {
    const doc = Text.of(["ab"]);
    const r = lspDiagnosticToRange(doc, {
      ...base,
      line: 99,
      column: 99,
      end_line: 99,
      end_column: 99,
    });
    expect(r.from).toBe(2); // clamped to end of the only line
    expect(r.to).toBeGreaterThanOrEqual(r.from);
  });
});

describe("notInAsBinding (suppress completion in `as <name>` binding)", () => {
  const sentinel = { from: 0, options: [] };

  it("suppresses completion right after `import x as `", () => {
    const inner = vi.fn().mockReturnValue(sentinel);
    expect(notInAsBinding(inner)(ctxFor("import polars as p", 18))).toBeNull();
    expect(inner).not.toHaveBeenCalled();
  });

  it("suppresses with no partial typed yet (`as ` then cursor)", () => {
    const inner = vi.fn().mockReturnValue(sentinel);
    expect(notInAsBinding(inner)(ctxFor("import polars as ", 17))).toBeNull();
  });

  it("delegates normally outside a binding (`import pol`)", () => {
    const inner = vi.fn().mockReturnValue(sentinel);
    expect(notInAsBinding(inner)(ctxFor("import pol", 10))).toBe(sentinel);
    expect(inner).toHaveBeenCalledOnce();
  });

  it("does not false-positive on identifiers containing 'as' (cast, as_of)", () => {
    const inner = vi.fn().mockReturnValue(sentinel);
    expect(notInAsBinding(inner)(ctxFor("pl.cast", 7))).toBe(sentinel);
    expect(notInAsBinding(inner)(ctxFor("as_of", 5))).toBe(sentinel);
  });
});

describe("signatureCoversHover (one hint, not two)", () => {
  const DOC = 'import polars as pl\ncat.get_schema("default")';
  const CALLEE = DOC.indexOf("get_schema");

  function stateWithSignatureAt(pos: number | null): EditorState {
    const base = EditorState.create({ doc: DOC, extensions: [sigTooltipField] });
    if (pos === null) return base;
    const tooltip = { pos, above: true, create: () => ({ dom: null }) } as unknown as Tooltip;
    return base.update({ effects: setSigTooltip.of(tooltip) }).state;
  }

  it("suppresses the hover on the line the signature tooltip is anchored to", () => {
    const state = stateWithSignatureAt(DOC.indexOf('"default"'));
    expect(signatureCoversHover(state, CALLEE)).toBe(true);
  });

  it("leaves hovers on other lines alone", () => {
    const state = stateWithSignatureAt(DOC.indexOf('"default"'));
    expect(signatureCoversHover(state, 7)).toBe(false); // "polars" on line 1
  });

  it("does nothing when no signature tooltip is showing", () => {
    expect(signatureCoversHover(stateWithSignatureAt(null), CALLEE)).toBe(false);
  });
});
