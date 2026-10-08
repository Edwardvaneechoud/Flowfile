import { describe, it, expect, vi } from "vitest";
import { EditorState, Text } from "@codemirror/state";
import { CompletionContext } from "@codemirror/autocomplete";
import { python } from "@codemirror/lang-python";
import type { Tooltip } from "@codemirror/view";

// The hover/signature modules reach the API layer, which boots axios + auth on import.
vi.mock("@/api/lsp.api", () => ({ LspApi: { capabilities: vi.fn(), hover: vi.fn() } }));

import {
  callArgsEmpty,
  currentArgText,
  insideCall,
  insideString,
  lspDiagnosticToRange,
  notInAsBinding,
} from "./lspPositions";
import { signatureCoversHover } from "./lspHover";
import { activeParamIndex, setSigTooltip, sigTooltipField, signatureApplies } from "./lspSignature";
import type { LspDiagnostic } from "@/api/lsp.api";

function stateFor(code: string): EditorState {
  return EditorState.create({ doc: code });
}

// `|` marks the caret; string positions need the real grammar, so these states carry python().
function pyStateAt(source: string): { state: EditorState; pos: number } {
  const pos = source.indexOf("|");
  if (pos < 0) throw new Error(`no caret marker in ${JSON.stringify(source)}`);
  const doc = source.slice(0, pos) + source.slice(pos + 1);
  return { state: EditorState.create({ doc, extensions: [python()] }), pos };
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

describe("insideString (caret in a string literal)", () => {
  function at(source: string): boolean {
    const { state, pos } = pyStateAt(source);
    return insideString(state, pos);
  }

  it("is true inside a double-quoted string", () => {
    expect(at('orders.rename({"a|"})')).toBe(true);
  });

  it("is true right after the opening quote", () => {
    expect(at('orders.rename({"|"})')).toBe(true);
  });

  it("is true inside a single-quoted string", () => {
    expect(at("orders.select('am|')")).toBe(true);
  });

  it("is true inside an f-string", () => {
    expect(at('print(f"am|ount")')).toBe(true);
  });

  it("is true right after an escape sequence, which resolves below the string", () => {
    expect(at('orders.select("a\\n|b")')).toBe(true);
  });

  it("is true anywhere under an f-string, replacement fields included", () => {
    expect(at('print(f"{orders.rename(|)}")')).toBe(true);
  });

  it("is true inside an unterminated string, including at its end", () => {
    expect(at('orders.rename({"|')).toBe(true);
    expect(at('orders.select("amount|')).toBe(true);
  });

  it("is false before the opening quote of a prefixed string", () => {
    expect(at('print(f|"amount")')).toBe(false);
    expect(at('print(r|"amount")')).toBe(false);
  });

  it("is false right after the closing quote, still inside the call", () => {
    expect(at('orders.select("amount"|)')).toBe(false);
    expect(at("orders.select('amount'|)")).toBe(false);
    expect(at('print(f"amount"|)')).toBe(false);
  });

  it("is false outside any string", () => {
    expect(at("orders.rename(|)")).toBe(false);
    expect(at("x = 1|")).toBe(false);
  });
});

describe("signatureApplies (signature help vs. string arguments)", () => {
  function at(source: string): boolean {
    const { state, pos } = pyStateAt(source);
    return signatureApplies(state, pos);
  }

  it("does not query while the caret sits in a double-quoted argument", () => {
    expect(at('orders.rename({"|"})')).toBe(false);
    expect(at('orders.select("amo|")')).toBe(false);
  });

  it("does not query in a single-quoted or f-string argument", () => {
    expect(at("orders.select('amo|')")).toBe(false);
    expect(at('print(f"amo|unt")')).toBe(false);
  });

  it("does not query in an unterminated string argument", () => {
    expect(at('orders.rename({"|')).toBe(false);
    expect(at('orders.select("amount|')).toBe(false);
  });

  it("still applies after the closing quote, inside the parens", () => {
    expect(at('orders.select("amount"|)')).toBe(true);
    expect(at('orders.select("amount", |)')).toBe(true);
  });

  it("still applies with no string in play", () => {
    expect(at("orders.rename(|)")).toBe(true);
  });

  it("does not apply outside a call", () => {
    expect(at("orders.rename()|")).toBe(false);
  });

  it("steps aside while a member access is typed as an argument", () => {
    expect(at("ff.read_api(df.|)")).toBe(false);
    expect(at("ff.read_api(df.sel|)")).toBe(false);
    expect(at("df.select(pl.col|)")).toBe(false);
    expect(at("f(a.b().c.|)")).toBe(false);
    expect(at("f(x[0].|)")).toBe(false);
  });

  it("comes back between arguments and after a numeric literal", () => {
    expect(at("ff.read_api(df.x, |)")).toBe(true);
    expect(at('df.select(pl.col("a")|)')).toBe(true);
    expect(at("round(1.|)")).toBe(true);
  });
});

describe("sigTooltipField (drops on the keystroke that leaves the arguments)", () => {
  const tooltip = { pos: 0, above: true, create: () => ({ dom: null }) } as unknown as Tooltip;

  function shownAt(source: string): EditorState {
    const { state, pos } = pyStateAt(source);
    const withField = EditorState.create({
      doc: state.doc,
      selection: { anchor: pos },
      extensions: [python(), sigTooltipField],
    });
    return withField.update({ effects: setSigTooltip.of({ ...tooltip, pos }) }).state;
  }

  it("clears when the user types a member-access dot", () => {
    const state = shownAt("ff.read_api(df|)");
    const pos = state.selection.main.head;
    const next = state.update({
      changes: { from: pos, insert: "." },
      selection: { anchor: pos + 1 },
    });
    expect(next.state.field(sigTooltipField)).toBeNull();
  });

  it("stays while typing an ordinary argument", () => {
    const state = shownAt("ff.read_api(|)");
    const next = state.update({ changes: { from: 12, insert: "x" }, selection: { anchor: 13 } });
    expect(next.state.field(sigTooltipField)).not.toBeNull();
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

describe("callArgsEmpty (full docs only right after the open paren)", () => {
  function at(source: string): boolean {
    const { state, pos } = pyStateAt(source);
    return callArgsEmpty(state, pos);
  }

  it("is true right after the paren, whitespace allowed", () => {
    expect(at("ff.read_database(|)")).toBe(true);
    expect(at("ff.read_database(\n    |)")).toBe(true);
  });

  it("is false once an argument is typed", () => {
    expect(at("ff.read_database(c|)")).toBe(false);
    expect(at("ff.read_database(connection_name=|)")).toBe(false);
    expect(at('ff.read_database("db", |)')).toBe(false);
  });

  it("follows the innermost call and is false outside one", () => {
    expect(at("f(g(|))")).toBe(true);
    expect(at("f(g(x), |)")).toBe(false);
    expect(at("f()|")).toBe(false);
  });
});

describe("currentArgText (the argument being typed)", () => {
  function at(source: string): string | null {
    const { state, pos } = pyStateAt(source);
    return currentArgText(state, pos);
  }

  it("is the text after the open paren or the last top-level comma", () => {
    expect(at("ff.read_database(table_n|)")).toBe("table_n");
    expect(at('ff.read_database("db", ta|)')).toBe(" ta");
    expect(at("f(a, b=|)")).toBe(" b=");
  });

  it("skips commas nested in brackets and strings", () => {
    expect(at("f(g(1, 2) + x|)")).toBe("g(1, 2) + x");
    expect(at('f("a,b" + c|)')).toBe('"a,b" + c');
    expect(at("f([1, 2], {3: 4, 5: 6}|)")).toBe(" {3: 4, 5: 6}");
  });

  it("is null outside a call", () => {
    expect(at("x = 1|")).toBeNull();
  });
});

describe("activeParamIndex (which parameter the typed argument targets)", () => {
  const sig = {
    label: "read_database(connection_name: 'str', *, table_name: 'str | None'=None, ...)",
    parameters: [
      "connection_name: 'str'",
      "table_name: 'str | None'=None",
      "schema_name: 'str | None'=None",
      "*args",
      "**kwargs",
    ],
    active_parameter: 0,
    documentation: "",
  };

  it("follows a partial keyword name before the `=` is typed", () => {
    expect(activeParamIndex(sig, "table_n")).toBe(1);
    expect(activeParamIndex(sig, " sch")).toBe(2);
  });

  it("follows a completed `name=`", () => {
    expect(activeParamIndex(sig, "schema_name=")).toBe(2);
    expect(activeParamIndex(sig, " table_name = 'x")).toBe(1);
  });

  it("keeps Jedi's index for positional values and unknown names", () => {
    expect(activeParamIndex(sig, '"db"')).toBe(0);
    expect(activeParamIndex(sig, "my_conn")).toBe(0);
    expect(activeParamIndex(sig, "x == 1")).toBe(0);
    expect(activeParamIndex({ ...sig, active_parameter: 1 }, "")).toBe(1);
    expect(activeParamIndex(sig, null)).toBe(0);
  });

  it("never matches the star parameters by name", () => {
    expect(activeParamIndex(sig, "args")).toBe(0);
    expect(activeParamIndex(sig, "kwargs=")).toBe(0);
  });
});
