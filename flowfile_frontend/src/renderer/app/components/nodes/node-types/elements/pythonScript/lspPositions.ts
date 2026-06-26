// Pure position/range helpers for the LSP editor features, kept free of the API layer so
// they unit-test in isolation (mirrors the kernel's dependency-free lsp/context.py). Only
// @codemirror/state types + an LSP type (elided at runtime) are imported here.
import type { CompletionContext, CompletionSource } from "@codemirror/autocomplete";
import type { EditorState, Text } from "@codemirror/state";
import type { LspDiagnostic } from "@/api/lsp.api";

const IDENT = /[A-Za-z0-9_]/;
const CALL_SCAN_WINDOW = 2000;

// `import x as p`, `with ... as f`, `except E as e` — the name after `as` is a binding the
// user is defining, not a reference, so identifier completions there are noise. The `\b`
// keeps `cast` / `as_of` (no trailing space) from matching.
export const AS_BINDING = /\bas\s+\w*$/;

export function notInAsBinding(source: CompletionSource): CompletionSource {
  return (context: CompletionContext) => (context.matchBefore(AS_BINDING) ? null : source(context));
}

// Cheap heuristic: is the cursor inside an unclosed "(" (i.e. typing call arguments)? Scans
// back a bounded window tracking paren depth so signature help only fires when plausible.
export function insideCall(state: EditorState, pos: number): boolean {
  const start = Math.max(0, pos - CALL_SCAN_WINDOW);
  const text = state.sliceDoc(start, pos);
  let depth = 0;
  for (let i = text.length - 1; i >= 0; i--) {
    const ch = text[i];
    if (ch === ")") depth++;
    else if (ch === "(") {
      if (depth === 0) return true;
      depth--;
    }
  }
  return false;
}

// Map an LSP (1-based line, 0-based column) range to absolute doc offsets, clamping to the
// document and widening a zero-width point (e.g. pyflakes) to the identifier under it.
export function lspDiagnosticToRange(doc: Text, d: LspDiagnostic): { from: number; to: number } {
  const fromLine = doc.line(Math.max(1, Math.min(d.line, doc.lines)));
  const from = fromLine.from + Math.max(0, Math.min(d.column, fromLine.length));
  const endLine = doc.line(Math.max(1, Math.min(d.end_line || d.line, doc.lines)));
  let to = endLine.from + Math.max(0, Math.min(d.end_column ?? d.column, endLine.length));
  if (to <= from) {
    const text = doc.sliceString(fromLine.from, fromLine.to);
    let e = from - fromLine.from;
    while (e < text.length && IDENT.test(text[e])) e++;
    to = e > from - fromLine.from ? fromLine.from + e : Math.min(from + 1, fromLine.to);
  }
  return { from, to };
}
