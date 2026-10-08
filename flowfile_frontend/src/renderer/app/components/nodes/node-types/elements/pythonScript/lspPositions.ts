// Pure position/range helpers for the LSP editor features, kept free of the API layer so
// they unit-test in isolation (mirrors the kernel's dependency-free lsp/context.py). Only
// editor/language modules and an LSP type (elided at runtime) are imported here.
import type { CompletionContext, CompletionSource } from "@codemirror/autocomplete";
import { ensureSyntaxTree, syntaxTree, syntaxTreeAvailable } from "@codemirror/language";
import type { EditorState, Text } from "@codemirror/state";
import type { SyntaxNode } from "@lezer/common";

import type { LspDiagnostic } from "@/api/lsp.api";

const IDENT = /[A-Za-z0-9_]/;
const CALL_SCAN_WINDOW = 2000;
const STRING_NODES = new Set(["String", "FormatString", "ContinuedString"]);
// An escape or an f-string replacement resolves below the string node, so climb a little.
const MAX_STRING_CLIMB = 8;
const QUOTE = /["']/;

// `import x as p`, `with ... as f`, `except E as e` — the name after `as` is a binding the
// user is defining, not a reference, so identifier completions there are noise. The `\b`
// keeps `cast` / `as_of` (no trailing space) from matching.
export const AS_BINDING = /\bas\s+\w*$/;

export function notInAsBinding(source: CompletionSource): CompletionSource {
  return (context: CompletionContext) => (context.matchBefore(AS_BINDING) ? null : source(context));
}

// Position of the unclosed "(" the cursor sits in, or -1. Scans back a bounded window
// tracking paren depth so signature help only fires when plausible.
export function openCallParen(state: EditorState, pos: number): number {
  const start = Math.max(0, pos - CALL_SCAN_WINDOW);
  const text = state.sliceDoc(start, pos);
  let depth = 0;
  for (let i = text.length - 1; i >= 0; i--) {
    const ch = text[i];
    if (ch === ")") depth++;
    else if (ch === "(") {
      if (depth === 0) return start + i;
      depth--;
    }
  }
  return -1;
}

// Cheap heuristic: is the cursor inside an unclosed "(" (i.e. typing call arguments)?
export function insideCall(state: EditorState, pos: number): boolean {
  return openCallParen(state, pos) >= 0;
}

// Just opened the call: nothing but whitespace between the "(" and the cursor.
export function callArgsEmpty(state: EditorState, pos: number): boolean {
  const paren = openCallParen(state, pos);
  return paren >= 0 && !state.sliceDoc(paren + 1, pos).trim();
}

// Text of the argument being typed: from the last top-level "," (or the open "(") to the
// cursor, skipping commas nested in brackets or strings. null outside a call.
export function currentArgText(state: EditorState, pos: number): string | null {
  const paren = openCallParen(state, pos);
  if (paren < 0) return null;
  const text = state.sliceDoc(paren + 1, pos);
  let start = 0;
  let depth = 0;
  let quote = "";
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quote) {
      if (ch === "\\") i++;
      else if (ch === quote) quote = "";
    } else if (ch === '"' || ch === "'") quote = ch;
    else if ("([{".includes(ch)) depth++;
    else if (")]}".includes(ch)) depth--;
    else if (ch === "," && depth === 0) start = i + 1;
  }
  return text.slice(start);
}

// Is the caret ending a member access (`df.`, `df.sel`, `f().x`, `a[0].`)? A numeric
// literal like `1.` is not a receiver, so it doesn't count.
const MEMBER_ACCESS = /(?:[A-Za-z_]\w*|[)\]])\s*\.\w*$/;

export function typingMemberAccess(state: EditorState, pos: number): boolean {
  const line = state.doc.lineAt(pos);
  return MEMBER_ACCESS.test(state.sliceDoc(line.from, pos));
}

// Is the cursor inside a string literal? Column completions own that position, so signature
// help stays out of it. An unterminated string still counts; the spot right after a closing
// quote does not.
export function insideString(state: EditorState, pos: number): boolean {
  const tree = syntaxTreeAvailable(state, pos)
    ? syntaxTree(state)
    : (ensureSyntaxTree(state, pos, 50) ?? syntaxTree(state));
  let node: SyntaxNode | null = tree.resolveInner(pos, -1);
  for (let climbed = 0; node && climbed < MAX_STRING_CLIMB; climbed++) {
    if (!STRING_NODES.has(node.name)) {
      node = node.parent;
      continue;
    }
    // `r`/`b`/`f` prefixes sit inside the node, so find the quote rather than assuming from.
    const quote = state.sliceDoc(node.from, Math.min(node.to, node.from + 4)).search(QUOTE);
    if (quote < 0) return false;
    const unterminated = node.lastChild?.name === "⚠";
    return pos > node.from + quote && (unterminated || pos < node.to);
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
