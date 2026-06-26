// Jedi-backed CodeMirror completion source. Async, abortable, and gated on a cached
// capabilities probe + a live kernel. Returns null (so the static client-side sources
// serve) when LSP is disabled, no kernel is selected, the request is superseded, or
// the backend degrades to empty.
import type {
  CompletionContext,
  CompletionResult,
  Completion,
  CompletionSource,
} from "@codemirror/autocomplete";

import { LspApi } from "@/api/lsp.api";
import type { LspCompletionItem } from "@/api/lsp.api";

/** Per-cell context resolved fresh on each completion (kernel/flow can change live). */
export interface LspContext {
  kernelId: string | null;
  flowId: number;
  nodeId?: number | null;
}

// Map Jedi completion types onto CodeMirror's completion type vocabulary (drives the icon).
const TYPE_MAP: Record<string, string> = {
  function: "function",
  method: "method",
  instance: "variable",
  statement: "variable",
  param: "variable",
  property: "property",
  module: "namespace",
  class: "class",
  keyword: "keyword",
  path: "text",
};

function mapType(t: string): string {
  return TYPE_MAP[t] ?? "variable";
}

// Rank public names first, then _private, then __dunder__ (CodeMirror boost: higher = earlier).
// Negative-only so these never outrank a closer prefix match from the static sources.
function boostFor(label: string): number {
  if (label.startsWith("__")) return -99;
  if (label.startsWith("_")) return -50;
  return 0;
}

/** True when Jedi is the active completion engine: a kernel is selected and LSP is enabled. */
export function lspActiveFor(getCtx: () => LspContext): () => Promise<boolean> {
  return async () => {
    if (!getCtx().kernelId) return false;
    return (await LspApi.capabilities()).enabled;
  };
}

/** Wrap a static source so it only fires when Jedi is NOT active — removes the overlap
 *  with Jedi while keeping the source as a full fallback when there's no kernel / LSP is off. */
export function fallbackWhenNoLsp(
  source: CompletionSource,
  isLspActive: () => Promise<boolean>,
): CompletionSource {
  return async (context: CompletionContext) => {
    if (await isLspActive()) return null;
    return source(context);
  };
}

export function createLspCompletionSource(getCtx: () => LspContext) {
  // Per-editor: a new keystroke in this cell aborts only this cell's previous fetch.
  let inflight: AbortController | null = null;
  return async (context: CompletionContext): Promise<CompletionResult | null> => {
    const ctx = getCtx();
    if (!ctx.kernelId) return null;

    const word = context.matchBefore(/[A-Za-z_][A-Za-z0-9_]*$/);
    const charBefore = context.state.sliceDoc(Math.max(0, context.pos - 1), context.pos);
    // Fire on an identifier prefix, right after a "." (attribute access), or explicit (Ctrl-Space).
    if (!context.explicit && !word && charBefore !== ".") return null;

    const caps = await LspApi.capabilities();
    if (!caps.enabled || context.aborted) return null;

    const line = context.state.doc.lineAt(context.pos);
    const payload = {
      code: context.state.doc.toString(),
      line: line.number, // CodeMirror lines are 1-based, matching Jedi
      column: context.pos - line.from, // 0-based column within the line
      flow_id: ctx.flowId,
      node_id: ctx.nodeId ?? null,
    };

    if (inflight) inflight.abort();
    inflight = new AbortController();
    const res = await LspApi.complete(ctx.kernelId, payload, inflight.signal);
    if (context.aborted || !res.items.length) return null;

    const options: Completion[] = res.items.map((it: LspCompletionItem) => ({
      label: it.label,
      type: mapType(it.type),
      detail: it.detail || undefined,
      info: it.documentation || undefined,
      boost: boostFor(it.label),
    }));
    const from = word ? word.from : context.pos;
    return { from, options, validFor: /^[A-Za-z0-9_]*$/ };
  };
}
