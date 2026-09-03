// Merged identifier completion source: curated static entries (catalog-ref chains, which
// carry typed signatures + apply snippets), Jedi-backed LSP items (async, abortable, gated
// on a cached capabilities probe + a live kernel) and prior-cell scope symbols, deduped by
// label in that precedence. CodeMirror never dedupes across override sources, so every
// identifier source that can overlap Jedi must merge here. Degrades cleanly: no kernel /
// LSP off / backend empty -> curated + scope only; nothing to offer -> null.
import type {
  CompletionContext,
  CompletionResult,
  Completion,
  CompletionSource,
} from "@codemirror/autocomplete";

import { LspApi } from "@/api/lsp.api";
import { createScopeCompletions } from "./flowfileCompletions";

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

export function createIdentifierCompletionSource(
  getCtx: () => LspContext,
  getPriorCellCodes: () => string[] = () => [],
  curatedSources: CompletionSource[] = [],
) {
  const scopeSource = createScopeCompletions(getPriorCellCodes);
  // Per-editor: a new keystroke in this cell aborts only this cell's previous fetch.
  let inflight: AbortController | null = null;
  return async (context: CompletionContext): Promise<CompletionResult | null> => {
    const word = context.matchBefore(/[A-Za-z_][A-Za-z0-9_]*$/);
    const charBefore = context.state.sliceDoc(Math.max(0, context.pos - 1), context.pos);
    // Fire on an identifier prefix, right after a "." (attribute access), or explicit (Ctrl-Space).
    if (!context.explicit && !word && charBefore !== ".") return null;

    const from = word ? word.from : context.pos;
    // Prior-cell symbols are bare globals — meaningless (and noisy) in attribute position.
    const inAttribute = context.state.sliceDoc(Math.max(0, from - 1), from) === ".";
    const scopeResult = inAttribute ? null : await scopeSource(context);

    // Label -> completion. Curated entries seed the map (hand-written signature + apply
    // snippet beat Jedi's bare `def name`), Jedi fills the rest, scope symbols last.
    const merged = new Map<string, Completion>();
    for (const source of curatedSources) {
      const result = await source(context);
      if (context.aborted) return null;
      if (!result || result.from !== from) continue;
      for (const opt of result.options) if (!merged.has(opt.label)) merged.set(opt.label, opt);
    }
    const ctx = getCtx();
    if (ctx.kernelId) {
      const caps = await LspApi.capabilities();
      if (context.aborted) return null;
      if (caps.enabled) {
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
        if (context.aborted) return null;
        for (const it of res.items) {
          if (merged.has(it.label)) continue; // Jedi can emit a name twice (parse + live namespace)
          merged.set(it.label, {
            label: it.label,
            type: mapType(it.type),
            detail: it.detail || undefined,
            info: it.documentation || undefined,
            boost: boostFor(it.label),
          });
        }
      }
    }
    if (scopeResult) {
      for (const opt of scopeResult.options) {
        if (!merged.has(opt.label)) merged.set(opt.label, opt);
      }
    }
    if (merged.size === 0) return null;
    return { from, options: [...merged.values()], validFor: /^[A-Za-z0-9_]*$/ };
  };
}
