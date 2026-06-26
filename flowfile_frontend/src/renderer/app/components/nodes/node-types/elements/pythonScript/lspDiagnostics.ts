// Jedi-backed diagnostics (syntax errors) surfaced through @codemirror/lint as
// underlines + a gutter marker. Debounced, gated on a kernel + capabilities, and
// failure-safe: no kernel / LSP off / backend degrade all resolve to zero diagnostics.
import { linter, lintGutter, type Diagnostic } from "@codemirror/lint";
import type { Extension } from "@codemirror/state";
import type { EditorView } from "@codemirror/view";

import { LspApi } from "@/api/lsp.api";
import type { LspContext } from "./lspCompletionSource";
import { lspDiagnosticToRange } from "./lspPositions";

export function createLspDiagnostics(getCtx: () => LspContext): Extension {
  const source = async (view: EditorView): Promise<Diagnostic[]> => {
    const ctx = getCtx();
    if (!ctx.kernelId) return [];
    const caps = await LspApi.capabilities();
    if (!caps.enabled) return [];
    const res = await LspApi.diagnostics(ctx.kernelId, {
      code: view.state.doc.toString(),
      line: 1,
      column: 0,
      flow_id: ctx.flowId,
      node_id: ctx.nodeId ?? null,
    });
    return res.diagnostics.map((d) => {
      const { from, to } = lspDiagnosticToRange(view.state.doc, d);
      return {
        from,
        to,
        severity: d.severity === "warning" ? "warning" : "error",
        message: d.message,
        source: d.source || "lsp",
      } as Diagnostic;
    });
  };
  return [linter(source, { delay: 400 }), lintGutter()];
}
