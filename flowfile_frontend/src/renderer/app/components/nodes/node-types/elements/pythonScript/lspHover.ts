// Jedi-backed hover tooltip. Mounts through the editor's existing bodyTooltips() so it
// renders above node-settings overflow. Degrades to no tooltip when LSP is off / no kernel.
import { hoverTooltip, type Tooltip } from "@codemirror/view";

import { LspApi } from "@/api/lsp.api";
import type { LspContext } from "./lspCompletionSource";

const IDENT = /[A-Za-z0-9_]/;

export function createLspHover(getCtx: () => LspContext) {
  return hoverTooltip(async (view, pos): Promise<Tooltip | null> => {
    const ctx = getCtx();
    if (!ctx.kernelId) return null;
    const caps = await LspApi.capabilities();
    if (!caps.enabled) return null;

    // Only hover over an identifier; widen to the whole token for the tooltip range.
    const line = view.state.doc.lineAt(pos);
    const text = line.text;
    let start = pos;
    let end = pos;
    while (start > line.from && IDENT.test(text[start - line.from - 1])) start--;
    while (end < line.to && IDENT.test(text[end - line.from])) end++;
    if (start === end) return null;

    const res = await LspApi.hover(ctx.kernelId, {
      code: view.state.doc.toString(),
      line: line.number,
      column: pos - line.from,
      flow_id: ctx.flowId,
      node_id: ctx.nodeId ?? null,
    });
    if (!res.contents) return null;

    return {
      pos: start,
      end,
      above: true,
      create() {
        const dom = document.createElement("div");
        dom.className = "cm-lsp-hover";
        dom.style.whiteSpace = "pre-wrap";
        dom.style.maxWidth = "480px";
        dom.style.padding = "4px 8px";
        dom.style.fontSize = "0.78rem";
        dom.textContent = res.contents ?? "";
        return { dom };
      },
    };
  });
}
