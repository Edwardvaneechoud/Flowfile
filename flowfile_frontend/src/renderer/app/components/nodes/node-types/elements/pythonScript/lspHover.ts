// Jedi-backed hover tooltip. Mounts through the editor's existing bodyTooltips() so it
// renders above node-settings overflow. Degrades to no tooltip when LSP is off / no kernel.
import { hoverTooltip, type Tooltip } from "@codemirror/view";
import type { EditorState } from "@codemirror/state";

import { LspApi } from "@/api/lsp.api";
import type { LspContext } from "./lspCompletionSource";
import { renderDocText } from "./lspDocRender";
import { signatureTooltipShownAt } from "./lspSignature";

const IDENT = /[A-Za-z0-9_]/;

/** Hover and signature help render the same Jedi text; on the signature's line it wins. */
export function signatureCoversHover(state: EditorState, hoverPos: number): boolean {
  const sigPos = signatureTooltipShownAt(state);
  if (sigPos === null) return false;
  return state.doc.lineAt(sigPos).number === state.doc.lineAt(hoverPos).number;
}

function titleRow(kind: string, name: string): HTMLElement {
  const row = document.createElement("div");
  row.className = "cm-lsp-doc-title";
  if (kind) {
    const kindEl = document.createElement("span");
    kindEl.className = "cm-lsp-doc-kind";
    kindEl.textContent = kind;
    row.appendChild(kindEl);
  }
  const nameEl = document.createElement("span");
  nameEl.className = "cm-lsp-doc-name";
  nameEl.textContent = name;
  row.appendChild(nameEl);
  return row;
}

function signatureRow(signature: string): HTMLElement {
  const row = document.createElement("div");
  row.className = "cm-lsp-doc-signature";
  row.textContent = signature;
  return row;
}

export function createLspHover(getCtx: () => LspContext) {
  return hoverTooltip(async (view, pos): Promise<Tooltip | null> => {
    const ctx = getCtx();
    if (!ctx.kernelId) return null;
    if (signatureCoversHover(view.state, pos)) return null;
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
    if (!res.contents && !res.signature && !res.name) return null;

    return {
      pos: start,
      end,
      above: true,
      create() {
        const dom = document.createElement("div");
        dom.className = "cm-lsp-doc cm-lsp-hover";
        if (res.name) dom.appendChild(titleRow(res.kind ?? "", res.name));
        if (res.signature) dom.appendChild(signatureRow(res.signature));
        if (res.contents) dom.appendChild(renderDocText(res.contents));
        // resize:false stops CodeMirror writing an inline height on the tooltip host,
        // which the doc box then overflowed; the theme's max-height owns sizing.
        return { dom, resize: false };
      },
    };
  });
}
