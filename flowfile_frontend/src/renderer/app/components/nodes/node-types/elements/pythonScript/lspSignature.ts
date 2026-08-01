// Jedi-backed signature help: a tooltip showing the call signature (active parameter
// bolded) while the cursor sits inside a function call's parentheses. CodeMirror has no
// built-in async signature help, so a ViewPlugin debounces + fetches and dispatches the
// resulting Tooltip into a StateField that feeds the showTooltip facet. Renders through
// the editor's bodyTooltips() (mounted on <body>) like the hover tooltip. Degrades to no
// tooltip when LSP is off / no kernel / not inside a call.
import { Prec, StateEffect, StateField, type EditorState, type Extension } from "@codemirror/state";
import {
  EditorView,
  ViewPlugin,
  keymap,
  showTooltip,
  type Tooltip,
  type ViewUpdate,
} from "@codemirror/view";

import { LspApi } from "@/api/lsp.api";
import type { LspSignatureResponse } from "@/api/lsp.api";
import type { LspContext } from "./lspCompletionSource";
import { renderDocText } from "./lspDocRender";
import { insideCall } from "./lspPositions";

const QUERY_DEBOUNCE_MS = 150;

export const setSigTooltip = StateEffect.define<Tooltip | null>();

export const sigTooltipField = StateField.define<Tooltip | null>({
  create: () => null,
  update(value, tr) {
    for (const e of tr.effects) if (e.is(setSigTooltip)) return e.value;
    // Keep the existing tooltip in place across edits until the plugin re-queries.
    if (value && tr.docChanged) return { ...value, pos: tr.changes.mapPos(value.pos) };
    return value;
  },
  provide: (f) => showTooltip.from(f),
});

/** Document position the signature tooltip is currently anchored at, or null when hidden. */
export function signatureTooltipShownAt(state: EditorState): number | null {
  return state.field(sigTooltipField, false)?.pos ?? null;
}

function buildTooltip(res: LspSignatureResponse, pos: number): Tooltip {
  const sig = res.signatures[res.active_signature] ?? res.signatures[0];
  const active = sig.parameters[sig.active_parameter];
  const doc = sig.documentation;
  return {
    pos,
    above: true,
    create() {
      const dom = document.createElement("div");
      dom.className = "cm-lsp-doc cm-lsp-signature";
      const label = document.createElement("div");
      label.className = "cm-lsp-doc-label";
      const idx = active ? sig.label.indexOf(active) : -1;
      if (idx >= 0 && active) {
        label.appendChild(document.createTextNode(sig.label.slice(0, idx)));
        const strong = document.createElement("strong");
        strong.textContent = active;
        label.appendChild(strong);
        label.appendChild(document.createTextNode(sig.label.slice(idx + active.length)));
      } else {
        label.textContent = sig.label;
      }
      dom.appendChild(label);
      if (doc) {
        const docEl = document.createElement("div");
        docEl.className = "cm-lsp-doc-body";
        docEl.appendChild(renderDocText(doc));
        dom.appendChild(docEl);
      }
      return { dom, resize: false };
    },
  };
}

function signaturePlugin(getCtx: () => LspContext) {
  return ViewPlugin.fromClass(
    class {
      private timer = 0;
      private seq = 0;
      constructor(private readonly view: EditorView) {}

      update(u: ViewUpdate) {
        // Blur means the user moved on — drop the tooltip instead of leaving it pinned.
        // Deferred: a view plugin may not dispatch from inside update().
        if (u.focusChanged && !u.view.hasFocus) {
          window.clearTimeout(this.timer);
          this.seq++; // invalidate any in-flight response
          this.timer = window.setTimeout(() => {
            if (!this.view.hasFocus) this.hide();
          }, 0);
          return;
        }
        if (u.docChanged || u.selectionSet) this.schedule();
      }

      private schedule() {
        window.clearTimeout(this.timer);
        this.timer = window.setTimeout(() => void this.query(), QUERY_DEBOUNCE_MS);
      }

      private hide() {
        if (this.view.state.field(sigTooltipField)) {
          this.view.dispatch({ effects: setSigTooltip.of(null) });
        }
      }

      private async query() {
        const ctx = getCtx();
        const pos = this.view.state.selection.main.head;
        if (!ctx.kernelId || !insideCall(this.view.state, pos)) {
          this.hide();
          return;
        }
        const caps = await LspApi.capabilities();
        if (!caps.enabled) {
          this.hide();
          return;
        }
        const token = ++this.seq;
        const line = this.view.state.doc.lineAt(pos);
        const res = await LspApi.signature(ctx.kernelId, {
          code: this.view.state.doc.toString(),
          line: line.number,
          column: pos - line.from,
          flow_id: ctx.flowId,
          node_id: ctx.nodeId ?? null,
        });
        // Drop stale responses and re-check we're still inside a call at the live cursor.
        const head = this.view.state.selection.main.head;
        if (token !== this.seq || !res.signatures.length || !insideCall(this.view.state, head)) {
          this.hide();
          return;
        }
        this.view.dispatch({ effects: setSigTooltip.of(buildTooltip(res, head)) });
      }

      destroy() {
        window.clearTimeout(this.timer);
      }
    },
  );
}

// Escape dismisses the signature tooltip, falling through to closeCompletion when none is up.
const dismissKeymap = Prec.high(
  keymap.of([
    {
      key: "Escape",
      run: (view) => {
        if (!view.state.field(sigTooltipField, false)) return false;
        view.dispatch({ effects: setSigTooltip.of(null) });
        return true;
      },
    },
  ]),
);

export function createLspSignature(getCtx: () => LspContext): Extension {
  return [sigTooltipField, signaturePlugin(getCtx), dismissKeymap];
}
