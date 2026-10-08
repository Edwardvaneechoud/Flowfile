// Jedi-backed signature help: a tooltip showing the call signature (active parameter
// highlighted) while the cursor sits inside a function call's parentheses — the full
// docstring right after the "(", then only that parameter's description and the summary
// line once an argument is typed. CodeMirror has no built-in async signature help, so a
// ViewPlugin debounces + fetches and dispatches the resulting Tooltip into a StateField
// that feeds the showTooltip facet. Renders through
// the editor's bodyTooltips() (mounted on <body>) like the hover tooltip. Degrades to no
// tooltip when LSP is off / no kernel / not inside a call / inside a string literal.
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
import type { LspSignatureInfo, LspSignatureResponse } from "@/api/lsp.api";
import type { LspContext } from "./lspCompletionSource";
import { appendInline, docSummary, paramDoc, paramName, renderDocText } from "./lspDocRender";
import {
  callArgsEmpty,
  currentArgText,
  insideCall,
  insideString,
  typingMemberAccess,
} from "./lspPositions";

const QUERY_DEBOUNCE_MS = 150;

export const setSigTooltip = StateEffect.define<Tooltip | null>();

export const sigTooltipField = StateField.define<Tooltip | null>({
  create: () => null,
  update(value, tr) {
    for (const e of tr.effects) if (e.is(setSigTooltip)) return e.value;
    if (!value) return value;
    // Drop it on the keystroke that leaves the arguments (e.g. typing `df.`), not after the debounce.
    if (
      (tr.docChanged || tr.selection) &&
      !signatureApplies(tr.state, tr.state.selection.main.head)
    ) {
      return null;
    }
    // Keep the existing tooltip in place across edits until the plugin re-queries.
    if (tr.docChanged) return { ...value, pos: tr.changes.mapPos(value.pos) };
    return value;
  },
  provide: (f) => showTooltip.from(f),
});

/** Document position the signature tooltip is currently anchored at, or null when hidden. */
export function signatureTooltipShownAt(state: EditorState): number | null {
  return state.field(sigTooltipField, false)?.pos ?? null;
}

/**
 * Signature help belongs to the call's arguments, not to the text of a string argument or a
 * member access being typed as one (`f(df.|)`): those positions are where column and member
 * completions live, and two popups fight over the caret.
 */
export function signatureApplies(state: EditorState, pos: number): boolean {
  return insideCall(state, pos) && !insideString(state, pos) && !typingMemberAccess(state, pos);
}

function renderLabel(sig: LspSignatureInfo, active: string | undefined): HTMLElement {
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
  return label;
}

// Only what helps fill in this argument: its description and the docstring's summary.
function renderCompactDoc(dom: HTMLElement, doc: string, active: string | undefined): void {
  const name = active ? paramName(active) : "";
  const param = paramDoc(doc, name);
  if (param) {
    const paramEl = document.createElement("div");
    paramEl.className = "cm-lsp-sig-param";
    const nameEl = document.createElement("span");
    nameEl.className = "cm-lsp-sig-param-name";
    nameEl.textContent = name;
    paramEl.appendChild(nameEl);
    appendInline(paramEl, ` — ${param}`);
    dom.appendChild(paramEl);
  }
  const summary = docSummary(doc);
  if (summary) {
    const summaryEl = document.createElement("div");
    summaryEl.className = "cm-lsp-doc-body";
    appendInline(summaryEl, summary);
    dom.appendChild(summaryEl);
  }
}

const PARTIAL_NAME = /^\s*([A-Za-z_]\w*)$/;
const KEYWORD_ARG = /^\s*([A-Za-z_]\w*)\s*=(?!=)/;

/**
 * The parameter the typed argument targets. Jedi answers by position until `name=` is
 * complete, so `f(table_n|` would still point at the first parameter: a `name=` or a bare
 * partial name that matches a parameter wins, anything else keeps Jedi's index.
 */
export function activeParamIndex(sig: LspSignatureInfo, argText: string | null): number {
  if (argText !== null) {
    const names = sig.parameters.map(
      (p) => (/^\*/.test(p.trim()) ? "" : paramName(p)), // *args / **kwargs take no keyword
    );
    const keyword = KEYWORD_ARG.exec(argText)?.[1];
    if (keyword) {
      const idx = names.indexOf(keyword);
      if (idx >= 0) return idx;
    }
    const partial = PARTIAL_NAME.exec(argText)?.[1];
    if (partial) {
      const exact = names.indexOf(partial);
      if (exact >= 0) return exact;
      const prefix = names.findIndex((n) => n.startsWith(partial));
      if (prefix >= 0) return prefix;
    }
  }
  return sig.active_parameter;
}

/**
 * Full docstring while the call has just been opened (nothing typed after the "("), compact
 * as soon as an argument is typed. The view re-renders itself on edits, so neither the
 * switch nor the highlighted parameter waits for the next kernel round-trip.
 */
function buildTooltip(res: LspSignatureResponse, pos: number): Tooltip {
  const sig = res.signatures[res.active_signature] ?? res.signatures[0];
  const doc = sig.documentation;
  return {
    pos,
    above: true,
    create(view) {
      const dom = document.createElement("div");
      dom.className = "cm-lsp-doc cm-lsp-signature";
      let shown = "";
      const render = (state: EditorState) => {
        const head = state.selection.main.head;
        const full = callArgsEmpty(state, head);
        const active = sig.parameters[activeParamIndex(sig, currentArgText(state, head))];
        const key = `${full}|${active ?? ""}`;
        if (key === shown) return;
        shown = key;
        dom.replaceChildren(renderLabel(sig, active));
        if (!doc) return;
        if (full) {
          const docEl = document.createElement("div");
          docEl.className = "cm-lsp-doc-body";
          docEl.appendChild(renderDocText(doc));
          dom.appendChild(docEl);
        } else {
          renderCompactDoc(dom, doc, active);
        }
      };
      render(view.state);
      return {
        dom,
        resize: false,
        update: (u: ViewUpdate) => {
          if (u.docChanged || u.selectionSet) render(u.state);
        },
      };
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
        if (!ctx.kernelId || !signatureApplies(this.view.state, pos)) {
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
        // Drop stale responses and re-check the live cursor still wants a signature — the
        // caret may have moved into a string argument while the request was in flight.
        const head = this.view.state.selection.main.head;
        if (
          token !== this.seq ||
          !res.signatures.length ||
          !signatureApplies(this.view.state, head)
        ) {
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
