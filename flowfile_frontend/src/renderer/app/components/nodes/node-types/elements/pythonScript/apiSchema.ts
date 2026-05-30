/**
 * Introspected API-schema overlay for the Python cell editor.
 *
 * Phase 1 of the "better type hints" work: the kernel exposes a runtime
 * introspection of `flowfile_ctx` + polars at `/kernels/{id}/api_schema`. We
 * fetch it once per kernel and use it to (a) enrich the hand-written static
 * completions with real signatures / docstrings, and (b) power a hover tooltip.
 *
 * LSP-READY SEAM
 * --------------
 * Everything type-hint-related funnels through this module: the completion
 * `enrich()` helper and the `apiHoverTooltip` extension. A future Pyright-based
 * language client (e.g. `codemirror-languageserver` talking to
 * `pyright-langserver` over a kernel websocket) can replace this module's
 * provider without touching the cell component or the static completion
 * sources — keep that boundary intact.
 */
import type { Completion } from "@codemirror/autocomplete";
import { hoverTooltip } from "@codemirror/view";

import { KernelApi } from "../../../../../api/kernel.api";
import type { ApiSymbol } from "../../../../../types";

const _key = (namespace: string, name: string) => `${namespace}:${name}`;

let _byKey = new Map<string, ApiSymbol>();
let _byName = new Map<string, ApiSymbol>(); // first symbol seen per bare name (for hover)
let _loadedKernel: string | null = null;
let _loading: Promise<void> | null = null;

// Hover lookup prefers the most "interesting" namespace when a name is ambiguous.
const _NAME_PRIORITY = ["flowfile_ctx", "pl", "LazyFrame", "DataFrame", "Expr"];

export function isSchemaLoaded(): boolean {
  return _byKey.size > 0;
}

/** Fetch + cache the API schema for a kernel. No-op if already loaded for it. */
export async function loadApiSchema(kernelId: string | null | undefined): Promise<void> {
  if (!kernelId || _loadedKernel === kernelId) return;
  if (_loading) return _loading;
  _loading = (async () => {
    try {
      const symbols = await KernelApi.getApiSchema(kernelId);
      if (symbols.length === 0) return; // keep static fallback
      const byKey = new Map<string, ApiSymbol>();
      const byName = new Map<string, ApiSymbol>();
      for (const s of symbols) {
        byKey.set(_key(s.namespace, s.name), s);
        const existing = byName.get(s.name);
        if (!existing || _rank(s.namespace) < _rank(existing.namespace)) {
          byName.set(s.name, s);
        }
      }
      _byKey = byKey;
      _byName = byName;
      _loadedKernel = kernelId;
    } finally {
      _loading = null;
    }
  })();
  return _loading;
}

function _rank(namespace: string): number {
  const idx = _NAME_PRIORITY.indexOf(namespace);
  return idx === -1 ? _NAME_PRIORITY.length : idx;
}

function _getSymbol(namespaces: string[], name: string): ApiSymbol | undefined {
  for (const ns of namespaces) {
    const s = _byKey.get(_key(ns, name));
    if (s) return s;
  }
  return undefined;
}

/** Human-readable signature line, e.g. `read_input(name='main') -> pl.LazyFrame`. */
function _signatureLine(s: ApiSymbol): string {
  if (!s.signature) return s.name;
  return `${s.name}${s.signature.replace(/^\(self,?\s*/, "(")}`;
}

function _infoText(s: ApiSymbol): string {
  const sig = _signatureLine(s);
  return s.doc ? `${sig}\n\n${s.doc}` : sig;
}

function _toCompletion(s: ApiSymbol): Completion {
  const apply = s.kind === "function" ? `${s.name}()` : s.name;
  return {
    label: s.name,
    type: s.kind === "function" ? "function" : s.kind === "property" ? "property" : s.kind,
    detail: s.signature ? _signatureLine(s) : undefined,
    info: _infoText(s),
    apply: s.kind === "function" ? apply : undefined,
  };
}

/**
 * Merge introspected signatures/docs into a static completion list and append
 * any introspected-only symbols for the given namespaces. Returns the input
 * unchanged when no schema is loaded (offline / kernel not running).
 */
export function enrich(options: Completion[], namespaces: string[]): Completion[] {
  if (_byKey.size === 0) return options;
  const seen = new Set(options.map((o) => o.label));
  const merged = options.map((o) => {
    const s = _getSymbol(namespaces, String(o.label));
    if (!s) return o;
    return {
      ...o,
      detail: s.signature ? _signatureLine(s) : o.detail,
      info: _infoText(s) || o.info,
    };
  });
  for (const [, s] of _byKey) {
    if (!namespaces.includes(s.namespace) || seen.has(s.name)) continue;
    merged.push(_toCompletion(s));
    seen.add(s.name);
  }
  return merged;
}

/**
 * CodeMirror hover tooltip showing the introspected signature + doc for the
 * identifier under the cursor. Silent when the symbol is unknown.
 */
export const apiHoverTooltip = hoverTooltip((view, pos) => {
  const range = view.state.wordAt(pos);
  if (!range) return null;
  const { from, to } = range;
  const text = view.state.sliceDoc(from, to);
  const s = text ? _byName.get(text) : undefined;
  if (!s) return null;
  return {
    pos: from,
    end: to,
    above: true,
    create() {
      const dom = document.createElement("div");
      dom.className = "cm-api-hover";
      const sig = document.createElement("div");
      sig.className = "cm-api-hover-sig";
      sig.textContent = `${s.namespace === "flowfile_ctx" ? "flowfile_ctx." : ""}${_signatureLine(s)}`;
      dom.appendChild(sig);
      if (s.doc) {
        const doc = document.createElement("div");
        doc.className = "cm-api-hover-doc";
        doc.textContent = s.doc;
        dom.appendChild(doc);
      }
      return { dom };
    },
  };
});
