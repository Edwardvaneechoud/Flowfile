// Shared CodeMirror setup for both notebook cell editors (avoids drift).
import type { Extension } from "@codemirror/state";
import { EditorState, Prec } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { globalCompletion, localCompletionSource, python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { acceptCompletion, autocompletion, type CompletionSource } from "@codemirror/autocomplete";
import { indentLess, indentMore } from "@codemirror/commands";
import { bodyTooltips } from "@/utils/codemirrorTooltips";
import {
  catalogRefChainCompletions,
  createNamedInputCompletions,
  createPolarsExprCompletions,
  createRefVariableCompletions,
  createUpstreamColumnCompletions,
  flowfileApiCompletions,
  globalIdentifierCompletions,
  polarsModuleCompletions,
} from "./flowfileCompletions";
import {
  createIdentifierCompletionSource,
  fallbackWhenNoLsp,
  lspActiveFor,
  type LspContext,
} from "./lspCompletionSource";
import { createLspHover } from "./lspHover";
import { createLspSignature } from "./lspSignature";
import { createLspDiagnostics } from "./lspDiagnostics";
import { createNoKernelHint } from "./lspNoKernelHint";
import { notInAsBinding } from "./lspPositions";
import type { UpstreamColumn } from "./useUpstreamColumns";

export interface NotebookEditorOptions {
  onRun: () => void;
  onRunAdvance?: () => void; // Mod+Enter; falls back to onRun
  getInputNames?: () => string[];
  getUpstreamColumns?: () => UpstreamColumn[];
  getPriorCellCodes?: () => string[];
  // Kernel/session identity for Jedi code intelligence. flow_id is the namespace key
  // (node flow_id, or the notebook sessionFlowId for the Catalog notebook). When no
  // kernel is selected the LSP sources resolve null and the static sources serve.
  getKernelId?: () => string | null;
  getFlowId?: () => number;
  getNodeId?: () => number;
}

const cellEditorTheme = EditorView.theme({
  "&": { fontSize: "0.8rem", maxHeight: "350px" },
  ".cm-content": {
    minHeight: "40px",
    padding: "0.4rem 0",
    fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
  },
  ".cm-gutters": { fontSize: "0.7rem", minWidth: "2.5rem" },
  ".cm-scroller": { overflow: "auto" },
  // Completion dropdown height (default is ~10em): a moderate cap so a useful number of
  // suggestions show without the list dominating the cell.
  ".cm-tooltip-autocomplete > ul": { maxHeight: "14rem" },
  // Hover + signature tooltips: scrollable so a long docstring can't fill the screen.
  ".cm-lsp-doc": {
    maxWidth: "560px",
    maxHeight: "24rem",
    overflow: "auto",
    padding: "6px 10px",
    fontSize: "0.78rem",
    lineHeight: "1.45",
    whiteSpace: "pre-wrap",
  },
  ".cm-lsp-doc code": {
    fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
    fontSize: "0.95em",
    background: "rgba(127, 127, 127, 0.14)",
    borderRadius: "3px",
    padding: "0 3px",
  },
  ".cm-lsp-doc-title": { marginBottom: "4px" },
  ".cm-lsp-doc-kind": { opacity: "0.6", fontStyle: "italic", marginRight: "6px" },
  ".cm-lsp-doc-name": { fontWeight: "600" },
  ".cm-lsp-doc-signature": {
    fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
    fontSize: "0.95em",
    opacity: "0.85",
    marginBottom: "6px",
    paddingBottom: "6px",
    borderBottom: "1px solid rgba(127, 127, 127, 0.25)",
  },
  ".cm-lsp-doc-label": { fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace" },
  ".cm-lsp-doc-body": { opacity: "0.8", marginTop: "4px" },
  ".cm-lsp-doc-section": { fontWeight: "600", marginTop: "6px" },
  ".cm-lsp-doc-gap": { height: "0.5em" },
  // Quiet, dismissible "no kernel" FYI panel (see lspNoKernelHint.ts).
  ".cm-lsp-no-kernel-hint": {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: "8px",
    padding: "2px 8px",
    fontSize: "0.68rem",
    color: "#8b95a5",
    background: "rgba(127, 127, 127, 0.08)",
    borderTop: "1px solid rgba(127, 127, 127, 0.18)",
  },
  ".cm-lsp-no-kernel-hint-dismiss": {
    border: "none",
    background: "transparent",
    color: "inherit",
    cursor: "pointer",
    fontSize: "0.72rem",
    lineHeight: "1",
    padding: "0 2px",
    opacity: "0.7",
  },
  ".cm-lsp-no-kernel-hint-dismiss:hover": { opacity: "1" },
});

// Resolved fresh per request so a live kernel selection / flow change takes effect.
function lspCtxGetter(opts: NotebookEditorOptions): () => LspContext {
  return () => ({
    kernelId: opts.getKernelId?.() ?? null,
    flowId: opts.getFlowId?.() ?? 0,
    nodeId: opts.getNodeId?.(),
  });
}

// No docs panel while typing (PyCharm / VS Code behaviour); docs live on hover.
function withoutInfo(source: CompletionSource): CompletionSource {
  return async (context) => {
    const result = await source(context);
    if (!result) return result;
    return { ...result, options: result.options.map((o) => ({ ...o, info: undefined })) };
  };
}

// The full source list for autocompletion({override}) — exported so it unit-tests headlessly.
export function buildNotebookCompletionSources(opts: NotebookEditorOptions): CompletionSource[] {
  const getInputNames = opts.getInputNames ?? (() => []);
  const getUpstreamColumns = opts.getUpstreamColumns ?? (() => []);
  const getPrior = opts.getPriorCellCodes ?? (() => []);
  const getLspCtx = lspCtxGetter(opts);
  const isLspActive = lspActiveFor(getLspCtx);
  const fb = (s: CompletionSource) => fallbackWhenNoLsp(s, isLspActive);
  const na = notInAsBinding; // identifier sources skip the `as <name>` binding position

  return [
    na(createIdentifierCompletionSource(getLspCtx, getPrior)),
    // Custom completions Jedi can't derive from introspection — always on.
    na(catalogRefChainCompletions),
    na(createRefVariableCompletions(getPrior)),
    // String-literal content sources Jedi can't provide — always on.
    createNamedInputCompletions(getInputNames),
    createUpstreamColumnCompletions(getUpstreamColumns),
    // Sources Jedi subsumes via live-namespace introspection: fallback only.
    na(fb(flowfileApiCompletions)),
    na(fb(globalIdentifierCompletions)),
    na(fb(polarsModuleCompletions)),
    na(fb(createPolarsExprCompletions(getPrior))),
    na(fb(localCompletionSource)),
    na(fb(globalCompletion)),
  ].map(withoutInfo);
}

export function buildNotebookEditorExtensions(opts: NotebookEditorOptions): Extension[] {
  const runAdvance = opts.onRunAdvance ?? opts.onRun;
  const getLspCtx = lspCtxGetter(opts);

  const runKeymap = keymap.of([
    { key: "Shift-Enter", run: () => (opts.onRun(), true) },
    { key: "Mod-Enter", run: () => (runAdvance(), true) },
  ]);

  const tabKeymap = keymap.of([
    { key: "Tab", run: (view: EditorView) => (acceptCompletion(view) ? true : indentMore(view)) },
    { key: "Shift-Tab", run: (view: EditorView) => indentLess(view) },
  ]);

  return [
    python(),
    createLspHover(getLspCtx),
    createLspSignature(getLspCtx),
    createLspDiagnostics(getLspCtx),
    createNoKernelHint(getLspCtx),
    oneDark,
    cellEditorTheme,
    EditorState.tabSize.of(4),
    autocompletion({
      override: buildNotebookCompletionSources(opts),
      defaultKeymap: true,
    }),
    bodyTooltips(),
    Prec.highest(runKeymap),
    Prec.high(tabKeymap),
  ];
}
