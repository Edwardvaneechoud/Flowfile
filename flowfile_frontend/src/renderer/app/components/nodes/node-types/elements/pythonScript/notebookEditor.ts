// Shared CodeMirror setup for both notebook cell editors (avoids drift).
import type { Extension } from "@codemirror/state";
import { EditorState, Prec } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { python, pythonLanguage } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { acceptCompletion, autocompletion } from "@codemirror/autocomplete";
import { indentLess, indentMore } from "@codemirror/commands";
import { bodyTooltips } from "@/utils/codemirrorTooltips";
import {
  catalogRefChainCompletions,
  createNamedInputCompletions,
  createPolarsExprCompletions,
  createRefVariableCompletions,
  createScopeCompletions,
  createUpstreamColumnCompletions,
  flowfileApiCompletions,
  globalIdentifierCompletions,
  polarsModuleCompletions,
} from "./flowfileCompletions";
import {
  createLspCompletionSource,
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

export function buildNotebookEditorExtensions(opts: NotebookEditorOptions): Extension[] {
  const getInputNames = opts.getInputNames ?? (() => []);
  const getUpstreamColumns = opts.getUpstreamColumns ?? (() => []);
  const getPrior = opts.getPriorCellCodes ?? (() => []);
  const runAdvance = opts.onRunAdvance ?? opts.onRun;

  // Resolved fresh per request so a live kernel selection / flow change takes effect.
  const getLspCtx = (): LspContext => ({
    kernelId: opts.getKernelId?.() ?? null,
    flowId: opts.getFlowId?.() ?? 0,
    nodeId: opts.getNodeId?.(),
  });
  // Static sources that Jedi subsumes run only as a fallback (no kernel / LSP off),
  // so there's no overlap once Jedi is active.
  const isLspActive = lspActiveFor(getLspCtx);
  const fb = (s: Parameters<typeof fallbackWhenNoLsp>[0]) => fallbackWhenNoLsp(s, isLspActive);
  const na = notInAsBinding; // identifier sources skip the `as <name>` binding position

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
    // Jedi (kernel-backed) sources first; resolve null/empty when no kernel/LSP -> fallbacks serve.
    pythonLanguage.data.of({ autocomplete: na(createLspCompletionSource(getLspCtx)) }),
    createLspHover(getLspCtx),
    createLspSignature(getLspCtx),
    createLspDiagnostics(getLspCtx),
    createNoKernelHint(getLspCtx),
    // Sources Jedi genuinely subsumes via live-namespace introspection: fallback only
    // (suppressed while Jedi is active) to avoid duplicate suggestions.
    pythonLanguage.data.of({ autocomplete: na(fb(flowfileApiCompletions)) }),
    pythonLanguage.data.of({ autocomplete: na(fb(globalIdentifierCompletions)) }),
    pythonLanguage.data.of({ autocomplete: na(fb(polarsModuleCompletions)) }),
    pythonLanguage.data.of({ autocomplete: na(fb(createPolarsExprCompletions(getPrior))) }),
    // Custom completions Jedi can't derive from introspection — always on, coexisting with
    // Jedi: catalog ref-chain snippets, ref-typed variable members, and symbols from prior
    // cells not yet executed (Jedi only sees the executed cells' live namespace).
    pythonLanguage.data.of({ autocomplete: na(catalogRefChainCompletions) }),
    pythonLanguage.data.of({ autocomplete: na(createRefVariableCompletions(getPrior)) }),
    pythonLanguage.data.of({ autocomplete: na(createScopeCompletions(getPrior)) }),
    // String-literal content sources Jedi can't provide — always on (no overlap):
    pythonLanguage.data.of({ autocomplete: createNamedInputCompletions(getInputNames) }),
    pythonLanguage.data.of({ autocomplete: createUpstreamColumnCompletions(getUpstreamColumns) }),
    oneDark,
    cellEditorTheme,
    EditorState.tabSize.of(4),
    autocompletion({ defaultKeymap: true, closeOnBlur: false }),
    bodyTooltips(),
    Prec.highest(runKeymap),
    Prec.high(tabKeymap),
  ];
}
