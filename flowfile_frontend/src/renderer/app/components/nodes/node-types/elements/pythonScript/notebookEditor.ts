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
import type { UpstreamColumn } from "./useUpstreamColumns";

export interface NotebookEditorOptions {
  onRun: () => void;
  onRunAdvance?: () => void; // Mod+Enter; falls back to onRun
  getInputNames?: () => string[];
  getUpstreamColumns?: () => UpstreamColumn[];
  getPriorCellCodes?: () => string[];
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
});

export function buildNotebookEditorExtensions(opts: NotebookEditorOptions): Extension[] {
  const getInputNames = opts.getInputNames ?? (() => []);
  const getUpstreamColumns = opts.getUpstreamColumns ?? (() => []);
  const getPrior = opts.getPriorCellCodes ?? (() => []);
  const runAdvance = opts.onRunAdvance ?? opts.onRun;

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
    pythonLanguage.data.of({ autocomplete: flowfileApiCompletions }),
    pythonLanguage.data.of({ autocomplete: globalIdentifierCompletions }),
    pythonLanguage.data.of({ autocomplete: catalogRefChainCompletions }),
    pythonLanguage.data.of({ autocomplete: createRefVariableCompletions(getPrior) }),
    pythonLanguage.data.of({ autocomplete: polarsModuleCompletions }),
    pythonLanguage.data.of({ autocomplete: createPolarsExprCompletions(getPrior) }),
    pythonLanguage.data.of({ autocomplete: createNamedInputCompletions(getInputNames) }),
    pythonLanguage.data.of({ autocomplete: createUpstreamColumnCompletions(getUpstreamColumns) }),
    pythonLanguage.data.of({ autocomplete: createScopeCompletions(getPrior) }),
    oneDark,
    cellEditorTheme,
    EditorState.tabSize.of(4),
    autocompletion({ defaultKeymap: true, closeOnBlur: false }),
    bodyTooltips(),
    Prec.highest(runKeymap),
    Prec.high(tabKeymap),
  ];
}
