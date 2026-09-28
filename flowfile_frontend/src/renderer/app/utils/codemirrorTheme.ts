// Token-based light editor look + One Dark in dark mode, swapped live with the app theme.
import { Compartment, type Extension } from "@codemirror/state";
import { EditorView, ViewPlugin } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { oneDark } from "@codemirror/theme-one-dark";
import { tags as t } from "@lezer/highlight";

const lightHighlightStyle = HighlightStyle.define([
  {
    tag: [t.variableName, t.propertyName, t.definition(t.variableName), t.labelName],
    color: "var(--color-text-primary)",
  },
  {
    tag: [
      t.keyword,
      t.controlKeyword,
      t.definitionKeyword,
      t.moduleKeyword,
      t.operatorKeyword,
      t.bool,
      t.null,
      t.self,
    ],
    color: "var(--color-accent-purple)",
  },
  {
    tag: [
      t.function(t.variableName),
      t.function(t.propertyName),
      t.function(t.definition(t.variableName)),
      t.definition(t.function(t.variableName)),
      t.className,
      t.definition(t.className),
      t.typeName,
      t.namespace,
      t.meta,
    ],
    color: "var(--color-accent-dark)",
  },
  {
    tag: [t.string, t.special(t.string), t.docString, t.character, t.regexp],
    color: "var(--color-success-dark)",
  },
  { tag: t.escape, color: "var(--color-warning-hover)" },
  { tag: [t.number, t.integer, t.float], color: "var(--color-warning-hover)" },
  {
    tag: [t.comment, t.lineComment, t.blockComment, t.docComment],
    color: "var(--color-text-muted)",
    fontStyle: "italic",
  },
  {
    tag: [
      t.operator,
      t.derefOperator,
      t.arithmeticOperator,
      t.logicOperator,
      t.compareOperator,
      t.updateOperator,
      t.definitionOperator,
      t.punctuation,
      t.separator,
      t.paren,
      t.squareBracket,
      t.brace,
    ],
    color: "var(--color-text-secondary)",
  },
  { tag: t.invalid, color: "var(--color-danger)" },
]);

const lightChrome = EditorView.theme(
  {
    "&.cm-focused .cm-matchingBracket, &.cm-focused .cm-nonmatchingBracket": {
      backgroundColor: "var(--color-focus-ring-accent-strong) !important",
      outline: "none",
    },
  },
  { dark: false },
);

const LIGHT: Extension = [lightChrome, syntaxHighlighting(lightHighlightStyle)];
const DARK: Extension = oneDark;

// `!important` + the extra `.cm-editor` class outrank the global main.css `.cm-editor` rules.
const sharedChrome = EditorView.theme({
  "&": { fontSize: "12.5px" },
  "&.cm-editor.cm-focused": { outline: "none" },
  ".cm-content": { fontFamily: "var(--font-family-mono)" },
  "&.cm-editor .cm-gutters": {
    fontFamily: "var(--font-family-mono)",
    fontSize: "11px",
    backgroundColor: "var(--color-background-secondary) !important",
    color: "var(--color-text-muted) !important",
    borderRight: "1px solid var(--color-border-light) !important",
  },
  "&.cm-editor:not(.cm-focused) .cm-activeLine, &.cm-editor:not(.cm-focused) .cm-activeLineGutter":
    { backgroundColor: "transparent !important" },
});

const themeCompartment = new Compartment();
const views = new Set<EditorView>();
let observer: MutationObserver | null = null;

const isDark = (): boolean =>
  typeof document !== "undefined" && document.documentElement.getAttribute("data-theme") === "dark";

const modeTheme = (): Extension => (isDark() ? DARK : LIGHT);

function syncView(view: EditorView) {
  const next = modeTheme();
  if (themeCompartment.get(view.state) !== next) {
    view.dispatch({ effects: themeCompartment.reconfigure(next) });
  }
}

// theme-store toggles `data-theme` on <html>; follow it for every open editor.
function ensureObserver() {
  if (observer || typeof MutationObserver === "undefined") return;
  observer = new MutationObserver(() => views.forEach(syncView));
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
}

const themeTracker = ViewPlugin.define((view) => {
  views.add(view);
  ensureObserver();
  // A state built before a toggle carries the old mode; correct it once the view exists.
  queueMicrotask(() => {
    if (views.has(view)) syncView(view);
  });
  return { destroy: () => views.delete(view) };
});

/** Editor theme for notebook cells and the code pane viewer: light token highlighting, One Dark in dark mode. */
export function flowfileEditorTheme(): Extension {
  return [themeCompartment.of(modeTheme()), sharedChrome, themeTracker];
}
