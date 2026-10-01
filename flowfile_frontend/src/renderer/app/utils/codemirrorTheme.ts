// Token-based editor look for light and dark, swapped live with the app theme.
import { Compartment, RangeSetBuilder, countColumn, type Extension } from "@codemirror/state";
import {
  Decoration,
  EditorView,
  ViewPlugin,
  type DecorationSet,
  type ViewUpdate,
} from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags as t } from "@lezer/highlight";

interface SyntaxPalette {
  identifier: string;
  keyword: string;
  fn: string;
  string: string;
  number: string;
  comment: string;
  operator: string;
}

const highlightStyleFor = (p: SyntaxPalette) =>
  HighlightStyle.define([
    {
      tag: [t.variableName, t.propertyName, t.definition(t.variableName), t.labelName],
      color: p.identifier,
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
      color: p.keyword,
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
      color: p.fn,
    },
    { tag: [t.string, t.special(t.string), t.docString, t.character, t.regexp], color: p.string },
    { tag: [t.escape, t.number, t.integer, t.float], color: p.number },
    {
      tag: [t.comment, t.lineComment, t.blockComment, t.docComment],
      color: p.comment,
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
      color: p.operator,
    },
    { tag: t.invalid, color: "var(--color-danger)" },
  ]);

const lightHighlightStyle = highlightStyleFor({
  identifier: "var(--color-text-primary)",
  keyword: "var(--color-accent-purple)",
  fn: "var(--color-accent-dark)",
  string: "var(--color-success-dark)",
  number: "var(--color-warning-hover)",
  comment: "var(--color-text-muted)",
  operator: "var(--color-text-secondary)",
});

// Dark tokens resolve to light tints (--color-success-dark is #6ee7b7, --color-warning-dark #fbbf24).
const darkHighlightStyle = highlightStyleFor({
  identifier: "var(--color-text-primary)",
  keyword: "#a5b4fc",
  fn: "var(--color-accent-light)",
  string: "var(--color-success-dark)",
  number: "var(--color-warning-dark)",
  comment: "var(--color-text-tertiary)",
  operator: "var(--color-text-secondary)",
});

const bracketChrome = {
  "&.cm-focused .cm-matchingBracket, &.cm-focused .cm-nonmatchingBracket": {
    backgroundColor: "var(--color-focus-ring-accent-strong) !important",
    outline: "none",
  },
};

const LIGHT: Extension = [
  EditorView.theme(bracketChrome, { dark: false }),
  syntaxHighlighting(lightHighlightStyle),
];
const DARK: Extension = [
  EditorView.theme(bracketChrome, { dark: true }),
  syntaxHighlighting(darkHighlightStyle),
];

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

/** Editor theme for notebook cells and the code pane viewer: token highlighting in either app theme. */
export function flowfileEditorTheme(): Extension {
  return [themeCompartment.of(modeTheme()), sharedChrome, themeTracker];
}

// Matches the base theme's `.cm-line` left padding, which the inline padding replaces.
const LINE_PAD_LEFT = 6;

function hangingIndentDecorations(view: EditorView): DecorationSet {
  const builder = new RangeSetBuilder<Decoration>();
  const charWidth = view.defaultCharacterWidth;
  const { doc, tabSize } = view.state;
  for (const { from, to } of view.visibleRanges) {
    for (let pos = from; pos <= to; ) {
      const line = doc.lineAt(pos);
      const lead = /^[ \t]*/.exec(line.text)![0];
      if (lead.length > 0 && lead.length < line.length) {
        const px = countColumn(lead, tabSize) * charWidth;
        builder.add(
          line.from,
          line.from,
          Decoration.line({
            attributes: { style: `padding-left: ${px + LINE_PAD_LEFT}px; text-indent: -${px}px` },
          }),
        );
      }
      pos = line.to + 1;
    }
  }
  return builder.finish();
}

/** Wrapped continuation lines align under their line's indentation instead of column 0. */
export const hangingIndent = ViewPlugin.fromClass(
  class {
    decorations: DecorationSet;
    charWidth: number;
    constructor(view: EditorView) {
      this.charWidth = view.defaultCharacterWidth;
      this.decorations = hangingIndentDecorations(view);
    }
    update(update: ViewUpdate) {
      const charWidth = update.view.defaultCharacterWidth;
      if (update.docChanged || update.viewportChanged || charWidth !== this.charWidth) {
        this.charWidth = charWidth;
        this.decorations = hangingIndentDecorations(update.view);
      }
    }
  },
  { decorations: (v) => v.decorations },
);
