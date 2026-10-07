/**
 * How a notebook cell's editor looks: it sits on the cell's own surface and takes
 * its colours from the app's theme tokens, so it follows light and dark mode.
 * The token colours are the `--nb-syntax-*` variables NotebookPane.vue defines.
 */
import { syntaxHighlighting } from '@codemirror/language'
import type { Extension } from '@codemirror/state'
import { EditorView } from '@codemirror/view'
import { tagHighlighter, tags } from '@lezer/highlight'

const highlighter = tagHighlighter([
  { tag: tags.comment, class: 'nb-tok-comment' },
  { tag: [tags.keyword, tags.operatorKeyword, tags.controlKeyword, tags.definitionKeyword, tags.moduleKeyword], class: 'nb-tok-keyword' },
  { tag: [tags.string, tags.special(tags.string)], class: 'nb-tok-string' },
  { tag: [tags.number, tags.bool, tags.null, tags.atom], class: 'nb-tok-number' },
  { tag: [tags.function(tags.variableName), tags.function(tags.propertyName)], class: 'nb-tok-call' },
  { tag: tags.definition(tags.variableName), class: 'nb-tok-name' },
  { tag: tags.function(tags.definition(tags.variableName)), class: 'nb-tok-call' },
  { tag: tags.propertyName, class: 'nb-tok-property' },
  { tag: [tags.operator, tags.punctuation, tags.bracket], class: 'nb-tok-operator' }
])

const theme = EditorView.theme({
  '&': {
    backgroundColor: 'transparent',
    color: 'var(--color-text-primary)',
    fontSize: '12.5px'
  },
  '&.cm-focused': { outline: 'none' },
  '.cm-scroller': {
    fontFamily: 'var(--font-family-mono)',
    lineHeight: '1.6'
  },
  '.cm-content': {
    padding: '8px 0',
    caretColor: 'var(--color-accent)'
  },
  '.cm-line': { padding: '0 12px 0 6px' },
  '.cm-gutters': {
    backgroundColor: 'transparent',
    color: 'var(--color-text-muted)',
    border: 'none'
  },
  '.cm-lineNumbers .cm-gutterElement': {
    minWidth: '26px',
    padding: '0 6px 0 4px',
    fontSize: '11px'
  },
  '.cm-gutter.cm-foldGutter': { display: 'none !important' },
  '.cm-activeLine, .cm-activeLineGutter': { backgroundColor: 'transparent' },
  '&.cm-focused .cm-activeLine': { backgroundColor: 'var(--nb-active-line)' },
  '&.cm-focused .cm-activeLineGutter': { color: 'var(--color-text-secondary)' },
  '.cm-cursor, .cm-dropCursor': { borderLeftColor: 'var(--color-accent)', borderLeftWidth: '2px' },
  '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground': {
    backgroundColor: 'var(--nb-selection)'
  },
  '.cm-matchingBracket, &.cm-focused .cm-matchingBracket': {
    backgroundColor: 'var(--nb-selection)',
    outline: 'none'
  },
  '.cm-tooltip': {
    backgroundColor: 'var(--color-background-primary)',
    border: '1px solid var(--color-border-primary)',
    borderRadius: '6px',
    color: 'var(--color-text-primary)',
    boxShadow: 'var(--shadow-md)'
  },
  '.cm-tooltip.cm-tooltip-autocomplete > ul': {
    fontFamily: 'var(--font-family-mono)',
    fontSize: '12px',
    maxHeight: '14em'
  },
  '.cm-tooltip.cm-tooltip-autocomplete > ul > li': { padding: '2px 10px' },
  '.cm-tooltip.cm-tooltip-autocomplete > ul > li[aria-selected]': {
    backgroundColor: 'var(--nb-selection)',
    color: 'var(--color-text-primary)'
  },
  '.cm-completionMatchedText': { textDecoration: 'none', color: 'var(--color-accent)', fontWeight: '600' },
  '.cm-completionDetail': { color: 'var(--color-text-muted)', fontStyle: 'normal', marginLeft: '1em' }
})

/** The theme and syntax colours every notebook cell shares. */
export const notebookEditorLook: Extension[] = [theme, syntaxHighlighting(highlighter)]
