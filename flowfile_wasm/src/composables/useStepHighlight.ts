/**
 * Highlight one node's block inside the whole generated script.
 *
 * The walkthrough shows the entire script rather than an isolated snippet, so
 * you can see where the step you are reading about sits in the bigger picture.
 * This is the decoration that marks it, updated by a StateEffect so stepping
 * never has to rebuild the editor (which would lose scroll position).
 */

import { StateEffect, StateField, type Extension } from '@codemirror/state'
import { Decoration, EditorView, type DecorationSet } from '@codemirror/view'

export interface StepRange {
  /** 1-based inclusive line numbers. */
  from: number
  to: number
}

export const setStepRange = StateEffect.define<StepRange | null>()

const currentLine = Decoration.line({ class: 'cm-step-line' })
const firstLine = Decoration.line({ class: 'cm-step-line cm-step-first' })
const lastLine = Decoration.line({ class: 'cm-step-line cm-step-last' })

const stepField = StateField.define<DecorationSet>({
  create: () => Decoration.none,
  update(decorations, transaction) {
    for (const effect of transaction.effects) {
      if (!effect.is(setStepRange)) continue
      const range = effect.value
      if (!range) return Decoration.none

      const doc = transaction.state.doc
      const from = Math.max(1, Math.min(doc.lines, range.from))
      const to = Math.max(from, Math.min(doc.lines, range.to))
      const marks = []
      for (let line = from; line <= to; line++) {
        const decoration = line === from ? firstLine : line === to ? lastLine : currentLine
        marks.push(decoration.range(doc.line(line).from))
      }
      return Decoration.set(marks)
    }
    return transaction.docChanged ? Decoration.none : decorations
  },
  provide: field => EditorView.decorations.from(field)
})

const stepTheme = EditorView.baseTheme({
  '.cm-step-line': {
    backgroundColor: 'rgba(56, 189, 248, 0.16)',
    boxShadow: 'inset 3px 0 0 rgba(56, 189, 248, 0.9)'
  },
  '.cm-step-first': { borderTopLeftRadius: '3px', borderTopRightRadius: '3px' },
  '.cm-step-last': { borderBottomLeftRadius: '3px', borderBottomRightRadius: '3px' },
  '.cm-glossary': {
    maxWidth: '340px',
    padding: '8px 10px',
    borderRadius: '6px',
    border: '1px solid rgba(148, 163, 184, 0.35)',
    background: '#1e293b',
    color: '#e2e8f0',
    fontFamily: 'system-ui, sans-serif',
    lineHeight: '1.5'
  },
  '.cm-glossary-name': {
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '12px',
    fontWeight: '600',
    color: '#7dd3fc',
    marginBottom: '3px'
  },
  '.cm-glossary-summary': { fontSize: '12px' },
  '.cm-glossary-example': {
    margin: '6px 0 0',
    padding: '5px 7px',
    borderRadius: '4px',
    background: 'rgba(15, 23, 42, 0.85)',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '11px',
    whiteSpace: 'pre-wrap',
    color: '#cbd5e1'
  }
})

export function stepHighlight(): Extension {
  return [stepField, stepTheme]
}

/** Move the highlight and bring it into view. */
export function showStep(view: EditorView, range: StepRange | null): void {
  const effects: StateEffect<unknown>[] = [setStepRange.of(range)]
  if (range) {
    const doc = view.state.doc
    const line = doc.line(Math.max(1, Math.min(doc.lines, range.from)))
    effects.push(EditorView.scrollIntoView(line.from, { y: 'center' }))
  }
  view.dispatch({ effects })
}
