/**
 * Highlight one node's block inside the whole generated script.
 *
 * Ranges are kept as document offsets, not line numbers, so they follow the
 * text when the reader edits it. Line numbers are re-derived on read, which is
 * what makes the header label and the paint physically incapable of disagreeing.
 */

import { StateEffect, StateField, type EditorState, type Extension } from '@codemirror/state'
import { Decoration, EditorView, type DecorationSet } from '@codemirror/view'

export interface StepRange {
  /** 1-based inclusive line numbers, as the generator produces them. */
  from: number
  to: number
}

interface Anchor {
  from: number
  to: number
}

interface StepState {
  anchors: Anchor[]
  current: number
}

/** Re-seed every step from generator line numbers. Dispatch after a programmatic doc replacement. */
export const seedSteps = StateEffect.define<StepRange[]>()
/** Move the highlight to step i; -1 clears it. */
export const selectStep = StateEffect.define<number>()

const EMPTY: StepState = { anchors: [], current: -1 }

export const stepState = StateField.define<StepState>({
  create: () => EMPTY,
  update(value, tr) {
    let next = value
    // Ride the text. Start maps with assoc -1 and end with +1, so typing inside
    // a block grows the block instead of falling out of it.
    if (tr.docChanged && next.anchors.length > 0) {
      next = {
        current: next.current,
        anchors: next.anchors.map(anchor => ({
          from: tr.changes.mapPos(anchor.from, -1),
          to: tr.changes.mapPos(anchor.to, 1)
        }))
      }
    }
    for (const effect of tr.effects) {
      if (effect.is(seedSteps)) {
        const doc = tr.state.doc
        next = {
          current: next.current < 0 ? -1 : Math.min(next.current, effect.value.length - 1),
          anchors: effect.value.map(range => {
            const from = Math.max(1, Math.min(doc.lines, range.from))
            const to = Math.max(from, Math.min(doc.lines, range.to))
            return { from: doc.line(from).from, to: doc.line(to).to }
          })
        }
      } else if (effect.is(selectStep)) {
        next = { anchors: next.anchors, current: effect.value }
      }
    }
    return next
  }
})

const midLine = Decoration.line({ class: 'cm-step-line' })
const firstLine = Decoration.line({ class: 'cm-step-line cm-step-first' })
const lastLine = Decoration.line({ class: 'cm-step-line cm-step-last' })
const soleLine = Decoration.line({ class: 'cm-step-line cm-step-first cm-step-last' })

const lineSpan = (state: EditorState, anchor: Anchor): [number, number] => {
  const doc = state.doc
  const from = doc.lineAt(Math.max(0, Math.min(anchor.from, doc.length))).number
  const to = doc.lineAt(Math.max(0, Math.min(Math.max(anchor.to, anchor.from), doc.length))).number
  return [from, Math.max(from, to)]
}

const build = (state: EditorState): DecorationSet => {
  const { anchors, current } = state.field(stepState)
  const anchor = anchors[current]
  if (!anchor) return Decoration.none
  const [first, last] = lineSpan(state, anchor)
  const marks = []
  for (let line = first; line <= last; line++) {
    const decoration =
      first === last ? soleLine : line === first ? firstLine : line === last ? lastLine : midLine
    marks.push(decoration.range(state.doc.line(line).from))
  }
  return Decoration.set(marks)
}

// Declared after stepState so it reads the value this transaction produced.
export const stepDecorations = StateField.define<DecorationSet>({
  create: state => build(state),
  update: (_decorations, tr) => build(tr.state),
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
  return [stepState, stepDecorations, stepTheme]
}

/** The line range currently PAINTED. The header label reads this, never PlainStep. */
export function stepLines(state: EditorState, index?: number): StepRange | null {
  const field = state.field(stepState, false)
  if (!field) return null
  const anchor = field.anchors[index ?? field.current]
  if (!anchor) return null
  const [from, to] = lineSpan(state, anchor)
  return { from, to }
}

/** Does any part of the current step's block intersect the visible scrollport? */
export function stepVisible(view: EditorView): boolean {
  const field = view.state.field(stepState, false)
  const anchor = field ? field.anchors[field.current] : undefined
  if (!anchor) return true
  const length = view.state.doc.length
  // Rendered ranges extend past the scrollport, so ask geometry instead.
  const top = view.lineBlockAt(Math.min(anchor.from, length)).top
  const bottom = view.lineBlockAt(Math.min(Math.max(anchor.to, anchor.from), length)).bottom
  const viewTop = view.scrollDOM.scrollTop
  return bottom > viewTop + 4 && top < viewTop + view.scrollDOM.clientHeight - 4
}

/** Seed all N ranges and select one, in a single transaction. */
export function seedStepsIn(view: EditorView, ranges: StepRange[], current: number): void {
  view.dispatch({ effects: [seedSteps.of(ranges), selectStep.of(current)] })
}

/** Move the highlight and bring it into view, verifying against real geometry. */
export function showStep(view: EditorView, index: number): void {
  const anchor = view.state.field(stepState, false)?.anchors[index]
  view.dispatch({
    effects: anchor
      ? [selectStep.of(index), EditorView.scrollIntoView(anchor.from, { y: 'center', yMargin: 24 })]
      : [selectStep.of(index)]
  })
  if (!anchor) return
  // Line heights are estimated until measured, and lineWrapping makes the
  // estimate worse; confirm against the real geometry and retry once.
  view.requestMeasure({
    read: () => stepVisible(view),
    write: visible => {
      if (visible) return
      // Dispatching inside the measure cycle is refused ("update in progress"),
      // so hop out of it before retrying.
      requestAnimationFrame(() => {
        if (!view.dom.isConnected) return
        const again = view.state.field(stepState, false)?.anchors[index]
        if (again) {
          view.dispatch({ effects: EditorView.scrollIntoView(again.from, { y: 'center', yMargin: 24 }) })
        }
      })
    }
  })
}
