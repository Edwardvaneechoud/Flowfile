/**
 * The step highlight has to survive the reader editing the script, which is the
 * whole reason the ranges are document offsets rather than line numbers.
 */

import { describe, it, expect } from 'vitest'
import { EditorState } from '@codemirror/state'
import {
  seedSteps,
  selectStep,
  stepDecorations,
  stepHighlight,
  stepLines,
  stepState
} from '../../src/composables/useStepHighlight'

const DOC = [
  'def run_etl_pipeline():', // 1
  '    # --- Manual input ---', // 2
  '    source = [', // 3
  '        {"product": "Widget"},', // 4
  '    ]', // 5
  '', // 6
  '    # --- Filter ---', // 7
  '    filtered = []', // 8
  '    for row in source:', // 9
  '        filtered.append(row)', // 10
  '', // 11
  '    # --- Group by ---', // 12
  '    grouped = {}', // 13
  '    for row in filtered:', // 14
  '        grouped.setdefault(row["product"], []).append(row)', // 15
  '', // 16
  '    return grouped' // 17
].join('\n')

const RANGES = [
  { from: 2, to: 5 },
  { from: 7, to: 10 },
  { from: 12, to: 15 }
]

function seeded(current = 2) {
  const state = EditorState.create({ doc: DOC, extensions: [stepHighlight()] })
  return state.update({ effects: [seedSteps.of(RANGES), selectStep.of(current)] }).state
}

/** The lines the decoration set actually paints. */
function paintedLines(state: EditorState): number[] {
  const lines: number[] = []
  const iterator = state.field(stepDecorations).iter()
  while (iterator.value) {
    lines.push(state.doc.lineAt(iterator.from).number)
    iterator.next()
  }
  return lines
}

/** What pressing Enter at the end of a 1-based line and typing does. */
function insertLineAfter(state: EditorState, line: number, text: string): EditorState {
  const at = state.doc.line(line).to
  return state.update({ changes: { from: at, insert: `\n${text}` } }).state
}

describe('step highlight anchors', () => {
  it('reads back the range it was seeded with', () => {
    expect(stepLines(seeded(2))).toEqual({ from: 12, to: 15 })
    expect(stepLines(seeded(2), 0)).toEqual({ from: 2, to: 5 })
  })

  it('shifts a later step when a line is inserted above it', () => {
    const state = insertLineAfter(seeded(2), 11, '    # a note to self')
    expect(stepLines(state, 2)).toEqual({ from: 13, to: 16 })
    expect(stepLines(state, 0)).toEqual({ from: 2, to: 5 })
  })

  it('grows the block when a line is inserted inside it', () => {
    const state = insertLineAfter(seeded(2), 13, '    # still part of the group by')
    expect(stepLines(state, 2)).toEqual({ from: 12, to: 16 })
  })

  it('keeps painting after an edit', () => {
    const state = insertLineAfter(seeded(2), 13, '    # still part of the group by')
    expect(paintedLines(state)).toEqual([12, 13, 14, 15, 16])
  })

  it('selects the block that still holds the step, not its original lines', () => {
    // Two lines pushed in above step 3; re-selecting must not re-read lineStart.
    let state = insertLineAfter(seeded(0), 11, '    # one')
    state = insertLineAfter(state, 12, '    # two')
    state = state.update({ effects: selectStep.of(2) }).state
    const range = stepLines(state)!
    const block = state.doc.sliceString(state.doc.line(range.from).from, state.doc.line(range.to).to)
    expect(block).toContain('grouped = {}')
    expect(block).not.toContain('# one')
  })

  it('re-derives from line numbers when the document is replaced', () => {
    const edited = insertLineAfter(seeded(2), 11, '    # pushed down')
    const replaced = edited.update({
      changes: { from: 0, to: edited.doc.length, insert: DOC },
      effects: [seedSteps.of(RANGES), selectStep.of(2)]
    }).state
    expect(stepLines(replaced)).toEqual({ from: 12, to: 15 })
  })

  it('cannot let the label and the paint disagree', () => {
    const state = insertLineAfter(seeded(1), 8, '    # inside the filter')
    const range = stepLines(state)!
    const painted = paintedLines(state)
    expect([painted[0], painted[painted.length - 1]]).toEqual([range.from, range.to])
  })

  it('clears when re-seeded with nothing', () => {
    const state = seeded(2).update({ effects: [seedSteps.of([]), selectStep.of(-1)] }).state
    expect(stepLines(state)).toBeNull()
    expect(paintedLines(state)).toEqual([])
    expect(state.field(stepState).anchors).toEqual([])
  })
})
