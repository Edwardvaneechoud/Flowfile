/**
 * The two halves of "check the learner's own work": instrumenting whatever is
 * in the editor, and comparing a run's rows against the canvas BY VALUE.
 * CPython-level proof that instrumented buffers capture correctly lives in
 * plain-python-walkthrough.test.ts; these pin the pure logic.
 */

import { describe, it, expect } from 'vitest'
import { instrumentScript, compareTables, type CanvasPreview } from '../../src/composables/usePlainTrace'

describe('instrumentScript', () => {
  const script = [
    'def run_etl_pipeline():',
    '    # --- Source ---',
    '    rows = [',
    '        {"a": 1},',
    '    ]',
    '',
    '    # --- Filter ---',
    '    kept = []',
    '    for row in rows:',
    '        kept.append(row)',
    '',
    '    return kept'
  ].join('\n')

  it('inserts a capture after each block, indented like the block itself', () => {
    const out = instrumentScript(script, [
      { fromLine: 2, toLine: 5, varName: 'rows' },
      { fromLine: 7, toLine: 10, varName: 'kept' }
    ]).split('\n')
    // The last line of the filter block sits inside the loop (8 spaces); the
    // capture must land at block level (4 spaces), taken from the first line.
    expect(out[5]).toBe('    __steps__["rows"] = rows')
    expect(out[11]).toBe('    __steps__["kept"] = kept')
  })

  it('never shifts earlier ranges — insertion runs bottom-up', () => {
    const out = instrumentScript(script, [
      { fromLine: 7, toLine: 10, varName: 'kept' },
      { fromLine: 2, toLine: 5, varName: 'rows' }
    ])
    expect(out.indexOf('__steps__["rows"]')).toBeLessThan(out.indexOf('__steps__["kept"]'))
  })

  it('skips a capture whose range fell off the document', () => {
    const out = instrumentScript(script, [{ fromLine: 90, toLine: 99, varName: 'ghost' }])
    expect(out).toBe(script)
  })
})

describe('compareTables', () => {
  const canvas = (columns: string[], rows: unknown[][], totalRows = rows.length): CanvasPreview => ({
    columns,
    rows,
    totalRows
  })

  it('fails on a row-count difference, naming both counts', () => {
    const verdict = compareTables([{ a: 1 }], canvas(['a'], [[1], [2]]))
    expect(verdict.status).toBe('mismatch')
    expect(verdict.message).toContain('2 rows')
    expect(verdict.message).toContain('1')
  })

  it('matches two empty results', () => {
    expect(compareTables([], canvas(['a'], [], 0)).status).toBe('match')
  })

  it('names a missing and an extra column', () => {
    expect(compareTables([{ a: 1 }], canvas(['a', 'b'], [[1, 2]])).message).toContain('missing the column "b"')
    expect(compareTables([{ a: 1, c: 3 }], canvas(['a'], [[1]])).message).toContain('extra column "c"')
  })

  it('catches wrong VALUES even when the row count is right', () => {
    const verdict = compareTables([{ a: 1, b: 'x' }], canvas(['a', 'b'], [[1, 'y']]))
    expect(verdict.status).toBe('mismatch')
    expect(verdict.message).toContain('Row 1, column "b"')
    expect(verdict.message).toContain('"x"')
    expect(verdict.message).toContain('"y"')
  })

  it('is order-sensitive: a wrongly sorted result is a mismatch', () => {
    const verdict = compareTables(
      [{ a: 2 }, { a: 1 }],
      canvas(['a'], [[1], [2]])
    )
    expect(verdict.status).toBe('mismatch')
  })

  it('tolerates float representation noise', () => {
    expect(compareTables([{ a: 449.99999999999994 }], canvas(['a'], [[450.0]])).status).toBe('match')
  })

  it('tolerates the T-vs-space datetime separator across the two bridges', () => {
    expect(
      compareTables([{ t: '2024-01-02 03:04:05' }], canvas(['t'], [['2024-01-02T03:04:05']])).status
    ).toBe('match')
  })

  it('treats null and None-rendered-as-null as equal, and null vs value as different', () => {
    expect(compareTables([{ a: null }], canvas(['a'], [[null]])).status).toBe('match')
    const verdict = compareTables([{ a: null }], canvas(['a'], [[0]]))
    expect(verdict.status).toBe('mismatch')
    expect(verdict.message).toContain('nothing (None)')
  })

  it('does not let a boolean pass as its string rendering', () => {
    expect(compareTables([{ a: 'true' }], canvas(['a'], [[true]])).status).toBe('mismatch')
  })

  it('accepts reordered columns — dict rows carry no meaningful order', () => {
    expect(compareTables([{ b: 2, a: 1 }], canvas(['a', 'b'], [[1, 2]])).status).toBe('match')
  })

  it('says exactly what a match checked', () => {
    const verdict = compareTables([{ a: 1 }, { a: 2 }], canvas(['a'], [[1], [2]]))
    expect(verdict.status).toBe('match')
    expect(verdict.message).toContain('2 rows')
    expect(verdict.message).toContain('1 columns')
    expect(verdict.message).toContain('first 2 rows')
  })

  it('checks values only as far as the canvas preview reaches', () => {
    // 60 script rows, canvas preview holds 3 of 60: counts + 3 rows checked.
    const scriptRows = Array.from({ length: 60 }, (_, i) => ({ a: i }))
    const verdict = compareTables(scriptRows, canvas(['a'], [[0], [1], [2]], 60))
    expect(verdict.status).toBe('match')
    expect(verdict.message).toContain('first 3 rows')
  })
})
