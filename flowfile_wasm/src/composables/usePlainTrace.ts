/**
 * Tracing and checking the walkthrough script AS THE LEARNER HAS IT.
 *
 * instrumentScript() inserts a `__steps__[...] = ...` capture after each step's
 * block in whatever text is in the editor — the generated script or an edited
 * one — using the live line ranges the step highlight tracks through edits.
 * That is what lets the Data tab show the learner's own solution, not just the
 * generated script. buildTraceCode() in usePlainPythonGeneration remains the
 * CPython-tested reference for the same capture contract.
 *
 * compareTables() checks a script run against the canvas result by VALUES, not
 * just row count — a wrong answer with the right cardinality must not pass.
 */

export interface StepCapture {
  /** 1-based inclusive line range of the step's block in the current buffer. */
  fromLine: number
  toLine: number
  /** The variable the step binds — also the capture key. */
  varName: string
}

/**
 * Insert a capture statement after each step's block.
 *
 * The capture is indented like the block's first line (the section banner),
 * which is the block's own statement level — the last line of a block may sit
 * deeper, inside a loop. `__steps__` itself is never defined here: the run
 * harness pre-seeds it in the exec namespace, so a raising exercise stub still
 * leaves everything captured before it readable.
 */
export function instrumentScript(text: string, captures: StepCapture[]): string {
  const lines = text.split('\n')
  // Bottom-up, so earlier insertions never shift later line numbers.
  const ordered = [...captures].sort((a, b) => b.toLine - a.toLine)
  for (const capture of ordered) {
    if (capture.toLine < 1 || capture.toLine > lines.length) continue
    const first = lines[capture.fromLine - 1] ?? ''
    const indent = first.match(/^\s*/)?.[0] ?? '    '
    lines.splice(capture.toLine, 0, `${indent}__steps__[${JSON.stringify(capture.varName)}] = ${capture.varName}`)
  }
  return lines.join('\n')
}

/** What the canvas computed for the node the script's return value maps to. */
export interface CanvasPreview {
  columns: string[]
  /** Preview rows as value arrays, aligned with `columns`. */
  rows: unknown[][]
  totalRows: number
}

export interface CompareResult {
  /** 'info' is guidance ("run the flow first"), not a verdict — style it neutrally. */
  status: 'match' | 'mismatch' | 'info'
  message: string
}

/** How many preview rows the value check walks. */
export const COMPARE_SAMPLE = 50

const show = (value: unknown): string =>
  value === null || value === undefined ? 'nothing (None)' : JSON.stringify(value)

/**
 * Loose cell equality across the two serialization paths: script rows arrive
 * via json.dumps(default=str), canvas rows via the engine's to_json_safe_value.
 * Datetimes differ only by the 'T' separator, floats by representation noise.
 */
function cellsEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (a === null || a === undefined || b === null || b === undefined) {
    return (a === null || a === undefined) && (b === null || b === undefined)
  }
  if (typeof a === 'number' && typeof b === 'number') {
    if (Number.isNaN(a) && Number.isNaN(b)) return true
    return Math.abs(a - b) <= 1e-9 * Math.max(Math.abs(a), Math.abs(b), 1)
  }
  if (typeof a === 'boolean' || typeof b === 'boolean') return false
  const textA = String(a).replace('T', ' ')
  const textB = String(b).replace('T', ' ')
  if (textA === textB) return true
  // One side numeric, the other its string form (default=str casts what JSON cannot).
  const numberA = Number(textA)
  const numberB = Number(textB)
  if (textA.trim() === '' || textB.trim() === '' || Number.isNaN(numberA) || Number.isNaN(numberB)) return false
  return Math.abs(numberA - numberB) <= 1e-9 * Math.max(Math.abs(numberA), Math.abs(numberB), 1)
}

/**
 * Value-aware comparison, honest about exactly what was checked.
 *
 * Row count first, then column sets, then cell values over the first
 * COMPARE_SAMPLE rows in order — order matters, a wrongly-sorted solution is a
 * mismatch. Column order is NOT compared: script rows are dicts, and a learner
 * who builds the right table with reordered keys deserves a pass.
 */
export function compareTables(scriptRows: Record<string, unknown>[], canvas: CanvasPreview): CompareResult {
  if (scriptRows.length !== canvas.totalRows) {
    return {
      status: 'mismatch',
      message: `Canvas produced ${canvas.totalRows} rows; this script produced ${scriptRows.length}.`
    }
  }
  if (scriptRows.length === 0) {
    return { status: 'match', message: 'Matches the canvas — both produced 0 rows.' }
  }

  const scriptColumns = Object.keys(scriptRows[0])
  const missing = canvas.columns.find(column => !scriptColumns.includes(column))
  if (missing !== undefined) {
    return { status: 'mismatch', message: `The script's rows are missing the column "${missing}".` }
  }
  const extra = scriptColumns.find(column => !canvas.columns.includes(column))
  if (extra !== undefined) {
    return { status: 'mismatch', message: `The script's rows have an extra column "${extra}".` }
  }

  const checked = Math.min(scriptRows.length, canvas.rows.length, COMPARE_SAMPLE)
  for (let row = 0; row < checked; row++) {
    for (let index = 0; index < canvas.columns.length; index++) {
      const column = canvas.columns[index]
      const scriptValue = scriptRows[row][column]
      const canvasValue = canvas.rows[row][index]
      if (!cellsEqual(scriptValue, canvasValue)) {
        return {
          status: 'mismatch',
          message:
            `Row ${row + 1}, column "${column}": the script produced ${show(scriptValue)}, ` +
            `the canvas ${show(canvasValue)}.`
        }
      }
    }
  }

  return {
    status: 'match',
    message:
      `Matches the canvas — ${canvas.totalRows} rows, ${canvas.columns.length} columns, ` +
      `values checked on the first ${checked} row${checked === 1 ? '' : 's'}.`
  }
}
