/**
 * The exported Polars script must produce the same rows as the engine — the
 * guard for "generated code does not do what the canvas did". Skips cleanly
 * without a CPython that has Polars.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { useCodeGeneration } from '../../src/composables/useCodeGeneration'
import { findPython } from '../helpers/python-runtime'
import {
  PARITY_FIXTURES,
  POLARS_ONLY_FIXTURES,
  columnsOf,
  engineRows,
  normalise,
  runPythonScript,
  toFlow
} from '../helpers/parity'
import type { Fixture } from '../helpers/parity'

let python: string | null = null
let workdir = ''
beforeAll(() => {
  python = findPython({ requirePolars: true })
  workdir = mkdtempSync(join(tmpdir(), 'flowfile-polars-'))
})

const DRIVER = `

import json
_out = run_etl_pipeline()
if isinstance(_out, pl.LazyFrame):
    _out = _out.collect()
print("@@@" + json.dumps(_out.to_dicts(), default=str))
`

function polarsRows(fixture: Fixture, label: string): unknown[] {
  const { generateCode } = useCodeGeneration()
  const { nodes, edges } = toFlow(fixture)
  const script = generateCode({ nodes, edges, flowName: fixture.name })
  return runPythonScript(python!, workdir, script + DRIVER, label)
}

describe('the exported Polars script matches the engine', () => {
  for (const [index, fixture] of [...PARITY_FIXTURES, ...POLARS_ONLY_FIXTURES].entries()) {
    it(fixture.name, ctx => {
      // Report as skipped, never as passed: a silent no-op here would read as
      // "parity verified" on a machine that never ran a single comparison.
      if (!python) ctx.skip('no CPython with Polars found (set FLOWFILE_TEST_PYTHON)')
      const expected = engineRows(python!, fixture)
      const actual = polarsRows(fixture, `fixture_${index}`)
      expect(normalise(actual, fixture.ordered ?? false)).toEqual(normalise(expected, fixture.ordered ?? false))
      expect(columnsOf(actual), 'column order').toEqual(columnsOf(expected))
    })
  }
})
