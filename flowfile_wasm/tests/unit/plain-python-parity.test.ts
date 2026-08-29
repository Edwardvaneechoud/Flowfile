/**
 * The generated plain-Python script must produce the same rows as the engine.
 * Skips cleanly without a CPython that has Polars.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { usePlainPythonGeneration } from '../../src/composables/usePlainPythonGeneration'
import { findPython } from '../helpers/python-runtime'
import { PARITY_FIXTURES, engineRows, normalise, runPythonScript, toFlow } from '../helpers/parity'
import type { Fixture } from '../helpers/parity'

let python: string | null = null
let workdir = ''
beforeAll(() => {
  python = findPython({ requirePolars: true })
  workdir = mkdtempSync(join(tmpdir(), 'flowfile-plain-'))
})

function plainRows(fixture: Fixture, label: string): unknown[] {
  const { generatePlainPython } = usePlainPythonGeneration()
  const { nodes, edges } = toFlow(fixture)
  const script = generatePlainPython({ nodes, edges, flowName: fixture.name })
  expect(script, `${fixture.name} must not reach for Polars`).not.toMatch(/\bpolars\b|\bpl\./)
  const driver = `${script}\n\nimport json\nprint("@@@" + json.dumps(run_etl_pipeline(), default=str))\n`
  return runPythonScript(python!, workdir, driver, label)
}

describe('plain Python matches the engine', () => {
  for (const [index, fixture] of PARITY_FIXTURES.entries()) {
    it(fixture.name, ctx => {
      // Report as skipped, never as passed: a silent no-op here would read as
      // "parity verified" on a machine that never ran a single comparison.
      if (!python) ctx.skip('no CPython with Polars found (set FLOWFILE_TEST_PYTHON)')
      const expected = normalise(engineRows(python!, fixture), fixture.ordered ?? false)
      const actual = normalise(plainRows(fixture, `fixture_${index}`), fixture.ordered ?? false)
      expect(actual).toEqual(expected)
    })
  }
})
