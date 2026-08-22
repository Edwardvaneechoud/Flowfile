/**
 * The Polars flavour of the walkthrough: unfused, one statement per node,
 * named like the plain flavour — plus CPython proof that the unfused script
 * computes the same rows as the fused export and that an instrumented run
 * captures per-step tables the engine agrees with.
 *
 * Needs a CPython with Polars; skips cleanly without one.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { buildPolarsWalkthrough, POLARS_DOC_FOR_NODE } from '../../src/composables/usePolarsWalkthrough'
import { useCodeGeneration } from '../../src/composables/useCodeGeneration'
import { usePlainPythonGeneration } from '../../src/composables/usePlainPythonGeneration'
import { instrumentScript } from '../../src/composables/usePlainTrace'
import { PYTHON_GLOSSARY } from '../../src/composables/usePythonGlossary'
import { makeNode, flowWith } from '../helpers/flow-builder'
import { findPython } from '../helpers/python-runtime'

const RUNNER = resolve(__dirname, '../python/engine_flow_runner.py')
const { generateCode } = useCodeGeneration()
const { buildWalkthrough } = usePlainPythonGeneration()

let python: string | null = null
let workdir = ''
beforeAll(() => {
  python = findPython({ requirePolars: true })
  workdir = mkdtempSync(join(tmpdir(), 'flowfile-polars-walk-'))
})

const SOURCE_SETTINGS = {
  raw_data_format: {
    columns: [{ name: 'product' }, { name: 'revenue' }],
    data: [
      ['Widget', 'Gadget', 'Widget'],
      [100, 200, 150]
    ]
  }
}
const FILTER_SETTINGS = {
  filter_input: { mode: 'basic', basic_filter: { field: 'revenue', operator: 'greater_than', value: '120' } }
}
const GROUP_SETTINGS = {
  groupby_input: {
    agg_cols: [
      { old_name: 'product', agg: 'groupby', new_name: 'product' },
      { old_name: 'revenue', agg: 'sum', new_name: 'total' }
    ]
  }
}

const SOURCE = makeNode(1, 'manual_input', SOURCE_SETTINGS)
const FILTER = makeNode(2, 'filter', FILTER_SETTINGS, [1])
const GROUP = makeNode(3, 'group_by', GROUP_SETTINGS, [2])

const canonical = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(canonical)
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value as object)
        .sort()
        .map(key => [key, canonical((value as any)[key])])
    )
  }
  if (typeof value === 'number') return Number.isInteger(value) ? value : Number(value.toFixed(9))
  return value
}
const normalise = (rows: unknown[]): string[] => rows.map(row => JSON.stringify(canonical(row))).sort()

function runPython(script: string, label: string): unknown[] {
  const path = join(workdir, `${label}.py`)
  writeFileSync(path, script)
  const output = execFileSync(python!, [path], { encoding: 'utf-8', timeout: 180_000 })
  const marker = output.lastIndexOf('@@@')
  if (marker === -1) throw new Error(`${label}: driver printed no result\n${output}`)
  return JSON.parse(output.slice(marker + 3))
}

describe('polars walkthrough step metadata', () => {
  it('produces one step per emitting node, never borrowing plain-Python concepts', () => {
    const { steps } = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, GROUP))
    expect(steps.map(s => s.nodeType)).toEqual(['manual_input', 'filter', 'group_by'])
    // The Why tab shows the Polars docs link for these steps, not the plain
    // flavour's list-of-dicts / loop cards.
    expect(steps.map(s => s.concept)).toEqual(['', '', ''])
    for (const step of steps) expect(POLARS_DOC_FOR_NODE[step.nodeType]).toBeTruthy()
  })

  it('links every code-generating node type to a Polars reference page', () => {
    for (const [type, doc] of Object.entries(POLARS_DOC_FOR_NODE)) {
      expect(doc.href, type).toMatch(/^https:\/\/docs\.pola\.rs\//)
      expect(doc.label, type).toBeTruthy()
      // The blurb IS the Why card in this flavour: one short sentence.
      expect(doc.blurb, type).toMatch(/^[A-Z].*\.$/)
      expect(doc.blurb.length, type).toBeLessThan(90)
    }
  })

  it('records the variables each step reads and writes, after renaming', () => {
    const { steps } = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, GROUP))
    expect(steps[0]).toMatchObject({ varName: 'source', inputVars: [] })
    expect(steps[1]).toMatchObject({ varName: 'filtered', inputVars: ['source'] })
    expect(steps[2]).toMatchObject({ varName: 'grouped', inputVars: ['filtered'] })
  })

  it('follows a node_reference into the step metadata', () => {
    const named = { ...FILTER, node_reference: 'big_sales' }
    const { steps } = buildPolarsWalkthrough(flowWith(SOURCE, named, GROUP))
    expect(steps[1].varName).toBe('big_sales')
    expect(steps[2].inputVars).toEqual(['big_sales'])
  })

  it('gives a node with no pattern card an empty concept, never "exercise"', () => {
    const formula = makeNode(3, 'formula', { function: { field: { name: 'x' }, function: '[revenue] * 2' } }, [2])
    const { steps } = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, formula))
    expect(steps[2].concept).toBe('')
    expect(steps.some(step => step.concept === 'exercise')).toBe(false)
  })
})

describe('polars walkthrough line ranges', () => {
  it('points at the lines that actually belong to each step', () => {
    const { script, steps } = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, GROUP))
    const lines = script.split('\n')
    for (const step of steps) {
      const block = lines.slice(step.lineStart - 1, step.lineEnd).join('\n')
      expect(block, `${step.nodeType} range is off`).toContain(`${step.varName} `)
    }
    expect(lines[steps[1].lineStart - 1]).toContain('filtered = ')
  })

  it('keeps the ranges in order, non-overlapping, with a blank line between blocks', () => {
    const { script, steps } = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, GROUP))
    const lines = script.split('\n')
    for (let i = 0; i < steps.length; i++) {
      expect(steps[i].lineEnd).toBeGreaterThanOrEqual(steps[i].lineStart)
      if (i > 0) expect(steps[i].lineStart).toBeGreaterThan(steps[i - 1].lineEnd)
      if (i < steps.length - 1) expect(lines[steps[i].lineEnd]).toBe('')
    }
  })

  it('keeps a multi-line statement whole, closing paren included', () => {
    const right = makeNode(4, 'manual_input', {
      raw_data_format: { columns: [{ name: 'product' }], data: [['Widget']] }
    })
    const joinNode = makeNode(
      5,
      'join',
      { join_input: { join_mapping: [{ left_col: 'product', right_col: 'product' }], how: 'inner' } },
      [],
      { leftInputId: 2, rightInputId: 4 }
    )
    const { script, steps } = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, right, joinNode))
    const joinStep = steps.find(step => step.nodeId === 5)!
    const block = script.split('\n').slice(joinStep.lineStart - 1, joinStep.lineEnd)
    expect(block.join('\n')).toContain('how="inner"')
    expect(block[block.length - 1].trim()).toBe(')')
  })

  it('keeps a translated formula on a single line', () => {
    const formula = makeNode(3, 'formula', { function: { field: { name: 'x' }, function: '[revenue] * 2' } }, [2])
    const { script, steps } = buildPolarsWalkthrough({
      ...flowWith(SOURCE, FILTER, formula),
      formulaCode: { 3: 'pl.col("revenue") * 2' }
    })
    const step = steps.find(s => s.nodeId === 3)!
    expect(step.lineEnd).toBe(step.lineStart)
    expect(script.split('\n')[step.lineStart - 1]).toContain('with_columns((pl.col("revenue") * 2)')
  })
})

describe('unfused layout', () => {
  it('keeps every node its own statement where the export fuses them', () => {
    const flow = flowWith(SOURCE, FILTER, GROUP)
    const walkScript = buildPolarsWalkthrough(flow).script
    const fused = generateCode(flow)
    expect(fused).toContain('grouped = (')
    expect(fused).not.toContain('filtered =')
    expect(walkScript).toContain('filtered = source.filter(')
    expect(walkScript).toContain('grouped = filtered.group_by(')
  })

  it('shares the export script header and footer', () => {
    const script = buildPolarsWalkthrough(flowWith(SOURCE, FILTER)).script
    expect(script.split('\n')[0]).toBe('import polars as pl')
    expect(script).toContain('def run_etl_pipeline():')
    expect(script.trimEnd().endsWith('print(pipeline_output)')).toBe(true)
  })
})

describe('cross-flavour agreement', () => {
  it('binds the same variables as the plain walkthrough, step for step', () => {
    const flow = flowWith(SOURCE, FILTER, GROUP)
    const polars = buildPolarsWalkthrough(flow).steps
    const plain = buildWalkthrough(flow).steps
    expect(polars.map(s => s.nodeId)).toEqual(plain.map(s => s.nodeId))
    for (let i = 0; i < polars.length; i++) {
      expect(polars[i].varName, `step ${i}`).toBe(plain[i].varName)
      expect(polars[i].inputVars, `step ${i}`).toEqual(plain[i].inputVars)
    }
  })

  it('agrees on the name of a formula node the plain flavour can only stub', () => {
    const formula = makeNode(3, 'formula', { function: { field: { name: 'x' }, function: '[revenue] * 2' } }, [2])
    const flow = flowWith(SOURCE, FILTER, formula)
    expect(buildPolarsWalkthrough(flow).steps[2].varName).toBe('computed')
    expect(buildWalkthrough(flow).steps[2].varName).toBe('computed')
  })

  it('names a write step after its input, the way the plain flavour does', () => {
    const output = makeNode(3, 'output', { output_settings: { file_type: 'csv', name: 'out.csv' } }, [2])
    const flow = flowWith(SOURCE, FILTER, output)
    const polarsStep = buildPolarsWalkthrough(flow).steps.find(step => step.nodeId === 3)!
    const plainStep = buildWalkthrough(flow).steps.find(step => step.nodeId === 3)!
    expect(polarsStep.varName).toBe('filtered')
    expect(polarsStep.varName).toBe(plainStep.varName)
  })
})

describe('polars glossary coverage', () => {
  it('explains the vocabulary the polars walkthrough actually emits', () => {
    const sort = makeNode(4, 'sort', { sort_input: [{ column: 'total', how: 'desc' }] }, [3])
    const unique = makeNode(5, 'unique', { unique_input: { columns: ['product'], strategy: 'first' } }, [4])
    const recordId = makeNode(6, 'record_id', { record_id_input: { name: 'record_id', offset: 1 } }, [5])
    const { script } = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, GROUP, sort, unique, recordId))
    const EXPECTED = [
      'pl',
      'LazyFrame',
      'col',
      'filter',
      'group_by',
      'agg',
      'alias',
      'sum',
      'sort',
      'descending',
      'unique',
      'subset',
      'keep',
      'with_row_index',
      'collect'
    ]
    for (const term of EXPECTED) {
      if (!new RegExp(`\\b${term}\\b`).test(script)) continue
      expect(PYTHON_GLOSSARY, `${term} is emitted but unexplained`).toHaveProperty(term)
    }
    // The core of the list must actually appear, or this test guards nothing.
    expect(script).toMatch(/\bgroup_by\b/)
    expect(script).toMatch(/\bcollect\b/)
  })
})

describe('CPython execution', () => {
  it('computes the same rows as the fused export script', ctx => {
    if (!python) return ctx.skip('no CPython with Polars found (set FLOWFILE_TEST_PYTHON)')
    const flow = flowWith(SOURCE, FILTER, GROUP)
    const driver = (script: string) =>
      `${script}\n\nimport json\nprint("@@@" + json.dumps(run_etl_pipeline().collect().to_dicts(), default=str))\n`
    const walkRows = runPython(driver(buildPolarsWalkthrough(flow).script), 'walk')
    const fusedRows = runPython(driver(generateCode(flow)), 'fused')
    expect(normalise(walkRows)).toEqual(normalise(fusedRows))
  })

  it('captures per-step tables an instrumented run, final step matching the engine', ctx => {
    if (!python) return ctx.skip('no CPython with Polars found (set FLOWFILE_TEST_PYTHON)')
    const walk = buildPolarsWalkthrough(flowWith(SOURCE, FILTER, GROUP))
    const instrumented = instrumentScript(
      walk.script,
      walk.steps.map(step => ({ fromLine: step.lineStart, toLine: step.lineEnd, varName: step.varName }))
    )
    const scriptPath = join(workdir, 'instrumented.py')
    writeFileSync(scriptPath, instrumented)
    const harness = [
      'import json',
      'import polars as pl',
      `_src = open(${JSON.stringify(scriptPath)}).read()`,
      '_ns = {"__steps__": {}}',
      'try:',
      '    exec(compile(_src, "pipeline.py", "exec"), _ns)',
      '    _ns["run_etl_pipeline"]()',
      'except Exception:',
      '    pass',
      'def _capture(value):',
      '    if isinstance(value, pl.LazyFrame):',
      '        try:',
      '            value = value.collect()',
      '        except Exception:',
      '            return None',
      '    if isinstance(value, pl.DataFrame):',
      '        return value.to_dicts()',
      '    if isinstance(value, list):',
      '        return value',
      '    return None',
      '_out = {k: _capture(v) for k, v in _ns["__steps__"].items()}',
      'print("@@@" + json.dumps({k: v for k, v in _out.items() if v is not None}, default=str))'
    ].join('\n')
    const harnessPath = join(workdir, 'harness.py')
    writeFileSync(harnessPath, harness)
    const output = execFileSync(python!, [harnessPath], { encoding: 'utf-8', timeout: 180_000 })
    const captured = JSON.parse(output.slice(output.lastIndexOf('@@@') + 3)) as Record<string, unknown[]>

    expect(captured.source).toHaveLength(3)
    expect(captured.filtered).toHaveLength(2)

    const spec = JSON.stringify({
      steps: [
        { id: 1, type: 'manual_input', settings: SOURCE_SETTINGS },
        { id: 2, type: 'filter', settings: FILTER_SETTINGS, inputs: [1] },
        { id: 3, type: 'group_by', settings: GROUP_SETTINGS, inputs: [2] }
      ],
      output: 3
    })
    const engineOut = execFileSync(python!, [RUNNER], { input: spec, encoding: 'utf-8', timeout: 180_000 })
    const engineRows = JSON.parse(engineOut.slice(engineOut.lastIndexOf('@@@') + 3)) as unknown[]
    expect(normalise(captured.grouped)).toEqual(normalise(engineRows))
  })
})
