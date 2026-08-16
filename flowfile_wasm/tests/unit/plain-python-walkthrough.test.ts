/**
 * The walkthrough view is only as good as its trace build: one run of the
 * instrumented script has to hand back every intermediate table, including
 * when a later step is still an unfilled exercise.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync, readdirSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import {
  usePlainPythonGeneration,
  CONCEPTS,
  CONCEPT_FOR_NODE,
  HELPER_NAMES,
  PLAIN_PYTHON_NODE_TYPES,
  type PlainStep
} from '../../src/composables/usePlainPythonGeneration'
import { instrumentScript } from '../../src/composables/usePlainTrace'
import { PYTHON_GLOSSARY } from '../../src/composables/usePythonGlossary'
import type { FlowNode, FlowEdge } from '../../src/types'

const { buildWalkthrough } = usePlainPythonGeneration()

function poetryVenvPythons(): string[] {
  const root = join(process.env.HOME ?? '', '.cache/pypoetry/virtualenvs')
  if (!existsSync(root)) return []
  return readdirSync(root)
    .filter(name => name.startsWith('flowfile-'))
    .map(name => join(root, name, 'bin', 'python'))
    .filter(existsSync)
}

function findPython(): string | null {
  for (const candidate of [process.env.FLOWFILE_TEST_PYTHON, 'python3', 'python', ...poetryVenvPythons()]) {
    if (!candidate) continue
    try {
      execFileSync(candidate, ['-c', ''], { stdio: 'ignore' })
      return candidate
    } catch {
      /* next */
    }
  }
  return null
}

let python: string | null = null
let workdir = ''
beforeAll(() => {
  python = findPython()
  workdir = mkdtempSync(join(tmpdir(), 'flowfile-walk-'))
})

function node(id: number, type: string, settings: any, inputIds: number[] = [], extra: any = {}): FlowNode {
  return {
    id,
    type,
    x: 0,
    y: 0,
    settings: { node_id: id, is_setup: true, cache_results: true, pos_x: 0, pos_y: 0, description: '', ...settings },
    inputIds,
    ...extra
  }
}

function flowOf(nodes: FlowNode[]): { nodes: Map<number, FlowNode>; edges: FlowEdge[] } {
  const map = new Map<number, FlowNode>()
  const edges: FlowEdge[] = []
  for (const item of nodes) {
    map.set(item.id, item)
    for (const input of item.inputIds) {
      edges.push({
        id: `e${input}-${item.id}`,
        source: String(input),
        target: String(item.id),
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
    }
  }
  return { nodes: map, edges }
}

const SOURCE = node(1, 'manual_input', {
  raw_data_format: {
    columns: [{ name: 'product' }, { name: 'revenue' }],
    data: [
      ['Widget', 'Gadget', 'Widget'],
      [100, 200, 150]
    ]
  }
})
const FILTER = node(
  2,
  'filter',
  { filter_input: { mode: 'basic', basic_filter: { field: 'revenue', operator: 'greater_than', value: '120' } } },
  [1]
)
const GROUP = node(
  3,
  'group_by',
  {
    groupby_input: {
      agg_cols: [
        { old_name: 'product', agg: 'groupby', new_name: 'product' },
        { old_name: 'revenue', agg: 'sum', new_name: 'total' }
      ]
    }
  },
  [2]
)

/** Run the instrumented script and hand back the captured tables. */
function runTrace(traceScript: string, label: string): Record<string, unknown[]> {
  const path = join(workdir, `${label}.py`)
  writeFileSync(
    path,
    `${traceScript}\n\nimport json\nns = globals()\ntry:\n    run_etl_pipeline()\nexcept Exception:\n    pass\nprint("@@@" + json.dumps(__steps__, default=str))\n`
  )
  const output = execFileSync(python!, [path], { encoding: 'utf-8', timeout: 120_000 })
  return JSON.parse(output.slice(output.lastIndexOf('@@@') + 3))
}

describe('walkthrough step metadata', () => {
  it('produces one step per emitting node, in pipeline order', () => {
    const { steps } = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    expect(steps.map(s => s.nodeType)).toEqual(['manual_input', 'filter', 'group_by'])
    expect(steps.map(s => s.concept)).toEqual(['list-of-dicts', 'guard-loop', 'accumulator-dict'])
  })

  it('records the variables each step reads and writes, after renaming', () => {
    const { steps } = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    expect(steps[0]).toMatchObject({ varName: 'source', inputVars: [] })
    expect(steps[1]).toMatchObject({ varName: 'filtered', inputVars: ['source'] })
    expect(steps[2]).toMatchObject({ varName: 'grouped', inputVars: ['filtered'] })
  })

  it('follows a node_reference into the step metadata', () => {
    const named = { ...FILTER, node_reference: 'big_sales' }
    const { steps } = buildWalkthrough(flowOf([SOURCE, named, { ...GROUP, inputIds: [2] }]))
    expect(steps[1].varName).toBe('big_sales')
    expect(steps[2].inputVars).toEqual(['big_sales'])
  })

  it('gives every step a snippet of just its own block', () => {
    const { snippets, steps } = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    for (const step of steps) expect(snippets[step.nodeId], `${step.nodeType} has no snippet`).toBeTruthy()
    expect(snippets[2]).toContain('for row in source:')
    expect(snippets[2]).not.toContain('groups')
  })

  it('marks a node with no loop form as an exercise', () => {
    const formula = node(3, 'formula', { function: { field: { name: 'x' }, function: '[revenue] * 2' } }, [2])
    const { steps } = buildWalkthrough(flowOf([SOURCE, FILTER, formula]))
    expect(steps[2].concept).toBe('exercise')
  })
})

describe('step line ranges', () => {
  // The walkthrough highlights the step inside the whole script, so these
  // numbers are what makes the feature point at the right thing.
  it('points at the lines that actually belong to each step', () => {
    const { script, steps } = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    const lines = script.split('\n')

    for (const step of steps) {
      const block = lines.slice(step.lineStart - 1, step.lineEnd).join('\n')
      expect(block, `${step.nodeType} range is off`).toContain(`${step.varName} `)
    }
    expect(lines[steps[1].lineStart - 1]).toContain('--- Filter')
    expect(lines.slice(steps[1].lineStart - 1, steps[1].lineEnd).join('\n')).toContain('for row in source:')
  })

  it('keeps the ranges in order and non-overlapping', () => {
    const { steps } = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    for (let i = 0; i < steps.length; i++) {
      expect(steps[i].lineEnd).toBeGreaterThanOrEqual(steps[i].lineStart)
      if (i > 0) expect(steps[i].lineStart).toBeGreaterThan(steps[i - 1].lineEnd)
    }
  })

  it('stays correct when helpers push the body further down the file', () => {
    // A CSV read emits two helper functions above run_etl_pipeline; the offset
    // has to account for them or every highlight lands in the wrong place.
    const read = node(1, 'read', {
      file_name: 'x.csv',
      received_file: { name: 'x.csv', path: 'x.csv', file_type: 'csv', table_settings: { file_type: 'csv' } }
    })
    const { script, steps } = buildWalkthrough(flowOf([read]))
    const lines = script.split('\n')
    expect(script).toContain('def read_csv_file(')
    expect(lines.slice(steps[0].lineStart - 1, steps[0].lineEnd).join('\n')).toContain('read_csv_file("x.csv")')
  })
})

describe('concept coverage', () => {
  it('has a concept for every node type that emits a loop', () => {
    const missing = [...PLAIN_PYTHON_NODE_TYPES].filter(
      type => !['explore_data', 'external_output'].includes(type) && !CONCEPT_FOR_NODE[type]
    )
    expect(missing).toEqual([])
  })

  it('points every mapping at a concept that exists', () => {
    const dangling = Object.entries(CONCEPT_FOR_NODE).filter(([, concept]) => !CONCEPTS[concept])
    expect(dangling).toEqual([])
  })

  it('gives every concept a title and at least one paragraph', () => {
    for (const [key, concept] of Object.entries(CONCEPTS)) {
      expect(concept.title, `${key} has no title`).toBeTruthy()
      expect(concept.body.length, `${key} has no body`).toBeGreaterThan(0)
    }
  })
})

describe('hover glossary', () => {
  /** Every helper the generator can define is a name a reader will meet. */
  it('explains every generated helper', () => {
    const missing = HELPER_NAMES.filter(name => !PYTHON_GLOSSARY[name])
    expect(missing).toEqual([])
  })

  it('explains the names that actually turn up in generated code', () => {
    // Build a flow touching most emitters, then check the vocabulary it emits
    // is covered. This is what stops the glossary drifting from the codegen.
    const wide = flowOf([
      SOURCE,
      FILTER,
      GROUP,
      node(4, 'sort', { sort_input: [{ column: 'total', how: 'desc' }] }, [3]),
      node(5, 'unique', { unique_input: { subset: ['product'], keep: 'first' } }, [4]),
      node(6, 'record_id', { record_id_input: { name: 'nr', offset: 1 } }, [5])
    ])
    const { script } = buildWalkthrough(wide)

    const EXPECTED = [
      'sorted', 'lambda', 'enumerate', 'setdefault', 'append', 'set', 'len', 'sum', 'continue', 'values_of'
    ]
    for (const term of EXPECTED) {
      if (!new RegExp(`\\b${term}\\b`).test(script)) continue
      expect(PYTHON_GLOSSARY, `${term} is emitted but unexplained`).toHaveProperty(term)
    }
  })

  it('gives every entry a summary', () => {
    for (const [term, entry] of Object.entries(PYTHON_GLOSSARY)) {
      expect(entry.summary, `${term} has no summary`).toBeTruthy()
    }
  })
})

describe('trace build', () => {
  it('captures every intermediate table in one run', ctx => {
    if (!python) ctx.skip('no CPython found (set FLOWFILE_TEST_PYTHON)')
    const { traceScript } = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    const captured = runTrace(traceScript, 'chain')

    expect(Object.keys(captured).sort()).toEqual(['filtered', 'grouped', 'source'])
    expect(captured.source).toHaveLength(3)
    expect(captured.filtered).toHaveLength(2)
    // Groups come out in first-appearance order, and the filter drops Widget(100),
    // so Gadget is the first row still standing.
    expect(captured.grouped).toEqual([{ product: 'Gadget', total: 200 }, { product: 'Widget', total: 150 }])
  })

  it('still returns the earlier steps when a later one is an unfilled exercise', ctx => {
    if (!python) ctx.skip('no CPython found (set FLOWFILE_TEST_PYTHON)')
    // This is the whole reason __steps__ is module-level rather than a local.
    const formula = node(3, 'formula', { function: { field: { name: 'x' }, function: '[revenue] * 2' } }, [2])
    const { traceScript } = buildWalkthrough(flowOf([SOURCE, FILTER, formula]))
    const captured = runTrace(traceScript, 'stubbed')

    expect(captured.source).toHaveLength(3)
    expect(captured.filtered).toHaveLength(2)
    expect(captured.computed).toBeUndefined()
  })

  it('keeps the trace script free of Polars, like the script it mirrors', () => {
    const { traceScript } = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    expect(traceScript).not.toMatch(/\bimport polars\b|\bpl\./)
  })
})

/**
 * The browser no longer runs traceScript: it instruments whatever is in the
 * editor via instrumentScript() and execs it with __steps__ pre-seeded in the
 * namespace — replayed here byte-for-byte against real CPython.
 */
function runInstrumentedBuffer(buffer: string, walk: { steps: PlainStep[] }, label: string): Record<string, unknown[]> {
  const instrumented = instrumentScript(
    buffer,
    walk.steps.map(step => ({ fromLine: step.lineStart, toLine: step.lineEnd, varName: step.varName }))
  )
  const scriptPath = join(workdir, `${label}-buffer.py`)
  writeFileSync(scriptPath, instrumented)
  const harnessPath = join(workdir, `${label}-harness.py`)
  writeFileSync(
    harnessPath,
    [
      'import json',
      `_src = open(${JSON.stringify(scriptPath)}).read()`,
      '_ns = {"__steps__": {}}',
      'try:',
      '    exec(compile(_src, "pipeline.py", "exec"), _ns)',
      '    _ns["run_etl_pipeline"]()',
      'except Exception:',
      '    pass',
      'print("@@@" + json.dumps(_ns["__steps__"], default=str))'
    ].join('\n')
  )
  const output = execFileSync(python!, [harnessPath], { encoding: 'utf-8', timeout: 120_000 })
  return JSON.parse(output.slice(output.lastIndexOf('@@@') + 3))
}

describe('instrumented-buffer trace (the browser path)', () => {
  it('captures the same tables from the generated buffer as the reference trace', ctx => {
    if (!python) ctx.skip('no CPython found (set FLOWFILE_TEST_PYTHON)')
    const walk = buildWalkthrough(flowOf([SOURCE, FILTER, GROUP]))
    const reference = runTrace(walk.traceScript, 'ref')
    const buffered = runInstrumentedBuffer(walk.script, walk, 'same')
    expect(buffered).toEqual(reference)
  })

  it('traces the learner\'s SOLVED exercise: the stub filled in, downstream steps get data', ctx => {
    if (!python) ctx.skip('no CPython found (set FLOWFILE_TEST_PYTHON)')
    const formula = node(3, 'formula', { function: { field: { name: 'doubled' }, function: '[revenue] * 2' } }, [2])
    const walk = buildWalkthrough(flowOf([SOURCE, FILTER, formula, { ...GROUP, id: 4, inputIds: [3] }]))

    // The learner replaces the raise with a working loop — same line count, so
    // the step ranges the highlight tracks still hold.
    const solved = walk.script.replace(
      /^(\s*)raise NotImplementedError\(.*\)$/m,
      '$1return [{**row, "doubled": row["revenue"] * 2} for row in rows]'
    )
    expect(solved).not.toBe(walk.script)

    const captured = runInstrumentedBuffer(solved, walk, 'solved')
    expect(captured.computed).toHaveLength(2)
    expect((captured.computed as any)[0].doubled).toBe(400)
    // The step AFTER the solved exercise now has data too — the whole point.
    expect(captured.grouped).toBeDefined()
  })

  it('still yields the pre-stub tables when the exercise is left unsolved', ctx => {
    if (!python) ctx.skip('no CPython found (set FLOWFILE_TEST_PYTHON)')
    const formula = node(3, 'formula', { function: { field: { name: 'x' }, function: '[revenue] * 2' } }, [2])
    const walk = buildWalkthrough(flowOf([SOURCE, FILTER, formula]))
    const captured = runInstrumentedBuffer(walk.script, walk, 'unsolved')
    expect(captured.source).toHaveLength(3)
    expect(captured.filtered).toHaveLength(2)
    expect(captured.computed).toBeUndefined()
  })
})
