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
  PLAIN_PYTHON_NODE_TYPES
} from '../../src/composables/usePlainPythonGeneration'
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
