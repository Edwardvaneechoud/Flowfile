/**
 * Differential tests: the generated plain-Python script must produce the same
 * rows as the engine the browser actually runs.
 *
 * Each fixture is built once and evaluated twice — through
 * `tests/python/engine_flow_runner.py` (real `engine.build_*` calls over Polars)
 * and through the plain-Python script this repo generates for the same flow.
 *
 * Needs a CPython with Polars on PATH or in a known venv; skips cleanly without
 * one, so `npm run test:run` stays green on a machine that has no Python.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync, readdirSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { usePlainPythonGeneration } from '../../src/composables/usePlainPythonGeneration'
import type { FlowNode, FlowEdge } from '../../src/types'

const RUNNER = resolve(__dirname, '../python/engine_flow_runner.py')

/** The Poetry venv for whichever checkout we are in, if Poetry is installed. */
function poetryPython(): string | null {
  try {
    return execFileSync('poetry', ['env', 'info', '--executable'], {
      cwd: resolve(__dirname, '../../..'),
      encoding: 'utf-8',
      stdio: ['ignore', 'pipe', 'ignore']
    }).trim()
  } catch {
    return null
  }
}

/**
 * Any monorepo Poetry venv. A git worktree gets its own (often empty) venv, so
 * `poetry env info` alone can point at an interpreter with no Polars while the
 * main checkout's venv has one.
 */
function poetryVenvPythons(): string[] {
  const root = join(process.env.HOME ?? '', '.cache/pypoetry/virtualenvs')
  if (!existsSync(root)) return []
  return readdirSync(root)
    .filter(name => name.startsWith('flowfile-'))
    .map(name => join(root, name, 'bin', 'python'))
    .filter(existsSync)
}

function findPython(): string | null {
  const candidates = [
    process.env.FLOWFILE_TEST_PYTHON,
    'python3',
    'python',
    poetryPython(),
    ...poetryVenvPythons()
  ].filter(Boolean) as string[]
  for (const candidate of candidates) {
    try {
      execFileSync(candidate, ['-c', 'import polars'], { stdio: 'ignore' })
      return candidate
    } catch {
      /* try the next one */
    }
  }
  return null
}

let python: string | null = null
let workdir = ''
beforeAll(() => {
  python = findPython()
  workdir = mkdtempSync(join(tmpdir(), 'flowfile-plain-'))
})

// --- fixture plumbing ------------------------------------------------------

interface Step {
  id: number
  type: string
  settings?: any
  inputs?: number[]
  left?: number
  right?: number
}

interface Fixture {
  name: string
  steps: Step[]
  output: number
  /** Row order is only guaranteed for flows that end in an order-preserving node. */
  ordered?: boolean
}

function toFlow(fixture: Fixture): { nodes: Map<number, FlowNode>; edges: FlowEdge[] } {
  const nodes = new Map<number, FlowNode>()
  const edges: FlowEdge[] = []
  for (const step of fixture.steps) {
    const inputIds = step.left !== undefined ? [] : (step.inputs ?? [])
    nodes.set(step.id, {
      id: step.id,
      type: step.type,
      x: 0,
      y: 0,
      settings: {
        node_id: step.id,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        ...(step.settings ?? {})
      } as any,
      inputIds,
      leftInputId: step.left,
      rightInputId: step.right
    })
    for (const source of [...(step.inputs ?? []), step.left, step.right]) {
      if (source === undefined) continue
      edges.push({
        id: `e${source}-${step.id}`,
        source: String(source),
        target: String(step.id),
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
    }
  }
  return { nodes, edges }
}

/** Numbers compare by value (2 == 2.0); everything else by canonical JSON. */
function canonical(value: unknown): unknown {
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

function normalise(rows: unknown[], ordered: boolean): string[] {
  const encoded = rows.map(row => JSON.stringify(canonical(row)))
  return ordered ? encoded : [...encoded].sort()
}

function runPython(script: string, label: string): unknown[] {
  const path = join(workdir, `${label}.py`)
  writeFileSync(path, script)
  const output = execFileSync(python!, [path], { encoding: 'utf-8', timeout: 180_000 })
  const marker = output.lastIndexOf('@@@')
  if (marker === -1) throw new Error(`${label}: driver printed no result\n${output}`)
  return JSON.parse(output.slice(marker + 3))
}

function engineRows(fixture: Fixture): unknown[] {
  const spec = JSON.stringify({ steps: fixture.steps, output: fixture.output })
  const output = execFileSync(python!, [RUNNER], { input: spec, encoding: 'utf-8', timeout: 180_000 })
  const marker = output.lastIndexOf('@@@')
  if (marker === -1) throw new Error(`engine runner printed no result\n${output}`)
  return JSON.parse(output.slice(marker + 3))
}

function plainRows(fixture: Fixture, label: string): unknown[] {
  const { generatePlainPython } = usePlainPythonGeneration()
  const { nodes, edges } = toFlow(fixture)
  const script = generatePlainPython({ nodes, edges, flowName: fixture.name })
  expect(script, `${fixture.name} must not reach for Polars`).not.toMatch(/\bpolars\b|\bpl\./)
  const driver = `${script}\n\nimport json\nprint("@@@" + json.dumps(run_etl_pipeline(), default=str))\n`
  return runPython(driver, label)
}

// --- fixtures --------------------------------------------------------------

const source = (id: number, columns: string[], data: any[][]): Step => ({
  id,
  type: 'manual_input',
  settings: { raw_data_format: { columns: columns.map(name => ({ name })), data } }
})

const SALES = source(
  1,
  ['product', 'region', 'revenue', 'flag'],
  [
    ['Widget', 'Gadget', 'Widget', 'Gizmo', 'Widget'],
    ['EU', 'US', 'US', 'EU', null],
    [100, 200, 150, null, 50],
    [true, false, true, null, false]
  ]
)

const filterFixture = (name: string, basic: any, ordered = true): Fixture => ({
  name,
  ordered,
  steps: [SALES, { id: 2, type: 'filter', inputs: [1], settings: { filter_input: { mode: 'basic', basic_filter: basic, advanced_filter: '' } } }],
  output: 2
})

const FIXTURES: Fixture[] = [
  filterFixture('filter greater_than over a column with nulls', { field: 'revenue', operator: 'greater_than', value: '100' }),
  filterFixture('filter equals against a text column', { field: 'region', operator: 'equals', value: 'EU' }),
  filterFixture('filter not_equals drops nulls like Polars', { field: 'region', operator: 'not_equals', value: 'EU' }),
  filterFixture('filter contains is a regex', { field: 'product', operator: 'contains', value: 'W.dget' }),
  filterFixture('filter starts_with', { field: 'product', operator: 'starts_with', value: 'G' }),
  filterFixture('filter is_null', { field: 'revenue', operator: 'is_null', value: '' }),
  filterFixture('filter is_not_null', { field: 'region', operator: 'is_not_null', value: '' }),
  filterFixture('filter in', { field: 'region', operator: 'in', value: 'EU, US' }),
  filterFixture('filter not_in', { field: 'product', operator: 'not_in', value: 'Widget' }),
  filterFixture('filter between', { field: 'revenue', operator: 'between', value: '100', value2: '200' }),
  filterFixture('filter equals on a boolean column', { field: 'flag', operator: 'equals', value: 'true' }),

  {
    name: 'select keeps position order and renames',
    ordered: true,
    steps: [
      SALES,
      {
        id: 2,
        type: 'select',
        inputs: [1],
        settings: {
          select_input: [
            { old_name: 'product', new_name: 'item', keep: true, position: 2, data_type: 'String' },
            { old_name: 'revenue', new_name: 'revenue', keep: true, position: 0, data_type: 'Int64' },
            { old_name: 'region', new_name: 'region', keep: false, position: 1, data_type: 'String' }
          ]
        }
      }
    ],
    output: 2
  },

  {
    name: 'sort ascending puts nulls first',
    ordered: true,
    steps: [SALES, { id: 2, type: 'sort', inputs: [1], settings: { sort_input: [{ column: 'revenue', how: 'asc' }] } }],
    output: 2
  },
  {
    name: 'sort descending also puts nulls first',
    ordered: true,
    steps: [SALES, { id: 2, type: 'sort', inputs: [1], settings: { sort_input: [{ column: 'revenue', how: 'desc' }] } }],
    output: 2
  },
  {
    name: 'sort on mixed directions',
    ordered: true,
    steps: [
      SALES,
      {
        id: 2,
        type: 'sort',
        inputs: [1],
        settings: { sort_input: [{ column: 'product', how: 'asc' }, { column: 'revenue', how: 'desc' }] }
      }
    ],
    output: 2
  },

  {
    name: 'unique keep first on a subset',
    ordered: true,
    steps: [SALES, { id: 2, type: 'unique', inputs: [1], settings: { unique_input: { subset: ['product'], columns: ['product'], keep: 'first', strategy: 'first' } } }],
    output: 2
  },
  {
    name: 'unique keep last on a subset',
    ordered: true,
    steps: [SALES, { id: 2, type: 'unique', inputs: [1], settings: { unique_input: { subset: ['product'], columns: ['product'], keep: 'last', strategy: 'last' } } }],
    output: 2
  },
  {
    name: 'unique keep none on a subset',
    ordered: true,
    steps: [SALES, { id: 2, type: 'unique', inputs: [1], settings: { unique_input: { subset: ['product'], columns: ['product'], keep: 'none', strategy: 'none' } } }],
    output: 2
  },
  {
    name: 'unique over the whole row',
    ordered: false,
    steps: [
      source(1, ['a', 'b'], [[1, 1, 2], ['x', 'x', 'y']]),
      { id: 2, type: 'unique', inputs: [1], settings: { unique_input: { subset: [], columns: [], keep: 'first', strategy: 'first' } } }
    ],
    output: 2
  },

  {
    name: 'group_by over every aggregation',
    ordered: false,
    steps: [
      SALES,
      {
        id: 2,
        type: 'group_by',
        inputs: [1],
        settings: {
          groupby_input: {
            agg_cols: [
              { old_name: 'product', agg: 'groupby', new_name: 'product' },
              { old_name: 'revenue', agg: 'sum', new_name: 'total' },
              { old_name: 'revenue', agg: 'min', new_name: 'lowest' },
              { old_name: 'revenue', agg: 'max', new_name: 'highest' },
              { old_name: 'revenue', agg: 'count', new_name: 'n' },
              { old_name: 'revenue', agg: 'mean', new_name: 'avg' },
              { old_name: 'revenue', agg: 'median', new_name: 'mid' },
              { old_name: 'region', agg: 'first', new_name: 'first_region' },
              { old_name: 'region', agg: 'last', new_name: 'last_region' },
              { old_name: 'region', agg: 'n_unique', new_name: 'regions' },
              { old_name: 'region', agg: 'concat', new_name: 'joined' }
            ]
          }
        }
      }
    ],
    output: 2
  },
  {
    name: 'group_by over a group whose values are all null',
    ordered: false,
    steps: [
      source(1, ['k', 'v'], [['a', 'a', 'b'], [null, null, 5]]),
      {
        id: 2,
        type: 'group_by',
        inputs: [1],
        settings: {
          groupby_input: {
            agg_cols: [
              { old_name: 'k', agg: 'groupby', new_name: 'k' },
              { old_name: 'v', agg: 'sum', new_name: 'total' },
              { old_name: 'v', agg: 'min', new_name: 'lowest' },
              { old_name: 'v', agg: 'count', new_name: 'n' },
              { old_name: 'v', agg: 'mean', new_name: 'avg' },
              { old_name: 'v', agg: 'n_unique', new_name: 'distinct' },
              { old_name: 'v', agg: 'first', new_name: 'head' }
            ]
          }
        }
      }
    ],
    output: 2
  },
  {
    name: 'group_by with no grouping columns',
    ordered: false,
    steps: [
      SALES,
      {
        id: 2,
        type: 'group_by',
        inputs: [1],
        settings: {
          groupby_input: {
            agg_cols: [
              { old_name: 'revenue', agg: 'sum', new_name: 'total' },
              { old_name: 'revenue', agg: 'count', new_name: 'n' }
            ]
          }
        }
      }
    ],
    output: 2
  },
  {
    name: 'group_by with no aggregations is a distinct',
    ordered: false,
    steps: [
      SALES,
      {
        id: 2,
        type: 'group_by',
        inputs: [1],
        settings: { groupby_input: { agg_cols: [{ old_name: 'product', agg: 'groupby', new_name: 'product' }] } }
      }
    ],
    output: 2
  },

  ...(['inner', 'left', 'semi', 'anti'] as const).map(how => ({
    name: `${how} join with null keys, duplicates and a colliding column`,
    ordered: false,
    steps: [
      source(1, ['id', 'name'], [[1, 2, 3, null], ['a', 'b', 'c', 'd']]),
      source(2, ['id', 'name', 'score'], [[1, 1, 3, null], ['x', 'y', 'z', 'w'], [10, 11, 30, 40]]),
      {
        id: 3,
        type: 'join',
        left: 1,
        right: 2,
        settings: {
          join_input: {
            join_type: how,
            how,
            join_mapping: [{ left_col: 'id', right_col: 'id' }],
            right_suffix: '_right'
          }
        }
      }
    ],
    output: 3
  })),

  {
    name: 'cross join',
    ordered: true,
    steps: [
      source(1, ['a'], [[1, 2]]),
      source(2, ['a', 'b'], [[10, 20], ['x', 'y']]),
      { id: 3, type: 'cross_join', left: 1, right: 2, settings: { cross_join_input: { right_suffix: '_right' } } }
    ],
    output: 3
  },

  {
    name: 'union vertical',
    ordered: true,
    steps: [
      source(1, ['a', 'b'], [[1, 2], ['x', 'y']]),
      source(2, ['a', 'b'], [[3], ['z']]),
      { id: 3, type: 'union', inputs: [1, 2], settings: { union_input: { mode: 'vertical' } } }
    ],
    output: 3
  },
  {
    name: 'union diagonal fills the gaps',
    ordered: true,
    steps: [
      source(1, ['a', 'b'], [[1, 2], ['x', 'y']]),
      source(2, ['a', 'c'], [[3], [true]]),
      { id: 3, type: 'union', inputs: [1, 2], settings: { union_input: { mode: 'diagonal' } } }
    ],
    output: 3
  },

  {
    name: 'record_id with an offset',
    ordered: true,
    steps: [SALES, { id: 2, type: 'record_id', inputs: [1], settings: { record_id_input: { name: 'nr', offset: 5 } } }],
    output: 2
  },
  {
    name: 'head takes the first rows',
    ordered: true,
    steps: [SALES, { id: 2, type: 'head', inputs: [1], settings: { sample_method: 'first', sample_size: 3 } }],
    output: 2
  },
  {
    name: 'head asking for more rows than exist',
    ordered: true,
    steps: [SALES, { id: 2, type: 'head', inputs: [1], settings: { sample_method: 'first', sample_size: 99 } }],
    output: 2
  },

  {
    name: 'unpivot goes column by column',
    ordered: true,
    steps: [
      source(1, ['id', 'q1', 'q2'], [[1, 2], [10, 20], [30, 40]]),
      {
        id: 2,
        type: 'unpivot',
        inputs: [1],
        settings: {
          unpivot_input: { index_columns: ['id'], value_columns: ['q1', 'q2'], data_type_selector_mode: 'column' }
        }
      }
    ],
    output: 2
  },

  {
    name: 'dynamic rename with a prefix',
    ordered: true,
    steps: [
      source(1, ['a', 'b'], [[1], [2]]),
      {
        id: 2,
        type: 'dynamic_rename',
        inputs: [1],
        settings: {
          dynamic_rename_input: {
            rename_mode: 'prefix',
            prefix: 'x_',
            suffix: '',
            formula: '',
            selection_mode: 'all',
            selected_columns: [],
            selected_data_type: null
          }
        }
      }
    ],
    output: 2
  },
  {
    name: 'dynamic rename with a suffix on a chosen list',
    ordered: true,
    steps: [
      source(1, ['a', 'b'], [[1], [2]]),
      {
        id: 2,
        type: 'dynamic_rename',
        inputs: [1],
        settings: {
          dynamic_rename_input: {
            rename_mode: 'suffix',
            prefix: '',
            suffix: '_out',
            formula: '',
            selection_mode: 'list',
            selected_columns: ['b'],
            selected_data_type: null
          }
        }
      }
    ],
    output: 2
  },

  {
    name: 'a longer chain: filter then group_by then sort',
    ordered: true,
    steps: [
      SALES,
      {
        id: 2,
        type: 'filter',
        inputs: [1],
        settings: {
          filter_input: { mode: 'basic', basic_filter: { field: 'revenue', operator: 'is_not_null', value: '' }, advanced_filter: '' }
        }
      },
      {
        id: 3,
        type: 'group_by',
        inputs: [2],
        settings: {
          groupby_input: {
            agg_cols: [
              { old_name: 'product', agg: 'groupby', new_name: 'product' },
              { old_name: 'revenue', agg: 'sum', new_name: 'total' }
            ]
          }
        }
      },
      { id: 4, type: 'sort', inputs: [3], settings: { sort_input: [{ column: 'total', how: 'desc' }] } }
    ],
    output: 4
  }
]

describe('plain Python matches the engine', () => {
  for (const [index, fixture] of FIXTURES.entries()) {
    it(fixture.name, ctx => {
      // Report as skipped, never as passed: a silent no-op here would read as
      // "parity verified" on a machine that never ran a single comparison.
      if (!python) ctx.skip('no CPython with Polars found (set FLOWFILE_TEST_PYTHON)')
      const expected = normalise(engineRows(fixture), fixture.ordered ?? false)
      const actual = normalise(plainRows(fixture, `fixture_${index}`), fixture.ordered ?? false)
      expect(actual).toEqual(expected)
    })
  }
})
