/**
 * Shared fixtures for the differential suites: a flow described once, then
 * evaluated through the real engine and through everything generated from it.
 */

import { execFileSync } from 'node:child_process'
import { writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { makeNode, flowWith } from './flow-builder'
import type { FlowNode, FlowEdge } from '../../src/types'

const RUNNER = resolve(__dirname, '../python/engine_flow_runner.py')

export interface Step {
  id: number
  type: string
  settings?: any
  inputs?: number[]
  left?: number
  right?: number
}

export interface Fixture {
  name: string
  steps: Step[]
  output: number
  /** Row order is only guaranteed for flows that end in an order-preserving node. */
  ordered?: boolean
  /** Column order is only guaranteed where both sides build the frame the same way. */
  columnOrdered?: boolean
  /** Why core reproduces different rows. The file must still open; rows stop being compared. */
  coreDivergence?: string
  /** Neither engine runs this flow: the phrase both refusals contain, or one per side. */
  bothRefuse?: string | { core: string | string[]; engine: string | string[] }
}

export function toFlow(fixture: Fixture): { nodes: Map<number, FlowNode>; edges: FlowEdge[] } {
  return flowWith(
    ...fixture.steps.map(step =>
      makeNode(step.id, step.type, step.settings ?? {}, step.left !== undefined ? [] : (step.inputs ?? []), {
        leftInputId: step.left,
        rightInputId: step.right
      })
    )
  )
}

/** Numbers compare by value (2 == 2.0); everything else by canonical JSON. */
export function canonical(value: unknown): unknown {
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

export function normalise(rows: unknown[], ordered: boolean): string[] {
  const encoded = rows.map(row => JSON.stringify(canonical(row)))
  return ordered ? encoded : [...encoded].sort()
}

/** The column names each side produced, in order, taken from the first row. */
export function columnsOf(rows: unknown[]): string[] {
  const first = rows[0]
  return first && typeof first === 'object' ? Object.keys(first as object) : []
}

function readResult(output: string, label: string): unknown[] {
  const marker = output.lastIndexOf('@@@')
  if (marker === -1) throw new Error(`${label}: driver printed no result\n${output}`)
  return JSON.parse(output.slice(marker + 3))
}

/** Run `script` with `python`, writing it into `workdir` as `<label>.py`. */
export function runPythonScript(python: string, workdir: string, script: string, label: string): unknown[] {
  const path = join(workdir, `${label}.py`)
  writeFileSync(path, script)
  return readResult(execFileSync(python, [path], { encoding: 'utf-8', timeout: 180_000 }), label)
}

/** Ground truth: the fixture run through the engine the browser actually uses. */
export function engineRows(python: string, fixture: Fixture): unknown[] {
  const spec = JSON.stringify({ steps: fixture.steps, output: fixture.output })
  return readResult(
    execFileSync(python, [RUNNER], { input: spec, encoding: 'utf-8', timeout: 180_000 }),
    'engine runner'
  )
}

// --- fixtures --------------------------------------------------------------

/** The dtype the Manual Input panel would infer for a column's values. */
function inferDataType(values: any[]): string {
  const present = values.filter(value => value !== null && value !== undefined && value !== '')
  if (present.length === 0) return 'String'
  if (present.every(value => typeof value === 'boolean')) return 'Boolean'
  if (present.every(value => typeof value === 'number')) {
    return present.every(value => Number.isInteger(value)) ? 'Int64' : 'Float64'
  }
  return 'String'
}

/** A manual_input step, carrying the declared dtypes the panel always writes. */
export const source = (id: number, columns: string[], data: any[][]): Step => ({
  id,
  type: 'manual_input',
  settings: {
    raw_data_format: {
      columns: columns.map((name, index) => ({ name, data_type: inferDataType(data[index] ?? []) })),
      data
    }
  }
})

export const SALES = source(
  1,
  ['product', 'region', 'revenue', 'flag'],
  [
    ['Widget', 'Gadget', 'Widget', 'Gizmo', 'Widget'],
    ['EU', 'US', 'US', 'EU', null],
    [100, 200, 150, null, 50],
    [true, false, true, null, false]
  ]
)

/** Long-format survey answers: not every respondent answered every question. */
export const SURVEY = source(
  1,
  ['respondent', 'question', 'rating'],
  [
    [1, 1, 2, 2, 3],
    ['clarity', 'speed', 'clarity', 'value', 'speed'],
    [4, 5, 3, 2, 1]
  ]
)

const pivotStep = (pivot_input: any): Step => ({ id: 2, type: 'pivot', inputs: [1], settings: { pivot_input } })

/** A select over node 1, in the shape the panel writes (a dtype on every entry). */
const selectStep = (select_input: any[]): Step => ({
  id: 2,
  type: 'select',
  inputs: [1],
  settings: { select_input }
})

const filterFixture = (name: string, basic: any, ordered = true): Fixture => ({
  name,
  ordered,
  steps: [SALES, { id: 2, type: 'filter', inputs: [1], settings: { filter_input: { mode: 'basic', basic_filter: basic, advanced_filter: '' } } }],
  output: 2
})

export const PARITY_FIXTURES: Fixture[] = [
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

  // --- select data-type changes ---------------------------------------------
  // Both engines cast non-strictly (a value that will not convert becomes null)
  // and cast under the incoming name, before the rename.
  {
    name: 'select casts a whole-number column to text',
    ordered: true,
    steps: [
      SALES,
      selectStep([
        { old_name: 'product', new_name: 'product', keep: true, position: 0, data_type: 'String' },
        { old_name: 'revenue', new_name: 'revenue', keep: true, position: 1, data_type: 'String', data_type_change: true }
      ])
    ],
    output: 2
  },

  {
    name: 'select casts a boolean column to text (Polars spells them lowercase)',
    ordered: true,
    steps: [
      SALES,
      selectStep([
        { old_name: 'flag', new_name: 'flag', keep: true, position: 0, data_type: 'String', data_type_change: true }
      ])
    ],
    output: 2
  },

  {
    name: 'select casts numeric text to whole numbers, the rest becoming null',
    ordered: true,
    steps: [
      source(1, ['code', 'label'], [['100', '-3', 'abc', '12.5', null], ['a', 'b', 'c', 'd', 'e']]),
      selectStep([
        { old_name: 'code', new_name: 'code', keep: true, position: 0, data_type: 'Int64', data_type_change: true },
        { old_name: 'label', new_name: 'label', keep: true, position: 1, data_type: 'String' }
      ])
    ],
    output: 2
  },

  {
    name: 'select casts decimals to whole numbers by truncating toward zero',
    ordered: true,
    steps: [
      source(1, ['amount'], [[1.9, -1.9, 2.5, null]]),
      selectStep([
        { old_name: 'amount', new_name: 'amount', keep: true, position: 0, data_type: 'Int64', data_type_change: true }
      ])
    ],
    output: 2
  },

  {
    name: 'select casts, renames and reorders in one pass',
    ordered: true,
    steps: [
      SALES,
      selectStep([
        { old_name: 'revenue', new_name: 'revenue_text', keep: true, position: 1, data_type: 'String', data_type_change: true },
        { old_name: 'product', new_name: 'item', keep: true, position: 0, data_type: 'String' },
        { old_name: 'region', new_name: 'region', keep: false, position: 2, data_type: 'String' },
        { old_name: 'flag', new_name: 'kept', keep: true, position: 3, data_type: 'Float64', data_type_change: true }
      ])
    ],
    output: 2
  },

  {
    // The panel writes TEXT cells next to declared dtypes; building from the
    // values alone gets a String column and sums it as text.
    name: 'manual input as the panel writes it (text cells, declared dtypes)',
    ordered: false,
    steps: [
      {
        id: 1,
        type: 'manual_input',
        settings: {
          raw_data_format: {
            columns: [
              { name: 'product', data_type: 'String' },
              { name: 'revenue', data_type: 'Int64' }
            ],
            data: [
              ['Widget', 'Gadget', 'Widget'],
              ['100', '200', '50']
            ]
          }
        }
      },
      {
        id: 2,
        type: 'group_by',
        inputs: [1],
        settings: {
          groupby_input: {
            agg_cols: [
              { old_name: 'product', agg: 'groupby', new_name: 'product' },
              { old_name: 'revenue', agg: 'sum', new_name: 'total' }
            ]
          }
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
    // subset/keep only: the shape that used to emit keep="undefined".
    name: 'unique as the settings panel writes it (subset/keep, no columns/strategy)',
    ordered: true,
    steps: [
      SALES,
      { id: 2, type: 'unique', inputs: [1], settings: { unique_input: { subset: ['product'], keep: 'last', maintain_order: true } } }
    ],
    output: 2
  },
  {
    name: 'unique without maintain_order',
    ordered: false,
    steps: [
      SALES,
      { id: 2, type: 'unique', inputs: [1], settings: { unique_input: { subset: ['product'], keep: 'first', maintain_order: false } } }
    ],
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
    // Dropping the group column's rename left downstream nodes unbound.
    name: 'group_by renaming its grouping column',
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
              { old_name: 'product', agg: 'groupby', new_name: 'item' },
              { old_name: 'revenue', agg: 'sum', new_name: 'total' }
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
    name: 'join configured with join_type alone and a custom suffix',
    ordered: false,
    steps: [
      source(1, ['id', 'name'], [[1, 2], ['a', 'b']]),
      source(2, ['id', 'name'], [[1, 3], ['x', 'y']]),
      {
        id: 3,
        type: 'join',
        left: 1,
        right: 2,
        settings: {
          join_input: {
            join_type: 'left',
            join_mapping: [{ left_col: 'id', right_col: 'id' }],
            left_suffix: '',
            right_suffix: '_from_right'
          }
        }
      }
    ],
    output: 3
  },

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
    name: 'head taking its row count from head_input',
    ordered: true,
    steps: [SALES, { id: 2, type: 'head', inputs: [1], settings: { sample_method: 'first', head_input: { n: 2 } } }],
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

  // Pivot's output row order comes out of a Polars group_by, which does not
  // promise one — hence ordered: false on every pivot fixture below.
  {
    name: 'pivot with one index column and one aggregation',
    ordered: false,
    steps: [
      SURVEY,
      pivotStep({ index_columns: ['respondent'], pivot_column: 'question', value_col: 'rating', aggregations: ['mean'] })
    ],
    output: 2
  },
  {
    name: 'pivot over every aggregation it implements, named value_agg',
    ordered: false,
    steps: [
      SURVEY,
      pivotStep({
        index_columns: ['respondent'],
        pivot_column: 'question',
        value_col: 'rating',
        aggregations: ['sum', 'mean', 'min', 'max', 'count', 'first', 'last', 'median']
      })
    ],
    output: 2
  },
  {
    name: 'pivot with two index columns',
    ordered: false,
    steps: [
      source(
        1,
        ['team', 'respondent', 'question', 'rating'],
        [
          ['red', 'red', 'blue'],
          [1, 1, 2],
          ['Speed', 'clarity', 'Speed'],
          [4, 5, 3]
        ]
      ),
      pivotStep({
        index_columns: ['team', 'respondent'],
        pivot_column: 'question',
        value_col: 'rating',
        aggregations: ['sum']
      })
    ],
    output: 2
  },
  {
    name: 'pivot with no index columns collapses to a single row',
    ordered: false,
    steps: [
      SURVEY,
      pivotStep({ index_columns: [], pivot_column: 'question', value_col: 'rating', aggregations: ['sum'] })
    ],
    output: 2
  },
  {
    name: 'pivot over a value column full of nulls',
    ordered: false,
    // (a,x) exists but holds only nulls; (a,y) never happened. sum 0 vs None.
    steps: [
      source(
        1,
        ['k', 'q', 'v'],
        [
          ['a', 'a', 'b', 'b'],
          ['x', 'x', 'x', 'y'],
          [null, null, 3, null]
        ]
      ),
      pivotStep({
        index_columns: ['k'],
        pivot_column: 'q',
        value_col: 'v',
        aggregations: ['sum', 'count', 'mean', 'min', 'first']
      })
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

/**
 * Flows the browser engine refuses outright, so there are no rows for the
 * script-parity suites to diff. When one runs again its `bothRefuse` assertion
 * fails and it belongs back in PARITY_FIXTURES.
 */
export const CORE_ONLY_FIXTURES: Fixture[] = [
  {
    name: 'pivot with no index columns over an empty table',
    // A column with no values is declared String, and neither engine sums a String.
    // Core hits one of two failure paths on the empty frame nondeterministically:
    // the pivot-labels fetch, or the sum aggregation itself.
    bothRefuse: {
      core: ['No unique values found in lazyframe', '`sum` operation not supported for dtype'],
      engine: '`sum` operation not supported for dtype'
    },
    ordered: false,
    steps: [
      source(1, ['k', 'q', 'v'], [[], [], []]),
      pivotStep({ index_columns: [], pivot_column: 'q', value_col: 'v', aggregations: ['sum'] })
    ],
    output: 2
  },
  {
    name: 'pivot over a pivot column with nulls',
    // A pivot turns each label into a column NAME, and a null is not a name.
    bothRefuse: {
      core: "'None' is not an instance of 'str'",
      engine: 'cannot become a column name'
    },
    ordered: false,
    steps: [
      source(
        1,
        ['k', 'q', 'v'],
        [
          [1, 1, 2],
          ['a', null, null],
          [1, 2, 3]
        ]
      ),
      pivotStep({ index_columns: ['k'], pivot_column: 'q', value_col: 'v', aggregations: ['sum'] })
    ],
    output: 2
  }
]

/**
 * Fixtures only the Polars export has to satisfy: node types and settings the
 * plain-Python flavour deliberately leaves to the canvas — either no
 * PLAIN_HANDLERS entry, or an emitter that raises PlainPythonUnsupported.
 */
export const POLARS_ONLY_FIXTURES: Fixture[] = [
  {
    // A temporal target parses text rather than casting it, by sniffing the
    // format — which a list of dicts cannot reproduce, so the plain flavour
    // leaves this select to the canvas.
    name: 'select parses a text column into dates',
    ordered: true,
    steps: [
      source(1, ['day', 'label'], [['2024-01-05', 'nope', null], ['a', 'b', 'c']]),
      selectStep([
        { old_name: 'day', new_name: 'day', keep: true, position: 0, data_type: 'Date', data_type_change: true },
        { old_name: 'label', new_name: 'label', keep: true, position: 1, data_type: 'String' }
      ])
    ],
    output: 2
  },

  {
    name: 'dynamic rename from the first row',
    ordered: true,
    steps: [
      source(1, ['a', 'b'], [['id', '1', '2'], ['label', 'x', 'y']]),
      {
        id: 2,
        type: 'dynamic_rename',
        inputs: [1],
        settings: {
          dynamic_rename_input: {
            rename_mode: 'first_row',
            prefix: '',
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
    name: 'dynamic rename by data type',
    ordered: true,
    steps: [
      source(1, ['a', 'b'], [[1, 2], ['x', 'y']]),
      {
        id: 2,
        type: 'dynamic_rename',
        inputs: [1],
        settings: {
          dynamic_rename_input: {
            rename_mode: 'prefix',
            prefix: 'num_',
            suffix: '',
            formula: '',
            selection_mode: 'data_type',
            selected_columns: [],
            selected_data_type: 'Numeric'
          }
        }
      }
    ],
    output: 2
  },

  {
    name: 'formula adds a computed column',
    ordered: true,
    steps: [
      SALES,
      {
        id: 2,
        type: 'formula',
        inputs: [1],
        settings: { function: { field: { name: 'doubled', data_type: 'Auto' }, function: '[revenue] * 2' } }
      }
    ],
    output: 2
  },
  {
    name: 'formula casting to a mapped dtype',
    ordered: true,
    steps: [
      SALES,
      {
        id: 2,
        type: 'formula',
        inputs: [1],
        settings: { function: { field: { name: 'as_text', data_type: 'String' }, function: '[revenue] + 1' } }
      }
    ],
    output: 2
  },
  {
    // "Integer" is not in the engine's dtype map, so the column stays uncast.
    name: 'formula with an unmapped dtype stays uncast',
    ordered: true,
    steps: [
      SALES,
      {
        id: 2,
        type: 'formula',
        inputs: [1],
        settings: { function: { field: { name: 'plain', data_type: 'Integer' }, function: '[revenue] + 1' } }
      }
    ],
    output: 2
  },
  {
    name: 'formula overwriting an existing column',
    ordered: true,
    steps: [
      SALES,
      {
        id: 2,
        type: 'formula',
        inputs: [1],
        settings: { function: { field: { name: 'revenue', data_type: 'Auto' }, function: '[revenue] + 1' } }
      }
    ],
    output: 2
  }
]
