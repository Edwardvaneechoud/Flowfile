/**
 * Shared plumbing for the notebook suites: the fixtures a render is checked on,
 * and the store build that turns a fixture into the flow file the download writes.
 */

import { setActivePinia, createPinia } from 'pinia'
import { useFlowStore } from '../../src/stores/flow-store'
import { toCoreCompatibleFlow } from '../../src/utils/coreExport'
import { CORE_ONLY_FIXTURES, PARITY_FIXTURES, POLARS_ONLY_FIXTURES, SALES, source } from './parity'
import type { Fixture, Step } from './parity'
import type { FlowfileData } from '../../src/types'

/** A fixture step that may also carry what the canvas shows on a node: its description and its reference. */
export interface NotebookStep extends Step {
  description?: string
  reference?: string
}

export interface NotebookFixture extends Omit<Fixture, 'steps'> {
  steps: NotebookStep[]
}

const csvRead = (id: number, tableSettings: Record<string, unknown> = {}): NotebookStep => ({
  id,
  type: 'read',
  settings: {
    file_name: 'sales.csv',
    received_file: {
      name: 'sales.csv',
      path: '/data/sales.csv',
      file_type: 'csv',
      table_settings: {
        file_type: 'csv',
        delimiter: ',',
        has_headers: true,
        encoding: 'utf-8',
        starting_from_line: 0,
        infer_schema_length: 10000,
        truncate_ragged_lines: false,
        ignore_errors: false,
        ...tableSettings
      }
    }
  }
})

const polarsCode = (id: number, inputs: number[], code: string): NotebookStep => ({
  id,
  type: 'polars_code',
  inputs,
  settings: { polars_code_input: { polars_code: code } }
})

const output = (id: number, input: number, fileType: string, tableSettings: Record<string, unknown>, name: string): NotebookStep => ({
  id,
  type: 'output',
  inputs: [input],
  settings: {
    output_settings: {
      name,
      directory: '/data/out',
      file_type: fileType,
      write_mode: 'overwrite',
      table_settings: { file_type: fileType, ...tableSettings }
    }
  }
})

const revenueFilter = (id: number, input: number | null, value = '100'): NotebookStep => ({
  id,
  type: 'filter',
  inputs: input === null ? [] : [input],
  settings: {
    filter_input: { mode: 'basic', basic_filter: { field: 'revenue', operator: 'greater_than', value, value2: '' }, advanced_filter: '' }
  }
})

const revenueSort = (id: number, input: number): NotebookStep => ({
  id,
  type: 'sort',
  inputs: [input],
  settings: { sort_input: [{ column: 'revenue', how: 'desc' }] }
})

const join = (how: string, mapping: Array<[string, string]>, extra: Record<string, unknown> = {}): NotebookStep => ({
  id: 3,
  type: 'join',
  left: 1,
  right: 2,
  settings: {
    join_input: {
      join_type: how,
      how,
      join_mapping: mapping.map(([left_col, right_col]) => ({ left_col, right_col })),
      left_suffix: '',
      right_suffix: '_right',
      ...extra
    }
  }
})

const LEFT = source(1, ['id', 'name'], [[1, 2, 3], ['a', 'b', 'c']])
const RIGHT = source(2, ['id', 'name', 'score'], [[1, 2, 4], ['x', 'y', 'z'], [10, 20, 40]])
const RIGHT_OTHER_KEY = source(2, ['key', 'score'], [[1, 2, 4], [10, 20, 40]])

/** Shapes the differential fixtures do not reach: readers, writers, code nodes, names, descriptions, placeholders. */
const NOTEBOOK_ONLY_FIXTURES: NotebookFixture[] = [
  { name: 'notebook: csv read with the defaults', steps: [csvRead(1), revenueFilter(2, 1)], output: 2 },
  {
    name: 'notebook: csv read with its own delimiter, no header and skipped rows',
    steps: [csvRead(1, { delimiter: ';', has_headers: false, starting_from_line: 2, ignore_errors: true })],
    output: 1
  },
  {
    name: 'notebook: parquet read',
    steps: [
      {
        id: 1,
        type: 'read',
        settings: { received_file: { name: 'sales.parquet', path: '/data/sales.parquet', file_type: 'parquet', table_settings: { file_type: 'parquet' } } }
      }
    ],
    output: 1
  },
  {
    name: 'notebook: excel read from a named sheet',
    steps: [
      {
        id: 1,
        type: 'read',
        settings: {
          received_file: { name: 'sales.xlsx', path: '/data/sales.xlsx', file_type: 'excel', table_settings: { file_type: 'excel', sheet_name: 'Q1' } }
        }
      }
    ],
    output: 1
  },
  { name: 'notebook: csv output', steps: [SALES, revenueFilter(2, 1), output(3, 2, 'csv', { delimiter: ',', encoding: 'utf-8' }, 'result.csv')], output: 3 },
  { name: 'notebook: csv output with a semicolon', steps: [SALES, output(2, 1, 'csv', { delimiter: ';', encoding: 'utf-8' }, 'result.csv')], output: 2 },
  { name: 'notebook: parquet output', steps: [SALES, output(2, 1, 'parquet', {}, 'result.parquet')], output: 2 },
  { name: 'notebook: excel output', steps: [SALES, output(2, 1, 'excel', { sheet_name: 'Sheet1' }, 'result.xlsx')], output: 2 },
  {
    name: 'notebook: polars code assigning output_df',
    steps: [SALES, polarsCode(2, [1], 'output_df = input_df.filter(pl.col("revenue") > 100)'), revenueSort(3, 2)],
    output: 3
  },
  { name: 'notebook: polars code that is one expression', steps: [SALES, polarsCode(2, [1], 'input_df.head(2)')], output: 2 },
  {
    name: 'notebook: polars code ending in its own assignment',
    steps: [SALES, polarsCode(2, [1], '# keep the big ones\nbig = input_df.filter(pl.col("revenue") > 100)\nresult = big.sort("revenue")')],
    output: 2
  },
  { name: 'notebook: polars code with a return', steps: [SALES, polarsCode(2, [1], 'x = input_df\nreturn x.head(1)')], output: 2 },
  {
    name: 'notebook: polars code over two inputs',
    steps: [LEFT, RIGHT, polarsCode(3, [1, 2], 'output_df = pl.concat([input_df_1, input_df_2], how="diagonal")')],
    output: 3
  },
  { name: 'notebook: polars code with no input', steps: [polarsCode(1, [], 'pl.LazyFrame({"a": [1, 2]})')], output: 1 },
  { name: 'notebook: polars code that does not parse', steps: [SALES, polarsCode(2, [1], 'output_df = input_df.filter(')], output: 2 },
  { name: 'notebook: record count', steps: [SALES, { id: 2, type: 'record_count', inputs: [1], settings: {} }], output: 2 },
  {
    name: 'notebook: explore data is a placeholder that still passes its frame on',
    steps: [SALES, { id: 2, type: 'explore_data', inputs: [1], settings: {} }, revenueSort(3, 2)],
    output: 3
  },
  {
    name: 'notebook: a description ends the statement it is written on',
    steps: [SALES, { ...revenueFilter(2, 1), description: 'Keep the "big" ones' }, revenueSort(3, 2)],
    output: 3
  },
  {
    name: 'notebook: a described group by carries it on group_by, not agg',
    steps: [
      SALES,
      {
        id: 2,
        type: 'group_by',
        inputs: [1],
        description: 'Total per product',
        settings: { groupby_input: { agg_cols: [{ old_name: 'product', agg: 'groupby', new_name: 'product' }, { old_name: 'revenue', agg: 'sum', new_name: 'total' }] } }
      }
    ],
    output: 2
  },
  {
    name: 'notebook: a named node keeps its name and is never fused away',
    steps: [SALES, { ...revenueFilter(2, 1), reference: 'big_sales' }, revenueSort(3, 2)],
    output: 3
  },
  {
    name: 'notebook: an unconnected node and what follows it are placeholders',
    steps: [SALES, revenueFilter(2, null), revenueSort(3, 2), revenueSort(4, 1)],
    output: 4
  },
  {
    name: 'notebook: a fan-out keeps the shared frame as its own statement',
    steps: [
      SALES,
      revenueFilter(2, 1, '100'),
      revenueFilter(3, 1, '50'),
      { id: 4, type: 'union', inputs: [2, 3], settings: { union_input: { mode: 'relaxed' } } }
    ],
    output: 4
  },
  { name: 'notebook: right join', steps: [LEFT, RIGHT, join('right', [['id', 'id']])], output: 3 },
  { name: 'notebook: full join', steps: [LEFT, RIGHT, join('full', [['id', 'id']])], output: 3 },
  { name: 'notebook: inner join on differently named keys', steps: [LEFT, RIGHT_OTHER_KEY, join('inner', [['id', 'key']])], output: 3 },
  { name: 'notebook: left join on differently named keys', steps: [LEFT, RIGHT_OTHER_KEY, join('left', [['id', 'key']])], output: 3 },
  { name: 'notebook: right join with a custom suffix', steps: [LEFT, RIGHT, join('right', [['id', 'id']], { right_suffix: '_r' })], output: 3 },
  { name: 'notebook: inner join with a custom suffix', steps: [LEFT, RIGHT, join('inner', [['id', 'id']], { right_suffix: '_r' })], output: 3 },
  {
    name: 'notebook: a formula the notebook cannot read back keeps its text',
    steps: [
      SALES,
      { id: 2, type: 'formula', inputs: [1], settings: { functions: [{ field: { name: 'tagged', data_type: 'Auto' }, function: 'concat([product], "-", [region])' }] } }
    ],
    output: 2
  },
  {
    name: 'notebook: an empty flow is only its imports',
    steps: [],
    output: 0
  }
]

/** Formulas whose verdict must match flowfile_core's: the same `ff.` code, or the same refusal to translate. */
export const NOTEBOOK_FORMULAS: string[] = [
  "[a] + 1", "[a] * 2", "[a] - [b]", "[a] / 3", "[a] > 1 and [b] < 2", "[a] = 1 or [b] = 2", "contains([s], \"W\")",
  "contains([s], \"W\") and [a] > 60", "uppercase([s])", "lowercase([s])", "length([s])", "trim([s])",
  "left([s], 2)", "right([s], 2)", "concat([s], \"-\", [t])", "[s] + \"x\"", "round([a] / 3, 2)", "abs([a])",
  "ceil([a])", "floor([a])", "sqrt([a])", "power([a], 2)", "mod([a], 2)", "log([a])", "exp([a])",
  "if [a] > 30 then \"S\" else \"J\" endif", "if [a] > 3 then 1 elseif [a] > 1 then 2 else 3 endif", "[a] in (1, 2)",
  "is_empty([s])", "is_not_empty([s])", "to_string([a])", "to_integer([s])", "to_float([s])",
  "to_date([s], \"%Y-%m-%d\")", "year([d])", "month([d])", "day([d])", "now()", "today()",
  "replace([s], \"a\", \"b\")", "starts_with([s], \"a\")", "ends_with([s], \"a\")", "titlecase([s])", "reverse([s])",
  "[a] != 2", "not([a] > 1)", "-[a]", "coalesce([a], [b])", "min([a], [b])", "max([a], [b])", "[a] >= 1",
  "[s] = \"x\"", "date_diff_days([d], [e])", "add_days([d], 3)", "md5([s])", "sha256([s])", "true", "1 + 1",
  "\"text\"", "[a]", "pad_left([s], 5, \"0\")", "find_position([s], \"a\")", "count_match([s], \"a\")",
  "string_similarity([s], [t], \"levenshtein\")"
]

/** Every differential fixture, once (the three lists overlap), then the notebook's own. */
export const NOTEBOOK_RENDER_FIXTURES: NotebookFixture[] = (() => {
  const seen = new Set<string>()
  const shared = [...PARITY_FIXTURES, ...POLARS_ONLY_FIXTURES, ...CORE_ONLY_FIXTURES].filter(fixture => {
    if (seen.has(fixture.name)) return false
    seen.add(fixture.name)
    return true
  })
  return [...shared, ...NOTEBOOK_ONLY_FIXTURES]
})()

/**
 * Build the fixture in the real store and export it the way the download does.
 * Settings *replace* the node defaults: a leftover default would change the flow.
 * `ids` maps a fixture step id to the node id the store handed out.
 */
export function buildInStore(fixture: NotebookFixture): { flow: FlowfileData; ids: Map<number, number>; terminalId: number } {
  setActivePinia(createPinia())
  const store = useFlowStore()
  const ids = new Map<number, number>()

  for (const step of fixture.steps) {
    const id = store.addNode(step.type, 0, 0)
    ids.set(step.id, id)
    const node = store.getNode(id)!
    const { node_id, is_setup, cache_results, pos_x, pos_y, description } = node.settings as any
    node.settings = { node_id, is_setup, cache_results, pos_x, pos_y, description, ...(step.settings ?? {}) } as any
    if (step.description) store.updateNodeDescription(id, step.description)
    if (step.reference) store.updateNodeReference(id, step.reference)
  }

  for (const step of fixture.steps) {
    const target = ids.get(step.id)!
    const node = store.getNode(target)!
    const link = (from: number, handle: string) =>
      store.addEdge({
        id: `e${from}-${target}-${handle}`,
        source: String(from),
        target: String(target),
        sourceHandle: 'output-0',
        targetHandle: handle
      })

    if (step.left !== undefined) {
      node.inputIds = [ids.get(step.left)!]
      node.rightInputId = ids.get(step.right!)!
      link(ids.get(step.left)!, 'input-0')
      link(ids.get(step.right!)!, 'input-1')
    } else if (step.inputs?.length) {
      node.inputIds = step.inputs.map(id => ids.get(id)!)
      for (const input of step.inputs) link(ids.get(input)!, 'input-0')
    }
  }

  return {
    flow: toCoreCompatibleFlow(store.exportToFlowfile(fixture.name)),
    ids,
    terminalId: ids.get(fixture.output)!
  }
}
