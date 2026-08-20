/**
 * The per-node Polars snippet behind the settings drawer's "same step in
 * Polars" block: standalone statements under the same final names the full
 * script uses, fused-away nodes included.
 */

import { describe, it, expect } from 'vitest'
import { FlowToPolarsConverter, useCodeGeneration } from '../../src/composables/useCodeGeneration'
import { usePlainPythonGeneration } from '../../src/composables/usePlainPythonGeneration'
import { makeNode, flowWith } from '../helpers/flow-builder'

const { generateCode, explainNodePolars } = useCodeGeneration()

const SOURCE = makeNode(1, 'manual_input', {
  raw_data_format: { columns: [{ name: 'a' }, { name: 'b' }], data: [[1, 2, 3], ['x', 'y', 'z']] }
})

const basicFilter = (id: number, inputId: number, field = 'a', value = '1') =>
  makeNode(
    id,
    'filter',
    { filter_input: { mode: 'basic', basic_filter: { field, operator: 'greater_than', value } } },
    [inputId]
  )

describe('explainNodePolars', () => {
  it('gives every node of a fused chain its own standalone statement', () => {
    const select = makeNode(
      3,
      'select',
      { select_input: [{ old_name: 'a', new_name: 'a', keep: true, position: 0 }] },
      [2]
    )
    const flow = flowWith(SOURCE, basicFilter(2, 1), select)

    // The full script fuses all three into one chain owned by the select.
    const script = generateCode(flow)
    expect(script).toContain('selected = (')
    expect(script).not.toContain('filtered =')

    // The snippets un-fuse them; with unique labels they match the script too.
    expect(explainNodePolars(flow, 1)).toContain('source = pl.LazyFrame({')
    expect(explainNodePolars(flow, 2)).toBe('filtered = source.filter(pl.col("a") > 1)')
    expect(explainNodePolars(flow, 3)).toContain('selected = filtered.select([')
  })

  it('numbers same-type twins the way the plain flavour does, not the fused script', () => {
    // The fused script names the sole surviving filter `filtered`; the drawer
    // stacks these snippets under the plain loops, which number both — one
    // identifier must mean one table across the two blocks.
    const flow = flowWith(SOURCE, basicFilter(2, 1), basicFilter(3, 2, 'a', '0'))
    expect(generateCode(flow)).toContain('filtered = (')
    expect(explainNodePolars(flow, 2)).toBe('filtered_1 = source.filter(pl.col("a") > 1)')
    expect(explainNodePolars(flow, 3)).toBe('filtered_2 = filtered_1.filter(pl.col("a") > 0)')
  })

  it('binds the same variable name as the plain flavour for every node', () => {
    const { explainNode } = usePlainPythonGeneration()
    const flow = flowWith(SOURCE, basicFilter(2, 1), basicFilter(3, 2, 'a', '0'))
    // The node's output is the LAST top-level assignment (plain blocks hoist
    // temporaries like `wanted = ...` above the list they build).
    const bound = (snippet: string) => [...snippet.matchAll(/^(\w+) = /gm)].at(-1)?.[1]
    for (const id of [1, 2, 3]) {
      expect(bound(explainNodePolars(flow, id)!), `node ${id}`).toBe(bound(explainNode(flow, id).code!))
    }
  })

  it('shows a join under its final input names', () => {
    const left = SOURCE
    const right = makeNode(2, 'manual_input', {
      raw_data_format: { columns: [{ name: 'a' }], data: [[1, 2]] }
    })
    const join = makeNode(
      3,
      'join',
      { join_input: { join_mapping: [{ left_col: 'a', right_col: 'a' }], how: 'inner' } },
      [],
      { leftInputId: 1, rightInputId: 2 }
    )
    const code = explainNodePolars(flowWith(left, right, join), 3)
    expect(code).toContain('joined = source_1.join(')
    expect(code).toContain('source_2,')
    expect(code).toContain('how="inner"')
  })

  it('uses the translated expression for a formula when one is provided', () => {
    const formula = makeNode(2, 'formula', { function: { field: { name: 'c' }, function: '[a] * 2' } }, [1])
    const flow = flowWith(SOURCE, formula)

    const translated = explainNodePolars({ ...flow, formulaCode: { 2: 'pl.col("a") * 2' } }, 2)
    expect(translated).toContain('with_columns((pl.col("a") * 2)')
    expect(translated).not.toContain('simple_function_to_expr')

    // Without a translation, the honest runtime fallback.
    const fallback = explainNodePolars(flow, 2)
    expect(fallback).toContain('simple_function_to_expr("[a] * 2")')
    expect(fallback).toContain('pip install polars-expr-transformer')
  })

  it('keeps a pinned node_reference verbatim', () => {
    const filter = basicFilter(2, 1)
    filter.node_reference = 'grown_ups'
    const code = explainNodePolars(flowWith(SOURCE, filter), 2)
    expect(code).toContain('grown_ups = source.filter(')
  })

  it('reads a Catalog table instead of refusing the whole flow', () => {
    const reader = makeNode(1, 'read_from_catalog', { dataset_name: 'sales' })
    const flow = flowWith(reader, basicFilter(2, 1))
    // The gap fix: this used to hit the unsupported branch and throw.
    expect(generateCode(flow)).toContain('pl.scan_csv("sales.csv")')
    expect(explainNodePolars(flow, 1)).toContain('source = pl.scan_csv("sales.csv")')
  })

  it('never renames variable-like text inside string literals', () => {
    const filter = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'basic', basic_filter: { field: 'b', operator: 'equals', value: 'df_1' } } },
      [1]
    )
    const code = explainNodePolars(flowWith(SOURCE, filter), 2)
    expect(code).toContain('source.filter(')
    expect(code).toContain('== "df_1"')
  })

  it('returns null where there is nothing to show', () => {
    const emptySelect = makeNode(2, 'select', { select_input: [] }, [1])
    expect(explainNodePolars(flowWith(SOURCE, emptySelect), 2)).toBeNull()

    const preview = makeNode(2, 'explore_data', {}, [1])
    expect(explainNodePolars(flowWith(SOURCE, preview), 2)).toBeNull()

    expect(explainNodePolars(flowWith(SOURCE), 42)).toBeNull()
  })

  it('is idempotent on one converter instance', () => {
    const flow = flowWith(SOURCE, basicFilter(2, 1))
    const converter = new FlowToPolarsConverter(flow)
    converter.convert()
    const first = converter.explain(2)
    expect(converter.explain(1)).toContain('source = ')
    expect(converter.explain(2)).toBe(first)
    expect(converter.explain(2)).toBe(first)
  })
})
