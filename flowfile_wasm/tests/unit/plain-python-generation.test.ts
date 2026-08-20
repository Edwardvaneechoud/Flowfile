/**
 * Structural tests for the plain-Python flavour: what it refuses to emit, what
 * it leaves as an exercise, and the guard that keeps Polars out of the output.
 *
 * Row-level correctness lives in plain-python-parity.test.ts, which diffs the
 * generated script against the real engine.
 */

import { describe, it, expect } from 'vitest'
import {
  usePlainPythonGeneration,
  PLAIN_PYTHON_NODE_TYPES,
  NODE_EXPLANATIONS
} from '../../src/composables/usePlainPythonGeneration'
import { NODE_TYPES } from '../../src/types'
import { makeNode, flowWith } from '../helpers/flow-builder'

const { generatePlainPython, explainNode } = usePlainPythonGeneration()

const SOURCE = makeNode(1, 'manual_input', {
  raw_data_format: { columns: [{ name: 'a' }, { name: 'b' }], data: [[1, 2, 3], ['x', 'y', 'z']] }
})

describe('plain-Python generation', () => {
  it('never reaches for Polars, whatever the flow contains', () => {
    // One node of every type the palette offers, wired into a chain where possible.
    for (const type of Object.values(NODE_TYPES)) {
      const node = makeNode(2, type, {}, [1])
      const code = generatePlainPython(flowWith(SOURCE, node))
      expect(code, `node type ${type} leaked Polars`).not.toMatch(/\bimport polars\b|\bpl\./)
    }
  })

  it('imports nothing at all for a flow that needs no helpers', () => {
    const code = generatePlainPython(flowWith(SOURCE))
    expect(code.split('\n')[0]).toBe('def run_etl_pipeline():')
  })

  it('opens on the pipeline and keeps the helpers below it', () => {
    const filter = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'greater_than', value: '1' } } },
      [1]
    )
    const code = generatePlainPython(flowWith(SOURCE, filter))
    // Newspaper order: the reader's flow first, the scaffolding after it.
    expect(code).toContain('def match_type_of(')
    expect(code.indexOf('def run_etl_pipeline():')).toBeLessThan(code.indexOf('def match_type_of('))
    expect(code.trimEnd().endsWith('print(row)')).toBe(true)
  })

  it('derives the supported set from the emitter table', () => {
    // Every explained node type is either emitted or deliberately an exercise —
    // there is no third state, and no hand-maintained second list to drift.
    for (const type of PLAIN_PYTHON_NODE_TYPES) {
      expect(NODE_EXPLANATIONS, `${type} has an emitter but no explanation`).toHaveProperty(type)
    }
    expect(PLAIN_PYTHON_NODE_TYPES.has('filter')).toBe(true)
    expect(PLAIN_PYTHON_NODE_TYPES.has('formula')).toBe(false)
    expect(PLAIN_PYTHON_NODE_TYPES.has('polars_code')).toBe(false)
    expect(PLAIN_PYTHON_NODE_TYPES.has('pivot')).toBe(true)
  })

  it('leaves an unsupported node as a passthrough note instead of failing the export', () => {
    const formula = makeNode(2, 'formula', { function: { field: { name: 'c' }, function: '[a] * 2' } }, [1])
    const code = generatePlainPython(flowWith(SOURCE, formula))

    expect(code).toContain('done by the canvas')
    expect(code).toContain('[a] * 2') // the rule is still quoted
    // No fake implementation — the rows pass through under the step's name.
    expect(code).not.toContain('raise NotImplementedError')
    expect(code).toContain('computed = source')
    // The rest of the flow still generated.
    expect(code).toContain('source = [')
  })

  it('falls back to a passthrough note when a supported node has an unsupported setting', () => {
    const advanced = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'advanced', advanced_filter: 'pl.col("a") > 1' } },
      [1]
    )
    const code = generatePlainPython(flowWith(SOURCE, advanced))
    expect(code).toContain('done by the canvas')
    expect(code).toContain('Polars expression')
    expect(code).not.toContain('raise NotImplementedError')
    // The half-written block was rolled back, not left behind.
    expect(code).not.toContain('for row in source:')
  })

  it('does not fuse nodes into a chain the way the Polars flavour does', () => {
    const filter = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'greater_than', value: '1' } } },
      [1]
    )
    const sort = makeNode(3, 'sort', { sort_input: [{ column: 'a', how: 'asc' }] }, [2])
    const code = generatePlainPython(flowWith(SOURCE, filter, sort))

    // Each node keeps its own named result; you cannot pipe two for-loops together.
    expect(code).toContain('filtered = []')
    expect(code).toContain('ordered = sorted(')
  })

  it('honours a node_reference instead of inventing a name', () => {
    const filter = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'greater_than', value: '1' } } },
      [1]
    )
    filter.node_reference = 'big_rows'
    const code = generatePlainPython(flowWith(SOURCE, filter))
    expect(code).toContain('big_rows = []')
    expect(code).toContain('return big_rows')
  })

  it('names loop temporaries plainly, falling back only on a collision', () => {
    const source = makeNode(1, 'manual_input', {
      raw_data_format: { columns: [{ name: 'a' }], data: [[1, 1, 2]] }
    })
    const unique = makeNode(2, 'unique', { unique_input: { subset: ['a'], keep: 'first' } }, [1])

    // The common case: no collision, so the temp is just `seen`.
    expect(generatePlainPython(flowWith(source, unique))).toContain('seen = set()')

    // A node_reference claims the plain name; the temp steps aside.
    const pinned = { ...unique, node_reference: 'seen' }
    const code = generatePlainPython(flowWith(source, pinned))
    expect(code).toContain('seen_2 = set()')
    expect(code).toContain('seen = []')
  })

  it('a passthrough note never defines a function at all', () => {
    const advanced = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'advanced', advanced_filter: 'pl.col("a") > 1' } },
      [1]
    )
    // The old exercise stubs defined functions; a note binds a name and moves on.
    const code = generatePlainPython(flowWith(SOURCE, advanced))
    expect(code).not.toContain('def filter')
    expect(code).toContain('filtered = source')
  })

  it('emits an empty pipeline rather than throwing on an empty flow', () => {
    const code = generatePlainPython({ nodes: new Map(), edges: [] })
    expect(code).toContain('def run_etl_pipeline():')
    expect(code).toContain('return []')
  })

  it('does not choke on a node whose upstream is missing', () => {
    const orphan = makeNode(9, 'filter', {
      filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'equals', value: '1' } }
    })
    expect(() => generatePlainPython(flowWith(orphan))).not.toThrow()
  })
})

describe('per-node explanations', () => {
  it('explains a node with prose and the snippet for its own settings', () => {
    const filter = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'basic', basic_filter: { field: 'b', operator: 'equals', value: 'x' } } },
      [1]
    )
    const explanation = explainNode(flowWith(SOURCE, filter), 2)

    expect(explanation.explanation).toContain('Keeps the rows that match')
    expect(explanation.code).toContain('for row in source:')
    expect(explanation.code).toContain('"b"') // the user's actual column, not a generic example
    // Only this node's block, not the whole script.
    expect(explanation.code).not.toContain('source = [')
    expect(explanation.isStub).toBe(false)
  })

  it('explains a node type that has no loop form without contradicting itself', () => {
    const formula = makeNode(2, 'formula', { function: { field: { name: 'c' }, function: '[a] * 2' } }, [1])
    const explanation = explainNode(flowWith(SOURCE, formula), 2)

    expect(explanation.explanation).not.toBe('')
    expect(explanation.explanation).toContain('the canvas applies this step for you')
    expect(explanation.code).not.toContain('raise NotImplementedError')
    // The drawer hides the plain block and shows the Polars snippet on this flag.
    expect(explanation.isStub).toBe(true)
  })

  it('drops the section banner from a drawer snippet but keeps it in the script', () => {
    // The drawer already names the node, and the banner rule wraps at that width.
    const filter = makeNode(
      2,
      'filter',
      { filter_input: { mode: 'basic', basic_filter: { field: 'b', operator: 'equals', value: 'x' } } },
      [1]
    )
    const flow = flowWith(SOURCE, filter)
    expect(explainNode(flow, 2).code!.startsWith('# --- ')).toBe(false)
    expect(generatePlainPython(flow)).toContain('# --- Filter ---')
  })

  it('has an explanation for every node type the palette can place', () => {
    // A node with no prose renders an empty panel, which reads like a bug.
    const undocumented = Object.values(NODE_TYPES).filter(type => !(type in NODE_EXPLANATIONS))
    expect(undocumented).toEqual([])
  })

  it('returns no snippet for a node that is not in the flow', () => {
    expect(explainNode(flowWith(SOURCE), 42).code).toBeNull()
  })
})

describe('YAML-loaded flows and interactive-only tails', () => {
  it('an explicit rightInputId: null never fabricates a df_right input', () => {
    // Flows loaded from YAML carry `right_input_id: null` on every node; a
    // null id must not register phantom rows_left / df_right names.
    const read = makeNode(
      1,
      'read',
      { received_file: { name: 'sales.csv', path: 'https://example.com/sales.csv', file_type: 'csv' } },
      [],
      { rightInputId: null, leftInputId: null }
    )
    const formula = makeNode(
      2,
      'formula',
      { function: { field: { name: 'x', data_type: 'Float64' }, function: '[a] * 2' } },
      [1],
      { rightInputId: null, leftInputId: null }
    )
    const code = generatePlainPython(flowWith(read, formula))
    expect(code).not.toContain('df_right')
    expect(code).not.toContain('rows_left')
  })

  it('reads a URL-sourced file for real instead of stubbing it', () => {
    const read = makeNode(1, 'read', {
      received_file: { name: 'sales.csv', path: 'https://example.com/sales.csv', file_type: 'csv' }
    })
    const code = generatePlainPython(flowWith(read))
    expect(code).toContain('read_csv_file("https://example.com/sales.csv")')
    expect(code).toContain('urllib.request.urlopen')
    expect(code).not.toContain('NotImplementedError')
  })

  it('a trailing explore_data returns its input, not an undefined df_N', () => {
    const unique = makeNode(2, 'unique', { unique_input: { columns: null, strategy: 'any' } }, [1])
    const explore = makeNode(3, 'explore_data', {}, [2])
    const code = generatePlainPython(flowWith(SOURCE, unique, explore))
    expect(code).not.toContain('df_3')
    expect(code).toMatch(/return deduped/)
  })
})
