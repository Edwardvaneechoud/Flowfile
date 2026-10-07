/**
 * The notebook's fingerprint moves exactly when the rendered code would: on a
 * node's type, settings, description, reference or incoming edges, and never
 * on where the node sits on the canvas.
 */

import { describe, it, expect } from 'vitest'
import { codeFingerprint } from '../../src/utils/notebookFingerprint'
import type { FlowEdge, FlowNode } from '../../src/types'

const node = (id: number, type: string, settings: Record<string, unknown> = {}, extra: Partial<FlowNode> = {}): FlowNode =>
  ({ id, type, x: 0, y: 0, inputIds: [], settings: { node_id: id, ...settings }, ...extra }) as FlowNode

const edge = (source: number, target: number, targetHandle = 'input-0', sourceHandle = 'output-0'): FlowEdge => ({
  id: `e${source}-${target}-${targetHandle}`,
  source: String(source),
  target: String(target),
  sourceHandle,
  targetHandle
})

const graph = (...nodes: FlowNode[]) => new Map(nodes.map(each => [each.id, each]))

const SOURCE = node(1, 'manual_input', { raw_data_format: { columns: [{ name: 'a' }], data: [[1, 2]] } })
const FILTER = node(2, 'filter', { filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'equals', value: '1' } } })
const BASE = codeFingerprint(graph(SOURCE, FILTER), [edge(1, 2)])

describe('codeFingerprint', () => {
  it('is the same for the same flow, whatever order its nodes, edges and keys arrive in', () => {
    const reordered = node(2, 'filter', {
      filter_input: { basic_filter: { value: '1', operator: 'equals', field: 'a' }, mode: 'basic' }
    })
    expect(codeFingerprint(graph(reordered, SOURCE), [edge(1, 2)])).toBe(BASE)
  })

  it('ignores where a node sits and the other layout fields', () => {
    const moved = { ...FILTER, x: 640, y: 320, settings: { ...FILTER.settings, pos_x: 640, pos_y: 320, is_setup: true, flow_id: 9 } } as FlowNode
    expect(codeFingerprint(graph(SOURCE, moved), [edge(1, 2)])).toBe(BASE)
  })

  it('ignores the id of an edge', () => {
    expect(codeFingerprint(graph(SOURCE, FILTER), [{ ...edge(1, 2), id: 'another-id' }])).toBe(BASE)
  })

  it.each([
    ['a setting', graph(SOURCE, node(2, 'filter', { filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'equals', value: '2' } } })), [edge(1, 2)]],
    ['a description', graph(SOURCE, { ...FILTER, description: 'Keep the ones' }), [edge(1, 2)]],
    ['a reference', graph(SOURCE, { ...FILTER, node_reference: 'ones' }), [edge(1, 2)]],
    ['a node type', graph(SOURCE, { ...FILTER, type: 'sort' }), [edge(1, 2)]],
    ['a removed edge', graph(SOURCE, FILTER), []],
    ['an input handle', graph(SOURCE, FILTER), [edge(1, 2, 'input-1')]],
    ['an output handle', graph(SOURCE, FILTER), [edge(1, 2, 'input-0', 'output-1')]],
    ['an added node', graph(SOURCE, FILTER, node(3, 'sort')), [edge(1, 2)]]
  ] as Array<[string, Map<number, FlowNode>, FlowEdge[]]>)('moves on %s', (_what, nodes, edges) => {
    expect(codeFingerprint(nodes, edges)).not.toBe(BASE)
  })

  it('reads a node with no settings', () => {
    const bare = { id: 1, type: 'manual_input', x: 0, y: 0, inputIds: [] } as unknown as FlowNode
    expect(() => codeFingerprint(graph(bare), [])).not.toThrow()
  })
})
