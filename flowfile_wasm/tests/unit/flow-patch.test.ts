/**
 * The pure graph operations behind undo/redo and batch edits: applying a patch
 * to a graph and telling two graphs apart.
 */

import { describe, it, expect } from 'vitest'
import { applyPatch, cloneGraph, diffGraph, edgeId, sameGraph, type GraphState } from '../../src/utils/flowPatch'
import type { FlowEdge, FlowNode } from '../../src/types'

const node = (id: number, type: string, settings: Record<string, unknown> = {}): FlowNode => ({
  id,
  type,
  x: id * 100,
  y: 0,
  settings: { node_id: id, ...settings } as any,
  inputIds: [],
  leftInputId: undefined,
  rightInputId: undefined,
  description: ''
})

const edge = (source: number, target: number, targetHandle = 'input-0'): FlowEdge => {
  const wiring = { source: String(source), target: String(target), sourceHandle: 'output-0', targetHandle }
  return { id: edgeId(wiring), ...wiring }
}

/** 1 → 2 → 3 */
function chain(): GraphState {
  const nodes = new Map<number, FlowNode>([
    [1, node(1, 'manual_input')],
    [2, { ...node(2, 'filter'), inputIds: [1], leftInputId: 1 }],
    [3, { ...node(3, 'sort'), inputIds: [2], leftInputId: 2 }]
  ])
  return { nodes, edges: [edge(1, 2), edge(2, 3)], fileContents: new Map(), nodeIdCounter: 3 }
}

describe('applyPatch', () => {
  it('leaves the graph it was given untouched', () => {
    const graph = chain()
    const before = cloneGraph(graph)

    applyPatch(graph, { removeNodeIds: [2], updateNodes: [{ id: 3, settings: { changed: true } as any }] })

    expect(sameGraph(graph, before)).toBe(true)
  })

  it('removes a node together with its edges and its place in downstream inputs', () => {
    const next = applyPatch(chain(), { removeNodeIds: [2] })

    expect([...next.nodes.keys()]).toEqual([1, 3])
    expect(next.edges).toEqual([])
    expect(next.nodes.get(3)!.inputIds).toEqual([])
    expect(next.nodes.get(3)!.leftInputId).toBeUndefined()
  })

  it('adds a node and wires it in one patch, deriving the input fields from the edges', () => {
    const { inputIds: _i, leftInputId: _l, rightInputId: _r, ...added } = node(7, 'join')
    const next = applyPatch(chain(), {
      addNodes: [added],
      addEdges: [
        { source: '2', target: '7', sourceHandle: 'output-0', targetHandle: 'input-0' },
        { source: '3', target: '7', sourceHandle: 'output-0', targetHandle: 'input-1' }
      ]
    })

    const join = next.nodes.get(7)!
    expect(join.inputIds).toEqual([2])
    expect(join.leftInputId).toBe(2)
    expect(join.rightInputId).toBe(3)
    expect(next.edges.map(e => e.id)).toContain('e3-7-output-0-input-1')
    expect(next.nodeIdCounter).toBe(7)
  })

  it('rewires a node: the old edge is removed before the new one is added', () => {
    const next = applyPatch(chain(), {
      removeEdges: [{ source: '2', target: '3', sourceHandle: 'output-0', targetHandle: 'input-0' }],
      addEdges: [{ source: '1', target: '3', sourceHandle: 'output-0', targetHandle: 'input-0' }]
    })

    expect(next.nodes.get(3)!.inputIds).toEqual([1])
    expect(next.nodes.get(3)!.leftInputId).toBe(1)
    expect(next.edges.map(e => e.id)).toEqual(['e1-2-output-0-input-0', 'e1-3-output-0-input-0'])
  })

  it('updates only the fields a patch names', () => {
    const next = applyPatch(chain(), { updateNodes: [{ id: 2, settings: { filter: 'x' } as any }] })

    expect(next.nodes.get(2)!.settings).toEqual({ filter: 'x' })
    expect(next.nodes.get(2)!.x).toBe(200)
    expect(next.nodes.get(2)!.inputIds).toEqual([1])
  })

  it('does not duplicate an edge that is already there', () => {
    const next = applyPatch(chain(), {
      addEdges: [{ source: '1', target: '2', sourceHandle: 'output-0', targetHandle: 'input-0' }]
    })
    expect(next.edges).toHaveLength(2)
  })

  it('refuses a patch that cannot be applied', () => {
    const { inputIds: _i, leftInputId: _l, rightInputId: _r, ...taken } = node(2, 'sort')
    expect(() => applyPatch(chain(), { addNodes: [taken] })).toThrow('already in use')
    expect(() => applyPatch(chain(), { updateNodes: [{ id: 9, description: 'x' }] })).toThrow('does not exist')
    expect(() =>
      applyPatch(chain(), { addEdges: [{ source: '1', target: '9', sourceHandle: 'output-0', targetHandle: 'input-0' }] })
    ).toThrow('both nodes must exist')
  })
})

describe('diffGraph', () => {
  it('separates a node that must run again from one that only moved', () => {
    const current = chain()
    const target = applyPatch(current, {
      removeNodeIds: [1],
      updateNodes: [
        { id: 3, x: 999, description: 'moved and described' }
      ],
      addNodes: [{ id: 4, type: 'head', x: 0, y: 0, settings: {} as any, description: '' }]
    })

    expect(diffGraph(current, target)).toEqual({
      added: [4],
      removed: [1],
      // 2 lost its input, so it has to run again; 3 only moved.
      changed: [2],
      touched: [3]
    })
  })

  it('finds nothing between a graph and its copy', () => {
    const graph = chain()
    expect(diffGraph(graph, cloneGraph(graph))).toEqual({ added: [], removed: [], changed: [], touched: [] })
  })
})

describe('sameGraph', () => {
  it('tells apart graphs that differ only in loaded input data', () => {
    const a = chain()
    const b = cloneGraph(a)
    expect(sameGraph(a, b)).toBe(true)

    b.fileContents.set(1, { kind: 'text', data: 'a\n1' })
    expect(sameGraph(a, b)).toBe(false)
  })

  it('tells apart graphs that differ only in a handle', () => {
    const a = chain()
    const b = cloneGraph(a)
    b.edges[1] = edge(2, 3, 'input-1')
    expect(sameGraph(a, b)).toBe(false)
  })
})
