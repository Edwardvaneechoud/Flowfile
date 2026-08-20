/** Shared FlowNode factory and flow builder for the plain-Python suites. */

import type { FlowNode, FlowEdge } from '../../src/types'

export function makeNode(
  id: number,
  type: string,
  settings: any = {},
  inputIds: number[] = [],
  extra: any = {}
): FlowNode {
  return {
    id,
    type,
    x: 0,
    y: 0,
    settings: {
      node_id: id,
      is_setup: true,
      cache_results: true,
      pos_x: 0,
      pos_y: 0,
      description: '',
      ...settings
    } as any,
    inputIds,
    ...extra
  }
}

/** Wire the given nodes into a flow, deriving edges from inputIds and left/right inputs. */
export function flowWith(...nodes: FlowNode[]): { nodes: Map<number, FlowNode>; edges: FlowEdge[] } {
  const map = new Map<number, FlowNode>()
  const edges: FlowEdge[] = []
  const link = (from: number, to: number) =>
    edges.push({
      id: `e${from}-${to}`,
      source: String(from),
      target: String(to),
      sourceHandle: 'output-0',
      targetHandle: 'input-0'
    })
  for (const node of nodes) {
    map.set(node.id, node)
    for (const input of node.inputIds) link(input, node.id)
    if (node.leftInputId !== undefined) link(node.leftInputId, node.id)
    if (node.rightInputId !== undefined) link(node.rightInputId, node.id)
  }
  return { nodes: map, edges }
}
