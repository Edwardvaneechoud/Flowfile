/**
 * The flow graph as plain data, and the two operations a batch edit needs:
 * apply a patch to a graph, and say what differs between two graphs.
 *
 * Pure: no store, no Vue. The flow store morphs its live graph into a target
 * with these, which is how one undo step, a redo and a notebook sync all land
 * as a single change that leaves untouched nodes (and their results) alone.
 */
import type { FlowEdge, FlowNode } from '../types'
import type { FileContent } from '../types/file-content'

export interface GraphState {
  nodes: Map<number, FlowNode>
  edges: FlowEdge[]
  /** Loaded input data by node id. Shared references, never copies. */
  fileContents: Map<number, FileContent>
  nodeIdCounter: number
}

/** A node's input fields are derived from the edges, so a patch never sets them. */
type DerivedNodeFields = 'inputIds' | 'leftInputId' | 'rightInputId'

export interface FlowPatch {
  /** New nodes. Their ids must be free; their inputs come from `addEdges`. */
  addNodes?: Array<Omit<FlowNode, DerivedNodeFields>>
  updateNodes?: Array<Pick<FlowNode, 'id'> & Partial<Omit<FlowNode, 'id' | DerivedNodeFields>>>
  /** Removing a node removes its edges too. */
  removeNodeIds?: number[]
  addEdges?: Array<Omit<FlowEdge, 'id'> & { id?: string }>
  removeEdges?: Array<Omit<FlowEdge, 'id'>>
}

export interface GraphDiff {
  added: number[]
  removed: number[]
  /** Type, settings or inputs differ: the node has to run again. */
  changed: number[]
  /** Only position, description or reference differ: results stay valid. */
  touched: number[]
}

export const edgeId = (edge: Omit<FlowEdge, 'id'>): string =>
  `e${edge.source}-${edge.target}-${edge.sourceHandle}-${edge.targetHandle}`

const sameEdge = (a: Omit<FlowEdge, 'id'>, b: Omit<FlowEdge, 'id'>): boolean =>
  a.source === b.source &&
  a.target === b.target &&
  a.sourceHandle === b.sourceHandle &&
  a.targetHandle === b.targetHandle

/** Record an incoming edge on its target node: `input-0` is the main (left) input, `input-1` a join's right one. */
export function linkEdge(target: FlowNode, edge: Omit<FlowEdge, 'id'>): void {
  const sourceId = parseInt(edge.source)
  if (edge.targetHandle === 'input-0' || !edge.targetHandle) {
    if (!target.inputIds.includes(sourceId)) target.inputIds.push(sourceId)
    target.leftInputId = sourceId
  } else if (edge.targetHandle === 'input-1') {
    target.rightInputId = sourceId
  }
}

/** Forget an incoming edge on its target node. */
export function unlinkEdge(target: FlowNode, edge: Omit<FlowEdge, 'id'>): void {
  const sourceId = parseInt(edge.source)
  target.inputIds = target.inputIds.filter(id => id !== sourceId)
  if (target.leftInputId === sourceId) target.leftInputId = undefined
  if (target.rightInputId === sourceId) target.rightInputId = undefined
}

export function cloneNode(node: FlowNode): FlowNode {
  return JSON.parse(JSON.stringify(node)) as FlowNode
}

/** A detached copy of a graph: nodes and edges are cloned, file contents are shared. */
export function cloneGraph(graph: GraphState): GraphState {
  const nodes = new Map<number, FlowNode>()
  graph.nodes.forEach((node, id) => nodes.set(id, cloneNode(node)))
  return {
    nodes,
    edges: graph.edges.map(edge => ({ ...edge })),
    fileContents: new Map(graph.fileContents),
    nodeIdCounter: graph.nodeIdCounter
  }
}

/**
 * The graph a patch produces, without touching `graph`.
 *
 * Order: removals, then additions and updates, then new edges — so a patch can
 * replace a node's wiring in one go. Throws on a patch that cannot be applied
 * (an id already taken, an edge to a node that is not there), before the caller
 * has changed anything.
 */
export function applyPatch(graph: GraphState, patch: FlowPatch): GraphState {
  const next = cloneGraph(graph)

  const dropEdges = (matches: (edge: FlowEdge) => boolean) => {
    next.edges = next.edges.filter(edge => {
      if (!matches(edge)) return true
      const target = next.nodes.get(parseInt(edge.target))
      if (target) unlinkEdge(target, edge)
      return false
    })
  }

  for (const id of patch.removeNodeIds ?? []) {
    if (!next.nodes.delete(id)) continue
    next.fileContents.delete(id)
    dropEdges(edge => edge.source === String(id) || edge.target === String(id))
  }
  for (const removal of patch.removeEdges ?? []) {
    dropEdges(edge => sameEdge(edge, removal))
  }

  for (const node of patch.addNodes ?? []) {
    if (next.nodes.has(node.id)) throw new Error(`Cannot add node ${node.id}: the id is already in use`)
    next.nodes.set(node.id, { ...cloneNode(node as FlowNode), inputIds: [], leftInputId: undefined, rightInputId: undefined })
    next.nodeIdCounter = Math.max(next.nodeIdCounter, node.id)
  }
  for (const { id, ...fields } of patch.updateNodes ?? []) {
    const node = next.nodes.get(id)
    if (!node) throw new Error(`Cannot update node ${id}: it does not exist`)
    Object.assign(node, JSON.parse(JSON.stringify(fields)))
  }

  for (const addition of patch.addEdges ?? []) {
    const target = next.nodes.get(parseInt(addition.target))
    if (!target || !next.nodes.has(parseInt(addition.source))) {
      throw new Error(`Cannot connect ${addition.source} to ${addition.target}: both nodes must exist`)
    }
    if (next.edges.some(edge => sameEdge(edge, addition))) continue
    next.edges.push({ ...addition, id: addition.id ?? edgeId(addition) })
    linkEdge(target, addition)
  }

  return next
}

const executionKey = (node: FlowNode): string =>
  JSON.stringify([node.type, node.settings, node.inputIds, node.leftInputId ?? null, node.rightInputId ?? null])

const cosmeticKey = (node: FlowNode): string =>
  JSON.stringify([node.x, node.y, node.description ?? '', node.node_reference ?? null])

/** What it takes to turn `current` into `target`, node by node. */
export function diffGraph(current: Pick<GraphState, 'nodes'>, target: Pick<GraphState, 'nodes'>): GraphDiff {
  const diff: GraphDiff = { added: [], removed: [], changed: [], touched: [] }
  current.nodes.forEach((_, id) => {
    if (!target.nodes.has(id)) diff.removed.push(id)
  })
  target.nodes.forEach((node, id) => {
    const existing = current.nodes.get(id)
    if (!existing) diff.added.push(id)
    else if (executionKey(existing) !== executionKey(node)) diff.changed.push(id)
    else if (cosmeticKey(existing) !== cosmeticKey(node)) diff.touched.push(id)
  })
  return diff
}

/** Whether two graphs are the same flow: nodes, wiring and loaded inputs. */
export function sameGraph(a: GraphState, b: GraphState): boolean {
  if (a.nodes.size !== b.nodes.size || a.edges.length !== b.edges.length) return false
  if (a.fileContents.size !== b.fileContents.size) return false
  for (const [id, content] of a.fileContents) {
    if (b.fileContents.get(id) !== content) return false
  }
  const diff = diffGraph(a, b)
  if (diff.added.length || diff.removed.length || diff.changed.length || diff.touched.length) return false
  return a.edges.every(edge => b.edges.some(other => sameEdge(edge, other)))
}
