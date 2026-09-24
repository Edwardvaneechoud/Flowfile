// Pure builders for the primitive operations one canvas gesture sends to core.
import type { GraphOperation, NodeConnection } from "../types";
import { plural } from "./text";

export interface EdgeLike {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

/** The one NodeConnection builder: sourceId's output handle -> targetId's input handle. */
export function connection(
  sourceId: number,
  sourceHandle: string,
  targetId: number,
  targetHandle: string,
): NodeConnection {
  return {
    input_connection: {
      node_id: targetId,
      connection_class: targetHandle as NodeConnection["input_connection"]["connection_class"],
    },
    output_connection: {
      node_id: sourceId,
      connection_class: sourceHandle as NodeConnection["output_connection"]["connection_class"],
    },
  };
}

export function edgeConnection(edge: Omit<EdgeLike, "id">): NodeConnection {
  return connection(
    Number(edge.source),
    edge.sourceHandle ?? "output-0",
    Number(edge.target),
    edge.targetHandle ?? "input-0",
  );
}

/**
 * Splice an existing node into an edge. Core replaces the edge in place, so the target
 * keeps the input's position (positional multi-input nodes compute the same data).
 */
export function insertOnEdgeOperation(nodeId: number, edge: Omit<EdgeLike, "id">): GraphOperation {
  return { op: "insert_on_edge", node_id: nodeId, connection: edgeConnection(edge) };
}

/**
 * A new node plus its settings. Positions ride only on add_node: core owns them, so the
 * settings carry none.
 */
export function addConfiguredNodeOperations(
  flowId: number,
  nodeId: number,
  nodeType: string,
  position: { x: number; y: number },
  settings: Record<string, unknown>,
): GraphOperation[] {
  return [
    { op: "add_node", node_id: nodeId, node_type: nodeType, pos_x: position.x, pos_y: position.y },
    {
      op: "update_settings",
      node_type: nodeType,
      settings: { ...settings, flow_id: flowId, node_id: nodeId },
    },
  ];
}

const isNodeId = (vueId: string) => /^\d+$/.test(vueId);

/** Only an edge between two data nodes exists in core; a group proxy edge is UI-only. */
export function isBackendEdge(edge: Pick<EdgeLike, "source" | "target">): boolean {
  return isNodeId(edge.source) && isNodeId(edge.target);
}

export function deleteConnectionOperations(edges: EdgeLike[]): GraphOperation[] {
  return edges
    .filter(isBackendEdge)
    .map((edge) => ({ op: "delete_connection", connection: edgeConnection(edge) }));
}

/** What one delete gesture removed from the canvas. */
export interface RemovalTick {
  flowId: number;
  // Removed node id -> its node type.
  nodes: Map<string, string | undefined>;
  edges: EdgeLike[];
  comments: number[];
}

/**
 * Collect the removals VueFlow reports within one tick (removeNodes emits the edge
 * changes, then the node changes, synchronously) and hand them to `flush` once, as one
 * gesture. The returned function gives the current tick's bucket.
 */
export function createRemovalCollector(
  currentFlowId: () => number,
  flush: (tick: RemovalTick) => void,
  schedule: (task: () => void) => void = queueMicrotask,
): () => RemovalTick {
  let pending: RemovalTick | null = null;
  return () => {
    if (!pending) {
      const tick: RemovalTick = {
        flowId: currentFlowId(),
        nodes: new Map(),
        edges: [],
        comments: [],
      };
      pending = tick;
      schedule(() => {
        pending = null;
        flush(tick);
      });
    }
    return pending;
  };
}

/**
 * One delete gesture (a VueFlow removal tick) as a single batch. `nodes` maps each
 * removed node id to its type. Edges touching a removed node are dropped: deleting
 * the node removes them on the backend, and a second delete would fail the batch.
 */
export function buildRemovalBatch(
  nodes: Map<string, string | undefined>,
  edges: EdgeLike[],
  comments: number[] = [],
): { label: string; operations: GraphOperation[] } {
  const seen = new Set<string>();
  const survivingEdges = edges.filter((edge) => {
    if (nodes.has(edge.source) || nodes.has(edge.target) || seen.has(edge.id)) return false;
    seen.add(edge.id);
    return true;
  });
  const uniqueComments = [...new Set(comments)];
  const operations: GraphOperation[] = [
    ...deleteConnectionOperations(survivingEdges),
    ...[...nodes.keys()].map((id): GraphOperation => ({ op: "delete_node", node_id: Number(id) })),
    ...uniqueComments.map((id): GraphOperation => ({ op: "delete_comment", comment_id: id })),
  ];
  return {
    label: removalLabel(nodes, survivingEdges.length, uniqueComments.length),
    operations,
  };
}

function removalLabel(
  nodes: Map<string, string | undefined>,
  edgeCount: number,
  commentCount: number,
): string {
  if (nodes.size === 1 && edgeCount === 0 && commentCount === 0) {
    const [type] = [...nodes.values()];
    return type ? `Delete ${type} node` : "Delete node";
  }
  if (nodes.size === 0 && edgeCount + commentCount === 1) {
    return edgeCount === 1 ? "Delete connection" : "Delete comment";
  }
  const parts = [
    nodes.size > 0 ? plural(nodes.size, "node") : null,
    edgeCount > 0 ? plural(edgeCount, "connection") : null,
    commentCount > 0 ? plural(commentCount, "comment") : null,
  ].filter((part): part is string => part !== null);
  const last = parts.pop();
  return parts.length > 0 ? `Delete ${parts.join(", ")} and ${last}` : `Delete ${last ?? ""}`;
}
