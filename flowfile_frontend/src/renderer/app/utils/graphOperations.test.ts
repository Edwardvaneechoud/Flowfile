import { describe, expect, it } from "vitest";
import {
  addConfiguredNodeOperations,
  buildRemovalBatch,
  connection,
  createRemovalCollector,
  deleteConnectionOperations,
  edgeConnection,
  insertOnEdgeOperation,
  isBackendEdge,
  type EdgeLike,
  type RemovalTick,
} from "./graphOperations";

const edge = (
  source: string,
  target: string,
  sourceHandle = "output-0",
  targetHandle = "input-0",
) =>
  ({
    id: `e${source}-${target}-${sourceHandle}-${targetHandle}`,
    source,
    target,
    sourceHandle,
    targetHandle,
  }) as EdgeLike;

describe("buildRemovalBatch", () => {
  it("deletes a node without re-deleting the edges its removal cascades", () => {
    // removeNodes(["2"]) emits 1->2 and 2->3 before the node change.
    const batch = buildRemovalBatch(new Map([["2", "filter"]]), [edge("1", "2"), edge("2", "3")]);
    expect(batch.operations).toEqual([{ op: "delete_node", node_id: 2 }]);
    expect(batch.label).toBe("Delete filter node");
  });

  it("keeps user-selected edges between surviving nodes in the same batch", () => {
    const nodes = new Map<string, string | undefined>([
      ["2", "filter"],
      ["3", "sort"],
    ]);
    const edges = [edge("2", "3"), edge("1", "2"), edge("4", "5", "output-1", "input-1")];
    const batch = buildRemovalBatch(nodes, edges);
    expect(batch.operations).toEqual([
      {
        op: "delete_connection",
        connection: edgeConnection(edge("4", "5", "output-1", "input-1")),
      },
      { op: "delete_node", node_id: 2 },
      { op: "delete_node", node_id: 3 },
    ]);
    expect(batch.label).toBe("Delete 2 nodes and 1 connection");
  });

  it("deletes several edges in one batch (no early return on multi-edge deletes)", () => {
    const batch = buildRemovalBatch(new Map(), [edge("1", "2"), edge("3", "4")]);
    expect(batch.operations.map((op) => op.op)).toEqual(["delete_connection", "delete_connection"]);
    expect(batch.label).toBe("Delete 2 connections");
  });

  it("sends an edge reported twice only once", () => {
    const batch = buildRemovalBatch(new Map(), [edge("1", "2"), edge("1", "2")]);
    expect(batch.operations).toHaveLength(1);
    expect(batch.label).toBe("Delete connection");
  });

  it("labels multi-node and untyped removals", () => {
    const three = new Map<string, string | undefined>([
      ["1", "read"],
      ["2", "filter"],
      ["3", undefined],
    ]);
    expect(buildRemovalBatch(three, []).label).toBe("Delete 3 nodes");
    expect(buildRemovalBatch(new Map([["9", undefined]]), []).label).toBe("Delete node");
  });

  it("is empty when nothing was removed", () => {
    expect(buildRemovalBatch(new Map(), []).operations).toEqual([]);
  });

  it("deletes comments in the same batch as the nodes and edges (one undo step)", () => {
    const batch = buildRemovalBatch(new Map([["2", "filter"]]), [edge("4", "5")], [3, 9, 3]);
    expect(batch.operations).toEqual([
      { op: "delete_connection", connection: edgeConnection(edge("4", "5")) },
      { op: "delete_node", node_id: 2 },
      { op: "delete_comment", comment_id: 3 },
      { op: "delete_comment", comment_id: 9 },
    ]);
    expect(batch.label).toBe("Delete 1 node, 1 connection and 2 comments");
  });

  it("sends a comment-only removal as a batch too", () => {
    const batch = buildRemovalBatch(new Map(), [], [5]);
    expect(batch.operations).toEqual([{ op: "delete_comment", comment_id: 5 }]);
    expect(batch.label).toBe("Delete comment");
  });
});

describe("insertOnEdgeOperation", () => {
  it("splices a node into the edge in place instead of delete + connect + connect", () => {
    // The target keeps its input slot, so polars_code/union inputs stay in order.
    expect(insertOnEdgeOperation(9, edge("1", "3", "output-1", "input-0"))).toEqual({
      op: "insert_on_edge",
      node_id: 9,
      connection: connection(1, "output-1", 3, "input-0"),
    });
  });
});

describe("addConfiguredNodeOperations", () => {
  it("adds the node at its position and saves settings that carry no position", () => {
    const operations = addConfiguredNodeOperations(
      4,
      12,
      "manual_input",
      { x: 10, y: 20 },
      { is_setup: true, raw_data_format: { columns: [], data: [] } },
    );
    expect(operations).toEqual([
      { op: "add_node", node_id: 12, node_type: "manual_input", pos_x: 10, pos_y: 20 },
      {
        op: "update_settings",
        node_type: "manual_input",
        settings: {
          is_setup: true,
          raw_data_format: { columns: [], data: [] },
          flow_id: 4,
          node_id: 12,
        },
      },
    ]);
  });
});

describe("createRemovalCollector", () => {
  it("turns one tick of VueFlow removals into one batch", async () => {
    const flushed: RemovalTick[] = [];
    const removalsThisTick = createRemovalCollector(
      () => 7,
      (tick) => flushed.push(tick),
    );
    // Delete key on node 2 plus a selected edge 4->5: removeNodes reports the node's
    // edges, then the node; removeEdges then reports the selected edge — same tick.
    removalsThisTick().edges.push(edge("1", "2"), edge("2", "3"));
    removalsThisTick().nodes.set("2", "filter");
    removalsThisTick().edges.push(edge("4", "5"));
    expect(flushed).toHaveLength(0);

    await Promise.resolve();
    expect(flushed).toHaveLength(1);
    expect(flushed[0].flowId).toBe(7);
    expect(buildRemovalBatch(flushed[0].nodes, flushed[0].edges).operations).toEqual([
      { op: "delete_connection", connection: edgeConnection(edge("4", "5")) },
      { op: "delete_node", node_id: 2 },
    ]);

    removalsThisTick().edges.push(edge("8", "9"));
    await Promise.resolve();
    expect(flushed).toHaveLength(2);
    expect(flushed[1].edges).toEqual([edge("8", "9")]);
  });
});

describe("deleteConnectionOperations", () => {
  it("never sends a collapsed group's proxy edges to core", () => {
    // A gate's else edge into a collapsed group shows as the real edge plus a proxy to the pill.
    const real = edge("4", "7", "output-1");
    const proxy = { ...edge("4", "group-2", "output-1", "group-target"), id: "group-proxy-2-in" };
    expect(isBackendEdge(proxy)).toBe(false);
    expect(deleteConnectionOperations([real, proxy])).toEqual([
      { op: "delete_connection", connection: edgeConnection(real) },
    ]);
  });
});

describe("edgeConnection", () => {
  it("defaults missing handles to the first input/output", () => {
    expect(edgeConnection({ source: "1", target: "2" })).toEqual({
      input_connection: { node_id: 2, connection_class: "input-0" },
      output_connection: { node_id: 1, connection_class: "output-0" },
    });
  });
});
