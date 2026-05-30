import type { NodeBase } from "../../../../../types/node.types";

export interface NodeApiResponse extends NodeBase {
  orientation: "records" | "columns";
  max_rows: number | null;
}

export function createDefaultApiResponse(flowId: number, nodeId: number): NodeApiResponse {
  return {
    flow_id: flowId,
    node_id: nodeId,
    cache_results: false,
    pos_x: 0,
    pos_y: 0,
    is_setup: false,
    description: "",
    orientation: "records",
    max_rows: null,
  };
}
