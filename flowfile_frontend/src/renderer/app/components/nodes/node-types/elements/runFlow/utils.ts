import type { NodeRunFlow } from "../../../../../types/node.types";

export const createNodeRunFlow = (flowId: number, nodeId: number): NodeRunFlow => {
  return {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    cache_results: false,
    flow_reference: null,
    flow_registration_id: null,
    parameter_mappings: [],
    delay_seconds: 0,
    max_rows: 1000,
  };
};
