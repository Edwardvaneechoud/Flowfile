// DEPRECATED: Import from '@/api' or '../../api' instead
// This file is kept for backward compatibility during migration

import { FlowApi } from "../../api/flow.api";
import { useHistoryStore } from "../../stores/history-store";
import type { NodeConnection, NodePromise } from "../../types";

// Re-export types
export type {
  AxiosResponse,
  NodeInputConnection,
  NodeOutputConnection,
  NodeConnection,
} from "../../types/canvas.types";
export type { FlowSettings } from "../../types/flow.types";

// Helper to refresh history status after state-changing operations
const refreshHistoryAfterAction = async (flowId: number) => {
  const historyStore = useHistoryStore();
  await historyStore.refreshStatus(flowId);
};

// Legacy function wrappers that delegate to the new API
export const connectNode = async (flowId: number, nodeConnection: NodeConnection) => {
  console.log("Connecting node where it should happen", nodeConnection);
  await FlowApi.connectNode(flowId, nodeConnection);
  await refreshHistoryAfterAction(flowId);
};

export const deleteConnection = async (
  flowId: number,
  nodeConnection: NodeConnection,
): Promise<any> => {
  const result = await FlowApi.deleteConnection(flowId, nodeConnection);
  await refreshHistoryAfterAction(flowId);
  return result;
};

export const closeFlow = async (flow_id: number): Promise<any> => {
  return FlowApi.closeFlow(flow_id);
};

export const deleteNode = async (flow_id: number, node_id: number): Promise<any> => {
  const result = await FlowApi.deleteNode(flow_id, node_id);
  await refreshHistoryAfterAction(flow_id);
  return result;
};

export const insertNode = async (
  flow_id: number,
  node_id: number,
  node_type: string,
  pos_x = 0,
  pos_y = 0,
): Promise<any> => {
  console.log("inserting a note");
  const result = await FlowApi.insertNode(flow_id, node_id, node_type, pos_x, pos_y);
  await refreshHistoryAfterAction(flow_id);
  return result;
};

export const copyNode = async (
  nodeIdToCopyFrom: number,
  flowIdToCopyFrom: number,
  nodePromise: NodePromise,
): Promise<any> => {
  console.log("copying a note");
  const result = await FlowApi.copyNode(nodeIdToCopyFrom, flowIdToCopyFrom, nodePromise);
  await refreshHistoryAfterAction(nodePromise.flow_id);
  return result;
};

export const getAllFlows = async () => {
  return FlowApi.getAllFlows();
};

export const getFlowData = async (flowId: number) => {
  return FlowApi.getFlowData(flowId);
};

export const importSavedFlow = async (flowPath: string) => {
  console.log("Importing flow from path:", flowPath);
  try {
    return await FlowApi.importFlow(flowPath);
  } catch (error) {
    console.error("There was an error fetching the flow:", error);
  }
};
