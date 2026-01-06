// Node API Service - Handles all node-related HTTP requests
import axios from "../services/axios.config";
import type { NodeData, TableExample } from "../types";

export class NodeApi {
  /**
   * Get node data for a specific node
   */
  static async getNodeData(flowId: number, nodeId: number): Promise<NodeData> {
    const response = await axios.get<NodeData>("/node", {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get table example/preview data for a node
   */
  static async getTableExample(flowId: number, nodeId: number): Promise<TableExample> {
    const response = await axios.get<TableExample>("/node/data", {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get downstream node IDs for a given node
   */
  static async getDownstreamNodeIds(flowId: number, nodeId: number): Promise<number[]> {
    const response = await axios.get<number[]>("/node/downstream_node_ids", {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get node description
   */
  static async getNodeDescription(flowId: number, nodeId: number): Promise<string> {
    const response = await axios.get<string>("/node/description", {
      params: { node_id: nodeId, flow_id: flowId },
    });
    return response.data;
  }

  /**
   * Set/update node description
   */
  static async setNodeDescription(
    flowId: number,
    nodeId: number,
    description: string,
  ): Promise<boolean> {
    const response = await axios.post("/node/description/", JSON.stringify(description), {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { "Content-Type": "application/json" },
    });
    return response.data;
  }

  /**
   * Update node settings directly
   */
  static async updateSettingsDirectly(nodeType: string, inputData: any): Promise<any> {
    const response = await axios.post("/update_settings/", inputData, {
      params: { node_type: nodeType },
    });
    return response.data;
  }

  /**
   * Update user-defined node settings
   */
  static async updateUserDefinedSettings(nodeType: string, inputData: any): Promise<any> {
    const response = await axios.post(
      "/user_defined_components/update_user_defined_node/",
      inputData,
      {
        params: { node_type: nodeType },
      },
    );
    return response.data;
  }
}
