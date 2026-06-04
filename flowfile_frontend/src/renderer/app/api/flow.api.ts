// Flow API Service - Handles all flow-related HTTP requests
import axios from "../services/axios.config";
import type {
  FlowSettings,
  RunInformation,
  VueFlowInput,
  LocalFileInfo,
  NodeConnection,
  NodePromise,
  InputNameInfo,
  HistoryState,
  UndoRedoResult,
  OperationResponse,
  FlowArtifactData,
  CreateGroupRequest,
  UpdateGroupRequest,
  UpdateLayoutRequest,
  GroupOperationResponse,
} from "../types";

export class FlowApi {
  // Flow CRUD Operations

  /**
   * Get all active flow sessions
   */
  static async getAllFlows(): Promise<FlowSettings[]> {
    const response = await axios.get<FlowSettings[]>("/active_flowfile_sessions/", {
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get flow data (nodes and edges) for a specific flow
   */
  static async getFlowData(flowId: number): Promise<VueFlowInput> {
    const response = await axios.get<VueFlowInput>("/flow_data/v2", {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get flow settings for a specific flow
   */
  static async getFlowSettings(flowId: number): Promise<FlowSettings | null> {
    try {
      const response = await axios.get<FlowSettings>("/editor/flow", {
        headers: { accept: "application/json" },
        params: { flow_id: flowId },
        validateStatus: (status) => status === 200 || status === 404,
      });

      if (response.status === 200) {
        return response.data;
      }
      return null;
    } catch (error) {
      return null;
    }
  }

  /**
   * Update flow settings
   */
  static async updateFlowSettings(flowSettings: FlowSettings): Promise<null> {
    const response = await axios.post("/flow_settings", flowSettings, {
      headers: { accept: "application/json" },
    });
    if (response.status === 200) {
      return null;
    }
    throw Error("Error updating flow settings");
  }

  /**
   * Create a new flow
   */
  static async createFlow(
    flowPath: string | null = null,
    name: string | null = null,
  ): Promise<number> {
    const response = await axios.post(
      "/editor/create_flow/",
      {},
      {
        headers: { accept: "application/json" },
        params: {
          flow_path: flowPath,
          name: name,
        },
      },
    );
    if (response.status === 200) {
      return response.data;
    }
    throw Error("Error creating flow");
  }

  /**
   * Close a flow
   */
  static async closeFlow(flowId: number): Promise<any> {
    const response = await axios.post(
      "/editor/close_flow/",
      {},
      {
        params: { flow_id: flowId },
        headers: { accept: "application/json" },
      },
    );
    return response.data;
  }

  /**
   * Import a saved flow from file path
   */
  static async importFlow(flowPath: string): Promise<number> {
    const response = await axios.get("/import_flow/", {
      params: { flow_path: flowPath },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get list of saved flow files
   */
  static async getSavedFlows(): Promise<LocalFileInfo[]> {
    const response = await axios.get<LocalFileInfo[]>("/files/available_flow_files", {
      headers: { accept: "application/json" },
    });
    if (response.status === 200) {
      return response.data;
    }
    throw Error("Error fetching flow data");
  }

  // Flow Execution Operations

  /**
   * Run the entire flow
   */
  static async runFlow(flowId: number): Promise<void> {
    await axios.post("/flow/run/", null, {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
  }

  /**
   * Cancel flow execution
   */
  static async cancelFlow(flowId: number): Promise<void> {
    await axios.post("/flow/cancel/", null, {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
  }

  /**
   * Get run status for a flow
   */
  static async getRunStatus(flowId: number): Promise<RunInformation> {
    const response = await axios.get<RunInformation>("/flow/run_status/", {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Trigger data fetch for a specific node
   */
  static async triggerNodeFetch(flowId: number, nodeId: number): Promise<void> {
    await axios.post("/node/trigger_fetch_data", null, {
      params: {
        flow_id: flowId,
        node_id: nodeId,
      },
      headers: { accept: "application/json" },
    });
  }

  // Node Operations within Flow

  /**
   * Insert a new node into the flow
   */
  static async insertNode(
    flowId: number,
    nodeId: number,
    nodeType: string,
    posX = 0,
    posY = 0,
  ): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>(
      "editor/add_node/",
      {},
      {
        params: {
          flow_id: flowId,
          node_id: nodeId,
          node_type: nodeType,
          pos_x: posX,
          pos_y: posY,
        },
        headers: { accept: "application/json" },
      },
    );
    return response.data;
  }

  /**
   * Copy a node from one flow to another
   */
  static async copyNode(
    nodeIdToCopyFrom: number,
    flowIdToCopyFrom: number,
    nodePromise: NodePromise,
  ): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>("editor/copy_node", nodePromise, {
      params: {
        node_id_to_copy_from: nodeIdToCopyFrom,
        flow_id_to_copy_from: flowIdToCopyFrom,
      },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Delete a node from the flow
   */
  static async deleteNode(flowId: number, nodeId: number): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>(
      "/editor/delete_node/",
      {},
      {
        params: { flow_id: flowId, node_id: nodeId },
        headers: { accept: "application/json" },
      },
    );
    return response.data;
  }

  /**
   * Connect two nodes
   */
  static async connectNode(
    flowId: number,
    nodeConnection: NodeConnection,
  ): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>("/editor/connect_node/", nodeConnection, {
      headers: {
        "Content-Type": "application/json",
        accept: "application/json",
      },
      params: { flow_id: flowId },
    });
    return response.data;
  }

  /**
   * Delete a connection between nodes
   */
  static async deleteConnection(
    flowId: number,
    nodeConnection: NodeConnection,
  ): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>(
      "/editor/delete_connection/",
      nodeConnection,
      {
        params: { flow_id: flowId },
        headers: { accept: "application/json" },
      },
    );
    return response.data;
  }

  // Node Group Operations (visual containers; organizational only)

  /** Create a visual group around a set of nodes. Returns the server-assigned group. */
  static async createGroup(
    flowId: number,
    request: CreateGroupRequest,
  ): Promise<GroupOperationResponse> {
    const response = await axios.post<GroupOperationResponse>("/editor/create_group/", request, {
      params: { flow_id: flowId },
      headers: { "Content-Type": "application/json", accept: "application/json" },
    });
    return response.data;
  }

  /** Rename / recolor / move / resize a group box. */
  static async updateGroup(
    flowId: number,
    groupId: number,
    request: UpdateGroupRequest,
  ): Promise<GroupOperationResponse> {
    const response = await axios.post<GroupOperationResponse>("/editor/update_group/", request, {
      params: { flow_id: flowId, group_id: groupId },
      headers: { "Content-Type": "application/json", accept: "application/json" },
    });
    return response.data;
  }

  /** Delete (ungroup) a group box. Member nodes are kept. */
  static async deleteGroup(flowId: number, groupId: number): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>(
      "/editor/delete_group/",
      {},
      {
        params: { flow_id: flowId, group_id: groupId },
        headers: { accept: "application/json" },
      },
    );
    return response.data;
  }

  /** Add nodes to an existing group. */
  static async addNodesToGroup(
    flowId: number,
    groupId: number,
    nodeIds: number[],
  ): Promise<GroupOperationResponse> {
    const response = await axios.post<GroupOperationResponse>(
      "/editor/group/add_nodes/",
      { node_ids: nodeIds },
      {
        params: { flow_id: flowId, group_id: groupId },
        headers: { "Content-Type": "application/json", accept: "application/json" },
      },
    );
    return response.data;
  }

  /** Remove nodes from their group (a group emptied this way is pruned). */
  static async removeNodesFromGroup(flowId: number, nodeIds: number[]): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>(
      "/editor/group/remove_nodes/",
      { node_ids: nodeIds },
      {
        params: { flow_id: flowId },
        headers: { "Content-Type": "application/json", accept: "application/json" },
      },
    );
    return response.data;
  }

  /** Persist dragged node positions and/or group bounds (one drag-end -> one call). */
  static async updateLayout(
    flowId: number,
    request: UpdateLayoutRequest,
  ): Promise<OperationResponse> {
    const response = await axios.post<OperationResponse>("/editor/update_layout/", request, {
      params: { flow_id: flowId },
      headers: { "Content-Type": "application/json", accept: "application/json" },
    });
    return response.data;
  }

  // History/Undo-Redo Operations

  /**
   * Undo the last action on the flow graph
   */
  static async undo(flowId: number): Promise<UndoRedoResult> {
    const response = await axios.post<UndoRedoResult>("/editor/undo/", null, {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Redo the last undone action on the flow graph
   */
  static async redo(flowId: number): Promise<UndoRedoResult> {
    const response = await axios.post<UndoRedoResult>("/editor/redo/", null, {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get the current state of the history system for a flow
   */
  static async getHistoryStatus(flowId: number): Promise<HistoryState> {
    const response = await axios.get<HistoryState>("/editor/history_status/", {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  // Artifact Operations

  /**
   * Get artifact visualization data for a flow (badges, edges)
   */
  static async getArtifacts(flowId: number): Promise<FlowArtifactData> {
    const response = await axios.get<FlowArtifactData>("/flow/artifacts", {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }

  /**
   * Get the transitive upstream node IDs for a given node in a flow.
   * Used to filter which artifacts are reachable via the DAG.
   */
  static async getNodeUpstreamIds(flowId: number, nodeId: number): Promise<number[]> {
    const response = await axios.get<{ upstream_node_ids: number[] }>("/flow/node_upstream_ids", {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { accept: "application/json" },
    });
    return response.data.upstream_node_ids;
  }

  /**
   * Get the named input keys available for a kernel node.
   * Each entry has: name, source_node_id, source_node_type.
   */
  static async getNodeInputNames(flowId: number, nodeId: number): Promise<InputNameInfo[]> {
    const response = await axios.get("/node/input_names", {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { accept: "application/json" },
    });
    return response.data;
  }
}
