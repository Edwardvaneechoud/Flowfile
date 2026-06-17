import axios from "axios";
import { NodeData, nodeData } from "./nodeInterfaces";

export const getNodeData = async (flow_id: number, node_id: number): Promise<NodeData | null> => {
  try {
    const response = await axios.get<NodeData>("/node", {
      params: { flow_id, node_id },
      headers: { accept: "application/json" },
    });

    nodeData.value = response.data;
    return response.data;
  } catch (error) {
    console.error("Error fetching node data:", error);
    nodeData.value = null;
    return null;
  }
};
