import axios from "axios";
import { NodeGraphicWalker } from "./interfaces";

export const fetchGraphicWalkerData = async (
  flowId: number,
  nodeId: number,
): Promise<NodeGraphicWalker> => {
  try {
    const response = await axios.get<NodeGraphicWalker>("/analysis_data/graphic_walker_input", {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { Accept: "application/json" },
      timeout: 30000, // Add timeout
    });

    if (!response.data || !response.data.graphic_walker_input) {
      throw new Error("Invalid response data structure");
    }

    return response.data;
  } catch (error: any) {
    // Enhanced error handling
    if (error.response) {
      // Server responded with an error status
      console.error(`[GraphicWalker] Server error ${error.response.status}:`, error.response.data);
    } else if (error.request) {
      // Request was made but no response received
      console.error("[GraphicWalker] No response received:", error.request);
    } else {
      // Error in setting up the request
      console.error("[GraphicWalker] Request error:", error.message);
    }

    // Re-throw for component-level handling
    throw error;
  }
};
