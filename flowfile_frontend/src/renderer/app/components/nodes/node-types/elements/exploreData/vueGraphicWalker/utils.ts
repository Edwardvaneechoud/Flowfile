import axios from "axios";
import type {
  VisualizationComputeResponse,
  VisualizationFieldsResponse,
} from "../../../../../../types/catalog.types";
import { NodeGraphicWalker } from "./interfaces";

// The first call against a node materialises its result to Arrow IPC on the
// worker, so the field fetch gets the generous budget; later chart queries hit
// the warm session and only need the worker's own 120s request ceiling.
const MATERIALIZE_TIMEOUT_MS = 300_000;
const COMPUTE_TIMEOUT_MS = 120_000;

export const fetchGraphicWalkerData = async (
  flowId: number,
  nodeId: number,
): Promise<NodeGraphicWalker> => {
  console.log(`[GraphicWalker] Fetching spec for flow ${flowId}, node ${nodeId}`);
  try {
    const response = await axios.get<NodeGraphicWalker>("/analysis_data/graphic_walker_input", {
      params: { flow_id: flowId, node_id: nodeId },
      headers: { Accept: "application/json" },
      timeout: 30000,
    });

    if (!response.data || !response.data.graphic_walker_input) {
      throw new Error("Invalid response data structure");
    }

    return response.data;
  } catch (error: any) {
    if (error.response) {
      console.error(`[GraphicWalker] Server error ${error.response.status}:`, error.response.data);
    } else if (error.request) {
      console.error("[GraphicWalker] No response received:", error.request);
    } else {
      console.error("[GraphicWalker] Request error:", error.message);
    }

    throw error;
  }
};

/** Field schema for a node's result, inferred by polars-gw on the worker. */
export const fetchNodeVisualizationFields = async (
  flowId: number,
  nodeId: number,
): Promise<VisualizationFieldsResponse> => {
  const response = await axios.post<VisualizationFieldsResponse>(
    "/analysis_data/fields",
    { flow_id: flowId, node_id: nodeId },
    { timeout: MATERIALIZE_TIMEOUT_MS },
  );
  return response.data;
};

/** Push one GraphicWalker IDataQueryPayload down to polars-gw on the worker. */
export const computeNodeVisualization = async (
  flowId: number,
  nodeId: number,
  payload: Record<string, any>,
  maxRows?: number,
): Promise<VisualizationComputeResponse> => {
  const response = await axios.post<VisualizationComputeResponse>(
    "/analysis_data/compute",
    { flow_id: flowId, node_id: nodeId, payload, max_rows: maxRows },
    { timeout: COMPUTE_TIMEOUT_MS },
  );
  return response.data;
};
