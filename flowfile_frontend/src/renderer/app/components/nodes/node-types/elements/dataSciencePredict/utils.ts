import type { NodeDataSciencePredict } from "@/types/node.types";

export const createDataSciencePredictNode = (
  flowId: number,
  nodeId: number,
): NodeDataSciencePredict => ({
  flow_id: flowId,
  node_id: nodeId,
  pos_x: 0,
  pos_y: 0,
  cache_results: false,
  depending_on_id: -1,
  data_science_predict_input: {
    artefact_name: "",
    artefact_version: null,
    feature_cols: [],
    prediction_col: "prediction",
  },
});
