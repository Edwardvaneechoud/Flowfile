import type { NodeDataScienceFit } from "@/types/node.types";

export const createDataScienceFitNode = (
  flowId: number,
  nodeId: number,
): NodeDataScienceFit => ({
  flow_id: flowId,
  node_id: nodeId,
  pos_x: 0,
  pos_y: 0,
  cache_results: false,
  depending_on_id: -1,
  data_science_fit_input: {
    kind: "linreg",
    feature_cols: [],
    target_col: null,
    artefact_name: "",
    prediction_col: "prediction",
    hyperparams: {},
  },
});
