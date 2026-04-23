import type { NodeLinearRegressionFit } from "@/types/node.types";

export const createLinearRegressionFitNode = (
  flowId: number,
  nodeId: number,
): NodeLinearRegressionFit => ({
  flow_id: flowId,
  node_id: nodeId,
  pos_x: 0,
  pos_y: 0,
  cache_results: false,
  depending_on_id: -1,
  linear_regression_fit_input: {
    feature_cols: [],
    target_col: null,
    artefact_name: "",
    prediction_col: "prediction",
    fit_intercept: true,
    null_policy: "skip",
    solver: "qr",
  },
});
