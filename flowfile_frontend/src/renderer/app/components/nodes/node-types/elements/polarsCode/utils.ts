import { NodePolarsCode, PolarsCodeInput } from "../../../baseNode/nodeInput";

export const createPolarsCodeNode = (flowId: number, nodeId: number): NodePolarsCode => {
  const polarsCodeInput: PolarsCodeInput = {
    polars_code: `# Each connected input arrives as one parameter (a Polars LazyFrame), in connection order.
# Return the result. \`pl\` is available; imports are not allowed.
def transform(input_df: pl.LazyFrame) -> pl.LazyFrame:
    return input_df`,
  };

  const nodePolarsCode: NodePolarsCode = {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    polars_code_input: polarsCodeInput,
    cache_results: false,
  };

  return nodePolarsCode;
};
