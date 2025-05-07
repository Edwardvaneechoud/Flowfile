import { NodePolarsCode, PolarsCodeInput } from "../../../baseNode/nodeInput";


export const createPolarsCodeNode = (flowId: number, nodeId: number): NodePolarsCode => {
    const polarsCodeInput: PolarsCodeInput = {
        polars_code: `# Example of usage (you can remove this)
# Single line transformations:
#   input_df.filter(pl.col('column_name') > 0)

# Multi-line transformations (must assign to output_df):
#   result = input_df.select(['a', 'b'])
#   filtered = result.filter(pl.col('a') > 0)
#   output_df = filtered.with_columns(pl.col('b').alias('new_b'))

# Multiple input dataframes are available as input_df_0, input_df_1, etc:
#   output_df = input_df_0.join(input_df_1, on='id')

# No inputs example (node will act as a starter node):
#   output_df = pl.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

# Your code here:
input_df`
    }
    
    const nodePolarsCode: NodePolarsCode = {
        flow_id: flowId,
        node_id: nodeId,
        pos_x: 0,
        pos_y: 0,
        polars_code_input: polarsCodeInput,
        cache_results: false,
    }
    
    return nodePolarsCode
}