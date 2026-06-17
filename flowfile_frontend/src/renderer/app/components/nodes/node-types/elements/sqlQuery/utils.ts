import type { NodeSqlQuery, SqlQueryInput } from "../../../baseNode/nodeInput";

export const createSqlQueryNode = (flowId: number, nodeId: number): NodeSqlQuery => {
  const sqlQueryInput: SqlQueryInput = {
    sql_code: `-- SQL query against connected inputs
-- Tables are named input_1, input_2, etc.
SELECT * FROM input_1`,
  };

  const nodeSqlQuery: NodeSqlQuery = {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    depending_on_ids: null,
    sql_query_input: sqlQueryInput,
    cache_results: false,
  };

  return nodeSqlQuery;
};
