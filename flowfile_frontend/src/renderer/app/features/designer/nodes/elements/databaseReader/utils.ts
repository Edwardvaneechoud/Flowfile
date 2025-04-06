import { NodeDatabaseReader, DatabaseConnection, DatabaseSettings } from "../../../baseNode/nodeInput";


export const createNodeDatabaseReader = (flowId: number, nodeId: number): NodeDatabaseReader => {
  const databaseSettings: DatabaseSettings = {
    query_mode: "table",
    schema_name: undefined,
    table_name: undefined,
    query: '',
    database_connection: {
      database_type: "postgresql",
      username: "",
      password_ref: "",
      host: "",
      port: 0,
      database: "",
      url: undefined,
    } as DatabaseConnection,
  };
  const nodePolarsCode: NodeDatabaseReader = {
      flow_id: flowId,
      node_id: nodeId,
      pos_x: 0,
      pos_y: 0,
      database_settings: databaseSettings,
      cache_results: false,
      fields: [],
  }
  
  return nodePolarsCode
}