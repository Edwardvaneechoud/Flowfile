import { NodeDatabaseWriter, DatabaseWriteSettings, DatabaseConnection } from "../../../baseNode/nodeInput";


export const createNodeDatabaseWriter = (flowId: number, nodeId: number): NodeDatabaseWriter => {
  const databaseWriteSettings: DatabaseWriteSettings = {
    if_exists: "replace",
    connection_mode: "reference",
    schema_name: undefined,
    table_name: undefined,
    database_connection: {
      database_type: "postgresql",
      username: "",
      password_ref: "",
      host: "localhost",
      port: 4322,
      database: "",
      url: undefined,
    } as DatabaseConnection,
  };
  const nodePolarsCode: NodeDatabaseWriter = {
      flow_id: flowId,
      node_id: nodeId,
      pos_x: 0,
      pos_y: 0,
      database_write_settings: databaseWriteSettings,
      cache_results: false,
  }
  
  return nodePolarsCode
}