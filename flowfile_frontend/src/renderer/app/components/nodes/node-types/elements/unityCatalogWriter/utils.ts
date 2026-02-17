export interface UnityCatalogWriteSettings {
  connection_name?: string;
  table_ref: {
    catalog_name: string;
    schema_name: string;
    table_name: string;
  };
  data_source_format: string;
  write_mode: string;
  register_table: boolean;
  table_comment?: string;
}

export interface NodeUnityCatalogWriter {
  flow_id: string | number;
  node_id: number;
  depending_on_id: number;
  pos_x: number;
  pos_y: number;
  cache_results: boolean;
  is_setup?: boolean;
  unity_catalog_settings: UnityCatalogWriteSettings;
}

export const createNodeUnityCatalogWriter = (
  flowId: number,
  nodeId: number,
): NodeUnityCatalogWriter => {
  return {
    flow_id: flowId,
    node_id: nodeId,
    depending_on_id: -1,
    pos_x: 0,
    pos_y: 0,
    cache_results: false,
    unity_catalog_settings: {
      connection_name: undefined,
      table_ref: {
        catalog_name: "",
        schema_name: "",
        table_name: "",
      },
      data_source_format: "DELTA",
      write_mode: "overwrite",
      register_table: true,
      table_comment: "",
    },
  };
};
