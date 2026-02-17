import type { NodeUnityCatalogReader, UnityCatalogReadSettings } from "../../../../../views/UnityCatalogView/UnityCatalogTypes";

export const createNodeUnityCatalogReader = (
  flowId: number,
  nodeId: number,
): NodeUnityCatalogReader => {
  const settings: UnityCatalogReadSettings = {
    connection_name: undefined,
    table_ref: {
      catalog_name: "",
      schema_name: "",
      table_name: "",
    },
  };
  return {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    cache_results: false,
    unity_catalog_settings: settings,
    fields: [],
  };
};
