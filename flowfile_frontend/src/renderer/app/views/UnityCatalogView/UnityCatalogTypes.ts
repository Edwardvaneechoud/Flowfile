// Unity Catalog connection types and interfaces

export interface PythonUnityCatalogConnectionInput {
  connection_name: string;
  server_url: string;
  auth_token?: string;
  default_catalog?: string;
  credential_vending_enabled: boolean;
}

export interface UnityCatalogConnectionInput {
  connectionName: string;
  serverUrl: string;
  authToken?: string;
  defaultCatalog?: string;
  credentialVendingEnabled: boolean;
}

export interface PythonUnityCatalogConnectionInterface {
  connection_name: string;
  server_url: string;
  default_catalog?: string;
  credential_vending_enabled: boolean;
}

export interface UnityCatalogConnectionInterface {
  connectionName: string;
  serverUrl: string;
  defaultCatalog?: string;
  credentialVendingEnabled: boolean;
}

// UC browse types
export interface CatalogInfo {
  name: string;
  comment?: string;
  id?: string;
}

export interface SchemaInfo {
  name: string;
  catalog_name: string;
  comment?: string;
  full_name?: string;
}

export interface TableInfo {
  name: string;
  catalog_name: string;
  schema_name: string;
  table_type: string;
  data_source_format: string;
  columns: ColumnInfo[];
  storage_location?: string;
  comment?: string;
  table_id?: string;
}

export interface ColumnInfo {
  name: string;
  type_name: string;
  type_text: string;
  position: number;
  comment?: string;
  nullable?: boolean;
}

// UC node settings
export interface UnityCatalogTableRef {
  catalog_name: string;
  schema_name: string;
  table_name: string;
}

export interface UnityCatalogReadSettings {
  connection_name?: string;
  table_ref: UnityCatalogTableRef;
}

export interface NodeUnityCatalogReader {
  flow_id: string | number;
  node_id: number;
  pos_x: number;
  pos_y: number;
  cache_results: boolean;
  is_setup?: boolean;
  unity_catalog_settings: UnityCatalogReadSettings;
  fields?: any[];
}
