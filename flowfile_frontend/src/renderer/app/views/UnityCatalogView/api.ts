import axios from "axios";
import type {
  UnityCatalogConnectionInput,
  UnityCatalogConnectionInterface,
  PythonUnityCatalogConnectionInput,
  PythonUnityCatalogConnectionInterface,
  CatalogInfo,
  SchemaInfo,
  TableInfo,
} from "./UnityCatalogTypes";

const API_BASE_URL = "/unity_catalog";

const toPythonFormat = (
  connection: UnityCatalogConnectionInput,
): PythonUnityCatalogConnectionInput => {
  return {
    connection_name: connection.connectionName,
    server_url: connection.serverUrl,
    auth_token: connection.authToken,
    default_catalog: connection.defaultCatalog,
    credential_vending_enabled: connection.credentialVendingEnabled,
  };
};

const fromPythonInterface = (
  pyConn: PythonUnityCatalogConnectionInterface,
): UnityCatalogConnectionInterface => {
  return {
    connectionName: pyConn.connection_name,
    serverUrl: pyConn.server_url,
    defaultCatalog: pyConn.default_catalog,
    credentialVendingEnabled: pyConn.credential_vending_enabled,
  };
};

export const fetchUcConnections = async (): Promise<UnityCatalogConnectionInterface[]> => {
  try {
    const response = await axios.get<PythonUnityCatalogConnectionInterface[]>(
      `${API_BASE_URL}/uc_connections`,
    );
    return response.data.map(fromPythonInterface);
  } catch (error) {
    console.error("API Error: Failed to load Unity Catalog connections:", error);
    throw error;
  }
};

export const createUcConnection = async (
  connection: UnityCatalogConnectionInput,
): Promise<void> => {
  try {
    await axios.post(`${API_BASE_URL}/uc_connection`, toPythonFormat(connection));
  } catch (error) {
    console.error("API Error: Failed to create Unity Catalog connection:", error);
    const errorMsg =
      (error as any).response?.data?.detail || "Failed to create connection";
    throw new Error(errorMsg);
  }
};

export const deleteUcConnection = async (connectionName: string): Promise<void> => {
  try {
    await axios.delete(
      `${API_BASE_URL}/uc_connection?connection_name=${encodeURIComponent(connectionName)}`,
    );
  } catch (error) {
    console.error("API Error: Failed to delete Unity Catalog connection:", error);
    throw error;
  }
};

export const testUcConnection = async (
  connection: UnityCatalogConnectionInput,
): Promise<{ success: boolean; message: string }> => {
  try {
    const response = await axios.post<{ success: boolean; message: string }>(
      `${API_BASE_URL}/test`,
      toPythonFormat(connection),
    );
    return response.data;
  } catch (error) {
    const errorMsg = (error as any).response?.data?.detail || "Connection test failed";
    return { success: false, message: errorMsg };
  }
};

// Browse endpoints
export const browseCatalogs = async (connectionName: string): Promise<CatalogInfo[]> => {
  const response = await axios.get<CatalogInfo[]>(
    `${API_BASE_URL}/browse/catalogs?connection_name=${encodeURIComponent(connectionName)}`,
  );
  return response.data;
};

export const browseSchemas = async (
  connectionName: string,
  catalogName: string,
): Promise<SchemaInfo[]> => {
  const response = await axios.get<SchemaInfo[]>(
    `${API_BASE_URL}/browse/schemas?connection_name=${encodeURIComponent(connectionName)}&catalog_name=${encodeURIComponent(catalogName)}`,
  );
  return response.data;
};

export const browseTables = async (
  connectionName: string,
  catalogName: string,
  schemaName: string,
): Promise<TableInfo[]> => {
  const response = await axios.get<TableInfo[]>(
    `${API_BASE_URL}/browse/tables?connection_name=${encodeURIComponent(connectionName)}&catalog_name=${encodeURIComponent(catalogName)}&schema_name=${encodeURIComponent(schemaName)}`,
  );
  return response.data;
};
